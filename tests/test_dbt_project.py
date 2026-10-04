from pathlib import Path

import pytest

# Jinja and dbt internals raise DeprecationWarnings we cannot fix here.
pytestmark = [pytest.mark.dbt, pytest.mark.filterwarnings("ignore::DeprecationWarning")]

TRANSFORM = Path(__file__).parents[1] / "transform"


def compile_inline(sql: str, target: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Compile a SQL snippet in the real project, offline (dbt compile needs no AWS for this)."""
    # Imported here, not at module level: importing dbt takes seconds, and the unit layer
    # still collects (imports) this module before deselecting it.
    dbt_main = pytest.importorskip(
        "dbt.cli.main", reason="needs the transform group (uv sync --all-groups)"
    )
    monkeypatch.setenv("DATA_BUCKET", "example-bucket")
    monkeypatch.setenv("USERNAME_SALT", "test-salt")
    args = [
        "compile",
        # No warehouse lookups: dbt-athena otherwise opens an AWS connection to cache existing
        # tables, which works on a laptop with SSO and fails in CI (no AWS profile).
        "--no-populate-cache",
        "--no-introspect",
        "--inline",
        sql,
        "--target",
        target,
        "--quiet",
        "--project-dir",
        str(TRANSFORM),
        "--profiles-dir",
        str(TRANSFORM),
        "--target-path",
        str(tmp_path / "target"),
        "--log-path",
        str(tmp_path / "logs"),
    ]
    res = dbt_main.dbtRunner().invoke(args)
    assert res.success, res.exception
    return res.result.results[0].node.compiled_code.strip()


@pytest.mark.parametrize(("target", "prefix"), [("staging", "stg"), ("prod", "prod")])
def test_layers_map_to_environment_databases(target, prefix, tmp_path, monkeypatch) -> None:
    sql = "{{ source('bronze', 'edits') }} {{ generate_schema_name('gold', none) }}"
    compiled = compile_inline(sql, target, tmp_path, monkeypatch)
    assert compiled == f'"awsdatacatalog"."{prefix}_bronze"."edits" {prefix}_gold'


def test_user_hash_is_salted_sha256_and_keeps_null(tmp_path, monkeypatch) -> None:
    compiled = compile_inline(
        "{{ user_hash('performer_user_text') }}", "prod", tmp_path, monkeypatch
    )
    assert "sha256(to_utf8('test-salt' || ':' || performer_user_text))" in compiled
    assert compiled.startswith("case when performer_user_text is not null")  # null stays null


def test_maintenance_covers_both_layers_and_limits_daily_optimize(tmp_path, monkeypatch) -> None:
    sql = (
        "{% for t, c in maintained_tables() %}{{ t }}:{{ c }},{% endfor %}|"
        "{{ optimize_sql('t', 1) }}|{{ optimize_sql('t', none) }}|"
        "{{ snapshot_retention_seconds() }}"
    )
    tables, daily, full, retention = compile_inline(sql, "prod", tmp_path, monkeypatch).split("|")
    assert tables == (
        "prod_bronze.edits:event_time,prod_bronze.baseline_scores:ingested_at,"
        "prod_bronze.edits_replay:event_time,"
        "prod_silver.edits:event_time,prod_silver.baseline_scores:event_time,"
        "prod_silver.labels:event_time,"
        "prod_gold.fact_edit:event_time,prod_gold.fact_baseline:event_time,"
        "prod_gold.fact_label:event_time,"
    )
    assert daily.endswith("WHERE event_time >= current_date - interval '1' day")
    assert full == "OPTIMIZE t REWRITE DATA USING BIN_PACK"
    assert retention == "604800"  # 7 days, the runbook's snapshot retention
