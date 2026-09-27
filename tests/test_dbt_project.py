from pathlib import Path

import pytest

dbt_main = pytest.importorskip(
    "dbt.cli.main", reason="needs the transform group (uv sync --all-groups)"
)

# Jinja and dbt internals raise DeprecationWarnings we cannot fix here.
pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

TRANSFORM = Path(__file__).parents[1] / "transform"


def compile_inline(sql: str, target: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Compile a SQL snippet in the real project, offline (dbt compile needs no AWS for this)."""
    monkeypatch.setenv("DATA_BUCKET", "example-bucket")
    args = [
        "compile",
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
