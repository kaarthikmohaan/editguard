"""The dbt project on DuckDB (target ci) from the 1,000-event fixture: every model and data
test passes, and silver keeps exactly one row per article event, filling replay gaps."""

import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.dbt, pytest.mark.filterwarnings("ignore::DeprecationWarning")]

TRANSFORM = Path(__file__).parents[1] / "transform"


def test_dbt_build_on_duckdb_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dbt_main = pytest.importorskip("dbt.cli.main", reason="needs the transform group")
    sys.path.insert(0, str(TRANSFORM / "ci"))
    from load_fixtures import GAPS, load

    database = tmp_path / "ci.duckdb"
    load(database)
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(database))
    monkeypatch.setenv("USERNAME_SALT", "ci-salt")
    monkeypatch.setenv("DATA_BUCKET", "unused")
    common = [
        "--target",
        "ci",
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
    runner = dbt_main.dbtRunner()
    for run in (1, 2):  # the second run is incremental: MERGE must not duplicate anything
        result = runner.invoke(["build", *common])
        assert result.success, f"run {run}: {result.exception}"

    # Ask dbt (it holds the DuckDB connection in this process) for the counts.
    sql = """
        select
          (select count(distinct event_id) from (
             select event_id from ci_bronze.edits where namespace_id = 0
             union all
             select event_id from ci_bronze.edits_replay where namespace_id = 0)) as bronze,
          (select count(*) from ci_silver.edits) as silver_rows,
          (select count(distinct event_id) from ci_silver.edits) as silver_distinct,
          (select count(*) from ci_silver.edits where source = 'replay') as replayed
    """
    shown = runner.invoke(["show", "--inline", sql, *common])
    assert shown.success, shown.exception
    bronze, rows, distinct, replayed = shown.result.results[0].agate_table.rows[0]
    assert rows == distinct == bronze
    assert replayed == GAPS  # only the events live missed come from the replay
