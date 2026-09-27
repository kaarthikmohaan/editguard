"""Static checks of the Airflow DAG file. Airflow is not in the laptop's environment (it runs
in its own container), so the file is parsed, not imported; `make batch` imports it for real."""

import ast
from pathlib import Path

DAGS = Path(__file__).parents[1] / "dags/editguard_batch.py"


def dag_calls() -> dict[str, dict[str, ast.expr]]:
    calls = {}
    for node in ast.walk(ast.parse(DAGS.read_text())):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "DAG":
            kwargs = {k.arg: k.value for k in node.keywords}
            calls[ast.literal_eval(kwargs["dag_id"])] = kwargs
    return calls


def test_hourly_build_and_daily_maintenance_without_catchup() -> None:
    calls = dag_calls()
    assert set(calls) == {"editguard_dbt_hourly", "editguard_maintenance_daily"}
    assert ast.literal_eval(calls["editguard_dbt_hourly"]["schedule"]) == "15 * * * *"
    assert ast.literal_eval(calls["editguard_maintenance_daily"]["schedule"]) == "30 2 * * *"
    for kwargs in calls.values():
        assert ast.literal_eval(kwargs["catchup"]) is False  # never backfill every missed hour
        assert ast.literal_eval(kwargs["max_active_runs"]) == 1  # no overlapping MERGEs
