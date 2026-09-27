"""EditGuard batch layer (design section 5: everything except flags is hourly batch on Athena).

editguard_dbt_hourly         dbt build: silver and gold, with all data tests
editguard_maintenance_daily  Iceberg OPTIMIZE (today and yesterday) and VACUUM

Both run dbt through infra/airflow/dbt.sh against EDITGUARD_DBT_TARGET (compose sets it).
"""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT = "/opt/editguard/infra/airflow/dbt.sh"
TARGET = os.environ.get("EDITGUARD_DBT_TARGET", "staging")
DEFAULTS = {"retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="editguard_dbt_hourly",
    schedule="15 * * * *",  # quarter past, so the live job's minute commits have landed
    start_date=datetime(2026, 9, 27),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["editguard", "batch"],
):
    BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} {TARGET} build",
        execution_timeout=timedelta(minutes=30),
    )

with DAG(
    dag_id="editguard_maintenance_daily",
    schedule="30 2 * * *",  # 02:30 UTC, a quiet hour
    start_date=datetime(2026, 9, 27),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["editguard", "maintenance"],
):
    BashOperator(
        task_id="maintain_tables",
        bash_command=f"{DBT} {TARGET} run-operation maintain_tables",
        execution_timeout=timedelta(minutes=45),
    )
