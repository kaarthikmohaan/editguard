# Airflow 3.3 for the hourly batch layer (design section 6: Airflow 3.3, LocalExecutor).
# dbt lives in its own virtualenv: its dependencies conflict with Airflow's pinned constraints.
FROM apache/airflow:3.3.2-python3.12

RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.5 dbt-athena==1.11.1 boto3==1.43.103
