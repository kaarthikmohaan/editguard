#!/usr/bin/env bash
# Start Airflow in one container: create its database in the shared Postgres on first run,
# then run every component (API server, scheduler, DAG processor, triggerer) with LocalExecutor.
set -euo pipefail
python - <<'PY'
import os
import psycopg2

conn = psycopg2.connect(
    host="postgres", dbname="editguard",
    user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
)
conn.autocommit = True
db = os.environ.get("AIRFLOW_DB_NAME", "airflow")
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
    if cur.fetchone() is None:
        cur.execute(f'CREATE DATABASE "{db}"')
        print(f"created database {db}")
PY
exec airflow standalone
