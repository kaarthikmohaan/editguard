#!/usr/bin/env bash
# Run dbt against Athena from the Airflow container: dbt.sh <target> <dbt args...>
# Same as `make dbt` on the laptop, but looks up the bucket and salt with boto3 (no AWS CLI
# in the image) and writes dbt's target/ and logs/ to /tmp (the repo is mounted read-only).
set -euo pipefail
target="$1"; shift
eval "$(/home/airflow/dbt-venv/bin/python - <<'PY'
import shlex

import boto3

session = boto3.Session(region_name="ap-south-1")
account = session.client("sts").get_caller_identity()["Account"]
salt = session.client("secretsmanager").get_secret_value(
    SecretId="editguard/username-salt")["SecretString"]
print(f"export DATA_BUCKET=editguard-data-{account}-ap-south-1")
print(f"export USERNAME_SALT={shlex.quote(salt)}")
PY
)"
cd /opt/editguard/transform
export DBT_PROFILES_DIR=. DBT_TARGET_PATH=/tmp/dbt/target DBT_LOG_PATH=/tmp/dbt/logs
exec /home/airflow/dbt-venv/bin/dbt "$@" --target "$target"
