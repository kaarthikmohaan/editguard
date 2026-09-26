#!/usr/bin/env bash
# Day-1 test: dbt-athena MERGE + OPTIMIZE (+ VACUUM) on an Iceberg v2 table, in staging.
# Usage: scripts/day1/dbt_merge/run.sh        (then: run.sh --drop to remove the table)
set -euo pipefail
cd "$(dirname "$0")"
export AWS_PROFILE="${AWS_PROFILE:-editguard-dev}"
EDITGUARD_DATA_BUCKET="editguard-data-$(aws sts get-caller-identity --query Account --output text)-ap-south-1"
export EDITGUARD_DATA_BUCKET DBT_PROFILES_DIR=.
dbt() { uv run --group transform dbt "$@" --quiet; }
show() { echo "-- $1"; dbt show --inline "$2" --limit 20; }

if [[ "${1:-}" == "--drop" ]]; then dbt run-operation drop_probe; exit 0; fi

dbt run-operation drop_probe
echo "== run 1: create"; dbt run --vars '{run: 1}'
show "after run 1 (expect e1 100, e2 200, e3 300)" "select event_id, rev_size from stg_silver.day1_merge_probe order by 1"
echo "== run 2: MERGE (update e2, insert e4)"; dbt run --vars '{run: 2}'
show "after run 2 (expect e2 201, e4 400; 4 rows)" "select event_id, rev_size from stg_silver.day1_merge_probe order by 1"
echo "== run 3: same input again (must change nothing)"; dbt run --vars '{run: 2}'
show "after run 3 (expect identical to run 2)" "select count(*) as rows, count(distinct event_id) as ids, sum(rev_size) as total from stg_silver.day1_merge_probe"
echo "== OPTIMIZE and VACUUM"; dbt run-operation optimize_probe; dbt run-operation vacuum_probe
metadata=$(aws glue get-table --database-name stg_silver --name day1_merge_probe --region ap-south-1 \
  --query 'Table.Parameters.metadata_location' --output text)
echo "-- Iceberg format version (expect 2): $(aws s3 cp "$metadata" - --quiet \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["format-version"])')"
show "snapshots" "select operation, count(*) as n from \"stg_silver\".\"day1_merge_probe\$snapshots\" group by 1 order by 1"
echo "PASS: MERGE, idempotent re-run, OPTIMIZE and VACUUM all succeeded"
