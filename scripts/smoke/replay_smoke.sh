#!/usr/bin/env bash
# Release smoke test (runbook section 2): the end-to-end replay test on staging, run with the
# release's own images.
#   1. a throwaway Kafka and Schema Registry (infra/smoke/compose.yaml)
#   2. producer image: replay a 10-minute window from EventStreams, ending 3 hours ago (the
#      replay stops once every upstream partition is past the window; the quiet codfw partition
#      gets about one event an hour, so a recent window would wait for it)
#   3. Spark image: replay job -> stg_bronze.edits_replay (fresh checkpoint)
#   4. dbt build on staging, then the replay-count check (T-DBT-REPLAY-COUNT)
# Usage: scripts/smoke/replay_smoke.sh <producer image> <spark image>
# AWS: uses AWS_ACCESS_KEY_ID/... if set (CI), otherwise AWS_PROFILE with ~/.aws (laptop).
# Opens one EventStreams connection: on the laptop, pause a producer first (2 per IP).
set -euo pipefail

producer_image=${1:?usage: replay_smoke.sh <producer image> <spark image>}
spark_image=${2:?usage: replay_smoke.sh <producer image> <spark image>}
: "${CONTACT_EMAIL:?CONTACT_EMAIL must be set}"
: "${DATA_BUCKET:?DATA_BUCKET must be set}"

smoke="docker compose -f infra/smoke/compose.yaml"
work=$(mktemp -d)
chmod 777 "$work"  # the images run as uid 1000

cleanup() {
  local status=$?
  $smoke down -v --remove-orphans >/dev/null 2>&1 || true
  # The containers' files belong to uid 1000; on a Linux runner only that user can delete them.
  docker run --rm -v "$work:/w" --entrypoint sh "$producer_image" -c 'rm -rf /w/*' \
    >/dev/null 2>&1 || true
  rm -rf "$work" 2>/dev/null || true
  exit "$status"  # the smoke test's result, never the cleanup's
}
trap cleanup EXIT

until_ts=$(python3 -c "from datetime import UTC, datetime, timedelta
t = datetime.now(UTC) - timedelta(hours=3)
print(t.replace(minute=t.minute - t.minute % 10, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ'))")
since_ts=$(python3 -c "from datetime import datetime, timedelta
t = datetime.strptime('$until_ts', '%Y-%m-%dT%H:%M:%SZ') - timedelta(minutes=10)
print(t.strftime('%Y-%m-%dT%H:%M:%SZ'))")
echo "smoke: replay window $since_ts to $until_ts"

$smoke up -d --wait
$smoke exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --create --topic edits.replay.v1 --partitions 3 --replication-factor 1

run=(docker run --rm --network editguard-smoke_default -v "$work:/opt/editguard/data"
  -e CONTACT_EMAIL -e KAFKA_BOOTSTRAP_SERVERS=kafka:19092
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081)
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  aws=(-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_REGION=ap-south-1)
else
  aws=(-e "AWS_PROFILE=${AWS_PROFILE:-editguard-dev}" -v "$HOME/.aws:/home/app/.aws")
fi

"${run[@]}" --entrypoint python "$producer_image" -m editguard.producer.replay \
  --since "$since_ts" --until "$until_ts"
"${run[@]}" "${aws[@]}" -e DATA_BUCKET --entrypoint python "$spark_image" \
  -m editguard.streaming.replay_job --env staging

make dbt ENV=staging CMD=build
make replay-check ENV=staging REPORT="$(ls "$work"/replays/replay-*.json)"
echo "smoke: PASSED"
