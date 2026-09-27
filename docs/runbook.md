# Runbook

How to run EditGuard, what each alert means, and how to recover. Every Grafana alert links to a section here.

## 1. Daily operation

| Task | Command | When |
| --- | --- | --- |
| Start infrastructure | `make up` | Start of a run window |
| Start streaming | `make stream` | After `make up` |
| Start batch (Airflow) | `make batch` | Alongside streaming |
| Declare a run window | `make window-start` / `make window-end` | Around any period you want SLOs measured |
| Check health | `curl localhost:8000/healthz` and Grafana "EditGuard overview" | Any time |
| Generate report | `make report SNAPSHOT=<id>` | After labels mature (48 h) |
| Stop everything | `make down` | End of a run window |

The producer resumes from its last acked event ID on start. Gaps up to about 7 days are recovered automatically; longer ones are permanent.

### Local services (`compose.yaml`)

All ports bind to `127.0.0.1` only. Settings come from `.env` (copy `.env.example`).

| Service | Host port | Memory cap | Health check |
| --- | --- | --- | --- |
| kafka-1, kafka-2, kafka-3 (KRaft, broker + controller) | 9092, 9094, 9096 | 1 GiB each (heap 512 MB) | `kafka-metadata-quorum.sh ... describe --status` shows 3 voters |
| schema-registry | 8081 | 768 MiB | `curl -s localhost:8081/config` returns `BACKWARD` |
| postgres | 5433 (5432 is often taken by a local install) | 512 MiB | `pg_isready` |

Measured idle on M4 Pro, Docker 12 GB: about 1.9 GiB total.

Kafka topics are defined in `infra/terraform/kafka/main.tf` (design section 9). Create or update them after the first `make up`, or after wiping volumes: `terraform -chdir=infra/terraform/kafka apply`. Check one with `docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic edits.raw.v1`.

Run the producers, one terminal each (Wikimedia allows 2 stream connections per IP, so do not run `producer.watch` at the same time). Stop with Ctrl+C; a restart resumes from the saved bookmark with no gaps:

```bash
until uv run python -m editguard.producer --stream edits; do sleep 30; done      # page_change -> edits.raw.v1
until uv run python -m editguard.producer --stream baseline; do sleep 30; done   # revert-risk -> baseline.raw.v1
```

The `until` loop restarts the producer after a non-zero exit (a stand-in for a container restart policy); Ctrl+C exits 0 and ends the loop. After the Mac wakes from sleep, Docker's VM clock can lag the host, and Kafka rejects messages stamped more than 1 hour ahead (`log.message.timestamp.after.max.ms`) with `INVALID_TIMESTAMP`. The producer then exits with `delivery_failed: true` without moving its bookmark past the rejected events; the loop restarts it once the clocks agree and it replays the missed hours.

Each logs a `stats` line every minute (`received`, `kept`, `dlq`, `gap_events`, `in_flight`). `upstream_gap` at error level means events were lost upstream.

Run the Spark live job in its own terminal. It runs two streaming queries: `bronze` (Kafka `edits.raw.v1` → `bronze.edits`, deduplicated on `event_id` within the 2-minute watermark, from the earliest offset, one Iceberg commit every 60 s) and `scoring` (the same stream from the latest offset on first start, every 10 s, rule score `rules-v0`, flagged edits → `edits.flagged`; it logs a `flags` line with `max_latency_s` for each batch that flagged something). In dev it writes a local Iceberg table under `data/warehouse/` with its checkpoint under `data/checkpoints/` (both gitignored); a restart resumes from the checkpoint. Stop with Ctrl+C: it finishes the current micro-batch first.

```bash
uv run python -m editguard.streaming.live_job                 # dev: local.bronze.edits
until uv run python -m editguard.streaming.live_job --env prod; do sleep 30; done   # prod
```

Each bronze commit writes a new Iceberg `metadata.json` that lists every snapshot, so the job sets `write.metadata.delete-after-commit.enabled=true` and keeps the newest 100 metadata files (`metadata_cleanup_sql`); without this, metadata grew to 2 GB in the first 1,858 commits. Old snapshots are expired by VACUUM (batch layer).

`--env staging|prod` writes `<prefix>_bronze.edits` on S3 through the Glue catalog, with its own checkpoint under `data/checkpoints/<env>/`. It needs `AWS_PROFILE` and `DATA_BUCKET` in `.env` and a valid SSO session. When the session expires (8 hours by default) the job stops with a credentials error; run `aws sso login --profile editguard-dev` and the loop resumes it from the checkpoint. Kafka keeps 7 days, so nothing is lost while it waits.

List the top flags (the MVP read path, until the triage page in M5). It loads `edits.flagged` into DuckDB, keeps one row per edit and prints each with its latency and a diff link:

```bash
uv run python -m editguard.tools.top_flags --hours 24 --limit 20
```

Inspect the dead letter queue (headers `error_stage`, `error_type`, `error_message`, `source_topic`; value is the original event as JSON):

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 \
  --topic edits.dlq --from-beginning --max-messages 5 --timeout-ms 10000 \
  --formatter-property print.headers=true --formatter-property print.key=true
```

### Batch layer (dbt on Athena)

The dbt project lives in `transform/` (models in `models/silver` and `models/gold`, sources in `models/sources.yml`). Run it through make, which sets the bucket from your account and uses the `transform` dependency group:

```bash
make dbt ENV=staging CMD="debug"     # check the Athena connection
make dbt ENV=staging CMD="build"     # run models and tests
make dbt ENV=prod CMD="build --select edits"   # one model and its tests
```

It also reads the username-hashing salt from Secrets Manager for that command (see `docs/security.md`). Targets map to the design's environments: `staging` writes `stg_silver` / `stg_gold` through workgroup `editguard-stg`; `prod` writes `prod_silver` / `prod_gold` through `editguard-prod`. dbt reads bronze (`<prefix>_bronze.edits`) and never writes it. Each Athena query is billed by data scanned and stopped at 1 GB by the workgroup.

### AWS access

Log in with IAM Identity Center (no long-lived keys): `aws sso login --profile editguard-dev`. Sessions last 8 hours. Check with `aws sts get-caller-identity --profile editguard-dev`; the ARN must contain `AWSReservedSSO_AdministratorAccess`.

Terraform state lives in the versioned bucket `editguard-tfstate-<account_id>-ap-south-1`, created once by `infra/terraform/aws/bootstrap` (local state, gitignored). If that local state is lost, the bucket still exists: re-create the state with `terraform import aws_s3_bucket.tfstate <bucket>` (and the four settings resources) rather than applying again.

## 2. Deploy

| Env | How |
| --- | --- |
| dev | `make up` from your branch |
| staging | Push a tag `vX.Y.Z`; CD applies Terraform and dbt to staging, then runs the end-to-end smoke test |
| prod | Approve the `prod` environment in GitHub Actions; then `make deploy VERSION=vX.Y.Z` on the laptop |

## 3. Alerts

| Alert | Threshold | Likely cause | Fix |
| --- | --- | --- | --- |
| NoEventsReceived | No events for 10 min | Stream down, network, producer crashed | Check `docker logs producer`; `curl -I https://stream.wikimedia.org/v2/ui/`; restart producer |
| UpstreamOffsetGap | Any gap | Producer resumed from a wrong ID | Stop producer; inspect `_producer_state`; replay the gap window via `make replay SINCE=<ts>`; write a postmortem |
| ConsumerLagHigh | > 2 min for 10 min | Spark slow or stopped | Check Spark UI (localhost:4040); memory; restart job (resumes from checkpoint) |
| StreamingQueryStopped | Query not active | Exception, `failOnDataLoss` | Read the exception; if data loss, record the gap and restart with a new checkpoint only after writing it down |
| DLQRateHigh | > 0.5% for 15 min | Upstream schema change | Inspect `edits.dlq` headers; compare with the schema changelog; update the contract via PR |
| ApiRateLimited | Any 429 for 5 min | Too many diff fetches | Confirm User-Agent; lower enricher concurrency |
| LLMBudgetReached | Daily budget hit | Flag rate too high | Raise flag threshold or accept shedding until midnight UTC |
| LoadShedHigh | > 10% for 30 min | LLM too slow | Check Ollama; route more to Haiku; raise threshold |
| FreshnessTestFailed | dbt freshness | Airflow or Athena failed | Airflow UI (localhost:8080); rerun the DAG; check Athena workgroup limits |
| AwsBudget | $5 forecast | Runaway queries or storage | Athena query history; check scan cutoff; VACUUM; lifecycle rules |

## 4. Chaos drills

| Drill | Command | Expected |
| --- | --- | --- |
| Producer kill | `make chaos-producer` | Zero gaps after restart |
| Spark kill | `make chaos-spark` | Zero duplicates in silver after restart |
| Broker stop | `make chaos-broker` | Writes continue (`min.insync.replicas=2`) |
| Ollama stop | `make chaos-llm` | Stream unaffected; fallback or shedding |
| Breaking schema | `make chaos-schema` | Registry rejects; CI fails |

Record the result and date of each drill in `docs/results.md`.

## 5. Rollback

| What broke | Rollback |
| --- | --- |
| Service image | `make deploy VERSION=<previous tag>` |
| dbt model | Revert the commit; `dbt build --target prod` |
| Bad data in a table | Spark: `CALL glue.system.rollback_to_snapshot('<db>.<table>', <snapshot_id>)` |
| Scoring change | Set the active `score_version` back in config; restart enricher and live job |
| Infrastructure | Check out the previous tag; `terraform apply` |

## 6. Backups and restore

| Data | Backup | Restore | Last drill (time taken) |
| --- | --- | --- | --- |
| S3 objects | Versioning, 30 days | Restore a noncurrent version | `<date, minutes>` |
| Iceberg tables | Snapshots, 7 days | `rollback_to_snapshot` | `<date, minutes>` |
| Postgres | Nightly `pg_dump` to S3, 14 days | `make restore-pg DATE=<date>` into a fresh container | `<date, minutes>` |
| Terraform state | Versioned S3 | Restore object version; `terraform plan` shows no changes | `<date, minutes>` |
| Kafka | Not backed up | Rebuild from bronze with `make replay-from-bronze` | n/a |

## 7. Disaster recovery

| Scenario | Recovery |
| --- | --- |
| Laptop lost | New Mac: `git clone`, `make setup`, `make infra ENV=prod` (no changes expected), `make up`, `make stream`. Upstream data older than ~7 days is lost; S3 data is intact |
| AWS account issue | Data is only in S3; restore from versioning; if the bucket is gone, rebuild from what upstream still has and record the gap |
| Wikimedia stream retired or changed | Pause; follow the Wikimedia API changelog; new contract version via ADR |

## 8. Suppressed revisions

A visibility change or delete event marks the revision hidden in Postgres immediately (API returns 410). The weekly `purge_hidden` DAG deletes `raw_json` for those revisions from bronze. To run it by hand: `make purge`.

## 9. Teardown

`make teardown` stops recording, runs `terraform destroy` for staging and prod, and deletes local volumes. Export `docs/results.md` first.
