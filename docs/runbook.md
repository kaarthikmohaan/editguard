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
