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

Streaming services (profile `stream`, started by `make stream`, stopped by `make stream-down`; images from the root `Dockerfile`):

| Service | Image target | Memory cap | Notes |
| --- | --- | --- | --- |
| producer-edits, producer-baseline | `producer` | 256 MiB each | Bookmarks live in Kafka, so a restart resumes with no gaps; 45 s to flush on stop |
| live-job | `spark` (Java 17, connector jars baked in) | 4.5 GiB | `--env ${EDITGUARD_ENV:-dev}`; checkpoints in `./data` (mounted at the same path); `~/.aws` mounted for SSO; Spark UI on 4040; 90 s to finish a micro-batch on stop |

Docker restarts them after any exit (`restart: unless-stopped`), which replaces the `until` loops below. Logs: `docker compose logs -f live-job` (or a producer). Never run the compose producers and the terminal producers at the same time: Wikimedia allows 2 stream connections per IP, and two live jobs would share a checkpoint.

Kafka topics are defined in `infra/terraform/kafka/main.tf` (design section 9). Create or update them after the first `make up`, or after wiping volumes: `terraform -chdir=infra/terraform/kafka apply`. Check one with `docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic edits.raw.v1`.

For development without Docker, run the producers one terminal each (Wikimedia allows 2 stream connections per IP, so do not run `producer.watch` at the same time). Stop with Ctrl+C; a restart resumes from the saved bookmark with no gaps:

```bash
until uv run python -m editguard.producer --stream edits; do sleep 30; done      # page_change -> edits.raw.v1
until uv run python -m editguard.producer --stream baseline; do sleep 30; done   # revert-risk -> baseline.raw.v1
```

The `until` loop restarts the producer after a non-zero exit (a stand-in for a container restart policy); Ctrl+C exits 0 and ends the loop. After the Mac wakes from sleep, Docker's VM clock can lag the host, and Kafka rejects messages stamped more than 1 hour ahead (`log.message.timestamp.after.max.ms`) with `INVALID_TIMESTAMP`. The producer then exits with `delivery_failed: true` without moving its bookmark past the rejected events; the loop restarts it once the clocks agree and it replays the missed hours.

Each logs a `stats` line every minute (`received`, `kept`, `dlq`, `gap_events`, `in_flight`). `upstream_gap` at error level means events were lost upstream.

Run the Spark live job in its own terminal. It runs three streaming queries: `bronze` (Kafka `edits.raw.v1` → `bronze.edits`, deduplicated on `event_id` within the 2-minute watermark, from the earliest offset, one Iceberg commit every 60 s), `baseline` (Kafka `baseline.raw.v1` → `bronze.baseline_scores`, from the earliest offset, every 60 s; ADR 0011) and `scoring` (the stream of edits from the latest offset on first start, every 10 s, rule score `rules-v0`, flagged edits → `edits.flagged`; it logs a `flags` line with `max_latency_s` for each batch that flagged something). In dev it writes a local Iceberg table under `data/warehouse/` with its checkpoint under `data/checkpoints/` (both gitignored); a restart resumes from the checkpoint. Stop with Ctrl+C: it finishes the current micro-batch first.

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

### Replay a past window (design section 8)

Use it to fill a gap (for example after `upstream_gap`, or the laptop being off for more than a few hours) within the last ~7 days. Replayed events go through their own topic (`edits.replay.v1`), Spark job, checkpoint and table (`bronze.edits_replay`), because days-old events would fall behind the live job's watermark. `silver.edits` MERGEs them in with `source = 'replay'`; an event the live job already delivered keeps `source = 'live'`.

1. Wikimedia allows 2 stream connections per IP: stop the baseline producer (Ctrl+C). It resumes from its bookmark afterwards.
2. `make replay SINCE=2026-09-26T12:00:00Z UNTIL=2026-09-26T13:00:00Z` (window on the event's emission time, UTC). It stops 5 minutes past `UNTIL` and writes `data/replays/replay-<window>.json` with the distinct events sent. Exit code 1 means it did not complete; run it again (a rerun is harmless).
3. Restart the baseline producer.
4. `make replay-bronze ENV=prod`: appends everything replayed so far to `bronze.edits_replay` and exits.
5. `make dbt ENV=prod CMD="build"`, then `make replay-check ENV=prod REPORT=data/replays/replay-<window>.json`. It fails unless `bronze.edits_replay` holds exactly the events the producer sent and every article event is in `silver.edits` once; it also prints how many gaps the replay filled.

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

### Evaluation report (ADR 0013)

```bash
make report SNAPSHOT=latest                     # development report, 26 Sep to 1 Oct, data/reports/
make report SNAPSHOT=<id> WINDOW=test           # the result: 5 to 11 Oct, docs/results.md (from 14 Oct)
```

`SNAPSHOT` is a `prod_gold.fact_label` snapshot; `latest` takes the newest and the report prints its ID, so record it. The first run for a (snapshot, window) runs one Athena UNLOAD (raw rows go under `athena-results/`, deleted after 7 days), scores every edit with the live rules code and saves frozen rows (scores, labels, Wikimedia's probability; no usernames or edit text) with a SHA-256 checksum under `s3://<bucket>/prod/reports/<snapshot>/<window>/`. Later runs with the same snapshot read those rows and stop if the checksum differs, so a report can be regenerated long after VACUUM has expired the snapshot. `WINDOW=test` refuses to run before the window's labels are final; a development window may not overlap the test window. The bootstrap (10,000 resamples) takes about a minute.

### Airflow (the batch schedule)

`make batch` builds and starts Airflow 3.3 (LocalExecutor, one container, metadata in the local Postgres database `airflow`); `make batch-down` stops it. Airflow's DAGs and dbt project come from `../editguard-release`, a git worktree at the deployed tag (`make release-tree VERSION=vX.Y.Z`; `make deploy` runs it), never from this working folder, so a branch you are working on cannot reach prod. To try a branch on staging, point that worktree at it (`git -C ../editguard-release switch --detach <branch>`) with `EDITGUARD_DBT_TARGET=staging`, and switch it back afterwards. UI: http://localhost:8080 (bound to 127.0.0.1, no login). Two DAGs in `dags/editguard_batch.py`, both with `catchup=False` and one run at a time:

| DAG | Schedule (UTC) | Runs |
| --- | --- | --- |
| `editguard_dbt_hourly` | 15 minutes past every hour | `dbt build` (silver, gold, 26 data tests) |
| `editguard_maintenance_daily` | 02:30 | `run-operation maintain_tables` |

They run dbt through `infra/airflow/dbt.sh` against `EDITGUARD_DBT_TARGET` from `.env` (`staging` if unset), with your SSO login from `~/.aws`. When the SSO session expires, tasks fail with a credentials error and retry twice; after `aws sso login` the next hourly run catches up (the models are incremental). A failed task's log is in the UI (DAG → run → task → Logs).

Table maintenance (Iceberg OPTIMIZE and VACUUM on Athena) for every incremental table (bronze, silver and gold facts), once a day (Airflow will schedule it):

```bash
make dbt ENV=prod CMD="run-operation maintain_tables"                     # today and yesterday
make dbt ENV=prod CMD="run-operation maintain_tables --args '{days: null}'"   # whole tables
```

OPTIMIZE compacts small files and is safe while the live job appends (ADR 0009). Athena bills it for the partitions it reads, so the daily run covers only today and yesterday; use the whole-table form after a replay into older days. VACUUM expires snapshots older than 7 days and then deletes files no snapshot refers to once they are older than 7 days. Time travel and `rollback_to_snapshot` therefore reach back 7 days.

### Dead letter queue: fix, then replay

Events that fail parsing or the contract go to `edits.dlq` (kept 30 days) with the reason in their headers. After fixing the cause (parser or contract, released):

1. `make dlq-replay ARGS=--dry-run`: counts which dead-lettered events now pass (no send, no commit, no Schema Registry calls).
2. `make dlq-replay`: sends them to `edits.replay.v1` (they are too old for the live watermark) and remembers where it stopped (consumer group `dlq-replay`), so a rerun sends only newer DLQ messages. Events that still fail stay in the DLQ and are counted by reason. The event IDs sent are in `data/replays/dlq-replay-<time>.json`.
3. `make replay-bronze ENV=prod`, then `make dbt ENV=prod CMD="build"`.

### Changing the contract on a running pipeline

Contract changes must stay BACKWARD compatible (CI checks). Kafka then holds messages written with more than one schema version; the live and replay jobs read the list of versions from Schema Registry at start and decode each message with its own writer schema. Order on prod:

1. Restart the live job (it reads the current version list).
2. Restart the producers: they register the new version on their first message.
3. The live job fails once on the first message with the unknown schema ID (`no writer schema for Schema Registry id …`), the `until` loop restarts it, and it reads the new list.

### Nightly Athena check (staging)

`.github/workflows/nightly.yml` runs `make dbt ENV=staging CMD=build` and `make format-check ENV=staging` at 02:00 IST, in the `staging` GitHub environment. Start it by hand from the Actions tab (**nightly → Run workflow**). AWS access is a one-hour OIDC session for `editguard-ci-staging`; its ARN is the `AWS_ROLE_ARN` secret of the `staging` environment (from `terraform output ci_role_arn`). If a run fails with `AccessDenied`, add exactly the named action to `ci_access` (test first with `aws iam simulate-principal-policy --policy-source-arn <role ARN> --action-names <action>`) in `infra/terraform/aws/env/main.tf` and `make infra ENV=staging`. If login fails with `Not authorized to perform sts:AssumeRoleWithWebIdentity`, compare the subject GitHub sent (CloudTrail event `AssumeRoleWithWebIdentity`, field `userIdentity.userName`) with `github_repo` there; GitHub's subject carries the owner and repo IDs (`repo:<owner>@<id>/<repo>@<id>:environment:<env>`).

### AWS access

Log in with IAM Identity Center (no long-lived keys): `aws sso login --profile editguard-dev`. Sessions last 8 hours. Check with `aws sts get-caller-identity --profile editguard-dev`; the ARN must contain `AWSReservedSSO_AdministratorAccess`.

Terraform state lives in the versioned bucket `editguard-tfstate-<account_id>-ap-south-1`, created once by `infra/terraform/aws/bootstrap` (local state, gitignored). If that local state is lost, the bucket still exists: re-create the state with `terraform import aws_s3_bucket.tfstate <bucket>` (and the four settings resources) rather than applying again.

## 2. Deploy

| Env | How |
| --- | --- |
| dev | `make up` from your branch |
| staging | Push a tag `vX.Y.Z` (it must equal `v` + the version in `pyproject.toml`). `.github/workflows/release.yml` builds the arm64 images, pushes them to GHCR with a GitHub release listing their digests, fails if staging differs from Terraform (ADR 0012: apply with `make infra ENV=staging` on the laptop), then runs the smoke test `scripts/smoke/replay_smoke.sh`: the release's images replay a 10-minute window into staging, dbt builds it and the replay-count check must pass; then the format check |
| prod | Approve the `prod` environment in GitHub Actions (the release's last job waits for it); then `make deploy VERSION=vX.Y.Z` on the laptop |

`make deploy VERSION=vX.Y.Z`, run from that tag (`git fetch --tags && git switch --detach vX.Y.Z`):

1. `editguard.tools.deploy` refuses unless HEAD is the tag with no local changes and GitHub shows the release's `prod` job finished (it runs only after a reviewer approves), then writes `.deploy.env` (gitignored): `EDITGUARD_ENV=prod` and both images pinned by digest from the GitHub release.
2. `make release-tree VERSION=vX.Y.Z`: Airflow's checkout moves to the release.
3. `make infra ENV=prod` (review the plan; type `yes` only if it is expected).
4. `make dbt ENV=prod CMD=build`.
5. `make stream`: pulls the pinned images and restarts the producers and the live job on them; they resume from their bookmarks and checkpoints.

The images must be pullable without a login: after the first release, set both GHCR packages (`editguard-producer`, `editguard-spark`) to public in GitHub (Packages → package settings → Change visibility). Delete `.deploy.env` to go back to local builds on dev.

## 3. Alerts

| Alert | Threshold | Likely cause | Fix |
| --- | --- | --- | --- |
| NoEventsReceived | No events for 10 min | Stream down, network, producer crashed | Check `docker logs producer`; `curl -I https://stream.wikimedia.org/v2/ui/`; restart producer |
| UpstreamOffsetGap | Any gap | Producer resumed from a wrong ID; or offsets EventStreams never serves (codfw between hourly canary events) | Re-read the window from EventStreams history (`since`): offsets it never serves are not a loss. Otherwise stop the producer, inspect `_producer_state`, replay the gap window (section 1, Replay a past window) and write a postmortem (see `docs/postmortems/2026-10-03-resume-skip.md`) |
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
