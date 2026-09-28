# Changelog

All notable changes are listed here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions: [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- M3 (contracts, CI/CD, environments):
  - Tests outside the `e2e` layer run with the AWS login hidden (`tests/conftest.py`), as in CI; the dbt compile tests no longer open a warehouse connection (`--no-populate-cache`, `--no-introspect`).
  - Test layers as pytest markers (`unit`, `dbt`, `spark`, `integration`, `e2e`); `make test` runs unit only, `make test-all` every local layer, `make coverage` a coverage report (pytest-cov).
  - CI on GitHub Actions (`.github/workflows/ci.yml`), on every pull request and push to `main`: ruff and gitleaks, unit tests with coverage, `make contract-check`, dbt on DuckDB, the Spark and integration tests, and pip-audit; Dependabot for uv and Actions updates weekly; CI badge in the README.
  - Pipeline integration test (T-I-RESUME-01): the fixture from a fake EventStreams through the real producer (stopped halfway, resumed from its bookmark), Kafka and Schema Registry in Docker (testcontainers), and the live job's bronze and scoring code on Spark; checks no gaps, bronze exactly once, and flags equal to offline scoring.
  - dbt on DuckDB (`make dbt-ci`, target `ci`, dbt-duckdb 1.11): every model and data test runs from the 1,000-event fixture with no AWS; Athena-only SQL moved to dispatch macros (`user_hash`, `yyyymmdd`, `iso_day_of_week`, `days_between`), giving identical hashes on both engines.
  - 1,000-event test fixture (`tests/fixtures/page_change_1k.jsonl.gz`, `baseline_1k.jsonl.gz`) and its reproducible recorder, with usernames replaced.
  - T-SKEW-01: the live scoring path (Kafka bytes → Spark → `scoring_rows`) and the offline path produce identical features and flags (Spark test); T-LEAK-01: `compute_features` reads only `POINT_IN_TIME_FIELDS`; T-DBT-FORMAT-V2: `make format-check ENV=…` checks every Iceberg table's format version through Glue.
  - `editguard.tools.dlq_replay` (`make dlq-replay`): re-sends dead-lettered edits that now pass the parser and contract through the replay lane; `--dry-run` touches nothing.
  - `make contract` regenerates `contracts/generated/` and the data dictionary's contract tables from `contracts/edits.odcs.yaml` (datacontract-cli 1.2.2 via uvx); `make contract-check` lints the contract and fails if anything generated is stale. The generated files now carry ADR 0010's `event_time` wording.
- M2 (stream and batch together; milestone tag `m2-stream-and-batch`):
  - dbt project for the Athena batch layer (`transform/`, `make dbt ENV=… CMD=…`).
  - `silver.edits`: incremental MERGE on `event_id` from `bronze.edits`, article edits only, usernames replaced by a salted hash, `raw_json` dropped; tests for unique and not-null `event_id` and accepted values.
  - `silver.baseline_scores`: incremental MERGE on (wiki_id, rev_id) from `bronze.baseline_scores`, joined to `silver.edits` for `event_time`; tests for one row per revision and probabilities in [0, 1].
  - Gold star schema: `fact_edit` (scored edits, editor attributes at edit time) and `fact_baseline` (incremental MERGE on (wiki_id, rev_id)), `dim_wiki`, `dim_date`, `dim_user_hashed` (rebuilt each run); relationship tests from `fact_edit` to each dimension.
  - Replay path (design section 8): `make replay` (EventStreams `since` → Kafka `edits.replay.v1`, with a report of events sent), `make replay-bronze` (Spark `replay_job`, availableNow → `bronze.edits_replay`), `silver.edits` MERGEs replayed events with `source = 'replay'`, and `make replay-check` (the replay-count check).
  - Airflow 3.3 (`make batch`, own container, LocalExecutor): `editguard_dbt_hourly` runs `dbt build` at :15 every hour; `editguard_maintenance_daily` runs `maintain_tables` at 02:30 UTC.
  - Iceberg maintenance macros (`maintain_tables`): OPTIMIZE (today and yesterday by default) and VACUUM with 7-day snapshot retention for every incremental table.

### Changed
- M3:
  - Contract 1.2.0: `rev_size` is optional (null for deletes of suppressed revisions, which went to the DLQ with `KeyError: 'rev_size'`); BACKWARD compatible.
  - The live and replay jobs decode each Kafka message with the writer schema its Schema Registry ID points to (fetched at start) and resolve it to the current contract, so messages from before and after a compatible contract change decode side by side; an unknown ID fails the query instead of misreading bytes.
- M2:
  - The live job runs a third query, `baseline`, landing Kafka `baseline.raw.v1` in the new table `bronze.baseline_scores` (ADR 0011). `--queries` runs a subset.

### Fixed
- M2:
  - Bronze Iceberg metadata no longer grows without bound: old `metadata.json` files are deleted after each commit (newest 100 kept), and the bronze query commits every 60 s instead of 10 s (scoring stays at 10 s, so flag latency is unchanged).

### Known limitations
- `silver.labels`, `silver.user_history_asof_hour` and `gold.fact_label` move to M4 with the labelling work; until then `rules-v0` scores without editor history.
- Prod bronze still holds about 2 GB of `metadata.json` files written before the cleanup fix; the daily VACUUM removes them as they pass the 7-day retention (around 2026-10-04).
- The live job and Airflow both need a valid SSO session; when it expires, they fail and retry until `aws sso login`, then catch up (Kafka keeps 7 days; dbt models are incremental).
- A replay needs one of Wikimedia's 2 connections per IP, so the baseline producer is paused while `make replay` runs.
- dbt inlines the username salt into Athena SQL, so it appears in Athena query history (45 days, account only) and in `transform/target/` (see `docs/security.md`).

## [0.1.0] - 2026-09-27 (MVP)

The thin slice works end to end: Wikipedia edit → producer → Kafka → Spark live job → `bronze.edits` on S3 → rule score → `edits.flagged` → DuckDB query of top flags, in 3 to 15 seconds.

### Added
- Design, ADRs 0001 to 0008, data contract, data dictionary, API spec, runbook, test strategy, security doc.
- M0 (start recording):
  - Project tooling: uv (Python 3.12), ruff, pytest, pre-commit with gitleaks, Makefile (`setup`, `up`, `down`, `test`, `lint`, `infra`).
  - Local stack in `compose.yaml`: Kafka 4.3.1 ×3 (KRaft), Schema Registry (BACKWARD), Postgres 18.
  - Kafka topics in Terraform: `edits.raw.v1`, `baseline.raw.v1`, `edits.dlq`, `_producer_state`.
  - Producer for `mediawiki.page_change.v1` and the revert-risk baseline stream: wiki filter, Avro against the contract, dead letter queue, safe resume from a compacted bookmark topic, out-of-order ack tracking, upstream offset gap detection across restarts, backoff with jitter, graceful shutdown.
  - AWS base in Terraform: versioned state bucket; data bucket; $5 budget alert (credits excluded); GitHub OIDC provider; username salt in Secrets Manager (write-only); staging and prod Glue databases, Athena workgroups with a 1 GB scan cutoff, and OIDC CI roles.
  - Day-1 checks: `editguard.tools.day1`, `llm_bench`, `snapshot_check`, `scripts/day1/dbt_merge`, `scripts/day1/optimize_concurrency.py`.
  - ADR 0009 with all nine day-1 results (accepted).
- M1 (MVP):
  - Spark live job (`editguard.streaming.live_job --env dev|staging|prod`): Kafka → Avro → `bronze.edits` (Iceberg v2, partitioned by day and wiki), deduplicated on `event_id` within a 2-minute watermark; local catalog for dev, Glue + S3 for staging and prod.
  - `features.py` v0 (point-in-time editor and size features) and the explainable rule score `rules-v0` (flag threshold 0.44).
  - `edits.flagged` topic; the live job's scoring query publishes flagged edits as `FlaggedEdit` Avro.
  - `editguard.tools.top_flags`: DuckDB query of the top flags with latency and diff links.

### Changed
- `event_time` is the event's top-level `dt` (when the change happened) for every kind; contract 1.0.0 → 1.1.0 (ADR 0010).

### Fixed
- Kafka and Schema Registry addresses use `127.0.0.1` to avoid IPv6 connection failures.
- Schema Registry restarts with the brokers (`depends_on … restart: true`).

### Known limitations
- `rules-v0` has low precision: most flags at the threshold come from "temporary account + no edit summary" alone, and a series of edits by one editor produces a series of flags. History features (M2), labels (M4), the LLM (M5) and the model (M6) address this.
- Suppressed-revision deletes without `rev_size` go to `edits.dlq` until the contract change in M3.
- The prod live job needs a valid SSO session; when it expires, the job waits for `aws sso login` and catches up from Kafka.

<!--
## [0.5.0] - YYYY-MM-DD  (first evaluation)
## [1.0.0] - YYYY-MM-DD  (acceptance checklist complete)
-->
