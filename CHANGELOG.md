# Changelog

All notable changes are listed here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions: [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- dbt project for the Athena batch layer (`transform/`, `make dbt ENV=… CMD=…`).
- `silver.edits`: incremental MERGE on `event_id` from `bronze.edits`, article edits only, usernames replaced by a salted hash, `raw_json` dropped; tests for unique and not-null `event_id` and accepted values.
- `silver.baseline_scores`: incremental MERGE on (wiki_id, rev_id) from `bronze.baseline_scores`, joined to `silver.edits` for `event_time`; tests for one row per revision and probabilities in [0, 1].
- Gold star schema: `fact_edit` (scored edits, editor attributes at edit time) and `fact_baseline` (incremental MERGE on (wiki_id, rev_id)), `dim_wiki`, `dim_date`, `dim_user_hashed` (rebuilt each run); relationship tests from `fact_edit` to each dimension.
- Replay path (design section 8): `make replay` (EventStreams `since` → Kafka `edits.replay.v1`, with a report of events sent), `make replay-bronze` (Spark `replay_job`, availableNow → `bronze.edits_replay`), `silver.edits` MERGEs replayed events with `source = 'replay'`, and `make replay-check` (the replay-count check).
- Iceberg maintenance macros (`maintain_tables`): OPTIMIZE (today and yesterday by default) and VACUUM with 7-day snapshot retention for every incremental table.

### Changed
- The live job runs a third query, `baseline`, landing Kafka `baseline.raw.v1` in the new table `bronze.baseline_scores` (ADR 0011). `--queries` runs a subset.

### Fixed
- Bronze Iceberg metadata no longer grows without bound: old `metadata.json` files are deleted after each commit (newest 100 kept), and the bronze query commits every 60 s instead of 10 s (scoring stays at 10 s, so flag latency is unchanged).

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
