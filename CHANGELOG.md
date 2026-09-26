# Changelog

All notable changes are listed here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions: [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

### Fixed
- Kafka and Schema Registry addresses use `127.0.0.1` to avoid IPv6 connection failures.
- Schema Registry restarts with the brokers (`depends_on … restart: true`).

<!--
## [0.1.0] - YYYY-MM-DD  (MVP)
### Added
- Producer with safe resume and gap detection.
- Spark live job to bronze; rule score; edits.flagged.

## [0.5.0] - YYYY-MM-DD  (first evaluation)
## [1.0.0] - YYYY-MM-DD  (acceptance checklist complete)
-->
