# 0003. Batch work on Athena via dbt-athena

- Status: accepted
- Date: 2026-09-25

## Context
The hourly silver MERGE, compaction and gold builds could not run on the laptop at the same time as Kafka, Spark streaming and Ollama without memory pressure. An earlier plan ran batch only when streaming was stopped, which broke the hourly design.

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| Local Spark batch | Full control | Competes for laptop memory; can't run with the stream |
| Athena + dbt-athena | Serverless; Iceberg MERGE, OPTIMIZE, VACUUM; $5/TB with 10 MB minimum | Trino SQL dialect differs from DuckDB used in CI |
| DuckDB writing to Glue | Free, fast | Glue writes only recently fixed; UPDATE/DELETE limited on partitioned tables |

## Decision
dbt-athena 1.11.1 writes silver and gold. DuckDB stays for CI and ad-hoc reads.

## Consequences
- Stream and batch run together.
- Dialect gap covered by a nightly `dbt build` on Athena in staging.
- Athena workgroup scan cutoff (1 GB) and budget alert guard cost.
