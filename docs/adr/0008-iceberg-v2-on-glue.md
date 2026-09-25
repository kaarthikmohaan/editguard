# 0008. Iceberg format v2 with the Glue catalog

- Status: accepted
- Date: 2026-09-25

## Context
Spark, Athena and DuckDB must all read the same tables. DuckDB 1.5.3 can write v3 tables; published sources disagree on Athena's v3 support.

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| Iceberg v2 on Glue | Supported by every engine used | Misses v3 features |
| Iceberg v3 | Newer features | Risk that Athena can't read it |
| Self-hosted REST catalog | Portable | Another service to run |

## Decision
Every table pinned to `format-version=2` in the Glue Data Catalog; `glue.skip-archive=true`.

## Consequences
- A CI check asserts format v2 on every table.
- Revisit when Athena documents v3 support.
