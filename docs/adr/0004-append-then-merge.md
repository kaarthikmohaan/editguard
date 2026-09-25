# 0004. Append to bronze, MERGE hourly into silver, one writer per table

- Status: accepted
- Date: 2026-09-25

## Context
A MERGE every micro-batch on S3 creates a snapshot per minute, delete files and small files, and invites commit conflicts with compaction.

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| MERGE in the stream | Silver always current | Snapshot and small-file blowup; conflicts |
| Append in stream, hourly batch MERGE | Cheap appends; one writer per table; duplicates beyond the watermark still fixed | Silver up to 1 hour behind |

## Decision
Spark streaming appends to bronze with dedup inside the watermark. dbt-athena MERGEs into silver hourly on event_id. Each table has exactly one writer; the weekly purge is the only documented exception.

## Consequences
- Compaction runs only on closed partitions.
- Live history features must use an as-of-hour-minus-1 snapshot so live and offline paths match.
