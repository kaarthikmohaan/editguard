# 0002. Spark Structured Streaming 4.1 over Flink

- Status: accepted
- Date: 2026-09-25

## Context
Flags need seconds, not milliseconds. The author works in Python. Spark appears in about 33% of postings. Iceberg 1.11.0 publishes Spark runtime jars up to Spark 4.1 only.

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| Spark 4.1.3 (PySpark) | Python-native; watermarks and dedup built in; widely asked | Micro-batch latency |
| Spark 4.2 | Newest | No Iceberg 1.11 runtime jar |
| Flink 2.x | True per-event, lower latency | Python support weaker; connector setup heavier |

## Decision
PySpark 4.1.3 with Iceberg 1.11.0 runtime.

## Consequences
- Latency floor is the trigger interval; fine for a 60 s SLO.
- Upgrade to Spark 4.2 waits for an Iceberg release that supports it.
