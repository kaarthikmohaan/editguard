# 0011. Land baseline scores in bronze with the live job

- Status: accepted
- Date: 2026-09-27

## Context
The design's headline metric compares EditGuard with Wikimedia's revert-risk model, and `silver.baseline_scores` is built by dbt on Athena. But the baseline scores exist only in the Kafka topic `baseline.raw.v1` (232,936 messages on 2026-09-27): Athena cannot read Kafka, and no job writes that topic to S3. Kafka keeps 7 days, so the first recordings (2026-09-25) expire around 2026-10-02.

The `BaselineScore` record has no timestamp of its own (wiki_id, rev_id, model_name, model_version, probability_true).

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| **A third query in the live job: `baseline.raw.v1` → `bronze.baseline_scores`** | Same pattern and guarantees as `bronze.edits` (checkpoint, atomic append, watermark dedup); one process | Slightly more memory in the live job |
| A separate Spark job | Isolated failures | Another process, tab and restart loop to run |
| A Python consumer writing Parquet | Light | A second write path with no Iceberg commits or dedup |

## Decision
The live job gets a third streaming query, `baseline`, that appends `baseline.raw.v1` to a new Iceberg table `bronze.baseline_scores` (only writer: the live job), reading from the earliest offset on first run. `ingested_at` is the Kafka message timestamp (when the producer sent it); the table is partitioned by `days(ingested_at), wiki_id` and deduplicated on (wiki_id, rev_id, model_name, model_version) within the 2-minute watermark. `silver.baseline_scores` is then built from it by dbt as designed.

## Consequences
- The design's tables list gains `bronze.baseline_scores` (writer: Spark live job; append; 90 days like `bronze.edits`).
- Baseline scores are safe on S3 before Kafka expires them.
- `silver.baseline_scores` takes `event_time` from `silver.edits` when it joins on (wiki_id, rev_id); scores for edits outside silver (other namespaces) are kept with a null `event_time` or dropped, decided in the silver model.
- The table gets the same metadata cleanup and daily OPTIMIZE and VACUUM as `bronze.edits`.
