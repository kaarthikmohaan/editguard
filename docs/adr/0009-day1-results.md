# 0009. Day-1 feasibility results

- Status: accepted
- Date: 2026-09-25 (opened), 2026-09-26 (accepted)

## Context
The design was frozen with nine open unknowns (design.md section 13). Each has a pass rule and a fallback, so no result can reopen the design. This ADR records what was measured.

Hardware for all measurements: Apple M4 Pro, 48 GB RAM, Docker Desktop 12 GB.

## Results

| Test | Pass rule | Result | Outcome |
| --- | --- | --- | --- |
| Baseline join coverage, 24 h | ≥ 80% of scored English article edits | 24 h (emitted 2026-09-25 15:15 to 2026-09-26 15:15 UTC): 98.7% of 115,732 English article edits have a revert-risk score; 98.2% of 82,298 non-bot. (Preliminary 10.6 h: 98.5%.) | Pass |
| English edit volume, 24 h | Recorded | Same 24 h: 115,732 English article edits (namespace 0, kind `edit`), of which 82,298 non-bot (about 3,430/h). At the 2% review budget that is about 69 non-bot flags/h. | Recorded |
| Lateness p99 | Measured | `emitted_at − event_time` over 180,717 events (edits and creations), same 24 h: p50 2.3 s, p99 22.6 s, max 201 s. (Preliminary 10.6 h: p99 21.8 s.) | Pass: 2-minute watermark = p99 + ~97 s margin; unchanged. The rare events later than 2 min are dropped by the live job (edge case T-U-EDGE-10). |
| `foreachBatch` sees new history snapshot | Snapshot ID changes | 2026-09-26, `editguard.tools.snapshot_check`: PySpark 4.1.3, Iceberg 1.11.0, local Hadoop catalog; a rate-source stream (5 s trigger) reads the table's snapshot ID in `foreachBatch` while a separate Spark process appends. Default catalog caching: the stream kept the old snapshot for 12+ batches (never refreshed; the 30 s cache expiry resets on every access). With `spark.catalog.refreshTable(...)` at the start of each batch: new snapshot seen in the next batch. | Pass, on condition: the live job refreshes the history table in every batch. Re-check once on the Glue catalog in M1. |
| dbt-athena MERGE + OPTIMIZE on v2 | Both succeed | 2026-09-26, `scripts/day1/dbt_merge/run.sh` in staging (`stg_silver.day1_merge_probe`, workgroup `editguard-stg`): dbt-core 1.12.5, dbt-athena 1.11.1, incremental `merge` on `event_id`, partitioned by `day(event_time)`. Run 2 updated 1 row and inserted 1; re-running the same input left 4 rows, 4 ids, same total (it still commits a new snapshot). OPTIMIZE `BIN_PACK` and VACUUM succeeded; metadata `format-version` = 2. | Pass |
| OPTIMIZE while Spark appends | 10 runs, no failure | 2026-09-26, `scripts/day1/optimize_concurrency.py` in staging: local Spark 4.1.3 + Iceberg 1.11.0 (Glue catalog, S3FileIO) streams 20 rows/s into `stg_bronze.day1_concurrency_probe` with a 3 s trigger while Athena runs `OPTIMIZE … BIN_PACK` 10 times on the same (open) partition. 10/10 OPTIMIZE succeeded (1 to 4 s each), no stream error, 32 append + 10 replace snapshots, 1,860 rows via Spark = 1,860 via Athena. Caveat: no commit conflict occurred (0 retries in the logs), so Iceberg's retry path was not exercised. | Pass |
| Local LLM throughput | ≥ 1.5x English flag rate | 2026-09-26: `qwen3:4b-instruct` (Q4_K_M, 2.5 GB) on Ollama 0.34.4, all 37 layers on Metal. 30 sequential ~450-token diff prompts, JSON verdict, temperature 0: p50 0.46 s, max 0.61 s, 2.09 calls/s. Required 0.032 calls/s (1.5 × 2% × 3,870 non-bot English edits/h, the 10.6 h rate); with the 24 h rate (3,430/h) it is 0.029 calls/s. Best case: 5 prompts repeat, so the prompt cache helps; speed only, not quality. | Pass (65x; 73x at the 24 h rate) |
| Producer kill -9 | Zero gaps | 2026-09-25 15:18:51 UTC: `kill -9` of the edits producer mid-stream, restarted 15:20:03. Resumed with `seeded: true`; `gap_events: 0` over the following minutes; 1 duplicate `event_id` in `edits.raw.v1` (sent just before the kill, re-sent on resume). | Pass |
| User-Agent recognised | Not 10 req/min tier | 2026-09-26 ~02:05 UTC: 15 `action=query&meta=siteinfo` requests to en.wikipedia.org in ~15 s with the `Settings.user_agent` string (tool name, repo URL, contact email): 15 × HTTP 200, no 429. Wikimedia sends no tier header, so this shows the client is above the 10/min tier, not that it is exactly in the 200/min tier. | Pass |

Coverage, volume and lateness measured with `uv run python -m editguard.tools.day1 --since 2026-09-25T15:15` at 2026-09-26 15:33 UTC. The window includes a 4.6 h producer outage (Mac asleep, 25 Sep 20:51 to 26 Sep 01:27 UTC) that was fully replayed from Wikimedia with `gap_events: 0`.

## Findings outside the nine tests

- **Deletes carry an old `event_time`.** For `delete` and `undelete`, upstream `revision.rev_dt` is the timestamp of the page's last revision, not of the change; 3,731 of these events in the 24 h window had `event_time` more than 1 hour before `emitted_at`. The contract maps `event_time` to `rev_dt`, so the live job's 2-minute watermark would drop almost every delete from bronze, and deletes drive the FR7 purge. To decide in M1 through the contract process (likely `event_time` = top-level `dt`, which equals `rev_dt` for edits).
- **Suppressed-revision deletes omit `rev_size`.** The contract marks `rev_size` required, so these go to `edits.dlq` (3 in the first 24 h, about 0.002%). Fix in M3 with `make contract` (make it nullable), then replay the DLQ before its 30-day retention ends.
- **Model choice.** The design names no local model. `qwen3:4b` now resolves to Qwen3-4B *Thinking*-2507, which reasons before every answer even with `think: false` (239 tokens for a one-word reply), so `qwen3:4b-instruct` was chosen. Quality is judged by the M5 evals; in this benchmark it labelled an added citation as `unsourced_change`.
- **Lateness is higher than the design profile** (p99 22.6 s vs 10.3 s from the 2-minute sample; max 201 s vs 38 s); still well inside the 2-minute watermark at p99.

## Decision
All nine day-1 tests pass (one on condition). No fallback from design.md section 13 is needed. The design stays as frozen, with these conditions carried into later milestones:

- M1: the live job calls `spark.catalog.refreshTable` on the history table at the start of every `foreachBatch`, and stops streams only between batches.
- M1: resolve the delete `event_time` finding through the contract process before the live job relies on the watermark.
- M3: make `rev_size` nullable via `make contract`, then replay `edits.dlq` within its 30-day retention.

## Consequences
- The one duplicate from the kill -9 test is expected under at-least-once delivery; silver's MERGE on `event_id` removes it.
- Baseline coverage is high enough (98.7%) to compare EditGuard and Wikimedia's model on almost all English article edits; the comparison still uses the intersection and reports coverage.
- A laptop sleep can stop the producers (Docker VM clock lag, `INVALID_TIMESTAMP`); the `until` retry loop in the runbook restarts them and the replay recovers the gap while it is under about 7 days.
