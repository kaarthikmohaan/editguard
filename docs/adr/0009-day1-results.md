# 0009. Day-1 feasibility results

- Status: proposed (results being filled in; accepted when all nine are recorded)
- Date: 2026-09-25

## Context
The design was frozen with nine open unknowns (design.md section 13). Each has a pass rule and a fallback, so no result can reopen the design. This ADR records what was measured.

Hardware for all measurements: Apple M4 Pro, 48 GB RAM, Docker Desktop 12 GB.

## Results

| Test | Pass rule | Result | Outcome |
| --- | --- | --- | --- |
| Baseline join coverage, 24 h | ≥ 80% of scored English article edits | Preliminary, 10.6 h (2026-09-25 15:15 to 2026-09-26 01:50 UTC): 98.5% of 56,204 English article edits; 97.9% of 40,977 non-bot. 24 h result `<TBD>` | |
| English edit volume, 24 h | Recorded | Preliminary, same 10.6 h: 56,204 English article edits (40,977 non-bot), about 127k/day. 24 h result `<TBD>` | |
| Lateness p99 | Measured | `emitted_at − event_time` over 90,474 events (edits and creations), 10.6 h: p50 2.3 s, p99 21.8 s, max 155 s | Pass: 2-minute watermark = p99 + ~98 s margin; unchanged |
| `foreachBatch` sees new history snapshot | Snapshot ID changes | 2026-09-26, `editguard.tools.snapshot_check`: PySpark 4.1.3, Iceberg 1.11.0, local Hadoop catalog; a rate-source stream (5 s trigger) reads the table's snapshot ID in `foreachBatch` while a separate Spark process appends. Default catalog caching: the stream kept the old snapshot for 12+ batches (never refreshed; the 30 s cache expiry resets on every access). With `spark.catalog.refreshTable(...)` at the start of each batch: new snapshot seen in the next batch. | Pass, on condition: the live job refreshes the history table in every batch. Re-check once on the Glue catalog in M1. |
| dbt-athena MERGE + OPTIMIZE on v2 | Both succeed | `<TBD>` | |
| OPTIMIZE while Spark appends | 10 runs, no failure | `<TBD>` | |
| Local LLM throughput | ≥ 1.5x English flag rate | 2026-09-26: `qwen3:4b-instruct` (Q4_K_M, 2.5 GB) on Ollama 0.34.4, all 37 layers on Metal. 30 sequential ~450-token diff prompts, JSON verdict, temperature 0: p50 0.46 s, max 0.61 s, 2.09 calls/s. Required 0.032 calls/s (1.5 × 2% × 3,870 non-bot English edits/h). Best case: 5 prompts repeat, so the prompt cache helps; speed only, not quality. | Pass (65x) |
| Producer kill -9 | Zero gaps | 2026-09-25 15:18:51 UTC: `kill -9` of the edits producer mid-stream, restarted 15:20:03. Resumed with `seeded: true`; `gap_events: 0` over the following minutes; 1 duplicate `event_id` in `edits.raw.v1` (sent just before the kill, re-sent on resume). | Pass |
| User-Agent recognised | Not 10 req/min tier | 2026-09-26 ~02:05 UTC: 15 `action=query&meta=siteinfo` requests to en.wikipedia.org in ~15 s with the `Settings.user_agent` string (tool name, repo URL, contact email): 15 × HTTP 200, no 429. Wikimedia sends no tier header, so this shows the client is above the 10/min tier, not that it is exactly in the 200/min tier. | Pass |

Measured with `uv run python -m editguard.tools.day1 --since 2026-09-25T15:15`.

## Findings outside the nine tests

- **Deletes carry an old `event_time`.** For `delete` and `undelete`, upstream `revision.rev_dt` is the timestamp of the page's last revision, not of the change; 1,097 of these events in the window had `event_time` more than 1 hour before `emitted_at`. The contract maps `event_time` to `rev_dt`, so the live job's 2-minute watermark would drop almost every delete from bronze, and deletes drive the FR7 purge. To decide in M1 through the contract process (likely `event_time` = top-level `dt`, which equals `rev_dt` for edits).
- **Suppressed-revision deletes omit `rev_size`.** The contract marks `rev_size` required, so these go to `edits.dlq` (3 so far, about 0.003%). Fix in M3 with `make contract` (make it nullable), then replay the DLQ before its 30-day retention ends.
- **Model choice.** The design names no local model. `qwen3:4b` now resolves to Qwen3-4B *Thinking*-2507, which reasons before every answer even with `think: false` (239 tokens for a one-word reply), so `qwen3:4b-instruct` was chosen. Quality is judged by the M5 evals; in this benchmark it labelled an added citation as `unsourced_change`.
- **Lateness is higher than the design profile** (p99 21.8 s vs 10.3 s from the 2-minute sample); still well inside the 2-minute watermark.

## Decision
Record each result here as it is measured. Where a test fails, apply the fallback listed in design.md section 13 and note it in this table.

## Consequences
- The one duplicate from the kill -9 test is expected under at-least-once delivery; silver's MERGE on `event_id` removes it.
