# 0009. Day-1 feasibility results

- Status: proposed (results being filled in; accepted when all nine are recorded)
- Date: 2026-09-25

## Context
The design was frozen with nine open unknowns (design.md section 13). Each has a pass rule and a fallback, so no result can reopen the design. This ADR records what was measured.

Hardware for all measurements: Apple M4 Pro, 48 GB RAM, Docker Desktop 12 GB.

## Results

| Test | Pass rule | Result | Outcome |
| --- | --- | --- | --- |
| Baseline join coverage, 24 h | ≥ 80% of scored English article edits | `<TBD>` | |
| English edit volume, 24 h | Recorded | `<TBD>` | |
| Lateness p99 | Measured | `<TBD>` | |
| `foreachBatch` sees new history snapshot | Snapshot ID changes | `<TBD>` | |
| dbt-athena MERGE + OPTIMIZE on v2 | Both succeed | `<TBD>` | |
| OPTIMIZE while Spark appends | 10 runs, no failure | `<TBD>` | |
| Local LLM throughput | ≥ 1.5x English flag rate | `<TBD>` | |
| Producer kill -9 | Zero gaps | 2026-09-25 15:18:51 UTC: `kill -9` of the edits producer mid-stream, restarted 15:20:03. Resumed with `seeded: true`; `gap_events: 0` over the following minutes; 1 duplicate `event_id` in `edits.raw.v1` (sent just before the kill, re-sent on resume). | Pass |
| User-Agent recognised | Not 10 req/min tier | `<TBD>` | |

## Decision
Record each result here as it is measured. Where a test fails, apply the fallback listed in design.md section 13 and note it in this table.

## Consequences
- The one duplicate from the kill -9 test is expected under at-least-once delivery; silver's MERGE on `event_id` removes it.
