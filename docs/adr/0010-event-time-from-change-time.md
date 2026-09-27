# 0010. event_time is when the change happened (top-level `dt`)

- Status: accepted
- Date: 2026-09-27

## Context
The contract maps `event_time` to `revision.rev_dt`, and the live job's 2-minute watermark runs on `event_time`. Day-1 data (ADR 0009) showed that for `delete`, `undelete` and `visibility_change` events, `rev_dt` is the timestamp of the page's last revision, not of the change: 3,731 such events in 24 h had `event_time` more than an hour old, so the watermark would drop almost all of them. Deletes and visibility changes drive the FR7 hide-and-purge path.

A check of 60,000 recorded events (2026-09-27) found the event's top-level `dt` equal to `rev_dt` (within 1 s) for every `edit` (55,423), `create` (3,608) and `move` (403), and later than `rev_dt` for every `delete` (550), `undelete` (14) and `visibility_change` (2).

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| Keep `rev_dt`; route deletes around the watermark | No contract change | Two code paths; easy to miss one; FR7 depends on it |
| Add a new field (e.g. `changed_at`) for the watermark | Keeps `event_time` meaning | Two time columns that differ only for rare kinds; partitioning and labels stay on the wrong one |
| **`event_time` = top-level `dt` for every kind** | One rule; identical to today for edits, creations and moves; correct for deletes and visibility changes | Changes a field's meaning; Kafka messages written before the change still hold `rev_dt` |

## Decision
`event_time` is the top-level `dt` of the page_change event: when the change happened. `rev_dt` stays available in `raw_json`. Contract version 1.0.0 → 1.1.0 (meaning change, no type or field change, BACKWARD compatible).

## Consequences
- No change for edits, creations and moves, so lateness figures, partitions and labels are unaffected.
- Deletes, undeletes and visibility changes now pass the watermark and reach bronze.
- Events already in Kafka from before the change keep `rev_dt`. The live job derives `event_time` from `raw_json.dt` for those, so bronze is consistent from the first run.
- The generated files (`contracts/generated/*`, `docs/data-dictionary.md`) still describe the old mapping. They are regenerated only by `make contract` (M3), which will update their wording; types are unchanged, so nothing downstream breaks meanwhile.
