# 0013. Evaluation protocol: a test week fixed in advance, and frozen report inputs

- Status: accepted
- Date: 2026-10-04

## Context
The headline metric (design section 1) compares EditGuard with Wikimedia's revert-risk model by recall at a 2% review budget, with a paired bootstrap 95% interval, reproducible from one command. The results template asks for test days "touched once", and M6's trained model must later be compared on the same test days while training only on other days. Labels are final 48 hours after an edit. On 2026-10-04 prod held final labels for 25 September to 2 October: about 4,000 English damaging edits a day, and 222 Indian-language ones over 26 September to 1 October (ADR 0006 requires at least 100 for a pooled number).

Iceberg snapshots are expired after 7 days (VACUUM), so pinning a snapshot alone cannot reproduce a report after a week.

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| Test on the latest 3 full days (29 Sep to 1 Oct) | Result now | Chosen after seeing the data exists; Indian pooled 104, just over the minimum; little left for M6 |
| Test on all days so far (26 Sep to 1 Oct) | Most data now | M6 would have to train on later days and test on earlier ones |
| **Fix a future week now (5 to 11 Oct, UTC)** | Chosen before the data exists; M6 trains on everything before it; Indian pooled about 270 expected | The first real result waits until the week's labels are final (14 Oct) |

## Decision
- The test window is 2026-10-05 00:00 to 2026-10-12 00:00 UTC (`editguard.evaluation.protocol.TEST_WINDOW`), fixed by this commit before it starts. It is evaluated once per score version; nothing is tuned on it.
- Earlier days are for development and for M6's training and validation. A report on them is a development report: clearly marked, never written to `docs/results.md`.
- `make report SNAPSHOT=<id>` reads prod as of a `gold.fact_label` snapshot. Its first run saves the evaluated rows (scores, labels, Wikimedia's probability; no usernames or edit text) to `s3://<bucket>/prod/reports/<snapshot>/<window>/rows.parquet` with a SHA-256 checksum; later runs with the same snapshot read that file and must produce an identical report (T-REPORT-REPRO).
- Segments: English and Indian-language pooled (ADR 0006), non-bot article edits with a final label other than `bot_caught` (the design's "bots did not revert within 5 minutes"), on the intersection with Wikimedia's scores; coverage is reported. Any segment with fewer than 100 damaging edits reports "insufficient data".

## Consequences
- The first real result is available from 2026-10-14 00:00 UTC (the last edits of the week are final 48 hours after 12 Oct 00:00); M4's "Done when" waits for it.
- Changing the window after 2026-10-05 needs a new ADR that says why, and the original result is kept.
- Frozen rows make every published number re-derivable without Athena; they cost a few MB of S3.
