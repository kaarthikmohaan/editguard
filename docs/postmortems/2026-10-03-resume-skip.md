# Postmortem: resume after a disconnect skipped one upstream event

- Date: 2026-10-03 · Duration: 2026-09-28 to 2026-10-03 (latent since M1) · Severity: data loss (none in EditGuard's wikis this time)
- Author: Karthik Mohan, with Claude Code · Status: final

## Summary
After long disconnects (the Mac asleep overnight), the edits producer twice resumed one event too late and skipped it. The resume ID Wikimedia gives for the eqiad partition is a message timestamp, and Kafka timestamps are not always in offset order, so a timestamp resume can land after an older-stamped event that was never read. The producer now resumes every partition by offset. The same review found two smaller issues from the 2026-10-03 restart: dead-lettered events while the Schema Registry was starting, and Airflow running dbt from the working folder.

## Impact
- 2 page_change events skipped, of about 2.5 million read: `uk.wikipedia.org` (eqiad offset 1110858455, 2026-09-28 18:49:45 UTC) and `de.wikipedia.org` (eqiad 1114187428, 2026-09-30 19:02:11 UTC). Neither is one of EditGuard's 8 wikis, so no row in bronze, silver or the reported numbers is affected.
- 4 further `upstream_gap` alerts (codfw, 1 to 10 offsets, at :15 past the hour) were not losses: re-reading EventStreams history shows those offsets are never served; codfw serves only hourly `canary` events.
- 2026-10-03 restart: 3 valid events dead-lettered because the Schema Registry was not up yet. The 2 edits were replayed (`make dlq-replay`, `make replay-bronze ENV=prod`, 2 rows); 1 Wikimedia baseline score is lost (the DLQ replay handles edits only).
- On 2026-10-01 at 16:49 UTC the hourly Airflow run picked up an uncommitted, unreviewed model (`silver.labels`) from the working folder. The run failed earlier (expired AWS signatures after the Docker clock fell behind) and skipped it, so nothing reached prod.

## Timeline (UTC)
| Time | Event |
| --- | --- |
| 2026-09-28 23:53 | First `upstream_gap` on eqiad (missing 1), logged after the Mac woke |
| 2026-09-30 23:56 | Second one (missing 1) |
| 2026-10-01 17:04 | Last bronze commit: Docker's clock behind, AWS signatures rejected; later a macOS update stopped Docker |
| 2026-10-03 03:54 | Docker restarted; producers resume (no gap); 3 events dead-lettered during start-up |
| 2026-10-03 05:10 | Detected: all 6 gaps re-read from EventStreams history; the 4 codfw gaps are upstream, the 2 eqiad ones are ours |
| 2026-10-03 05:15 | Root cause proven: resuming with the ID of offset 1110858454 serves 1110858456 next; offset 1110858455 is stamped 1 ms earlier |
| 2026-10-03 | Fixed: resume by offset; registry errors retried; Airflow on a release checkout |

## Root cause
EventStreams' Last-Event-ID is a list of assignments per upstream partition. For eqiad it carries `timestamp`, and EventStreams resumes at the first message with a timestamp at or after it. Producers write Wikimedia's Kafka in parallel, so timestamps can go backwards between neighbouring offsets (1110858455 is stamped 1 ms before 1110858454). When the bookmark lands on such an event, the next one falls before the resume point and is never served. Offsets, by contrast, always increase, and EventStreams starts an `offset` assignment at exactly that offset (measured: resuming with offset 1110858455 serves 1110858455 first).

Why it was not caught: the integration test's fake EventStreams resumed by matching IDs exactly, and real Wikimedia timestamps were never part of a test. The gap detector did its job: both losses were logged at error level, but nobody looked at the logs until 2026-10-03.

## What went well
- The gap detector reported every skip, with topic, partition and offsets, which made the investigation exact.
- Re-reading EventStreams history settled each case with evidence, not guesses.
- Bookmarks in Kafka and checkpoints on disk meant the 35-hour outage lost nothing.

## What went badly
- Error-level gap logs went unnoticed for five days: there is no alerting yet (M7).
- Airflow ran whatever code was in the working folder, including uncommitted work.
- Producers treated a registry that was still starting as bad data.

## Actions
| Action | Type | Status |
| --- | --- | --- |
| Producer bookmark carries the last offset of every upstream partition; resume starts each at offset + 1 (`resume_id`, unit test with the real 2026-09-28 case) | prevent | Done |
| Fake EventStreams in the integration test resumes per partition by offset, as the real service does | detect | Done |
| Registry connection errors and 5xx are retried with backoff, never dead-lettered; 4xx still dead-letters | prevent | Done |
| Airflow mounts `../editguard-release`, a git worktree at the deployed tag (`make release-tree`, run by `make deploy`) | prevent | Done |
| Turn on Docker Desktop's "start when you sign in" | mitigate | Done |
| Alert on `upstream_gap`, DLQ growth and a stopped live job | detect | M7 (Grafana alerts) |
