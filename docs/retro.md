# Retrospective

Written at v1.0.0. Honest over flattering.

## Outcomes vs goals

| Goal | Target | Result |
| --- | --- | --- |
| Flag latency p95 | < 60 s | |
| Zero duplicates and gaps | 0 | |
| Recall vs baseline | Report with CI | |
| AWS cost | < $5/month | |
| LLM cost | < $1/day | |

## Estimates vs actual

| Milestone | Estimated hours | Actual hours | Why the difference |
| --- | --- | --- | --- |
| M0 Start recording | 8 | 11.6 | Design, 11 ADRs and the day-1 tests on real Wikimedia traffic took longer than planned; they made M1 fast |
| M1 MVP | 8 | 1.9 | The design and day-1 code were already done, so the MVP was mostly wiring; the 24 h run happened unattended |
| M2 Stream and batch | 10 | 5.1 | dbt on Athena fitted the design well; time went to the bronze metadata fix and two replay bugs |
| M3 Contracts, CI/CD, environments | 10 | 7.8 | Many small real-world failures (CI setup, OIDC subject, IAM, a Linux-only cleanup bug), each quick to fix once seen |
| M4 Evaluation | 10 | | |
| M5 LLM enricher and API | 9 | | |
| M6 ML model and backfill | 8 | | |
| M7 Operations hardening | 10 | | |
| M8 Release | 7 | | |

Actual hours are active time estimated from the Claude Code session timestamps: time between messages, ignoring any gap over 30 minutes, split at each milestone's tag. Unattended runs (the 24-hour day-1 and MVP runs) and time spent away from the session are not counted.

## What worked
## What didn't
## Three decisions I would change
1.
2.
3.

## Lessons for the next project

## What's next
v2 backlog opened as issues, or `make teardown` run on <date>.
