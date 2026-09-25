# 0007. LLM verdicts are add-only and off the hot path

- Status: accepted
- Date: 2026-09-25

## Context
Diff text is written by editors, including vandals, so it can carry prompt injection. A local 4 to 8B model takes seconds per call and is weak on Indian languages.

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| LLM scores every edit in the stream | Simple | Slow; outage stalls the stream; injection can hide flags |
| LLM enriches flagged edits only, add-only | Stream unaffected; injection cannot remove a flag | Explanations arrive later |

## Decision
A separate enricher reads `edits.flagged`. English: local model, Haiku when confidence < 0.6. Indian languages: Haiku. The LLM can add a flag or reason, never remove one. Model ID, prompt version and temperature 0 are stored with each verdict.

## Consequences
- Bounded queue with load shedding and a daily budget.
- 20 injection test cases must pass on every prompt change.
