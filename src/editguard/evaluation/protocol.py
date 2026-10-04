"""The evaluation protocol (ADR 0013): what is evaluated, on which days, with which budget.

The test window was fixed on 2026-10-04, before it started. Do not move it: a new window
needs a new ADR, and the result on this one is kept.
"""

from datetime import UTC, datetime

from editguard.evaluation.metrics import BUDGET, RESAMPLES, SEED

TEST_WINDOW = (datetime(2026, 10, 5, tzinfo=UTC), datetime(2026, 10, 12, tzinfo=UTC))
# Final labels: the window can be evaluated once every edit in it is 48 hours old.
TEST_WINDOW_FINAL = datetime(2026, 10, 14, tzinfo=UTC)
MIN_DAMAGING = 100  # ADR 0006: no segment number below 100 damaging edits
SEGMENTS = ("english", "indian")  # dim_wiki.language_group; Indian languages pooled

__all__ = [
    "BUDGET",
    "MIN_DAMAGING",
    "RESAMPLES",
    "SEED",
    "SEGMENTS",
    "TEST_WINDOW",
    "TEST_WINDOW_FINAL",
]
