"""Recall at a review budget, and its paired bootstrap interval (design: the headline metric).

Recall at budget b: patrollers review the top b of edits by score (b = 2% of the edits
evaluated); recall is the share of all damaging edits that land in that list.

Ties: rules-v0 scores in whole points, so many edits share the score at the cut. Edits tied at
the cut get fractional credit, k_left / n_tied each: the expected recall if the tie were broken
at random. Every model is treated the same way.

Paired bootstrap: both models are scored on the same edits; each resample draws whole hours
with replacement (edits in one hour are related: same vandal, same page) and recomputes both
recalls on the same draw, so the interval is for the difference. Fixed seed: reproducible.
"""

from dataclasses import dataclass

import numpy as np

BUDGET = 0.02
RESAMPLES = 10_000
SEED = 20261004


class Ranking:
    """One model's scores on the evaluated edits, sorted once so each resample is O(n)."""

    def __init__(self, scores: np.ndarray, damaging: np.ndarray, hour: np.ndarray) -> None:
        scores = np.asarray(scores, dtype=float)
        if not (len(scores) == len(damaging) == len(hour)):
            raise ValueError("scores, damaging and hour must have the same length")
        order = np.argsort(-scores, kind="stable")
        ranked = scores[order]
        self._damaging = np.asarray(damaging, dtype=bool)[order]
        self._hour = np.asarray(hour)[order]
        # Start of each run of equal scores: edits in one run are tied.
        self._ties = np.flatnonzero(np.r_[True, ranked[1:] != ranked[:-1]])

    def recall(self, hour_weight: np.ndarray, budget: float = BUDGET) -> float:
        """Recall at the budget when each edit counts hour_weight[its hour] times."""
        weight = np.asarray(hour_weight, dtype=float)[self._hour]
        bad = weight * self._damaging
        total_bad = bad.sum()
        if total_bad == 0:
            return float("nan")
        group_weight = np.add.reduceat(weight, self._ties)
        group_bad = np.add.reduceat(bad, self._ties)
        k = budget * weight.sum()
        reviewed = np.cumsum(group_weight)
        cut = int(np.searchsorted(reviewed, k, side="left"))  # first group reaching k
        if cut >= len(group_weight):
            return 1.0
        left = k - (reviewed[cut] - group_weight[cut])  # reviews left for the tied group
        share = left / group_weight[cut] if group_weight[cut] > 0 else 0.0
        return float((group_bad[:cut].sum() + share * group_bad[cut]) / total_bad)


@dataclass(frozen=True)
class Comparison:
    """EditGuard against the baseline on the same edits."""

    edits: int
    damaging: int
    hours: int
    editguard: float
    baseline: float
    difference: float
    low: float
    high: float


def paired_bootstrap(
    editguard: Ranking,
    baseline: Ranking,
    n_hours: int,
    damaging_count: int,
    edit_count: int,
    resamples: int = RESAMPLES,
    seed: int = SEED,
    budget: float = BUDGET,
) -> Comparison:
    """Point recalls, their difference, and its 95% percentile interval over hour resamples."""
    ones = np.ones(n_hours)
    ours, theirs = editguard.recall(ones, budget), baseline.recall(ones, budget)
    rng = np.random.default_rng(seed)
    diffs = np.empty(resamples)
    for i in range(resamples):
        draw = np.bincount(rng.integers(0, n_hours, n_hours), minlength=n_hours)
        diffs[i] = editguard.recall(draw, budget) - baseline.recall(draw, budget)
    low, high = np.nanpercentile(diffs, [2.5, 97.5])
    return Comparison(
        edits=edit_count,
        damaging=damaging_count,
        hours=n_hours,
        editguard=ours,
        baseline=theirs,
        difference=ours - theirs,
        low=float(low),
        high=float(high),
    )
