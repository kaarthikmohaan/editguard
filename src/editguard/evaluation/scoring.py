"""Score edits offline with the same code as the live job (T-SKEW-01 checks they agree)."""

from collections.abc import Iterable
from typing import Any

from editguard.features.features import compute_features
from editguard.features.score import SCORE_VERSION, rule_score


def rules_scores(records: Iterable[dict[str, Any]]) -> list[float]:
    """The rules-v0 score of each silver.edits record, in order."""
    return [rule_score(compute_features(record)).value for record in records]


__all__ = ["SCORE_VERSION", "rules_scores"]
