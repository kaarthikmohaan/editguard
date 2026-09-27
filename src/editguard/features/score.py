"""Rule score v0: a few explainable signals, each worth points, scaled to 0..1.

Not a model. Each rule says why it fired, so a patroller can see the reason, and M6's model
must beat this in the comparison table to earn its place.
"""

from dataclasses import dataclass

from editguard.features.features import Features

SCORE_VERSION = "rules-v0"
# Flag at 4+ points out of 9 (score >= 0.44): about 3% of scored edits on the first 1.5 days
# of recordings, close to the design's 2% review budget. Revisit when labels mature (M4).
FLAG_THRESHOLD = 0.44

# (name, points, plain-language reason)
RULES = (
    ("temp_account", 3, "edited by a temporary (logged-out) account"),
    ("new_account", 2, "account younger than 1 day or fewer than 10 edits"),
    ("large_removal", 3, "removed more than 30% of the page"),
    ("big_removal_bytes", 2, "removed more than 2,000 bytes"),
    ("no_summary", 1, "no edit summary"),
)
# temp_account and new_account never fire together, so the reachable maximum is 3+3+2+1 = 9.
MAX_POINTS = 9


@dataclass(frozen=True)
class Score:
    """A score in 0..1, why it is that high, and which rules version produced it."""

    value: float
    reasons: tuple[str, ...]
    version: str = SCORE_VERSION

    @property
    def flagged(self) -> bool:
        return self.value >= FLAG_THRESHOLD


def fired_rules(f: Features) -> list[str]:
    """Names of the rules that apply to these features."""
    if f.is_trusted or f.is_revert:
        return []  # trusted editors and reverts are overwhelmingly fine: never flag on v0 rules
    fired = []
    if f.is_temp:
        fired.append("temp_account")
    new = (f.account_age_days is not None and f.account_age_days < 1) or (
        f.edit_count is not None and f.edit_count < 10
    )
    if new and not f.is_temp:
        fired.append("new_account")
    if f.relative_delta is not None and f.relative_delta <= -0.3:
        fired.append("large_removal")
    if f.byte_delta is not None and f.byte_delta <= -2000:
        fired.append("big_removal_bytes")
    if not f.has_summary:
        fired.append("no_summary")
    return fired


def rule_score(f: Features) -> Score:
    """Score in 0..1 plus the reasons that produced it."""
    fired = set(fired_rules(f))
    points = sum(p for name, p, _ in RULES if name in fired)
    reasons = tuple(reason for name, _, reason in RULES if name in fired)
    return Score(value=round(points / MAX_POINTS, 4), reasons=reasons)
