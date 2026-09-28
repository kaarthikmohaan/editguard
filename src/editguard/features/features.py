"""Point-in-time features for one edit, computed only from fields on the event itself.

v0 uses editor attributes as they were at edit time (they arrive with the event) and the
size change. History features (reverts the editor received earlier) join in M2 from
user_history_asof_hour. Pure functions: the same code serves live, replay and training.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

TRUSTED_GROUPS = frozenset(
    {"extendedconfirmed", "rollbacker", "reviewer", "autoreviewer", "sysop", "patroller"}
)


def is_scored(record: dict[str, Any]) -> bool:
    """The scored population: non-bot edits to articles (design: bots and creations excluded)."""
    return (
        record["page_change_kind"] == "edit"
        and record["namespace_id"] == 0
        and not record["performer_is_bot"]
    )


@dataclass(frozen=True)
class Features:
    """Everything the v0 rules look at. None means unknown, never guessed."""

    is_temp: bool
    account_age_days: float | None
    edit_count: int | None
    is_trusted: bool
    byte_delta: int | None
    relative_delta: float | None
    has_summary: bool
    is_revert: bool


def account_age_days(event_time: datetime, registration: datetime | None) -> float | None:
    """Days between registration and the edit; None if registration is unknown (~0.9%)."""
    if registration is None:
        return None
    return max(0.0, (event_time - registration).total_seconds() / 86400)


# The only record fields compute_features may read (T-LEAK-01 checks it). Each one is known at
# the moment of the edit: it arrives with the event itself. Anything learned later (reverts of
# this edit, the editor's later activity, labels) must never be added here.
POINT_IN_TIME_FIELDS = frozenset(
    {
        "event_time",
        "rev_size",
        "prior_rev_size",
        "comment",
        "is_comment_visible",
        "performer_is_temp",
        "performer_registration_dt",
        "performer_edit_count",
        "performer_groups",
        "revert_method",
    }
)


def compute_features(record: dict[str, Any]) -> Features:
    """Features for one bronze/EditEvent record (a dict with the contract's fields)."""
    prior = record["prior_rev_size"]
    size = record["rev_size"]
    delta = None if prior is None or size is None else size - prior
    relative = None if delta is None or not prior else delta / prior
    comment = record["comment"]
    return Features(
        is_temp=record["performer_is_temp"],
        account_age_days=account_age_days(
            record["event_time"], record["performer_registration_dt"]
        ),
        edit_count=record["performer_edit_count"],
        is_trusted=bool(TRUSTED_GROUPS.intersection(record["performer_groups"] or [])),
        byte_delta=delta,
        relative_delta=relative,
        has_summary=bool(comment and comment.strip()) or not record["is_comment_visible"],
        is_revert=record["revert_method"] is not None,
    )
