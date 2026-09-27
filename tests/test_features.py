import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from editguard.features.features import account_age_days, compute_features, is_scored
from editguard.features.score import FLAG_THRESHOLD, MAX_POINTS, rule_score
from editguard.producer.parse import to_edit_event

ROOT = Path(__file__).parents[1]


@pytest.fixture
def record() -> dict[str, Any]:
    raw = json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text())
    rec = to_edit_event(raw)
    rec["revert_method"] = None  # a plain edit, not a revert
    return rec


def test_scored_population_is_non_bot_article_edits(record: dict[str, Any]) -> None:
    assert is_scored(record)
    assert not is_scored({**record, "performer_is_bot": True})
    assert not is_scored({**record, "namespace_id": 2})
    assert not is_scored({**record, "page_change_kind": "create"})


def test_account_age_handles_missing_registration() -> None:
    t = datetime(2026, 9, 26, tzinfo=UTC)
    assert account_age_days(t, datetime(2026, 9, 25, tzinfo=UTC)) == 1.0
    assert account_age_days(t, None) is None


def test_byte_delta_and_relative_delta(record: dict[str, Any]) -> None:
    f = compute_features({**record, "rev_size": 500, "prior_rev_size": 1000})
    assert f.byte_delta == -500
    assert f.relative_delta == -0.5


def test_creation_has_no_delta(record: dict[str, Any]) -> None:
    f = compute_features({**record, "prior_rev_size": None})
    assert f.byte_delta is None
    assert f.relative_delta is None


def test_hidden_summary_does_not_count_as_missing(record: dict[str, Any]) -> None:
    f = compute_features({**record, "comment": None, "is_comment_visible": False})
    assert f.has_summary


def test_established_editor_small_edit_scores_zero(record: dict[str, Any]) -> None:
    rec = {**record, "performer_edit_count": 5000, "rev_size": 1010, "prior_rev_size": 1000}
    assert rule_score(compute_features(rec)).value == 0


def test_temp_account_blanking_without_summary_is_flagged(record: dict[str, Any]) -> None:
    rec = {
        **record,
        "performer_is_temp": True,
        "performer_groups": ["*"],
        "rev_size": 100,
        "prior_rev_size": 6000,
        "comment": "",
    }
    score = rule_score(compute_features(rec))
    assert score.value == 1.0  # temp + large removal + big removal + no summary = 9 of 9
    assert score.flagged
    assert "removed more than 30% of the page" in score.reasons


def test_trusted_editor_and_reverts_are_never_flagged(record: dict[str, Any]) -> None:
    risky = {**record, "performer_is_temp": True, "rev_size": 0, "prior_rev_size": 9000}
    assert rule_score(compute_features({**risky, "performer_groups": ["rollbacker"]})).value == 0
    assert rule_score(compute_features({**risky, "revert_method": "undo"})).value == 0


def test_score_is_between_zero_and_one_and_threshold_is_reachable() -> None:
    assert MAX_POINTS == 9
    assert 0 < FLAG_THRESHOLD < 1


def test_new_account_alone_is_not_flagged_but_with_removal_it_is(record: dict[str, Any]) -> None:
    new = {**record, "performer_edit_count": 3, "performer_groups": ["*"]}
    assert not rule_score(compute_features(new)).flagged  # 2 points
    removal = {**new, "rev_size": 100, "prior_rev_size": 1000}
    assert rule_score(compute_features(removal)).flagged  # 2 + 3 = 5 points
