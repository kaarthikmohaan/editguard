from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from editguard.tools.day1 import is_english_article_edit, measure, percentile

T0 = datetime(2026, 9, 25, 12, 0, tzinfo=UTC)


def edit(n: int, **overrides: Any) -> dict[str, Any]:
    record = {
        "event_id": f"e{n}",
        "event_time": T0 + timedelta(minutes=n),
        "emitted_at": T0 + timedelta(minutes=n, seconds=n),
        "wiki_id": "enwiki",
        "rev_id": 1000 + n,
        "namespace_id": 0,
        "page_change_kind": "edit",
        "performer_is_bot": False,
    }
    return {**record, **overrides}


def test_percentile_nearest_rank() -> None:
    values = [float(v) for v in range(1, 101)]
    assert percentile(values, 50) == 50
    assert percentile(values, 99) == 99
    with pytest.raises(ValueError):
        percentile([], 50)


def test_only_english_article_edits_count() -> None:
    assert is_english_article_edit(edit(1))
    assert not is_english_article_edit(edit(1, wiki_id="hiwiki"))
    assert not is_english_article_edit(edit(1, namespace_id=2))
    assert not is_english_article_edit(edit(1, page_change_kind="create"))


def test_coverage_counts_scored_edits_in_window() -> None:
    edits = [edit(1), edit(2), edit(3, performer_is_bot=True), edit(99)]
    scored = {("enwiki", 1001), ("enwiki", 1003)}
    result = measure(edits, scored, T0, T0 + timedelta(minutes=10))
    assert result["english_article_edits"] == 3  # edit 99 was emitted after the window
    assert result["coverage"] == pytest.approx(2 / 3)
    assert result["english_article_edits_non_bot"] == 2
    assert result["coverage_non_bot"] == pytest.approx(1 / 2)


def test_duplicates_are_counted_once() -> None:
    result = measure([edit(1), edit(1)], set(), T0, T0 + timedelta(minutes=10))
    assert result["events"] == 1
    assert result["english_article_edits"] == 1


def test_lateness_is_emitted_minus_event_time() -> None:
    result = measure([edit(1), edit(2), edit(3)], set(), T0, T0 + timedelta(minutes=10))
    assert result["lateness_max_s"] == 3


def test_old_rev_dt_on_deletes_is_not_lateness() -> None:
    delete = edit(4, page_change_kind="delete", event_time=T0 - timedelta(days=3000))
    result = measure([edit(1), delete], set(), T0, T0 + timedelta(minutes=10))
    assert result["lateness_max_s"] == 1
    assert result["other_kinds_event_time_over_1h_old"] == 1
