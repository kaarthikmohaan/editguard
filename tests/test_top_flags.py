from datetime import UTC, datetime, timedelta

from editguard.tools.top_flags import diff_url, top_flags

T = datetime(2026, 9, 27, 4, 0, tzinfo=UTC)


def flag(rev_id: int, score: float, minutes_ago: int = 1, scored_later: int = 5) -> dict:
    event_time = T - timedelta(minutes=minutes_ago)
    return {
        "wiki_id": "enwiki",
        "rev_id": rev_id,
        "page_title": f"Page_{rev_id}",
        "event_time": event_time,
        "score": score,
        "score_version": "rules-v0",
        "history_snapshot_id": 0,
        "scored_at": event_time + timedelta(seconds=scored_later),
    }


def test_ranks_by_score_and_keeps_one_row_per_edit() -> None:
    flags = [flag(1, 0.44), flag(2, 1.0), flag(2, 1.0, scored_later=9), flag(3, 0.56)]
    rows = top_flags(flags, T - timedelta(hours=1), limit=10)
    assert [r[3] for r in rows] == [2, 3, 1]  # rev_ids by score; the re-sent flag for 2 once


def test_old_flags_fall_outside_the_window() -> None:
    rows = top_flags([flag(1, 1.0, minutes_ago=120)], T - timedelta(hours=1), limit=10)
    assert rows == []


def test_latency_is_seconds_from_edit_to_flag() -> None:
    [row] = top_flags([flag(1, 1.0, scored_later=7)], T - timedelta(hours=1), limit=10)
    assert row[5] == 7


def test_diff_url_points_at_the_right_wiki() -> None:
    assert diff_url("hiwiki", 42) == "https://hi.wikipedia.org/w/index.php?diff=42"
