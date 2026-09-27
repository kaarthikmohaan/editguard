from datetime import UTC, datetime
from pathlib import Path

import pytest

from editguard.producer.replay import ReplayWindow, report_path

WINDOW = ReplayWindow(datetime(2026, 9, 26, 10, tzinfo=UTC), datetime(2026, 9, 26, 11, tzinfo=UTC))


def event(emitted: str) -> dict:
    return {"meta": {"dt": emitted}}


def test_window_includes_since_and_excludes_until() -> None:
    assert WINDOW.contains(event("2026-09-26T10:00:00Z"))
    assert WINDOW.contains(event("2026-09-26T10:59:59.999Z"))
    assert not WINDOW.contains(event("2026-09-26T11:00:00Z"))
    assert not WINDOW.contains(event("2026-09-26T09:59:59Z"))


def test_replay_waits_for_late_partitions_before_stopping() -> None:
    assert not WINDOW.finished(event("2026-09-26T11:02:00Z"))  # other partitions may lag
    assert WINDOW.finished(event("2026-09-26T11:05:00Z"))


def test_window_must_move_forward() -> None:
    with pytest.raises(ValueError, match="after since"):
        ReplayWindow(WINDOW.until, WINDOW.since)


def test_report_name_records_the_window() -> None:
    assert report_path(WINDOW) == Path("data/replays/replay-20260926T1000-20260926T1100.json")


def test_check_arguments_come_from_the_report() -> None:
    import json

    from editguard.tools.replay_args import replay_args

    report = {"since": "a", "until": "b", "complete": True, "sent": 12, "distinct_event_ids": 11}
    assert json.loads(replay_args(report)) == {"since": "a", "until": "b", "expected": 11}
    with pytest.raises(SystemExit, match="did not complete"):
        replay_args({**report, "complete": False})
