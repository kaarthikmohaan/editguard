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


def test_replay_sends_only_target_events_in_window_and_stops_past_it() -> None:
    from editguard.producer.replay import replay_events
    from editguard.producer.sse import StreamEvent

    def ev(n: int, emitted: str, wiki: str = "enwiki") -> StreamEvent:
        return StreamEvent(
            id=str(n), data={"meta": {"dt": emitted, "id": f"e{n}"}, "wiki_id": wiki}
        )

    stream = [
        ev(1, "2026-09-26T09:59:00Z"),  # before the window
        ev(2, "2026-09-26T10:30:00Z"),
        ev(3, "2026-09-26T10:31:00Z", wiki="dewiki"),  # not a target wiki
        ev(4, "2026-09-26T11:02:00Z"),  # past the window, but within the margin: keep reading
        ev(5, "2026-09-26T10:59:00Z"),  # a late partition, still in the window
        ev(6, "2026-09-26T11:06:00Z"),  # past the margin: stop
        ev(7, "2026-09-26T10:45:00Z"),  # never read
    ]
    sent: list[str] = []
    counts, ids = replay_events(
        stream, WINDOW, lambda data: sent.append(data["meta"]["id"]) or True
    )
    assert sent == ["e2", "e5"]
    assert ids == {"e2", "e5"}
    assert counts == {"received": 6, "in_window": 2, "sent": 2, "dlq": 0}
