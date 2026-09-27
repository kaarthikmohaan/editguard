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


def test_an_event_is_past_the_end_only_after_the_margin() -> None:
    assert not WINDOW.past_end(event("2026-09-26T11:02:00Z"))
    assert WINDOW.past_end(event("2026-09-26T11:05:00Z"))


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


def test_replay_reads_every_data_centre_before_stopping() -> None:
    """EventStreams sends the quiet codfw topic first (hourly events), then eqiad. Seeing codfw
    past the window must not stop the replay before eqiad's edits arrive."""
    import json

    from editguard.producer.replay import replay_events
    from editguard.producer.sse import StreamEvent

    positions = json.dumps(
        [{"topic": "eqiad.pc", "partition": 0, "offset": 1}, {"topic": "codfw.pc", "partition": 0}]
    )

    def ev(n: int, topic: str, emitted: str, wiki: str = "enwiki") -> StreamEvent:
        data = {"meta": {"dt": emitted, "id": f"e{n}", "topic": topic, "partition": 0}}
        return StreamEvent(id=positions, data={**data, "wiki_id": wiki})

    stream = [
        ev(1, "codfw.pc", "2026-09-26T10:15:00Z"),
        ev(2, "codfw.pc", "2026-09-26T11:15:00Z"),  # codfw is past the window...
        ev(3, "eqiad.pc", "2026-09-26T09:59:00Z"),  # ...but eqiad has only just started
        ev(4, "eqiad.pc", "2026-09-26T10:30:00Z"),
        ev(5, "eqiad.pc", "2026-09-26T10:31:00Z", wiki="dewiki"),  # not a target wiki
        ev(6, "eqiad.pc", "2026-09-26T11:02:00Z"),  # past the window, within the margin
        ev(7, "eqiad.pc", "2026-09-26T11:06:00Z"),  # every partition past the margin: stop
        ev(8, "eqiad.pc", "2026-09-26T10:45:00Z"),  # never read
    ]
    sent: list[str] = []
    counts, ids = replay_events(
        stream, WINDOW, lambda data: sent.append(data["meta"]["id"]) or True
    )
    assert sent == ["e1", "e4"]
    assert ids == {"e1", "e4"}
    assert counts == {"received": 7, "in_window": 2, "sent": 2, "dlq": 0}
