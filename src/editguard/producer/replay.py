"""Replay a past time window from EventStreams into edits.replay.v1 (design section 8).

Usage: uv run python -m editguard.producer.replay --since 2026-09-26T10:00:00Z \\
           --until 2026-09-26T11:00:00Z
Replays go to their own topic because days-old events would fall behind the live job's
watermark. Wikimedia allows 2 stream connections per IP: pause the baseline producer first.
Writes a report (window and distinct event IDs sent) for the replay-count check.
"""

import argparse
import json
import signal
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Any

import httpx

from editguard.common.config import get_settings
from editguard.common.logs import configure_logging
from editguard.producer.kafka_sink import RecordSink, build_producer, build_serializer
from editguard.producer.parse import is_target, parse_ts
from editguard.producer.sse import StreamEvent, iter_events
from editguard.producer.streams import STREAMS

REPLAY_TOPIC = "edits.replay.v1"
REPORT_DIR = Path("data/replays")
# EventStreams interleaves partitions, so an event just after `until` does not mean every
# earlier one has arrived. Stop only once events are this far past the window.
END_MARGIN = timedelta(minutes=5)


@dataclass(frozen=True)
class ReplayWindow:
    """[since, until) on the event's emission time (meta.dt), which EventStreams `since` uses."""

    since: datetime
    until: datetime

    def __post_init__(self) -> None:
        if self.until <= self.since:
            raise ValueError("until must be after since")

    def contains(self, event: dict[str, Any]) -> bool:
        emitted = parse_ts(event["meta"]["dt"])
        return emitted is not None and self.since <= emitted < self.until

    def finished(self, event: dict[str, Any]) -> bool:
        emitted = parse_ts(event["meta"]["dt"])
        return emitted is not None and emitted >= self.until + END_MARGIN


def replay_events(
    events: Iterable[StreamEvent],
    window: ReplayWindow,
    send: Callable[[dict[str, Any]], bool],
    should_stop: Callable[[], bool] = lambda: False,
) -> tuple[dict[str, int], set[str]]:
    """Send the target-wiki events inside the window; stop once the stream is past it.
    Returns the counts and the IDs of the events sent to the replay topic."""
    counts = {"received": 0, "in_window": 0, "sent": 0, "dlq": 0}
    sent_ids: set[str] = set()
    for event in events:
        data = event.data
        counts["received"] += 1
        if window.finished(data) or should_stop():
            break
        if not window.contains(data) or not is_target(data):
            continue
        counts["in_window"] += 1
        if send(data):
            counts["sent"] += 1
            sent_ids.add(data["meta"]["id"])
        else:
            counts["dlq"] += 1
    return counts, sent_ids


def report_path(window: ReplayWindow, report_dir: Path = REPORT_DIR) -> Path:
    stamp = f"{window.since:%Y%m%dT%H%M}-{window.until:%Y%m%dT%H%M}"
    return report_dir / f"replay-{stamp}.json"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--since", required=True, help="ISO 8601 UTC, e.g. 2026-09-26T10:00:00Z")
    parser.add_argument("--until", required=True, help="ISO 8601 UTC, exclusive")
    parser.add_argument("--topic", default=REPLAY_TOPIC)
    args = parser.parse_args()
    window = ReplayWindow(parse_ts(args.since), parse_ts(args.until))  # type: ignore[arg-type]

    settings = get_settings()
    log = configure_logging("replay", settings.log_level).bind(topic=args.topic)
    spec = replace(STREAMS["edits"], topic=args.topic)
    kafka = build_producer(settings)
    sink = RecordSink(kafka, build_serializer(settings, spec), spec)
    stopping = False

    def stop(signum: int, _frame: FrameType | None) -> None:
        nonlocal stopping
        log.info("stop_requested", signal=signal.Signals(signum).name)
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    log.info("starting", since=args.since, until=args.until)
    headers = {"User-Agent": settings.user_agent}
    with httpx.Client(headers=headers, timeout=httpx.Timeout(10.0, read=60.0)) as client:
        counts, sent_ids = replay_events(
            iter_events(client, spec.name, since=args.since),
            window,
            sink.send,
            should_stop=lambda: stopping,
        )
    unsent = kafka.flush(60)
    complete = not stopping and unsent == 0
    report = {
        "since": args.since,
        "until": args.until,
        "topic": args.topic,
        "complete": complete,
        **counts,
        "distinct_event_ids": len(sent_ids),
    }
    path = report_path(window)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    log.info("finished", report=str(path), **report)
    sys.exit(0 if complete else 1)


if __name__ == "__main__":
    main()
