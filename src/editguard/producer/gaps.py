"""Detect lost upstream events by checking that offsets never skip a number.

Every event carries meta.topic, meta.partition and meta.offset from Wikimedia's own Kafka.
Within one (topic, partition), offsets go up by exactly 1. A jump means events were lost.
This runs on every event, before wiki filtering, or filtered events would look like gaps.
"""

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Gap:
    """A run of missing offsets in one upstream partition."""

    topic: str
    partition: int
    expected: int
    got: int

    @property
    def missing(self) -> int:
        """How many events were skipped."""
        return self.got - self.expected


class GapDetector:
    """Remembers the last offset per upstream partition and reports any jump."""

    def __init__(self, seed: dict[tuple[str, int], int] | None = None) -> None:
        self._last: dict[tuple[str, int], int] = dict(seed or {})

    def observe(self, topic: str, partition: int, offset: int) -> Gap | None:
        """Record one offset. Returns a Gap if offsets were skipped since the last one."""
        key = (topic, partition)
        last = self._last.get(key)
        if last is not None and offset <= last:
            return None  # a replayed duplicate after resume; harmless, dedup happens downstream
        self._last[key] = offset
        if last is not None and offset > last + 1:
            return Gap(topic, partition, expected=last + 1, got=offset)
        return None


Positions = dict[tuple[str, int], int]  # (upstream topic, partition) -> last offset handled


def encode_checkpoint(event_id: str, positions: Positions) -> str:
    """Pack Wikimedia's resume ID and the last offset handled in every upstream partition."""
    return json.dumps(
        {
            "last_event_id": event_id,
            "positions": [
                {"topic": t, "partition": p, "offset": o} for (t, p), o in sorted(positions.items())
            ],
        }
    )


def resume_id(event_id: str, positions: Positions) -> str:
    """The Last-Event-ID to resume with: start each known partition at its next offset.

    Wikimedia's own IDs resume eqiad by message timestamp, and Kafka timestamps are not
    always in offset order (offset 1110858455 is 1 ms older than 1110858454), so a
    timestamp resume can skip an event (docs/postmortems/2026-10-03-resume-skip.md).
    EventStreams starts an `offset` assignment at that offset, so offset + 1 is exact.
    Partitions we have no position for keep Wikimedia's entry.
    """
    entries = {(e["topic"], e["partition"]): e for e in json.loads(event_id)}
    for (topic, partition), offset in positions.items():
        entries[(topic, partition)] = {"topic": topic, "partition": partition, "offset": offset + 1}
    return json.dumps(list(entries.values()))


def decode_checkpoint(value: str | None) -> tuple[str | None, Positions]:
    """Unpack a bookmark into (resume ID, positions; the gap detector's seed).

    Bookmarks written before the fix hold one position (topic, partition, offset); the
    oldest are a bare Last-Event-ID (a JSON list) with no position at all.
    """
    if value is None:
        return None, {}
    parsed = json.loads(value)
    if isinstance(parsed, list):
        return value, {}
    if "positions" in parsed:
        positions = {(e["topic"], e["partition"]): e["offset"] for e in parsed["positions"]}
    else:
        positions = {(parsed["topic"], parsed["partition"]): parsed["offset"]}
    return resume_id(parsed["last_event_id"], positions), positions
