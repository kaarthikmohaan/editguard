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


def encode_checkpoint(event_id: str, topic: str, partition: int, offset: int) -> str:
    """Pack the resume ID and its upstream position into one bookmark string."""
    return json.dumps(
        {"last_event_id": event_id, "topic": topic, "partition": partition, "offset": offset}
    )


def decode_checkpoint(value: str | None) -> tuple[str | None, dict[tuple[str, int], int]]:
    """Unpack a bookmark into (resume ID, gap-detector seed).

    Older bookmarks are a bare Last-Event-ID (a JSON list) with no position; they resume
    fine but give the gap detector nothing to compare against.
    """
    if value is None:
        return None, {}
    parsed = json.loads(value)
    if isinstance(parsed, list):
        return value, {}
    seed = {(parsed["topic"], parsed["partition"]): parsed["offset"]}
    return parsed["last_event_id"], seed
