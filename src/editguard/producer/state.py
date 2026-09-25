"""Save and load the producer's resume bookmark in the compacted _producer_state topic.

One message per save, keyed by stream name. Compaction keeps only the latest value per key,
so the topic stays tiny and the newest bookmark always wins.
"""

import time
from collections.abc import Iterable
from typing import Any

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from editguard.common.config import Settings

STATE_TOPIC = "_producer_state"


def latest_by_key(messages: Iterable[tuple[bytes | None, bytes | None]]) -> dict[str, str]:
    """Replay (key, value) pairs in order and keep the last value for each key."""
    latest: dict[str, str] = {}
    for key, value in messages:
        if key is not None and value is not None:
            latest[key.decode()] = value.decode()
    return latest


def load_bookmark(settings: Settings, stream: str, timeout_s: float = 30.0) -> str | None:
    """Read the whole state topic and return the last saved event ID for one stream."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "editguard-producer-state-reader",
            "enable.auto.commit": False,
        }
    )
    try:
        partition = TopicPartition(STATE_TOPIC, 0)
        low, high = consumer.get_watermark_offsets(partition, timeout=10)
        if high == low:
            return None
        consumer.assign([TopicPartition(STATE_TOPIC, 0, OFFSET_BEGINNING)])
        pairs: list[tuple[bytes | None, bytes | None]] = []
        deadline = time.monotonic() + timeout_s
        last_offset = -1
        while last_offset < high - 1:
            if time.monotonic() > deadline:
                raise TimeoutError(f"could not read {STATE_TOPIC} to offset {high - 1}")
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            pairs.append((msg.key(), msg.value()))
            last_offset = msg.offset()
        return latest_by_key(pairs).get(stream)
    finally:
        consumer.close()


class BookmarkWriter:
    """Writes the bookmark at most once per interval; a lost write only means a few duplicates."""

    def __init__(self, producer: Any, stream: str, interval_s: float = 1.0) -> None:
        self._producer = producer
        self._stream = stream
        self._interval_s = interval_s
        self._last_saved: str | None = None
        self._last_time = 0.0

    def maybe_save(self, event_id: str | None, force: bool = False) -> bool:
        """Save event_id if it changed and the interval has passed (or force). True if saved."""
        now = time.monotonic()
        if event_id is None or event_id == self._last_saved:
            return False
        if not force and now - self._last_time < self._interval_s:
            return False
        self._producer.produce(STATE_TOPIC, key=self._stream.encode(), value=event_id.encode())
        self._last_saved = event_id
        self._last_time = now
        return True
