from typing import Any

import pytest

from editguard.producer.state import STATE_TOPIC, BookmarkWriter, latest_by_key


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.sent.append({"topic": topic, **kwargs})


def test_latest_by_key_keeps_newest_value_per_stream() -> None:
    messages = [
        (b"mediawiki.page_change.v1", b"id-1"),
        (b"baseline", b"b-1"),
        (b"mediawiki.page_change.v1", b"id-2"),
    ]
    assert latest_by_key(messages) == {"mediawiki.page_change.v1": "id-2", "baseline": "b-1"}


def test_latest_by_key_ignores_tombstones_and_keyless() -> None:
    assert latest_by_key([(None, b"x"), (b"s", None)]) == {}


def test_writer_saves_keyed_by_stream() -> None:
    producer = FakeProducer()
    assert BookmarkWriter(producer, "s1").maybe_save("id-1") is True
    [msg] = producer.sent
    assert msg == {"topic": STATE_TOPIC, "key": b"s1", "value": b"id-1"}


def test_writer_throttles_and_skips_unchanged() -> None:
    producer = FakeProducer()
    writer = BookmarkWriter(producer, "s1", interval_s=60)
    writer.maybe_save("id-1")
    assert writer.maybe_save("id-2") is False  # too soon
    assert writer.maybe_save("id-2", force=True) is True
    assert writer.maybe_save("id-2", force=True) is False  # unchanged
    assert writer.maybe_save(None, force=True) is False
    assert len(producer.sent) == 2


def test_first_save_is_not_throttled_on_a_just_booted_machine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """time.monotonic() counts from boot; on a fresh CI machine it can be below the interval."""
    monkeypatch.setattr("editguard.producer.state.time.monotonic", lambda: 5.0)
    producer = FakeProducer()
    assert BookmarkWriter(producer, "s1", interval_s=60).maybe_save("id-1") is True
    assert len(producer.sent) == 1
