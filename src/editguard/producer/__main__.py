"""Run the producer: EventStreams -> filter -> Kafka, resuming safely after any stop.

Usage: uv run python -m editguard.producer
"""

import signal
import sys
import time
from types import FrameType

import httpx
from confluent_kafka import KafkaError, Message

from editguard.common.config import get_settings
from editguard.common.logs import configure_logging
from editguard.producer.acks import AckTracker
from editguard.producer.backoff import backoff_delay
from editguard.producer.kafka_sink import (
    DeliveryCallback,
    EditSink,
    build_edit_serializer,
    build_producer,
)
from editguard.producer.parse import is_target
from editguard.producer.sse import iter_events
from editguard.producer.state import BookmarkWriter, load_bookmark

STREAM = "mediawiki.page_change.v1"
STATS_EVERY_S = 60.0


class Producer:
    """Holds the run state so callbacks and the signal handler can reach it."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.log = configure_logging("producer", self.settings.log_level)
        self.kafka = build_producer(self.settings)
        self.sink = EditSink(self.kafka, build_edit_serializer(self.settings))
        self.bookmark = BookmarkWriter(self.kafka, STREAM)
        self.stopping = False
        self.delivery_failed = False
        self.counts = {"received": 0, "kept": 0, "dlq": 0}

    def stop(self, signum: int, _frame: FrameType | None) -> None:
        """Signal handler: finish the current event, flush, save the bookmark, exit."""
        self.log.info("stop_requested", signal=signal.Signals(signum).name)
        self.stopping = True

    def run(self) -> int:
        resume_id = load_bookmark(self.settings, STREAM)
        self.log.info("starting", stream=STREAM, resuming=resume_id is not None)
        attempt = 0
        while not self.stopping and not self.delivery_failed:
            tracker = AckTracker()
            try:
                received_before = self.counts["received"]
                self._consume(tracker, resume_id)
            except (httpx.HTTPError, BufferError) as exc:
                self.log.warning("stream_interrupted", error=type(exc).__name__, detail=str(exc))
            # Every exit from _consume lands here: wait for Kafka, then move the bookmark.
            self.kafka.flush(30)
            resume_id = tracker.safe_event_id() or resume_id
            self.bookmark.maybe_save(resume_id, force=True)
            self.kafka.flush(10)
            if self.stopping or self.delivery_failed:
                break
            attempt = 0 if self.counts["received"] > received_before else attempt + 1
            delay = backoff_delay(attempt)
            self.log.info("reconnecting", attempt=attempt, delay_s=round(delay, 1))
            time.sleep(delay)
        self.log.info("stopped", **self.counts, delivery_failed=self.delivery_failed)
        return 1 if self.delivery_failed else 0

    def _consume(self, tracker: AckTracker, resume_id: str | None) -> None:
        timeout = httpx.Timeout(10.0, read=60.0)
        headers = {"User-Agent": self.settings.user_agent}
        last_stats = time.monotonic()
        with httpx.Client(headers=headers, timeout=timeout) as client:
            for event in iter_events(client, STREAM, resume_id):
                seq = tracker.register(event.id)
                self.counts["received"] += 1
                if is_target(event.data):
                    sent = self.sink.send(event.data, on_delivery=self._on_delivery(tracker, seq))
                    self.counts["kept" if sent else "dlq"] += 1
                else:
                    tracker.done(seq)
                self.kafka.poll(0)
                self.bookmark.maybe_save(tracker.safe_event_id())
                if time.monotonic() - last_stats > STATS_EVERY_S:
                    self.log.info("stats", **self.counts, in_flight=tracker.in_flight)
                    last_stats = time.monotonic()
                if self.stopping or self.delivery_failed:
                    return

    def _on_delivery(self, tracker: AckTracker, seq: int) -> DeliveryCallback:
        def callback(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                # Never mark it done: the bookmark stays before it, so a restart re-reads it.
                self.log.error("delivery_failed", error=str(err), topic=msg.topic())
                self.delivery_failed = True
                return
            tracker.done(seq)

        return callback


def main() -> None:
    producer = Producer()
    signal.signal(signal.SIGINT, producer.stop)
    signal.signal(signal.SIGTERM, producer.stop)
    sys.exit(producer.run())


if __name__ == "__main__":
    main()
