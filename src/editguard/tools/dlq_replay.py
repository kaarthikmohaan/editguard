"""Re-send dead-lettered edit events through the replay lane once the bug is fixed.

Usage: uv run python -m editguard.tools.dlq_replay [--dry-run] [--topic edits.replay.v1]
Reads edits.dlq from where this tool last stopped (consumer group dlq-replay), re-parses each
event from edits.raw.v1 with the current parser and contract, and sends the ones that now pass
to edits.replay.v1 (they are too old for the live watermark). Events that still fail stay in
the DLQ and are counted. Writes data/replays/dlq-replay-<time>.json with the event IDs sent.
"""

import argparse
import io
import json
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.serialization import MessageField, SerializationContext
from fastavro import parse_schema, schemaless_writer

from editguard.common.config import get_settings
from editguard.common.logs import configure_logging
from editguard.producer.kafka_sink import DLQ_TOPIC, build_producer, build_serializer
from editguard.producer.replay import REPLAY_TOPIC
from editguard.producer.streams import STREAMS

SOURCE_TOPIC = STREAMS["edits"].topic


def reparse(event: dict[str, Any], serialize: Any, context: SerializationContext) -> bytes:
    """Parse and serialize one dead-lettered event with today's code. Raises if it still fails."""
    record = STREAMS["edits"].parse(event)
    return serialize(record, context)


def local_serializer(schema_path: Path) -> Any:
    """Validate against the contract without touching Schema Registry (for --dry-run)."""
    schema = parse_schema(json.loads(schema_path.read_text()))

    def serialize(record: dict[str, Any], _context: SerializationContext) -> bytes:
        buf = io.BytesIO()
        schemaless_writer(buf, schema, record)
        return buf.getvalue()

    return serialize


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="count only: no send, no commit, no registry"
    )
    parser.add_argument("--topic", default=REPLAY_TOPIC, help="for tests: a throwaway topic")
    args = parser.parse_args()
    settings = get_settings()
    log = configure_logging("dlq_replay", settings.log_level)
    spec = replace(STREAMS["edits"], topic=args.topic)
    serialize = (
        local_serializer(spec.schema_path) if args.dry_run else build_serializer(settings, spec)
    )
    context = SerializationContext(args.topic, MessageField.VALUE)
    producer = build_producer(settings)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            # One group per target topic: a test run never moves the real run's position.
            "group.id": "dlq-replay" if args.topic == REPLAY_TOPIC else f"dlq-replay-{args.topic}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    partitions = consumer.list_topics(DLQ_TOPIC).topics[DLQ_TOPIC].partitions
    ends = {p: consumer.get_watermark_offsets(TopicPartition(DLQ_TOPIC, p))[1] for p in partitions}
    consumer.subscribe([DLQ_TOPIC])
    counts: Counter[str] = Counter()
    sent_ids: list[str] = []
    still_failing: Counter[str] = Counter()
    done: set[int] = {p for p, end in ends.items() if end == 0}
    while len(done) < len(partitions):
        msg = consumer.poll(5)
        if msg is None:
            break  # nothing new since the last run
        if msg.error():
            continue
        if msg.offset() >= ends[msg.partition()] - 1:
            done.add(msg.partition())
        headers = {k: v.decode() for k, v in (msg.headers() or [])}
        if headers.get("source_topic") != SOURCE_TOPIC:
            counts["other_source"] += 1
            continue
        event = json.loads(msg.value())
        try:
            value = reparse(event, serialize, context)
        except Exception as exc:  # still broken: leave it in the DLQ, report the reason
            still_failing[f"{type(exc).__name__}: {str(exc)[:80]}"] += 1
            continue
        counts["fixed"] += 1
        sent_ids.append(event["meta"]["id"])
        if not args.dry_run:
            producer.produce(args.topic, key=spec.key(STREAMS["edits"].parse(event)), value=value)
    unsent = producer.flush(30)
    if not args.dry_run and unsent == 0:
        consumer.commit(asynchronous=False)  # the next run starts after these messages
    consumer.close()
    report = {
        "dry_run": args.dry_run,
        "complete": unsent == 0,
        "fixed_and_sent": 0 if args.dry_run else len(sent_ids),
        "fixable": counts["fixed"],
        "still_failing": dict(still_failing),
        "other_source": counts["other_source"],
        "event_ids": sent_ids,
    }
    if not args.dry_run:
        path = Path("data/replays") / f"dlq-replay-{datetime.now(UTC):%Y%m%dT%H%M%S}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2) + "\n")
        report["report"] = str(path)
    log.info("finished", **{k: v for k, v in report.items() if k != "event_ids"})


if __name__ == "__main__":
    main()
