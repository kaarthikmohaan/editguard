"""Day-1 feasibility measurements from the recorded Kafka topics (design section 13).

Measures, over one window of emitted_at (when Wikimedia sent each event):
- lateness: emitted_at - event_time (meta.dt - rev_dt) for edits and creations, p50/p99/max.
  Deletes and undeletes carry the old revision's rev_dt, so they are counted separately.
- English article-edit volume (namespace 0, kind edit), all and non-bot
- baseline join coverage: share of those edits that have a revert-risk score

Usage: uv run python -m editguard.tools.day1 [--since 2026-09-25T15:15] [--hours 24]
Use --since to start after both producers were running, or coverage is understated.
"""

import argparse
import math
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from editguard.common.config import get_settings

SETTLE = timedelta(minutes=10)  # let baseline scores for the newest edits arrive
NEW_REVISION_KINDS = {"edit", "create"}  # kinds whose rev_dt is when the change happened


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile; p in 0..100."""
    if not values:
        raise ValueError("no values")
    ordered = sorted(values)
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


def is_english_article_edit(record: dict[str, Any]) -> bool:
    """English Wikipedia, article namespace, a normal edit (not create, move or delete)."""
    return (
        record["wiki_id"] == "enwiki"
        and record["namespace_id"] == 0
        and record["page_change_kind"] == "edit"
    )


def measure(
    edits: Iterable[dict[str, Any]],
    scored: set[tuple[str, int]],
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    """All day-1 numbers for events with start <= emitted_at < end."""
    lateness: list[float] = []
    english = english_non_bot = covered = covered_non_bot = old_event_time = 0
    seen: set[str] = set()
    for record in edits:
        if not start <= record["emitted_at"] < end or record["event_id"] in seen:
            continue
        seen.add(record["event_id"])  # ignore duplicates from producer restarts
        lag = (record["emitted_at"] - record["event_time"]).total_seconds()
        if record["page_change_kind"] in NEW_REVISION_KINDS:
            lateness.append(lag)
        elif lag > 3600:
            old_event_time += 1
        if not is_english_article_edit(record):
            continue
        has_score = (record["wiki_id"], record["rev_id"]) in scored
        english += 1
        covered += has_score
        if not record["performer_is_bot"]:
            english_non_bot += 1
            covered_non_bot += has_score
    return {
        "events": len(seen),
        "lateness_p50_s": percentile(lateness, 50) if lateness else None,
        "lateness_p99_s": percentile(lateness, 99) if lateness else None,
        "lateness_max_s": max(lateness) if lateness else None,
        "other_kinds_event_time_over_1h_old": old_event_time,
        "english_article_edits": english,
        "english_article_edits_non_bot": english_non_bot,
        "coverage": covered / english if english else None,
        "coverage_non_bot": covered_non_bot / english_non_bot if english_non_bot else None,
    }


def read_topic(topic: str) -> Iterator[dict[str, Any]]:
    """Decode every message currently in a topic, from the beginning."""
    settings = get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "editguard-day1",
            "enable.auto.commit": False,
        }
    )
    decode = AvroDeserializer(SchemaRegistryClient({"url": settings.schema_registry_url}))
    context = SerializationContext(topic, MessageField.VALUE)
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        partitions = sorted(metadata.topics[topic].partitions)
        ends = {p: consumer.get_watermark_offsets(TopicPartition(topic, p))[1] for p in partitions}
        remaining = {p for p, end in ends.items() if end > 0}
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in remaining])
        while remaining:
            msg = consumer.poll(5.0)
            if msg is None or msg.error():
                continue
            yield decode(msg.value(), context)
            if msg.offset() >= ends[msg.partition()] - 1:
                remaining.discard(msg.partition())
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--hours", type=float, default=24.0, help="window length")
    parser.add_argument(
        "--since", type=datetime.fromisoformat, help="window start, UTC (default: first event)"
    )
    args = parser.parse_args()

    scored = {(r["wiki_id"], r["rev_id"]) for r in read_topic("baseline.raw.v1")}
    edits = list(read_topic("edits.raw.v1"))
    if not edits:
        raise SystemExit("edits.raw.v1 is empty")

    first = min(r["emitted_at"] for r in edits)
    if args.since is not None:
        first = max(first, args.since.replace(tzinfo=UTC))
    last = max(r["emitted_at"] for r in edits)
    end = min(first + timedelta(hours=args.hours), last - SETTLE)
    result = measure(edits, scored, first, end)
    hours = (end - first).total_seconds() / 3600

    print(f"Window (emitted_at, UTC): {first:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M}")
    print(f"Window length: {hours:.1f} h (asked for {args.hours:g} h)")
    for key, value in result.items():
        shown = f"{value:.3f}" if isinstance(value, float) else value
        print(f"  {key}: {shown}")


if __name__ == "__main__":
    main()
