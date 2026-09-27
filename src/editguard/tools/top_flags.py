"""MVP read path: load edits.flagged into DuckDB and list the top flags.

A stand-in for the triage queue until M5 (Postgres + API). Reads the whole topic (7 days),
keeps the latest flag per (wiki_id, rev_id) and ranks by score, newest first on ties.

Usage: uv run python -m editguard.tools.top_flags [--hours 24] [--limit 20]
"""

import argparse
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from editguard.common.config import get_settings
from editguard.streaming.flags import FLAGGED_TOPIC

WIKI_HOSTS = {
    "enwiki": "en",
    "bnwiki": "bn",
    "hiwiki": "hi",
    "knwiki": "kn",
    "mlwiki": "ml",
    "mrwiki": "mr",
    "tawiki": "ta",
    "tewiki": "te",
}

TOP_FLAGS_SQL = """
WITH latest AS (
    SELECT *, row_number() OVER (
        PARTITION BY wiki_id, rev_id ORDER BY scored_at DESC
    ) AS copy
    FROM flags
    WHERE event_time >= ?
)
SELECT score, wiki_id, page_title, rev_id, event_time,
       date_diff('second', event_time, scored_at) AS latency_s
FROM latest
WHERE copy = 1
ORDER BY score DESC, event_time DESC
LIMIT ?
"""


def diff_url(wiki_id: str, rev_id: int) -> str:
    """Link to the edit on Wikipedia, so a patroller can review it."""
    return f"https://{WIKI_HOSTS[wiki_id]}.wikipedia.org/w/index.php?diff={rev_id}"


def read_flags() -> list[dict[str, Any]]:
    """Decode every message currently in edits.flagged."""
    settings = get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "editguard-top-flags",
            "enable.auto.commit": False,
        }
    )
    decode = AvroDeserializer(SchemaRegistryClient({"url": settings.schema_registry_url}))
    context = SerializationContext(FLAGGED_TOPIC, MessageField.VALUE)
    try:
        partitions = sorted(
            consumer.list_topics(FLAGGED_TOPIC, timeout=10).topics[FLAGGED_TOPIC].partitions
        )
        ends = {
            p: consumer.get_watermark_offsets(TopicPartition(FLAGGED_TOPIC, p))[1]
            for p in partitions
        }
        remaining = {p for p, end in ends.items() if end > 0}
        consumer.assign([TopicPartition(FLAGGED_TOPIC, p, OFFSET_BEGINNING) for p in remaining])
        flags = []
        while remaining:
            msg = consumer.poll(5.0)
            if msg is None or msg.error():
                continue
            flags.append(decode(msg.value(), context))
            if msg.offset() >= ends[msg.partition()] - 1:
                remaining.discard(msg.partition())
        return flags
    finally:
        consumer.close()


def naive_utc(value: datetime) -> datetime:
    """DuckDB's plain TIMESTAMP has no zone; store every time as UTC without tzinfo."""
    return value.astimezone(UTC).replace(tzinfo=None)


def top_flags(flags: list[dict[str, Any]], since: datetime, limit: int) -> list[tuple[Any, ...]]:
    """Rank flags in DuckDB: one row per edit, highest score first. Times are UTC."""
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE flags (wiki_id VARCHAR, rev_id BIGINT, page_title VARCHAR,"
        " event_time TIMESTAMP, score DOUBLE, score_version VARCHAR,"
        " history_snapshot_id BIGINT, scored_at TIMESTAMP)"
    )
    con.executemany(
        "INSERT INTO flags VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                f["wiki_id"],
                f["rev_id"],
                f["page_title"],
                naive_utc(f["event_time"]),
                f["score"],
                f["score_version"],
                f["history_snapshot_id"],
                naive_utc(f["scored_at"]),
            )
            for f in flags
        ],
    )
    return con.execute(TOP_FLAGS_SQL, [naive_utc(since), limit]).fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    flags = read_flags()
    since = datetime.now(UTC) - timedelta(hours=args.hours)
    rows = top_flags(flags, since, args.limit)
    print(f"{len(flags)} flag messages in {FLAGGED_TOPIC}.")
    print(f"Top {len(rows)} of the last {args.hours:g} h (times UTC):\n")
    print(f"{'score':>6}  {'wiki':<7} {'latency':>7}  page / diff")
    for score, wiki_id, title, rev_id, _event_time, latency in rows:
        print(f"{score:>6.3f}  {wiki_id:<7} {latency:>6}s  {title}")
        print(f"{'':>24}{diff_url(wiki_id, rev_id)}")


if __name__ == "__main__":
    main()
