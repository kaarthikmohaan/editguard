"""Record the 1,000-event test fixture from the local Kafka (docs/testing.md, section 4).

Usage: uv run python scripts/fixtures/record_fixture.py [--since 2026-09-26T12:00:00Z]
Writes tests/fixtures/page_change_1k.jsonl.gz (raw upstream events, in emission order) and
tests/fixtures/baseline_1k.jsonl.gz (the Wikimedia scores recorded for those revisions).
Every username is replaced by a placeholder, consistently, including inside edit summaries
(revert summaries name the reverted editor). Gzipped to stay under the large-file hook.
"""

import argparse
import gzip
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

FIXTURES = Path(__file__).parents[2] / "tests/fixtures"
USERNAME_KEYS = ("user_text",)
TEXT_KEYS = ("comment", "parsedcomment")
# User and User talk pages are named after their owner, so their titles and URIs name editors.
USER_NAMESPACES = (2, 3)
TITLE_KEYS = ("page_title", "uri")


class Scrubber:
    """Replaces each username with user-0001, user-0002, ... (temporary accounts keep '~')."""

    def __init__(self) -> None:
        self.names: dict[str, str] = {}

    def placeholder(self, name: str) -> str:
        if name not in self.names:
            prefix = "~temp" if name.startswith("~") else "user"
            self.names[name] = f"{prefix}-{len(self.names) + 1:04d}"
        return self.names[name]

    def collect_event(self, event: dict[str, Any]) -> None:
        """Every editor in the event, plus the owner of a user page (who may not be editing)."""
        self.collect(event)
        page = event["page"]
        if page["namespace_id"] in USER_NAMESPACES and ":" in page["page_title"]:
            owner = page["page_title"].split(":", 1)[1].split("/")[0].replace("_", " ")
            self.placeholder(owner)

    def collect(self, obj: Any) -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in USERNAME_KEYS and isinstance(value, str):
                    self.placeholder(value)
                else:
                    self.collect(value)
        elif isinstance(obj, list):
            for value in obj:
                self.collect(value)

    def scrub_event(self, event: dict[str, Any]) -> dict[str, Any]:
        text_keys = TEXT_KEYS
        if event["page"]["namespace_id"] in USER_NAMESPACES:
            text_keys = TEXT_KEYS + TITLE_KEYS
        return self.scrub(event, text_keys)

    def scrub(self, obj: Any, text_keys: tuple[str, ...]) -> Any:
        if isinstance(obj, dict):
            return {
                key: self.names.get(value, value)
                if key in USERNAME_KEYS
                else self.scrub_text(value)
                if key in text_keys and isinstance(value, str)
                else self.scrub(value, text_keys)
                for key, value in obj.items()
            }
        if isinstance(obj, list):
            return [self.scrub(value, text_keys) for value in obj]
        return obj

    def scrub_text(self, text: str) -> str:
        for name, placeholder in sorted(self.names.items(), key=lambda kv: -len(kv[0])):
            variants = {name, name.replace(" ", "_"), name.replace(" ", "%20")}
            for variant in variants:
                text = re.sub(rf"(?<![\w-]){re.escape(variant)}(?![\w-])", placeholder, text)
        return text


def read_topic(topic: str, since_ms: int, until_ms: int, registry: str) -> list[Any]:
    """Every message with a Kafka timestamp in [since_ms, until_ms), from every partition.
    Reading a whole window (not the first N messages) makes the fixture reproducible."""
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "record-fixture",
            "enable.auto.commit": False,
        }
    )
    partitions = consumer.list_topics(topic).topics[topic].partitions
    starts = consumer.offsets_for_times([TopicPartition(topic, p, since_ms) for p in partitions])
    ends = {p: consumer.get_watermark_offsets(TopicPartition(topic, p))[1] for p in partitions}
    open_parts = {tp.partition for tp in starts if 0 <= tp.offset < ends[tp.partition]}
    consumer.assign([tp for tp in starts if tp.partition in open_parts])
    decode = AvroDeserializer(SchemaRegistryClient({"url": registry}))
    out = []
    while open_parts:
        msg = consumer.poll(5)
        if msg is None:
            break
        if msg.error() or msg.partition() not in open_parts:
            continue
        ts = msg.timestamp()[1]
        if ts >= until_ms or msg.offset() >= ends[msg.partition()] - 1:
            open_parts.discard(msg.partition())
            if ts >= until_ms:
                continue
        record = decode(msg.value(), SerializationContext(topic, MessageField.VALUE))
        out.append((record, ts))
    consumer.close()
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default="2026-09-26T12:00:00Z")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--registry", default="http://127.0.0.1:8081")
    args = parser.parse_args()
    since = datetime.fromisoformat(args.since.replace("Z", "+00:00")).astimezone(UTC)
    since_ms = int(since.timestamp() * 1000)

    window_ms = 60 * 60 * 1000  # read an hour, keep the earliest --count events
    records = read_topic("edits.raw.v1", since_ms, since_ms + window_ms, args.registry)
    raw = [json.loads(r["raw_json"]) for r, _ in records]
    events = sorted(raw, key=lambda e: (e["meta"]["dt"], e["meta"]["id"]))[: args.count]
    scrubber = Scrubber()
    for event in events:
        scrubber.collect_event(event)
    events = [scrubber.scrub_event(e) for e in events]

    revisions = {(e["wiki_id"], e["revision"]["rev_id"]) for e in events}
    scores = sorted(
        (
            {**record, "ingested_at": ts}
            for record, ts in read_topic(
                "baseline.raw.v1", since_ms - 60_000, since_ms + 2 * window_ms, args.registry
            )
            if (record["wiki_id"], record["rev_id"]) in revisions
        ),
        key=lambda r: (r["wiki_id"], r["rev_id"], r["ingested_at"]),
    )
    FIXTURES.mkdir(exist_ok=True)
    for name, rows in (("page_change_1k", events), ("baseline_1k", scores)):
        path = FIXTURES / f"{name}.jsonl.gz"
        text = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)
        # mtime=0: the gzip header would otherwise hold the write time, so reruns would differ.
        path.write_bytes(gzip.compress(text.encode("utf-8"), mtime=0))
        print(f"{path.name}: {len(rows)} rows, {path.stat().st_size / 1024:.0f} KiB")
    print(f"usernames replaced: {len(scrubber.names)}")


if __name__ == "__main__":
    main()
