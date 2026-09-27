"""Score a micro-batch of bronze rows and publish the flagged ones to edits.flagged.

Scoring is plain Python (features.py and score.py), the same code replay and training use.
Publishing is at-least-once: the batch waits for Kafka acks before Spark checkpoints it,
and consumers upsert on (wiki_id, rev_id), so a retried batch causes no double counting.
"""

from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from editguard.common.config import Settings
from editguard.features.features import compute_features, is_scored
from editguard.features.score import rule_score

FLAGGED_TOPIC = "edits.flagged"
FLAGGED_SCHEMA = Path(__file__).parents[3] / "contracts/generated/flagged_edits.avsc"
TIMESTAMP_FIELDS = ("event_time", "performer_registration_dt")
# rules-v0 reads no history table, so no Iceberg snapshot was used. Real IDs arrive with M2.
NO_HISTORY_SNAPSHOT = 0


def flag_records(rows: Iterable[dict[str, Any]], scored_at: datetime) -> list[dict[str, Any]]:
    """Score each row; return FlaggedEdit records for the ones at or above the threshold."""
    flagged = []
    for row in rows:
        if not is_scored(row):
            continue
        score = rule_score(compute_features(row))
        if score.flagged:
            flagged.append(
                {
                    "wiki_id": row["wiki_id"],
                    "rev_id": row["rev_id"],
                    "page_title": row["page_title"],
                    "event_time": row["event_time"],
                    "score": score.value,
                    "score_version": score.version,
                    "history_snapshot_id": NO_HISTORY_SNAPSHOT,
                    "scored_at": scored_at,
                }
            )
    return flagged


def flag_key(record: dict[str, Any]) -> bytes:
    """Key wiki_id:rev_id (design section 9), so an edit's flags share a partition."""
    return f"{record['wiki_id']}:{record['rev_id']}".encode()


class FlagPublisher:
    """Avro-serializes flags against the contract and sends them with acks=all."""

    def __init__(self, settings: Settings) -> None:
        registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._serialize = AvroSerializer(registry, FLAGGED_SCHEMA.read_text())
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "editguard-live-job-flags",
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        self._context = SerializationContext(FLAGGED_TOPIC, MessageField.VALUE)

    def publish(self, flags: list[dict[str, Any]]) -> None:
        """Send all flags and wait until Kafka has them; raise if any failed."""
        errors: list[str] = []

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(str(err))

        for record in flags:
            value = self._serialize(record, self._context)
            self._producer.produce(
                FLAGGED_TOPIC, key=flag_key(record), value=value, on_delivery=on_delivery
            )
            self._producer.poll(0)
        remaining = self._producer.flush(60)
        if errors or remaining:
            raise RuntimeError(f"flag delivery failed: {errors[:3]}, {remaining} unsent")


def utc_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Spark hands timestamps to Python as naive UTC (we format them in UTC); mark them UTC."""
    fixed = []
    for row in rows:
        row = dict(row)
        for field in TIMESTAMP_FIELDS:
            value = row.get(field)
            if isinstance(value, str):
                row[field] = datetime.fromisoformat(value.replace("Z", "+00:00"))
            elif isinstance(value, datetime) and value.tzinfo is None:
                row[field] = value.replace(tzinfo=UTC)
        fixed.append(row)
    return fixed
