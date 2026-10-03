"""Write parsed records to Kafka as Avro, and anything that fails to the dead letter queue."""

import json
from collections.abc import Callable
from typing import Any, Protocol

import httpx
from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

from editguard.common.config import Settings
from editguard.producer.streams import StreamSpec

DLQ_TOPIC = "edits.dlq"


class RegistryUnavailable(Exception):
    """Schema Registry could not be reached (or failed): retry later, never dead-letter."""


DeliveryCallback = Callable[[KafkaError | None, Message], None]
ValueSerializer = Callable[[dict[str, Any], SerializationContext], bytes | None]


class KafkaProducerLike(Protocol):
    """The subset of confluent_kafka.Producer that RecordSink uses. Lets tests pass a fake."""

    def produce(self, topic: str, **kwargs: Any) -> None: ...
    def poll(self, timeout: float) -> int: ...


def build_producer(settings: Settings) -> Producer:
    """Kafka producer configured for no loss and no duplicates from retries (design section 8)."""
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "editguard-producer",
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 120_000,
            "linger.ms": 50,
            "compression.type": "zstd",
        }
    )


def build_serializer(settings: Settings, spec: StreamSpec) -> AvroSerializer:
    """Avro serializer that checks every record against the stream's contract schema."""
    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    return AvroSerializer(registry, spec.schema_path.read_text())


class RecordSink:
    """Parses, validates and sends events; failures go to the DLQ with the error in headers."""

    def __init__(
        self,
        producer: KafkaProducerLike,
        serializer: ValueSerializer,
        spec: StreamSpec,
        dlq_topic: str = DLQ_TOPIC,
    ) -> None:
        self._producer = producer
        self._serializer = serializer
        self._spec = spec
        self._dlq_topic = dlq_topic

    def send(self, event: dict[str, Any], on_delivery: DeliveryCallback | None = None) -> bool:
        """Send one upstream event. True if it went to the main topic, False if to the DLQ."""
        stage = "parse"
        try:
            record = self._spec.parse(event)
            stage = "serialize"
            context = SerializationContext(self._spec.topic, MessageField.VALUE)
            value = self._serializer(record, context)
        except (httpx.TransportError, SchemaRegistryError) as exc:
            if isinstance(exc, SchemaRegistryError) and exc.http_status_code < 500:
                self._send_to_dlq(event, stage, exc, on_delivery)  # the registry rejected it
                return False
            # The registry is unreachable or failing: the event is fine, so do not dead-letter
            # it. The producer reconnects with backoff and re-reads it from the bookmark.
            raise RegistryUnavailable(str(exc)) from exc
        except Exception as exc:  # any bad event goes to the DLQ; it must never stop the stream
            self._send_to_dlq(event, stage, exc, on_delivery)
            return False
        self._producer.produce(
            self._spec.topic, key=self._spec.key(record), value=value, on_delivery=on_delivery
        )
        self._producer.poll(0)
        return True

    def _send_to_dlq(
        self,
        event: dict[str, Any],
        stage: str,
        exc: Exception,
        on_delivery: DeliveryCallback | None,
    ) -> None:
        headers = [
            ("error_stage", stage.encode()),
            ("error_type", type(exc).__name__.encode()),
            ("error_message", str(exc)[:500].encode()),
            ("source_topic", self._spec.topic.encode()),
        ]
        page = event.get("page") or {}
        key = f"{event.get('wiki_id')}:{page.get('page_id')}".encode()
        value = json.dumps(event, ensure_ascii=False).encode()
        self._producer.produce(
            self._dlq_topic, key=key, value=value, headers=headers, on_delivery=on_delivery
        )
        self._producer.poll(0)
