import copy
import io
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationContext
from fastavro import parse_schema, schemaless_writer

from editguard.producer.kafka_sink import DLQ_TOPIC, RecordSink, RegistryUnavailable
from editguard.producer.streams import STREAMS

ROOT = Path(__file__).parents[1]
SCHEMA = parse_schema(json.loads((ROOT / "contracts/generated/edits.avsc").read_text()))


class FakeProducer:
    """Records produce() calls instead of talking to Kafka."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.sent.append({"topic": topic, **kwargs})

    def poll(self, timeout: float) -> int:
        return 0


def avro_bytes(record: dict[str, Any], ctx: SerializationContext) -> bytes:
    """Stand-in for AvroSerializer: same schema check, no Schema Registry needed."""
    buf = io.BytesIO()
    schemaless_writer(buf, SCHEMA, record)
    return buf.getvalue()


@pytest.fixture
def raw_edit() -> dict[str, Any]:
    return json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text())


def test_valid_event_goes_to_edits_topic_with_page_key(raw_edit: dict[str, Any]) -> None:
    producer = FakeProducer()
    assert RecordSink(producer, avro_bytes, STREAMS["edits"]).send(raw_edit) is True
    [msg] = producer.sent
    assert msg["topic"] == "edits.raw.v1"
    assert msg["key"] == b"enwiki:12345"
    assert isinstance(msg["value"], bytes)


def test_missing_field_goes_to_dlq_with_error_headers(raw_edit: dict[str, Any]) -> None:
    broken = copy.deepcopy(raw_edit)
    del broken["revision"]["rev_id"]
    producer = FakeProducer()
    assert RecordSink(producer, avro_bytes, STREAMS["edits"]).send(broken) is False
    [msg] = producer.sent
    headers = dict(msg["headers"])
    assert msg["topic"] == DLQ_TOPIC
    assert msg["key"] == b"enwiki:12345"
    assert headers["error_stage"] == b"parse"
    assert headers["error_type"] == b"KeyError"
    assert json.loads(msg["value"]) == broken


def test_schema_violation_goes_to_dlq(raw_edit: dict[str, Any]) -> None:
    wrong_type = copy.deepcopy(raw_edit)
    wrong_type["revision"]["rev_size"] = "not a number"
    producer = FakeProducer()
    assert RecordSink(producer, avro_bytes, STREAMS["edits"]).send(wrong_type) is False
    assert dict(producer.sent[0]["headers"])["error_stage"] == b"serialize"


def test_delivery_callback_is_passed_through(raw_edit: dict[str, Any]) -> None:
    producer = FakeProducer()

    def callback(err: Any, msg: Any) -> None: ...

    RecordSink(producer, avro_bytes, STREAMS["edits"]).send(raw_edit, on_delivery=callback)
    assert producer.sent[0]["on_delivery"] is callback


def failing_serializer(exc: Exception):
    def serialize(record: dict[str, Any], ctx: SerializationContext) -> bytes:
        raise exc

    return serialize


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("[Errno 111] Connection refused"),  # registry still starting
        SchemaRegistryError(503, 50301, "service unavailable"),
    ],
)
def test_registry_outage_is_retried_not_dead_lettered(raw_edit, exc) -> None:
    producer = FakeProducer()
    sink = RecordSink(producer, failing_serializer(exc), STREAMS["edits"])
    with pytest.raises(RegistryUnavailable):
        sink.send(raw_edit)
    assert producer.sent == []  # nothing to the DLQ; the producer re-reads it after a backoff


def test_registry_rejection_still_goes_to_the_dlq(raw_edit) -> None:
    producer = FakeProducer()
    exc = SchemaRegistryError(409, 409, "incompatible schema")
    sink = RecordSink(producer, failing_serializer(exc), STREAMS["edits"])
    assert sink.send(raw_edit) is False
    assert [m["topic"] for m in producer.sent] == [DLQ_TOPIC]
