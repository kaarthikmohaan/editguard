import copy
import io
import json
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka.serialization import SerializationContext
from fastavro import parse_schema, schemaless_writer

from editguard.producer.kafka_sink import DLQ_TOPIC, EDITS_TOPIC, EditSink

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
    assert EditSink(producer, avro_bytes).send(raw_edit) is True
    [msg] = producer.sent
    assert msg["topic"] == EDITS_TOPIC
    assert msg["key"] == b"enwiki:12345"
    assert isinstance(msg["value"], bytes)


def test_missing_field_goes_to_dlq_with_error_headers(raw_edit: dict[str, Any]) -> None:
    broken = copy.deepcopy(raw_edit)
    del broken["revision"]["rev_id"]
    producer = FakeProducer()
    assert EditSink(producer, avro_bytes).send(broken) is False
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
    assert EditSink(producer, avro_bytes).send(wrong_type) is False
    assert dict(producer.sent[0]["headers"])["error_stage"] == b"serialize"


def test_delivery_callback_is_passed_through(raw_edit: dict[str, Any]) -> None:
    producer = FakeProducer()

    def callback(err: Any, msg: Any) -> None: ...

    EditSink(producer, avro_bytes).send(raw_edit, on_delivery=callback)
    assert producer.sent[0]["on_delivery"] is callback
