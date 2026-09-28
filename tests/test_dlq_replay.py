import json
from pathlib import Path

import pytest
from confluent_kafka.serialization import MessageField, SerializationContext
from fastavro import parse_schema, schemaless_writer

from editguard.tools.dlq_replay import reparse

ROOT = Path(__file__).parents[1]
SCHEMA = parse_schema(json.loads((ROOT / "contracts/generated/edits.avsc").read_text()))
CONTEXT = SerializationContext("edits.replay.v1", MessageField.VALUE)


def fake_serialize(record: dict, _context: SerializationContext) -> bytes:
    import io

    buf = io.BytesIO()
    schemaless_writer(buf, SCHEMA, record)  # raises if the record breaks the contract
    return buf.getvalue()


def test_a_dead_lettered_suppressed_delete_now_reparses() -> None:
    event = json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text())
    event["page_change_kind"] = "delete"
    del event["revision"]["rev_size"]  # what put it in the DLQ
    assert reparse(event, fake_serialize, CONTEXT)


def test_an_event_that_still_breaks_the_contract_raises() -> None:
    event = json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text())
    del event["meta"]
    with pytest.raises(KeyError):
        reparse(event, fake_serialize, CONTEXT)
