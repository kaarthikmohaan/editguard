import io
import json
import os
import shutil
import struct
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastavro import parse_schema, schemaless_writer

from editguard.producer.parse import to_edit_event

pytestmark = pytest.mark.skipif(
    shutil.which("java") is None or os.environ.get("EDITGUARD_SPARK_TESTS") != "1",
    reason="Spark test: set EDITGUARD_SPARK_TESTS=1 (needs Java 17 and the connector jars)",
)

ROOT = Path(__file__).parents[1]
SCHEMA = parse_schema(json.loads((ROOT / "contracts/generated/edits.avsc").read_text()))


def confluent_bytes(record: dict[str, Any], schema_id: int = 7) -> bytes:
    """What the producer puts on Kafka: magic byte 0, 4-byte schema ID, Avro body."""
    buf = io.BytesIO()
    schemaless_writer(buf, SCHEMA, record)
    return b"\x00" + struct.pack(">I", schema_id) + buf.getvalue()


def test_decode_edits_reads_confluent_avro_and_uses_change_time() -> None:
    from pyspark.sql import SparkSession

    from editguard.streaming.bronze import decode_edits
    from editguard.streaming.live_job import PACKAGES

    raw = json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text())
    raw["page_change_kind"] = "delete"
    raw["dt"] = "2026-09-26T10:00:00Z"
    record = to_edit_event(raw)
    record["event_time"] = datetime(2012, 5, 12, tzinfo=UTC)  # as written before ADR 0010

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        df = spark.createDataFrame([(bytearray(confluent_bytes(record)),)], "value binary")
        # Cast to string inside Spark: Python would convert timestamps to the Mac's local zone.
        row = (
            decode_edits(df)
            .selectExpr("event_id", "page_change_kind", "cast(event_time AS STRING) AS event_time")
            .first()
        )
        assert row["event_id"] == record["event_id"]
        assert row["page_change_kind"] == "delete"
        assert row["event_time"] == "2026-09-26 10:00:00"  # from raw_json.dt, in UTC
    finally:
        spark.stop()
