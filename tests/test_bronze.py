import io
import json
import struct
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastavro import parse_schema, schemaless_writer

from editguard.producer.parse import to_edit_event

pytestmark = pytest.mark.spark

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


def test_metadata_cleanup_keeps_only_recent_metadata_files(tmp_path: Path) -> None:
    from pyspark.sql import SparkSession

    from editguard.streaming.bronze import BRONZE_DDL, metadata_cleanup_sql
    from editguard.streaming.live_job import PACKAGES

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.catalog.t", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.t.type", "hadoop")
        .config("spark.sql.catalog.t.warehouse", str(tmp_path))
        .getOrCreate()
    )
    try:
        spark.sql(BRONZE_DDL.format(table="t.bronze.edits"))
        spark.sql(metadata_cleanup_sql("t.bronze.edits", keep=2))
        for i in range(5):  # 5 commits, as the live job makes one per batch
            spark.sql(
                "INSERT INTO t.bronze.edits (event_id, event_time, wiki_id) "
                "VALUES (:id, timestamp '2026-09-27 10:00:00', 'enwiki')",
                args={"id": f"e{i}"},
            )
        kept = list((tmp_path / "bronze/edits/metadata").glob("*.metadata.json"))
        assert len(kept) == 3  # the current version plus the 2 previous ones
    finally:
        spark.stop()


def test_decode_baseline_takes_time_from_kafka_timestamp() -> None:
    from pyspark.sql import SparkSession

    from editguard.streaming.bronze import decode_baseline
    from editguard.streaming.live_job import PACKAGES

    schema = parse_schema(
        json.loads((ROOT / "contracts/generated/baseline_scores.avsc").read_text())
    )
    buf = io.BytesIO()
    record = {
        "wiki_id": "enwiki",
        "rev_id": 42,
        "model_name": "revertrisk-language-agnostic",
        "model_version": "3",
        "probability_true": 0.91,
    }
    schemaless_writer(buf, schema, record)
    value = b"\x00" + struct.pack(">I", 9) + buf.getvalue()

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        df = spark.createDataFrame(
            # tz-aware: a naive datetime would be read as the Mac's local time (IST).
            [(bytearray(value), datetime(2026, 9, 27, 10, 0, tzinfo=UTC), 2, 1234)],
            "value binary, timestamp timestamp, partition int, offset long",
        )
        row = (
            decode_baseline(df)
            .selectExpr("*", "cast(ingested_at AS STRING) AS ingested_at_utc")
            .first()
        )
        assert (row["wiki_id"], row["rev_id"], row["probability_true"]) == ("enwiki", 42, 0.91)
        assert row["ingested_at_utc"] == "2026-09-27 10:00:00"
        assert (row["upstream_partition"], row["upstream_offset"]) == (2, 1234)
    finally:
        spark.stop()


def test_decode_edits_reads_each_message_with_its_writer_schema() -> None:
    """Contract 1.2.0 made rev_size optional. Kafka then holds messages written with both
    schema versions; each must be read with its own schema, and an unknown one must fail."""
    from pyspark.sql import SparkSession

    from editguard.streaming.bronze import decode_edits
    from editguard.streaming.live_job import PACKAGES

    new_json = json.loads((ROOT / "contracts/generated/edits.avsc").read_text())
    old_json = json.loads(json.dumps(new_json))
    for field in old_json["fields"]:
        if field["name"] == "rev_size":
            field["type"] = "long"  # contract 1.1.0: required
    old_schema, new_schema = parse_schema(old_json), parse_schema(new_json)

    def framed(record: dict[str, Any], schema: Any, schema_id: int) -> bytearray:
        buf = io.BytesIO()
        schemaless_writer(buf, schema, record)
        return bytearray(b"\x00" + struct.pack(">I", schema_id) + buf.getvalue())

    raw = json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text())
    old_record = to_edit_event(raw)
    new_record = {**to_edit_event(raw), "event_id": "new-1", "rev_size": None}

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        writers = {1: json.dumps(old_json), 2: json.dumps(new_json)}
        df = spark.createDataFrame(
            [(framed(old_record, old_schema, 1),), (framed(new_record, new_schema, 2),)],
            "value binary",
        )
        rows = {
            r["event_id"]: r
            for r in decode_edits(df, writers).select("event_id", "rev_size", "wiki_id").collect()
        }
        assert rows[old_record["event_id"]]["rev_size"] == old_record["rev_size"]
        assert rows["new-1"]["rev_size"] is None
        assert {r["wiki_id"] for r in rows.values()} == {
            old_record["wiki_id"]
        }  # later fields intact

        unknown = spark.createDataFrame([(framed(new_record, new_schema, 3),)], "value binary")
        with pytest.raises(Exception, match="no writer schema for Schema Registry id 3"):
            decode_edits(unknown, writers).collect()
    finally:
        spark.stop()
