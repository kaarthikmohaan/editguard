"""T-SKEW-01: the live path (Kafka bytes -> Spark -> scoring_rows) and the offline path (the
parsed record in Python) produce identical features for the same edits."""

import io
import json
import struct
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from fastavro import parse_schema, schemaless_writer

from editguard.features.features import compute_features
from editguard.producer.parse import to_edit_event
from editguard.streaming.flags import flag_records

pytestmark = pytest.mark.spark

ROOT = Path(__file__).parents[1]
SCHEMA_JSON = (ROOT / "contracts/generated/edits.avsc").read_text()


def variants() -> list[dict[str, Any]]:
    """Edits that exercise every feature: timestamps near midnight UTC (the IST trap), a temp
    account, unknown registration, a large removal, a revert, no summary, hidden summary."""
    raw = json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text())
    base = to_edit_event(raw)
    # Bronze takes event_time from raw_json's dt (ADR 0010), so a variant with another time
    # must be parsed from an event with that dt, exactly as the producer would.
    late = to_edit_event({**raw, "dt": "2026-09-26T23:50:05Z"})
    return [
        base,
        {
            **late,
            "event_id": "v1",
            "performer_registration_dt": late["event_time"] - timedelta(hours=3),
        },
        {
            **base,
            "event_id": "v2",
            "performer_is_temp": True,
            "performer_registration_dt": None,
            "performer_edit_count": None,
            "comment": None,
        },
        {**base, "event_id": "v3", "rev_size": 1_000, "prior_rev_size": 20_000},
        {**base, "event_id": "v4", "revert_method": "undo", "comment": "   "},
        {
            **base,
            "event_id": "v5",
            "is_comment_visible": False,
            "comment": None,
            "performer_groups": ["*", "user", "rollbacker"],
        },
        {**base, "event_id": "v6", "rev_size": None},
        {  # built to be flagged: temporary account, no summary, most of the page removed
            **late,
            "event_id": "v7",
            "revert_method": None,
            "performer_is_temp": True,
            "performer_groups": ["*"],
            "performer_registration_dt": None,
            "performer_edit_count": 0,
            "comment": None,
            "rev_size": 500,
            "prior_rev_size": 9_000,
        },
    ]


def test_live_and_offline_features_are_identical() -> None:
    from pyspark.sql import SparkSession

    from editguard.streaming.bronze import decode_edits
    from editguard.streaming.live_job import PACKAGES, scoring_rows

    records = variants()
    schema = parse_schema(json.loads(SCHEMA_JSON))

    def framed(record: dict[str, Any]) -> bytearray:
        buf = io.BytesIO()
        schemaless_writer(buf, schema, record)
        return bytearray(b"\x00" + struct.pack(">I", 1) + buf.getvalue())

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        kafka = spark.createDataFrame([(framed(r),) for r in records], "value binary")
        live_rows = scoring_rows(decode_edits(kafka))
    finally:
        spark.stop()

    # Offline: the same records as Python parsed them.
    live = {row["event_id"]: compute_features(row) for row in live_rows}
    offline = {r["event_id"]: compute_features(r) for r in records}
    assert live.keys() == offline.keys()
    for event_id, features in offline.items():
        assert live[event_id] == features, event_id

    # The flags themselves (score, reasons, event_time) must match too: a timestamp shifted to
    # local time would leave the features equal but publish the wrong event_time.
    scored_at = records[0]["ingested_at"]
    live_flags, offline_flags = flag_records(live_rows, scored_at), flag_records(records, scored_at)
    assert live_flags, "the variants must include at least one flagged edit"
    assert live_flags == offline_flags
