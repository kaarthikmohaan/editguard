"""Decode Kafka messages into bronze.edits rows. Shared by the live and replay jobs."""

from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

EDITS_SCHEMA = (Path(__file__).parents[3] / "contracts/generated/edits.avsc").read_text()

# Confluent wire format: 1 magic byte + 4-byte schema ID, then the Avro body.
CONFLUENT_HEADER_BYTES = 5

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  event_id STRING, event_time TIMESTAMP, emitted_at TIMESTAMP, ingested_at TIMESTAMP,
  wiki_id STRING, page_id BIGINT, page_title STRING, namespace_id INT,
  page_change_kind STRING, rev_id BIGINT, rev_parent_id BIGINT, rev_size BIGINT,
  prior_rev_size BIGINT, is_minor_edit BOOLEAN, comment STRING,
  is_content_visible BOOLEAN, is_comment_visible BOOLEAN, is_editor_visible BOOLEAN,
  performer_user_text STRING, performer_is_bot BOOLEAN, performer_is_temp BOOLEAN,
  performer_groups ARRAY<STRING>, performer_edit_count BIGINT,
  performer_registration_dt TIMESTAMP, revert_method STRING,
  rev_reverted_oldest_id BIGINT, rev_reverted_newest_id BIGINT, rev_original_id BIGINT,
  upstream_topic STRING, upstream_partition INT, upstream_offset BIGINT,
  schema_version STRING, raw_json STRING
) USING iceberg
PARTITIONED BY (days(event_time), wiki_id)
TBLPROPERTIES ('format-version' = '2')
"""


def change_time(raw_json: Column) -> Column:
    """ADR 0010: event_time is the event's top-level dt, read from raw_json so that
    messages written before the change are corrected too."""
    return F.to_timestamp(F.get_json_object(raw_json, "$.dt"))


def decode_edits(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows (binary value) -> one row per edit with the contract's columns."""
    body = F.expr(f"substring(value, {CONFLUENT_HEADER_BYTES + 1}, length(value))")
    edits = kafka_df.select(from_avro(body, EDITS_SCHEMA).alias("e")).select("e.*")
    return edits.withColumn("event_time", change_time(F.col("raw_json")))
