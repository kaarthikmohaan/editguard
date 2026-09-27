"""Decode Kafka messages into bronze.edits rows. Shared by the live and replay jobs."""

from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

CONTRACTS = Path(__file__).parents[3] / "contracts/generated"
EDITS_SCHEMA = (CONTRACTS / "edits.avsc").read_text()
BASELINE_SCHEMA = (CONTRACTS / "baseline_scores.avsc").read_text()

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

# ADR 0011: Wikimedia's revert-risk scores, landed from baseline.raw.v1. The record has no time of
# its own, so ingested_at is the Kafka message timestamp (when the producer sent it).
BASELINE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  wiki_id STRING, rev_id BIGINT, model_name STRING, model_version STRING,
  probability_true DOUBLE, ingested_at TIMESTAMP,
  upstream_partition INT, upstream_offset BIGINT
) USING iceberg
PARTITIONED BY (days(ingested_at), wiki_id)
TBLPROPERTIES ('format-version' = '2')
"""
BASELINE_KEY = ["wiki_id", "rev_id", "model_name", "model_version"]

# Every commit writes a new metadata.json listing all snapshots, so keeping every old one
# grows storage quadratically (2 GB after 1,858 commits). Keep the newest versions only.
METADATA_VERSIONS_KEPT = 100


def metadata_cleanup_sql(table: str, keep: int = METADATA_VERSIONS_KEPT) -> str:
    """Make Iceberg delete old metadata.json files after each commit. Idempotent."""
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES ("
        "'write.metadata.delete-after-commit.enabled' = 'true', "
        f"'write.metadata.previous-versions-max' = '{keep}')"
    )


def change_time(raw_json: Column) -> Column:
    """ADR 0010: event_time is the event's top-level dt, read from raw_json so that
    messages written before the change are corrected too."""
    return F.to_timestamp(F.get_json_object(raw_json, "$.dt"))


def decode_edits(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows (binary value) -> one row per edit with the contract's columns."""
    body = F.expr(f"substring(value, {CONFLUENT_HEADER_BYTES + 1}, length(value))")
    edits = kafka_df.select(from_avro(body, EDITS_SCHEMA).alias("e")).select("e.*")
    return edits.withColumn("event_time", change_time(F.col("raw_json")))


def decode_baseline(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows from baseline.raw.v1 -> bronze.baseline_scores rows (ADR 0011)."""
    body = F.expr(f"substring(value, {CONFLUENT_HEADER_BYTES + 1}, length(value))")
    return kafka_df.select(
        from_avro(body, BASELINE_SCHEMA).alias("b"),
        F.col("timestamp").alias("ingested_at"),
        F.col("partition").alias("upstream_partition"),
        F.col("offset").alias("upstream_offset"),
    ).select("b.*", "ingested_at", "upstream_partition", "upstream_offset")
