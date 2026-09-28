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


def schema_id(value: Column) -> Column:
    """The 4-byte schema ID after the magic byte of a Confluent-framed message."""
    return F.conv(F.hex(F.substring(value, 2, 4)), 16, 10).cast("int")


def decode_edits(kafka_df: DataFrame, writer_schemas: dict[int, str] | None = None) -> DataFrame:
    """Kafka rows (binary value) -> one row per edit with the contract's columns.

    writer_schemas maps each Schema Registry ID to the schema a message was written with.
    Every message is read with its own writer schema and resolved to the current contract
    (EDITS_SCHEMA), so messages written before and after a compatible contract change decode
    correctly side by side. An unknown ID fails the query rather than misreading bytes.
    Without writer_schemas, every message is assumed to use the current contract (tests).
    """
    body = F.expr(f"substring(value, {CONFLUENT_HEADER_BYTES + 1}, length(value))")
    if writer_schemas:
        ids = schema_id(F.col("value"))
        decoded = F.raise_error(
            F.concat(F.lit("no writer schema for Schema Registry id "), ids.cast("string"))
        )
        options = {"avroSchema": EDITS_SCHEMA}
        for sid, writer in sorted(writer_schemas.items()):
            decoded = F.when(ids == sid, from_avro(body, writer, options)).otherwise(decoded)
    else:
        decoded = from_avro(body, EDITS_SCHEMA)
    edits = kafka_df.select(decoded.alias("e")).select("e.*")
    return edits.withColumn("event_time", change_time(F.col("raw_json")))


def registered_schemas(registry_url: str, subject: str) -> dict[int, str]:
    """Every version registered for a subject, as {schema id: schema text}."""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    client = SchemaRegistryClient({"url": registry_url})
    return {
        version.schema_id: version.schema.schema_str
        for version in (client.get_version(subject, v) for v in client.get_versions(subject))
    }


def decode_baseline(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows from baseline.raw.v1 -> bronze.baseline_scores rows (ADR 0011)."""
    body = F.expr(f"substring(value, {CONFLUENT_HEADER_BYTES + 1}, length(value))")
    return kafka_df.select(
        from_avro(body, BASELINE_SCHEMA).alias("b"),
        F.col("timestamp").alias("ingested_at"),
        F.col("partition").alias("upstream_partition"),
        F.col("offset").alias("upstream_offset"),
    ).select("b.*", "ingested_at", "upstream_partition", "upstream_offset")
