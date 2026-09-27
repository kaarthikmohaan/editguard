"""Live job: Kafka edits.raw.v1 -> bronze.edits (Iceberg), deduplicated within the watermark.

Usage: uv run python -m editguard.streaming.live_job [--env dev|staging|prod]
dev writes a local Iceberg table under data/warehouse; staging and prod write to S3 through
the Glue catalog using the AWS_PROFILE from .env (SSO). Checkpoints: data/checkpoints/<env>/.
"""

import argparse
import os
import signal
import time
from datetime import UTC, datetime
from types import FrameType

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from editguard.common.config import Settings, get_settings
from editguard.common.logs import configure_logging
from editguard.streaming.bronze import BRONZE_DDL, decode_edits
from editguard.streaming.catalogs import Catalog, catalog_for
from editguard.streaming.flags import TIMESTAMP_FIELDS, FlagPublisher, flag_records, utc_rows

PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
        # Lets the AWS SDK use an SSO profile (sso-session); same SDK version as the bundle.
        "software.amazon.awssdk:ssooidc:2.44.4",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3",
        "org.apache.spark:spark-avro_2.13:4.1.3",
    ]
)
WATERMARK = "2 minutes"  # design section 8; confirmed by ADR 0009 (p99 lateness 22.6 s)


def spark_session(catalog: Catalog) -> SparkSession:
    builder = (
        SparkSession.builder.appName(f"editguard-live-job-{catalog.env}")
        .master("local[4]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.driver.memory", "3g")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in catalog.spark_conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def read_edits(
    spark: SparkSession, settings: Settings, starting: str, max_offsets: int
) -> DataFrame:
    """edits.raw.v1 decoded and deduplicated on event_id within the watermark."""
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", "edits.raw.v1")
        .option("startingOffsets", starting)
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )
    return (
        decode_edits(kafka)
        .withWatermark("event_time", WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--env", choices=["dev", "staging", "prod"], default="dev")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--max-offsets", type=int, default=50_000, help="per micro-batch")
    args = parser.parse_args()

    settings = get_settings()
    catalog = catalog_for(args.env, settings)
    table = catalog.bronze_edits
    log = configure_logging("live_job", settings.log_level).bind(env=args.env)
    if args.env != "dev" and settings.aws_profile:
        # The JVM's AWS SDK reads AWS_PROFILE from the environment, not from .env.
        os.environ.setdefault("AWS_PROFILE", settings.aws_profile)
    spark = spark_session(catalog)
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(BRONZE_DDL.format(table=table))
    trigger = f"{args.trigger_seconds} seconds"

    # Query 1: bronze. Reads Kafka from the beginning so bronze holds the full history.
    bronze_query = (
        read_edits(spark, settings, "earliest", args.max_offsets)
        .writeStream.queryName("bronze")
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime=trigger)
        .option("checkpointLocation", str(catalog.checkpoint_root / "live_job_bronze"))
        .option("fanout-enabled", "true")
        .toTable(table)
    )

    # Query 2: scoring. Starts at the newest offsets on its first run: flags are for
    # patrollers now; old edits are scored offline (replay), not flooded into the queue.
    publisher = FlagPublisher(settings)

    def score_batch(batch: DataFrame, batch_id: int) -> None:
        columns = [
            F.date_format(name, "yyyy-MM-dd'T'HH:mm:ss.SSSX").alias(name)
            if name in TIMESTAMP_FIELDS
            else F.col(name)
            for name in batch.columns
            if name != "raw_json"
        ]
        rows = utc_rows(row.asDict() for row in batch.select(*columns).collect())
        scored_at = datetime.now(UTC)
        flags = flag_records(rows, scored_at)
        publisher.publish(flags)
        if flags:
            oldest = min(flag["event_time"] for flag in flags)
            log.info(
                "flags",
                batch_id=batch_id,
                rows=len(rows),
                flagged=len(flags),
                max_latency_s=round((scored_at - oldest).total_seconds(), 1),
            )

    scoring_query = (
        read_edits(spark, settings, "latest", args.max_offsets)
        .writeStream.queryName("scoring")
        .foreachBatch(score_batch)
        .trigger(processingTime=trigger)
        .option("checkpointLocation", str(catalog.checkpoint_root / "live_job_scoring"))
        .start()
    )
    queries = [bronze_query, scoring_query]
    log.info("started", table=table, queries=[q.name for q in queries])

    stopping = False

    def stop(signum: int, _frame: FrameType | None) -> None:
        nonlocal stopping
        log.info("stop_requested", signal=signal.Signals(signum).name)
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    last_batch = {q.name: -1 for q in queries}
    while not stopping and all(q.isActive for q in queries):
        time.sleep(1)
        for q in queries:
            progress = q.lastProgress
            if progress and progress["batchId"] != last_batch[q.name]:
                last_batch[q.name] = progress["batchId"]
                if q.name == "bronze":
                    log.info(
                        "batch",
                        query=q.name,
                        batch_id=progress["batchId"],
                        input_rows=progress["numInputRows"],
                        duration_ms=progress["durationMs"].get("triggerExecution"),
                    )
    for q in queries:
        while q.isActive and q.status["isTriggerActive"]:  # stop between batches (ADR 0009)
            time.sleep(0.2)
        q.stop()
    failed = [q.name for q in queries if q.exception() is not None]
    log.info("stopped", failed=failed)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
