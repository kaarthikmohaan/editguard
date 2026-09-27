"""Live job: Kafka edits.raw.v1 -> bronze.edits (Iceberg), deduplicated within the watermark.

Usage: uv run python -m editguard.streaming.live_job [--trigger-seconds 10]
Dev writes to a local Iceberg catalog under data/warehouse (gitignored).
"""

import argparse
import signal
import time
from pathlib import Path
from types import FrameType

from pyspark.sql import SparkSession

from editguard.common.config import get_settings
from editguard.common.logs import configure_logging
from editguard.streaming.bronze import BRONZE_DDL, decode_edits

PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3",
        "org.apache.spark:spark-avro_2.13:4.1.3",
    ]
)
TABLE = "local.bronze.edits"
WAREHOUSE = Path("data/warehouse")
CHECKPOINT = Path("data/checkpoints/live_job_bronze")
WATERMARK = "2 minutes"  # design section 8; confirmed by ADR 0009 (p99 lateness 22.6 s)


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("editguard-live-job")
        .master("local[4]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.driver.memory", "3g")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(WAREHOUSE.resolve()))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--max-offsets", type=int, default=50_000, help="per micro-batch")
    args = parser.parse_args()

    settings = get_settings()
    log = configure_logging("live_job", settings.log_level)
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(BRONZE_DDL.format(table=TABLE))

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", "edits.raw.v1")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )
    bronze = (
        decode_edits(kafka)
        .withWatermark("event_time", WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    query = (
        bronze.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .option("checkpointLocation", str(CHECKPOINT))
        .option("fanout-enabled", "true")
        .toTable(TABLE)
    )
    log.info("started", table=TABLE, query_id=str(query.id))

    stopping = False

    def stop(signum: int, _frame: FrameType | None) -> None:
        nonlocal stopping
        log.info("stop_requested", signal=signal.Signals(signum).name)
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    last_batch = -1
    while not stopping and query.isActive:
        time.sleep(1)
        progress = query.lastProgress
        if progress and progress["batchId"] != last_batch:
            last_batch = progress["batchId"]
            log.info(
                "batch",
                batch_id=last_batch,
                input_rows=progress["numInputRows"],
                rows_per_s=round(progress.get("processedRowsPerSecond", 0.0), 1),
                duration_ms=progress["durationMs"].get("triggerExecution"),
            )
    while query.status["isTriggerActive"]:  # stop only between batches (ADR 0009)
        time.sleep(0.2)
    query.stop()
    log.info("stopped", batches=last_batch + 1)


if __name__ == "__main__":
    main()
