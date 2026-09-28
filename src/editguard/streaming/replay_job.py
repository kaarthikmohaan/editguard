"""Replay job: Kafka edits.replay.v1 -> bronze.edits_replay (Iceberg), then stop.

Usage: uv run python -m editguard.streaming.replay_job --env dev|staging|prod
Runs as a bounded batch (trigger availableNow): it appends everything the replay producer has
sent since its last run and exits. It has its own checkpoint and table, and no watermark:
replayed events are hours or days old and would be dropped by the live job's. Duplicates
(with live data or earlier replays) are removed by the silver MERGE on event_id.
"""

import argparse
import os

from editguard.common.config import get_settings
from editguard.common.logs import configure_logging
from editguard.producer.replay import REPLAY_TOPIC
from editguard.streaming.bronze import (
    BRONZE_DDL,
    decode_edits,
    metadata_cleanup_sql,
    registered_schemas,
)
from editguard.streaming.catalogs import catalog_for
from editguard.streaming.live_job import read_topic, spark_session


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--env", choices=["dev", "staging", "prod"], default="dev")
    parser.add_argument("--topic", default=REPLAY_TOPIC)
    parser.add_argument("--max-offsets", type=int, default=50_000, help="per micro-batch")
    args = parser.parse_args()

    settings = get_settings()
    catalog = catalog_for(args.env, settings)
    table = catalog.bronze_replay
    log = configure_logging("replay_job", settings.log_level).bind(env=args.env, table=table)
    if args.env != "dev" and settings.aws_profile:
        os.environ.setdefault("AWS_PROFILE", settings.aws_profile)
    spark = spark_session(catalog)
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(BRONZE_DDL.format(table=table))
    spark.sql(metadata_cleanup_sql(table))

    writers = registered_schemas(settings.schema_registry_url, f"{args.topic}-value")
    log.info("writer_schemas", ids=sorted(writers))
    # One checkpoint per topic, so a test topic never moves the real replay's position.
    checkpoint = "replay_job" if args.topic == REPLAY_TOPIC else f"replay_job_{args.topic}"
    topic = read_topic(spark, settings, args.topic, "earliest", args.max_offsets)
    query = (
        decode_edits(topic, writers)
        .writeStream.queryName("replay")
        .format("iceberg")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", str(catalog.checkpoint_root / checkpoint))
        .option("fanout-enabled", "true")
        .toTable(table)
    )
    log.info("started", topic=args.topic)
    query.awaitTermination()
    rows = sum(p["numInputRows"] for p in query.recentProgress)
    log.info("finished", batches=len(query.recentProgress), rows=rows)
    spark.stop()


if __name__ == "__main__":
    main()
