"""Day-1 test: does a running foreachBatch see a new snapshot of the history table?

The live job reads user_history_asof_hour inside foreachBatch, while a different writer
(dbt-athena, hourly) replaces it. Iceberg's Spark catalog caches table metadata, so a
long-running stream might keep reading the old snapshot. Pass: the snapshot ID seen
inside foreachBatch changes after an external writer commits.

Usage: uv run python -m editguard.tools.snapshot_check [--seconds 90] [--refresh]
--refresh runs REFRESH TABLE at the start of each batch (the candidate fix).
Writes only under data/day1/ (gitignored).
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import desc

from editguard.tools.spark_local import local_spark

WAREHOUSE = Path("data/day1/warehouse")
TABLE = "local.history.user_history_asof_hour"


def current_snapshot_id(spark: SparkSession, table: str) -> int:
    """The table's current snapshot ID as the session sees it right now."""
    row = spark.table(f"{table}.snapshots").orderBy(desc("committed_at")).first()
    return row["snapshot_id"]


def external_append() -> None:
    """Run as a separate process: a different writer adds one row, like the hourly refresh."""
    spark = local_spark("external-writer", WAREHOUSE)
    spark.sql(
        "INSERT INTO local.history.user_history_asof_hour "
        "VALUES ('hash-b', TIMESTAMP '2026-09-26 01:00:00', 3)"
    )
    print(f"external writer committed snapshot {current_snapshot_id(spark, TABLE)}", flush=True)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--seconds", type=int, default=90)
    parser.add_argument("--refresh", action="store_true", help="REFRESH TABLE in each batch")
    parser.add_argument("--external-append", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.external_append:
        external_append()
        return

    spark = local_spark("snapshot-check", WAREHOUSE)
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("DROP TABLE IF EXISTS local.history.user_history_asof_hour")
    spark.sql(
        "CREATE TABLE local.history.user_history_asof_hour "
        "(user_hash STRING, hour TIMESTAMP, reverts INT) USING iceberg"
    )
    spark.sql(
        "INSERT INTO local.history.user_history_asof_hour "
        "VALUES ('hash-a', TIMESTAMP '2026-09-26 00:00:00', 1)"
    )
    first = current_snapshot_id(spark, TABLE)
    print(f"initial snapshot {first}", flush=True)

    seen: list[tuple[int, int, int]] = []  # (batch, snapshot, rows)

    def each_batch(batch: DataFrame, batch_id: int) -> None:
        if args.refresh:
            batch.sparkSession.catalog.refreshTable(TABLE)
        history = batch.sparkSession.table(TABLE)  # what the live job would read
        snapshot = current_snapshot_id(batch.sparkSession, TABLE)
        seen.append((batch_id, snapshot, history.count()))
        print(f"batch {batch_id}: snapshot {snapshot}, rows {history.count()}", flush=True)

    stream = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 1)
        .load()
        .writeStream.foreachBatch(each_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )
    time.sleep(15)
    subprocess.run(  # noqa: S603 - fixed arguments, our own module
        [sys.executable, "-m", "editguard.tools.snapshot_check", "--external-append"],
        check=True,
    )
    time.sleep(args.seconds)
    stream.stop()
    spark.stop()

    mode = "REFRESH TABLE per batch" if args.refresh else "default catalog caching"
    after = [s for s in seen if s[1] != first]
    if after:
        batch, _, rows = after[0]
        print(f"PASS ({mode}): new snapshot seen from batch {batch} ({rows} rows)")
    else:
        print(f"FAIL ({mode}): foreachBatch never saw the external writer's snapshot")


if __name__ == "__main__":
    main()
