"""Day-1 test: Athena OPTIMIZE while Spark appends to the same Iceberg table (design 13).

Local Spark streams small appends every few seconds into stg_bronze.day1_concurrency_probe
(Glue catalog, staging S3) while Athena runs OPTIMIZE on it N times. Pass: every OPTIMIZE
succeeds, the stream never fails, and Athena counts exactly the rows Spark wrote.
Fallback if it fails: compact only while streaming is paused.

Usage (staging only; uses temporary SSO credentials):
  eval "$(aws configure export-credentials --profile editguard-dev --format env)"
  uv run --group transform python scripts/day1/optimize_concurrency.py [--runs 10]
"""

import argparse
import time
from pathlib import Path

import boto3
from pyspark.sql import SparkSession

REGION = "ap-south-1"
WORKGROUP = "editguard-stg"
TABLE = "stg_bronze.day1_concurrency_probe"
PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
    ]
)
CHECKPOINT = Path("data/day1/checkpoints/optimize_concurrency")


def glue_spark(bucket: str) -> SparkSession:
    """Spark with an Iceberg catalog `glue` backed by AWS Glue and S3 (staging)."""
    return (
        SparkSession.builder.appName("optimize-concurrency")
        .master("local[2]")
        .config("spark.jars.packages", PACKAGES)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue.warehouse", f"s3://{bucket}/stg/bronze/")
        .config("spark.sql.catalog.glue.client.region", REGION)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def athena(sql: str) -> tuple[str, str, float]:
    """Run one Athena statement in the staging workgroup. Returns (state, reason, seconds)."""
    client = boto3.client("athena", region_name=REGION)
    qid = client.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
    start = time.monotonic()
    while True:
        status = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            return status["State"], status.get("StateChangeReason", ""), time.monotonic() - start
        time.sleep(1)


def athena_count() -> int:
    client = boto3.client("athena", region_name=REGION)
    state, reason, _ = athena(f"SELECT count(*) FROM {TABLE}")  # noqa: S608 - constant
    if state != "SUCCEEDED":
        raise RuntimeError(reason)
    qid = client.list_query_executions(WorkGroup=WORKGROUP, MaxResults=1)["QueryExecutionIds"][0]
    rows = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return int(rows[1]["Data"][0]["VarCharValue"])


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--drop", action="store_true", help="drop the probe table and exit")
    args = parser.parse_args()

    account = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
    bucket = f"editguard-data-{account}-{REGION}"
    spark = glue_spark(bucket)
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"DROP TABLE IF EXISTS glue.{TABLE} PURGE")
    if args.drop:
        print(f"dropped {TABLE}")
        return
    spark.sql(
        f"CREATE TABLE glue.{TABLE} (id BIGINT, ts TIMESTAMP, payload STRING) USING iceberg "
        "PARTITIONED BY (days(ts)) TBLPROPERTIES ('format-version'='2')"
    )

    for path in sorted(CHECKPOINT.rglob("*"), reverse=True):
        path.unlink() if path.is_file() else path.rmdir()
    stream = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 20)
        .load()
        .selectExpr("value AS id", "timestamp AS ts", "repeat('x', 200) AS payload")
        .writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="3 seconds")
        .option("checkpointLocation", str(CHECKPOINT))
        .toTable(f"glue.{TABLE}")
    )
    time.sleep(20)  # let a few small files pile up

    results = []
    for run in range(1, args.runs + 1):
        state, reason, seconds = athena(f"OPTIMIZE {TABLE} REWRITE DATA USING BIN_PACK")
        results.append(state)
        print(
            f"OPTIMIZE {run}/{args.runs}: {state} in {seconds:.0f} s {reason}".rstrip(), flush=True
        )
        if stream.exception() is not None:
            break
        time.sleep(5)

    stream_error = stream.exception()
    while stream.status["isTriggerActive"]:  # stop between batches, never mid-commit
        time.sleep(0.2)
    stream.stop()
    table = spark.table(f"glue.{TABLE}")
    written = table.count()
    operations = {
        row["operation"]: row["n"]
        for row in spark.table(f"glue.{TABLE}.snapshots")
        .groupBy("operation")
        .count()
        .withColumnRenamed("count", "n")
        .collect()
    }
    counted = athena_count()
    spark.stop()

    ok = results.count("SUCCEEDED")
    print(f"stream error: {stream_error}")
    print(f"snapshots by operation: {operations} (append = Spark batches, replace = OPTIMIZE)")
    print(f"OPTIMIZE succeeded {ok}/{args.runs}; rows via Spark {written}, via Athena {counted}")
    passed = ok == args.runs and stream_error is None and written == counted
    print("PASS" if passed else "FAIL")


if __name__ == "__main__":
    main()
