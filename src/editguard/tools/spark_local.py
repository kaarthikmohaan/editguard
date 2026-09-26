"""Local Spark session with an Iceberg catalog on disk (dev environment: no AWS)."""

from pathlib import Path

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def local_spark(app: str, warehouse: Path) -> SparkSession:
    """Spark with a Hadoop-type Iceberg catalog named `local` rooted at `warehouse`."""
    return (
        SparkSession.builder.appName(app)
        .master("local[2]")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(warehouse.resolve()))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
