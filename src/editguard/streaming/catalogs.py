"""Where each environment's Iceberg tables live (design section 10).

dev: a local Hadoop catalog under data/warehouse, checkpoints under data/checkpoints/.
staging/prod: the AWS Glue catalog, data in s3://<bucket>/<prefix>/, databases <prefix>_bronze
etc. (created by infra/terraform/aws/env), checkpoints under data/checkpoints/<env>/ so
environments never share progress.
"""

from dataclasses import dataclass
from pathlib import Path

from editguard.common.config import Settings

LOCAL_WAREHOUSE = Path("data/warehouse")
PREFIXES = {"staging": "stg", "prod": "prod"}


@dataclass(frozen=True)
class Catalog:
    """Spark settings and table names for one environment."""

    env: str
    spark_conf: dict[str, str]
    bronze_edits: str
    checkpoint_root: Path

    @property
    def bronze_baseline(self) -> str:
        """bronze.baseline_scores in the same catalog and database (ADR 0011)."""
        return self.bronze_edits.removesuffix(".edits") + ".baseline_scores"


def catalog_for(env: str, settings: Settings) -> Catalog:
    """Build the catalog settings for dev, staging or prod."""
    if env == "dev":
        conf = {
            "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type": "hadoop",
            "spark.sql.catalog.local.warehouse": str(LOCAL_WAREHOUSE.resolve()),
        }
        return Catalog(env, conf, "local.bronze.edits", Path("data/checkpoints"))
    if env not in PREFIXES:
        raise ValueError(f"unknown env {env!r}; use dev, staging or prod")
    if not settings.data_bucket:
        raise ValueError("DATA_BUCKET must be set in .env for staging and prod")
    prefix = PREFIXES[env]
    conf = {
        "spark.sql.catalog.glue": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.glue.warehouse": f"s3://{settings.data_bucket}/{prefix}/",
        "spark.sql.catalog.glue.client.region": settings.aws_region,
        "spark.sql.catalog.glue.glue.skip-archive": "true",  # ADR 0008
    }
    return Catalog(env, conf, f"glue.{prefix}_bronze.edits", Path("data/checkpoints") / env)
