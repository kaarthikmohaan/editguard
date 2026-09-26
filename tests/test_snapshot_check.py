import os
import shutil
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("java") is None or os.environ.get("EDITGUARD_SPARK_TESTS") != "1",
    reason="Spark test: set EDITGUARD_SPARK_TESTS=1 (needs Java 17 and the Iceberg jar)",
)


def test_current_snapshot_id_reads_latest_commit(tmp_path: Path) -> None:
    from editguard.tools.snapshot_check import current_snapshot_id
    from editguard.tools.spark_local import local_spark

    spark = local_spark("test", tmp_path / "wh")
    try:
        spark.sql("CREATE TABLE local.t.x (a INT) USING iceberg")
        spark.sql("INSERT INTO local.t.x VALUES (1)")
        first = current_snapshot_id(spark, "local.t.x")
        spark.sql("INSERT INTO local.t.x VALUES (2)")
        assert current_snapshot_id(spark, "local.t.x") != first
    finally:
        spark.stop()
