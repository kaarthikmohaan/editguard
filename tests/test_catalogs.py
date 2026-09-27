from pathlib import Path

import pytest

from editguard.common.config import Settings
from editguard.streaming.catalogs import catalog_for


def settings(monkeypatch: pytest.MonkeyPatch, bucket: str | None = "editguard-data-x") -> Settings:
    monkeypatch.setenv("CONTACT_EMAIL", "dev@example.com")
    if bucket:
        monkeypatch.setenv("DATA_BUCKET", bucket)
    else:
        monkeypatch.delenv("DATA_BUCKET", raising=False)
    return Settings(_env_file=None)


def test_dev_is_local_and_keeps_its_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = catalog_for("dev", settings(monkeypatch, bucket=None))
    assert catalog.bronze_edits == "local.bronze.edits"
    assert catalog.bronze_baseline == "local.bronze.baseline_scores"
    assert catalog.checkpoint_root == Path("data/checkpoints")


def test_prod_uses_glue_prefix_and_skip_archive(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = catalog_for("prod", settings(monkeypatch))
    assert catalog.bronze_edits == "glue.prod_bronze.edits"
    assert catalog.spark_conf["spark.sql.catalog.glue.warehouse"] == "s3://editguard-data-x/prod/"
    assert catalog.spark_conf["spark.sql.catalog.glue.glue.skip-archive"] == "true"
    assert catalog.checkpoint_root == Path("data/checkpoints/prod")
    assert catalog.bronze_baseline == "glue.prod_bronze.baseline_scores"
    assert catalog.bronze_replay == "glue.prod_bronze.edits_replay"


def test_staging_uses_stg_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    assert catalog_for("staging", settings(monkeypatch)).bronze_edits == "glue.stg_bronze.edits"


def test_cloud_env_needs_a_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="DATA_BUCKET"):
        catalog_for("prod", settings(monkeypatch, bucket=None))
