"""Test layers: a test is `unit` unless its module is marked with another layer."""

import shutil

import pytest

LAYERS = {"dbt", "spark", "integration", "e2e"}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        layers = {m.name for m in item.iter_markers()} & LAYERS
        if not layers:
            item.add_marker(pytest.mark.unit)
        if "spark" in layers and shutil.which("java") is None:
            item.add_marker(
                pytest.mark.skip(reason="Spark tests need Java 17 (brew install openjdk@17)")
            )
