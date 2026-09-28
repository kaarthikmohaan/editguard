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


@pytest.fixture(autouse=True)
def no_aws_outside_e2e(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Only the e2e layer may reach AWS. Hide the laptop's AWS login from every other test, so
    a test that quietly needs AWS fails here as it would in CI (which has no AWS profile)."""
    if request.node.get_closest_marker("e2e") is None:
        monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
        monkeypatch.delenv("AWS_PROFILE", raising=False)
