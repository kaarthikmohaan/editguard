"""make deploy's gate: prod approval from GitHub deployments and image digests from the release."""

import pytest

from editguard.tools.deploy import DeployRefused, check_checkout, image_refs, prod_approved

DIGEST = "sha256:" + "a" * 64
NOTES = (
    f"producer: `ghcr.io/kaarthikmohaan/editguard-producer@{DIGEST}`\n"
    f"spark: `ghcr.io/kaarthikmohaan/editguard-spark@{DIGEST}`\n"
    "\nDeploy to prod after approval: `make deploy VERSION=v0.2.0`\n"
)


def status(state: str) -> dict:
    return {"state": state}


def test_prod_approved_when_the_newest_deployment_succeeded():
    deployments = [{"id": 1, "created_at": "2026-09-28T10:00:00Z"}]
    assert prod_approved(deployments, {1: [status("success"), status("in_progress")]})


def test_not_approved_without_a_prod_deployment():
    assert not prod_approved([], {})


@pytest.mark.parametrize("newest", ["waiting", "in_progress", "failure", "error"])
def test_not_approved_until_the_prod_job_has_succeeded(newest):
    deployments = [{"id": 1, "created_at": "2026-09-28T10:00:00Z"}]
    assert not prod_approved(deployments, {1: [status(newest), status("queued")]})


def test_a_rerun_that_failed_after_an_earlier_success_is_not_approved():
    deployments = [
        {"id": 1, "created_at": "2026-09-28T10:00:00Z"},
        {"id": 2, "created_at": "2026-09-28T11:00:00Z"},
    ]
    statuses = {1: [status("success")], 2: [status("failure")]}
    assert not prod_approved(deployments, statuses)


def test_image_refs_are_read_pinned_by_digest():
    assert image_refs(NOTES) == {
        "producer": f"ghcr.io/kaarthikmohaan/editguard-producer@{DIGEST}",
        "spark": f"ghcr.io/kaarthikmohaan/editguard-spark@{DIGEST}",
    }


def test_image_refs_refuse_tags_without_a_digest():
    notes = NOTES.replace(f"editguard-spark@{DIGEST}", "editguard-spark:v0.2.0")
    with pytest.raises(DeployRefused, match="both image digests"):
        image_refs(notes)


@pytest.mark.parametrize("version", ["0.2.0", "v0.2", "main", "v0.2.0; rm -rf /"])
def test_only_version_tags_are_accepted(version):
    with pytest.raises(DeployRefused, match="not a version tag"):
        check_checkout(version)
