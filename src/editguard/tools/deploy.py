"""Check that a release may go to prod, and pin its images (runbook section 2; ADR 0012).

Usage: uv run python -m editguard.tools.deploy vX.Y.Z   (make deploy VERSION=vX.Y.Z calls this)
Refuses unless the working copy is exactly the tag and GitHub shows the release's `prod`
environment job finished successfully, which happens only after a reviewer approves it. Then
writes .deploy.env with the image digests from the GitHub release, which the Makefile passes
to docker compose, so prod runs exactly the images the smoke test ran on staging.
Reads GitHub's public API without a token.
"""

import re
import subprocess
import sys
from pathlib import Path

import httpx

REPO = "kaarthikmohaan/editguard"
API = f"https://api.github.com/repos/{REPO}"
DEPLOY_ENV = Path(".deploy.env")
IMAGE_LINE = re.compile(r"^(producer|spark): `(ghcr\.io/[^`@]+@sha256:[0-9a-f]{64})`$", re.M)
VERSION = re.compile(r"^v\d+\.\d+\.\d+$")


class DeployRefused(Exception):
    """A precondition for deploying to prod is not met."""


def git(*args: str) -> str:
    # Fixed git subcommands; the only variable part is the version, checked against VERSION.
    result = subprocess.run(  # noqa: S603
        ["git", *args],  # noqa: S607 - git from PATH, as in every make target
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def check_checkout(version: str) -> str:
    """The working copy is exactly the tagged commit, with no local changes; returns its SHA."""
    if not VERSION.match(version):
        raise DeployRefused(f"{version!r} is not a version tag like v0.2.0")
    try:
        tagged = git("rev-parse", f"{version}^{{commit}}")
    except subprocess.CalledProcessError:
        raise DeployRefused(f"no tag {version} here; run git fetch --tags") from None
    if git("rev-parse", "HEAD") != tagged:
        raise DeployRefused(f"HEAD is not {version}; run git switch --detach {version} first")
    if git("status", "--porcelain", "--untracked-files=no"):
        raise DeployRefused("the working copy has uncommitted changes")
    return tagged


def prod_approved(deployments: list[dict], statuses: dict[int, list[dict]]) -> bool:
    """True if this commit's newest prod deployment ended in success (statuses: newest first)."""
    if not deployments:
        return False
    newest = max(deployments, key=lambda d: d["created_at"])
    states = statuses.get(newest["id"], [])
    return bool(states) and states[0]["state"] == "success"


def image_refs(release_body: str) -> dict[str, str]:
    """The producer and Spark image references, pinned by digest, from the release notes."""
    refs = dict(IMAGE_LINE.findall(release_body))
    if set(refs) != {"producer", "spark"}:
        raise DeployRefused("the GitHub release does not list both image digests")
    return refs


def fetch(client: httpx.Client, path: str) -> object:
    response = client.get(f"{API}/{path}")
    response.raise_for_status()
    return response.json()


def main() -> None:
    try:
        version = sys.argv[1]
        sha = check_checkout(version)
        with httpx.Client(timeout=20, headers={"Accept": "application/vnd.github+json"}) as client:
            deployments = fetch(client, f"deployments?environment=prod&sha={sha}")
            statuses = {
                d["id"]: fetch(client, f"deployments/{d['id']}/statuses") for d in deployments
            }
            if not prod_approved(deployments, statuses):
                raise DeployRefused(
                    f"{version} is not approved for prod: approve the release run's prod job "
                    "in GitHub Actions and wait for it to finish"
                )
            refs = image_refs(fetch(client, f"releases/tags/{version}")["body"])
    except (DeployRefused, IndexError) as exc:
        sys.exit(f"deploy refused: {exc or 'usage: deploy.py vX.Y.Z'}")
    DEPLOY_ENV.write_text(
        f"# Written by make deploy {version}: images pinned by digest.\n"
        "# Delete this file to go back to local builds and EDITGUARD_ENV from .env.\n"
        f"EDITGUARD_ENV=prod\n"
        f"EDITGUARD_PRODUCER_IMAGE={refs['producer']}\n"
        f"EDITGUARD_SPARK_IMAGE={refs['spark']}\n"
    )
    print(f"{version} approved for prod; images pinned in {DEPLOY_ENV}")


if __name__ == "__main__":
    main()
