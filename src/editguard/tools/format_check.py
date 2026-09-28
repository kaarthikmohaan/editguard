"""T-DBT-FORMAT-V2: every Iceberg table in an environment is format version 2 (ADR 0008).

Usage: uv run --group transform python -m editguard.tools.format_check --env staging|prod
Reads each table's current metadata.json through the Glue catalog (Athena does not report the
format version). No Athena queries, so no scan cost. Exits 1 if any table is not version 2.
"""

import argparse
import json
import sys
from typing import Any

from editguard.common.config import get_settings
from editguard.streaming.catalogs import PREFIXES

EXPECTED_VERSION = 2
LAYERS = ("bronze", "silver", "gold")


def violations(versions: dict[str, int | None]) -> list[str]:
    """Tables whose format version is not the expected one (None: no metadata found)."""
    return sorted(
        f"{table}: format-version {version}"
        for table, version in versions.items()
        if version != EXPECTED_VERSION
    )


def iceberg_versions(session: Any, prefix: str) -> dict[str, int | None]:
    glue, s3 = session.client("glue"), session.client("s3")
    versions: dict[str, int | None] = {}
    for layer in LAYERS:
        database = f"{prefix}_{layer}"
        for page in glue.get_paginator("get_tables").paginate(DatabaseName=database):
            for table in page["TableList"]:
                params = table.get("Parameters", {})
                if params.get("table_type", "").upper() != "ICEBERG":
                    continue
                location = params.get("metadata_location")
                if not location:
                    versions[f"{database}.{table['Name']}"] = None
                    continue
                bucket, key = location.removeprefix("s3://").split("/", 1)
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                versions[f"{database}.{table['Name']}"] = json.loads(body)["format-version"]
    return versions


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", choices=sorted(PREFIXES), required=True)
    args = parser.parse_args()
    import boto3

    settings = get_settings()
    session = boto3.Session(profile_name=settings.aws_profile, region_name=settings.aws_region)
    versions = iceberg_versions(session, PREFIXES[args.env])
    for table, version in sorted(versions.items()):
        print(f"{table:<40} format-version {version}")
    if bad := violations(versions):
        print("NOT format v2 (ADR 0008):", *bad, sep="\n  ")
        sys.exit(1)
    print(f"All {len(versions)} Iceberg tables are format v2.")


if __name__ == "__main__":
    main()
