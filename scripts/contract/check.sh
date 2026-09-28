#!/usr/bin/env bash
# CI check: the contract is valid ODCS and contracts/generated/ matches what it generates.
set -euo pipefail
cd "$(dirname "$0")/../.."
uvx -q --from "datacontract-cli==${DATACONTRACT_VERSION:-1.2.2}" datacontract lint contracts/edits.odcs.yaml
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
scripts/contract/generate.sh "$tmp"
if ! diff -ru contracts/generated "$tmp"; then
    echo "contracts/generated/ is out of date: run 'make contract' and commit the result." >&2
    exit 1
fi
uv run python scripts/contract/dictionary.py --check
echo "contracts/generated/ and the data dictionary are up to date."
