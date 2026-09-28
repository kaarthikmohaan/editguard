#!/usr/bin/env bash
# Regenerate contracts/generated/ (Avro, Pydantic, dbt schema) and the data dictionary's contract
# tables from contracts/edits.odcs.yaml (CONTRIBUTING: never edit generated files by hand).
# Usage: generate.sh [output_dir]   (default: contracts/generated; the dictionary is always docs/)
# datacontract-cli runs through uvx, pinned, so it stays out of the project's dependencies.
set -euo pipefail
cd "$(dirname "$0")/../.."
out="${1:-contracts/generated}"
contract=contracts/edits.odcs.yaml
dc() { uvx -q --from "datacontract-cli==${DATACONTRACT_VERSION:-1.2.2}" datacontract "$@"; }

mkdir -p "$out"
for schema in edits flagged_edits baseline_scores; do
    dc export avro "$contract" --schema-name "$schema" --output "$out/$schema.avsc"
done
dc export pydantic-model "$contract" --output "$out/models.py"
dc export dbt-models "$contract" --output "$out/dbt_schema.yml"
# End every file with exactly one newline, as the end-of-file-fixer hook would.
for f in "$out"/*; do
    [ -n "$(tail -c1 "$f")" ] && echo >> "$f"
done
python3 - "$out/dbt_schema.yml" <<'PY'
import sys
path = sys.argv[1]
text = open(path).read().rstrip("\n") + "\n"
open(path, "w").write(text)
PY
if [ "$out" = contracts/generated ]; then
    uv run python scripts/contract/dictionary.py
fi
