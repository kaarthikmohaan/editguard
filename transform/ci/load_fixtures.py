"""Build the DuckDB database the dbt `ci` target runs on, from the 1,000-event fixture.

Usage: uv run python transform/ci/load_fixtures.py [path]   (default: transform/target/ci.duckdb)
ci_bronze.edits is what the live job would write for the fixture (the producer's parser, then
ADR 0010's event_time). To exercise the replay path, 10 article events are held back from live
bronze and appear only in ci_bronze.edits_replay (gaps), next to 20 that live already has
(duplicates). ci_bronze.baseline_scores holds the recorded Wikimedia scores.
"""

import gzip
import json
import sys
from datetime import timedelta
from pathlib import Path

import duckdb

from editguard.producer.parse import parse_ts, to_edit_event

ROOT = Path(__file__).parents[2]
FIXTURES = ROOT / "tests/fixtures"
EDITS_AVSC = ROOT / "contracts/generated/edits.avsc"
GAPS, DUPLICATES = 10, 20

AVRO_TO_DUCKDB = {"string": "VARCHAR", "long": "BIGINT", "int": "INTEGER", "boolean": "BOOLEAN"}


def duckdb_type(avro: object) -> str:
    """DuckDB column type for an Avro field type from the generated contract schema."""
    if isinstance(avro, list):  # ["null", X]
        return duckdb_type(next(t for t in avro if t != "null"))
    if isinstance(avro, dict):
        if avro.get("logicalType", "").startswith("timestamp"):
            return "TIMESTAMPTZ"
        if avro["type"] == "array":
            return duckdb_type(avro["items"]) + "[]"
        return duckdb_type(avro["type"])
    return AVRO_TO_DUCKDB[avro]


def edit_columns() -> dict[str, str]:
    return {f["name"]: duckdb_type(f["type"]) for f in json.loads(EDITS_AVSC.read_text())["fields"]}


def read_gz(name: str) -> list[dict]:
    return [json.loads(line) for line in gzip.open(FIXTURES / name, "rt", encoding="utf-8")]


def bronze_rows() -> list[dict]:
    rows = []
    for event in read_gz("page_change_1k.jsonl.gz"):
        emitted = parse_ts(event["meta"]["dt"])
        record = to_edit_event(event, ingested_at=emitted + timedelta(seconds=1))
        rows.append({k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in record.items()})
    return rows


def load(path: Path) -> dict[str, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    rows = bronze_rows()
    articles = [r for r in rows if r["namespace_id"] == 0]
    gaps = {r["event_id"] for r in articles[:GAPS]}
    live = [r for r in rows if r["event_id"] not in gaps]
    replay = [r for r in articles[: GAPS + DUPLICATES]]
    scores = read_gz("baseline_1k.jsonl.gz")
    for score in scores:
        score["ingested_at"] = (
            parse_ts("1970-01-01T00:00:00Z") + timedelta(milliseconds=score["ingested_at"])
        ).isoformat()

    con = duckdb.connect(str(path))
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE SCHEMA ci_bronze")
    columns = edit_columns()
    for table, data in (("edits", live), ("edits_replay", replay)):
        con.execute(
            f"CREATE TABLE ci_bronze.{table} ({', '.join(f'{c} {t}' for c, t in columns.items())})"
        )
        con.executemany(
            # table comes from the fixed tuple above, never from input
            f"INSERT INTO ci_bronze.{table} VALUES ({', '.join('?' for _ in columns)})",  # noqa: S608
            [[row[c] for c in columns] for row in data],
        )
    con.execute(
        "CREATE TABLE ci_bronze.baseline_scores (wiki_id VARCHAR, rev_id BIGINT,"
        " model_name VARCHAR, model_version VARCHAR, probability_true DOUBLE,"
        " ingested_at TIMESTAMPTZ, upstream_partition INTEGER, upstream_offset BIGINT)"
    )
    con.executemany(
        "INSERT INTO ci_bronze.baseline_scores VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
        [
            [
                s["wiki_id"],
                s["rev_id"],
                s["model_name"],
                s["model_version"],
                s["probability_true"],
                s["ingested_at"],
                i,
            ]
            for i, s in enumerate(scores)
        ],
    )
    con.close()
    return {
        "edits": len(live),
        "edits_replay": len(replay),
        "baseline_scores": len(scores),
        "gaps": GAPS,
    }


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "transform/target/ci.duckdb"
    print(target, load(target))
