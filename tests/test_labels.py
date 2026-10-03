"""silver.labels on hand-built revert cases (ADR 0005; T-DBT-LABEL-01..08, T-U-EDGE-05..07).

Each line of tests/fixtures/edge/labels.jsonl is one article edit; lines with `expect` say the
label it must get. The events become bronze rows in a fresh DuckDB, dbt builds silver.edits
and silver.labels with a fixed clock, and the labels are compared case by case.
"""

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

pytestmark = [pytest.mark.dbt, pytest.mark.filterwarnings("ignore::DeprecationWarning")]

ROOT = Path(__file__).parents[1]
TRANSFORM = ROOT / "transform"
CASES = ROOT / "tests/fixtures/edge/labels.jsonl"
START = datetime(2026, 9, 26, tzinfo=UTC)
AS_OF = "2026-09-28 12:00:00"  # edits before 2026-09-26 12:00 UTC have final labels


def cases() -> list[dict]:
    return [json.loads(line) for line in CASES.read_text().splitlines() if line.strip()]


def bronze_row(case: dict, columns: dict[str, str]) -> list:
    when = START + timedelta(minutes=case["minutes"])
    method, oldest, newest = case.get("revert", [None, None, None])
    values = {
        "event_id": f"edge-{case['rev_id']}",
        "event_time": when,
        "emitted_at": when,
        "ingested_at": when + timedelta(seconds=1),
        "wiki_id": "enwiki",
        "page_id": case["page_id"],
        "page_title": f"Page {case['page_id']}",
        "namespace_id": 0,
        "page_change_kind": "edit",
        "rev_id": case["rev_id"],
        "rev_size": 1000,
        "prior_rev_size": 1000,
        "comment": "edit",
        "is_content_visible": True,
        "is_comment_visible": True,
        "is_editor_visible": True,
        "performer_user_text": case["user"],
        "performer_is_bot": case.get("bot", False),
        "performer_is_temp": False,
        "performer_groups": [],
        "revert_method": method,
        "rev_reverted_oldest_id": oldest,
        "rev_reverted_newest_id": newest,
        "schema_version": "1.2.0",
        "raw_json": "{}",
    }
    return [values.get(name) for name in columns]


def load(path: Path) -> None:
    sys.path.insert(0, str(TRANSFORM / "ci"))
    from load_fixtures import edit_columns

    columns = edit_columns()
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE SCHEMA ci_bronze")
    con.execute(
        f"CREATE TABLE ci_bronze.edits ({', '.join(f'{c} {t}' for c, t in columns.items())})"
    )
    con.executemany(
        f"INSERT INTO ci_bronze.edits VALUES ({', '.join('?' for _ in columns)})",  # noqa: S608
        [bronze_row(case, columns) for case in cases()],
    )
    con.close()


def test_labels_follow_the_revert_rules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dbt_main = pytest.importorskip("dbt.cli.main", reason="needs the transform group")
    database = tmp_path / "labels.duckdb"
    load(database)
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(database))
    monkeypatch.setenv("USERNAME_SALT", "ci-salt")
    monkeypatch.setenv("DATA_BUCKET", "unused")
    common = [
        "--target", "ci", "--quiet",
        "--project-dir", str(TRANSFORM), "--profiles-dir", str(TRANSFORM),
        "--target-path", str(tmp_path / "target"), "--log-path", str(tmp_path / "logs"),
        "--vars", json.dumps({"labels_as_of": AS_OF}),
    ]  # fmt: skip
    runner = dbt_main.dbtRunner()
    for run in (1, 2):  # the second run is incremental: frozen labels must not change
        result = runner.invoke(["build", "--select", "edits", "labels", *common])
        assert result.success, f"run {run}: {result.exception}"

    shown = runner.invoke(
        [
            "show", "--limit", "100", "--inline",
            "select rev_id, label, reverting_rev_id, reverter_is_bot, minutes_to_revert"
            " from ci_silver.labels",
            *common,
        ]
    )  # fmt: skip
    assert shown.success, shown.exception
    got = {row[0]: row[1:] for row in shown.result.results[0].agate_table.rows}

    assert len(got) == len(cases())  # every edit gets exactly one label
    for case in cases():
        if "expect" not in case:
            continue
        label, reverting, by_bot, minutes = got[case["rev_id"]]
        assert label == case["expect"], case["case"]
        if "reverting_rev_id" in case:
            assert reverting == case["reverting_rev_id"], case["case"]
        if "reverter_is_bot" in case:
            assert by_bot == case["reverter_is_bot"], case["case"]
        if "minutes_to_revert" in case:
            assert float(minutes) == case["minutes_to_revert"], case["case"]
