"""silver.user_history_asof_hour on hand-built cases (tests/fixtures/edge/history.jsonl).

What an editor's history says at the end of each hour, and the anti-leakage rule: an edit at
hour H reads the latest row at or before H - 1, so it never sees its own hour.
"""

import json
from pathlib import Path

import pytest
from test_labels import TRANSFORM, load

pytestmark = [pytest.mark.dbt, pytest.mark.filterwarnings("ignore::DeprecationWarning")]

CASES = Path(__file__).parent / "fixtures/edge/history.jsonl"
AS_OF = "2026-10-28 00:00:00"  # after every 720-hour window in the cases has closed

# The H - 1 lookup every feature path must use (data dictionary).
LOOKUP = """
    select e.rev_id, h.edits_30d, h.reverts_received_30d
    from ci_silver.edits as e
    left join ci_silver.user_history_asof_hour as h
      on h.wiki_id = e.wiki_id and h.user_hash = e.user_hash
     and h.asof_hour = (
         select max(asof_hour) from ci_silver.user_history_asof_hour as h2
         where h2.wiki_id = e.wiki_id and h2.user_hash = e.user_hash
           and h2.asof_hour <= date_trunc('hour', e.event_time) - interval '1' hour)
"""
HISTORY_OF = """
    select strftime(h.asof_hour, '%Y-%m-%d %H:%M'), h.edits_30d, h.reverts_received_30d
    from ci_silver.user_history_asof_hour as h
    where h.user_hash = (select user_hash from ci_silver.edits where rev_id = {rev_id})
    order by h.asof_hour
"""


@pytest.fixture(scope="module")
def dbt(tmp_path_factory: pytest.TempPathFactory):
    dbt_main = pytest.importorskip("dbt.cli.main", reason="needs the transform group")
    tmp = tmp_path_factory.mktemp("history")
    database = tmp / "history.duckdb"
    load(database, [json.loads(line) for line in CASES.read_text().splitlines() if line.strip()])
    mp = pytest.MonkeyPatch()
    mp.setenv("DBT_DUCKDB_PATH", str(database))
    mp.setenv("USERNAME_SALT", "ci-salt")
    mp.setenv("DATA_BUCKET", "unused")
    common = [
        "--target", "ci", "--quiet",
        "--project-dir", str(TRANSFORM), "--profiles-dir", str(TRANSFORM),
        "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs"),
        "--vars", json.dumps({"labels_as_of": AS_OF}),
    ]  # fmt: skip
    runner = dbt_main.dbtRunner()
    models = ["edits", "labels", "user_history_asof_hour"]  # labels: edits' tests need it
    built = runner.invoke(["build", "--select", *models, *common])
    assert built.success, built.exception

    def show(sql: str) -> list[tuple]:
        shown = runner.invoke(["show", "--limit", "100", "--inline", sql, *common])
        assert shown.success, shown.exception
        return [tuple(row) for row in shown.result.results[0].agate_table.rows]

    yield show
    mp.undo()


def test_history_counts_edits_and_reverts_received_by_hour(dbt) -> None:
    alice = dbt(HISTORY_OF.format(rev_id=1))
    assert alice[:3] == [
        ("2026-09-26 10:00", 2, 0),  # two edits in hour 10
        ("2026-09-26 11:00", 3, 1),  # bob's 11:20 revert of her 10:05 edit, plus her 11:50 edit
        ("2026-09-26 12:00", 6, 1),  # 3 more; not her self-revert, not carol's other page
    ]
    assert alice[-1][1:] == (0, 0)  # 720 hours later everything has left the window


def test_edit_at_hour_h_reads_history_from_hour_h_minus_1(dbt) -> None:
    got = {row[0]: row[1:] for row in dbt(LOOKUP)}
    assert got[10] == (2, 0)  # 11:50 reads hour 10: the 11:20 revert is not known yet
    assert got[4] == (3, 1)  # 12:10 reads hour 11: three edits and one revert received
    assert got[1] == (None, None)  # her first edit has no history at all


def test_edits_leave_the_window_after_720_hours(dbt) -> None:
    dave = dbt(HISTORY_OF.format(rev_id=8))
    assert dave == [
        ("2026-09-26 00:00", 1, 0),
        ("2026-10-26 00:00", 0, 0),  # exactly 720 hours later
        ("2026-10-27 05:00", 1, 0),  # the second edit, 31 days after the first
    ]
