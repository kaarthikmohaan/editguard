"""Label-audit sheets (ADR 0005's 50-case audit; the design's 200-edit noise audit)."""

import csv
from datetime import datetime
from pathlib import Path

import pytest

from editguard.evaluation.audit import (
    links,
    summarize_logic,
    summarize_noise,
    write_sample,
)
from editguard.evaluation.protocol import TEST_WINDOW
from editguard.tools.audit import DEV_SINCE, DEV_UNTIL, logic_sql, noise_sql

ROW = {
    "wiki_id": "hiwiki",
    "page_title": "भारत का संविधान",
    "rev_id": "6789",
    "label": "damaging",
    "reverting_rev_id": "6790",
}


def test_links_open_the_diff_the_revert_and_the_history() -> None:
    got = links(ROW)
    assert got["diff_url"] == "https://hi.wikipedia.org/w/index.php?diff=6789"
    assert got["reverting_diff_url"] == "https://hi.wikipedia.org/w/index.php?diff=6790"
    assert got["history_url"].startswith("https://hi.wikipedia.org/w/index.php?title=%E0%A4")
    assert got["history_url"].endswith("&action=history")
    assert links({**ROW, "reverting_rev_id": ""})["reverting_diff_url"] == ""


def fill(path: Path, column: str, values: list[str]) -> None:
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    for row, value in zip(rows, values, strict=False):
        row[column] = value
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_logic_sheet_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "logic.csv"
    write_sample(
        path, [ROW, {**ROW, "rev_id": "6791"}, {**ROW, "rev_id": "6792"}], ["correct", "note"]
    )
    assert summarize_logic(path) == (0, 0)  # nothing audited yet
    fill(path, "correct", ["yes", "No", ""])
    assert summarize_logic(path) == (1, 2)
    fill(path, "correct", ["maybe"])
    with pytest.raises(ValueError, match="yes or no"):
        summarize_logic(path)


def test_noise_sheet_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "noise.csv"
    rows = [ROW, {**ROW, "wiki_id": "enwiki", "rev_id": "1"}, {**ROW, "wiki_id": "knwiki"}]
    write_sample(path, rows, ["category", "note"])
    fill(path, "category", ["Vandalism", "content dispute", ""])
    counts, audited, languages = summarize_noise(path)
    assert (counts["vandalism"], counts["content dispute"], audited) == (1, 1, 2)
    assert languages == ["en", "hi"]  # Kannada row not audited yet
    fill(path, "category", ["spam"])
    with pytest.raises(ValueError, match="unknown categories"):
        summarize_noise(path)


def test_samples_never_touch_the_test_window() -> None:
    until = datetime.fromisoformat(DEV_UNTIL).replace(tzinfo=TEST_WINDOW[0].tzinfo)
    assert until <= TEST_WINDOW[0]
    for sql in (logic_sql(), noise_sql()):
        assert f"timestamp '{DEV_SINCE}'" in sql and f"timestamp '{DEV_UNTIL}'" in sql
        assert "2026-10-05" not in sql
