"""The evaluation report from frozen rows (ADR 0013): T-REPORT-REPRO and the segment rules."""

from pathlib import Path

import pytest

np = pytest.importorskip("numpy", reason="needs the evaluation group")
duckdb = pytest.importorskip("duckdb")

from editguard.evaluation.report import Rows, render, segments  # noqa: E402
from editguard.tools.report import keep_later_sections, load_rows, sha256  # noqa: E402

META = {
    "snapshot": "123",
    "committed_at": "2026-10-14 01:00:00 UTC",
    "window": "2026-10-05 to 2026-10-12 (end exclusive)",
    "score_version": "rules-v0",
    "rows_path": "s3://<bucket>/prod/reports/123/20261005-20261012/rows.parquet",
    "rows_sha256": "0" * 64,
}


def frozen_rows(path: Path, seed: int = 7) -> None:
    """Synthetic frozen rows: 6,000 English edits (5% damaging), 400 Indian, no usernames."""
    rng = np.random.default_rng(seed)
    con = duckdb.connect()
    con.execute(
        "create table t (wiki_id varchar, rev_id bigint, hour timestamp, language_group varchar,"
        " label varchar, rules_score double, wikimedia_probability double)"
    )
    rows = []
    for i in range(6400):
        english = i < 6000
        bad = rng.random() < (0.05 if english else 0.1)
        wikimedia = None if rng.random() < 0.02 else float(rng.random() * 0.5 + 0.5 * bad)
        rows.append(
            [
                "enwiki" if english else "bnwiki",
                i,
                f"2026-10-{5 + i % 7:02d} {i % 24:02d}:00:00",
                "english" if english else "indian",
                "damaging" if bad else "ok",
                float(rng.integers(0, 10)) / 9,
                wikimedia,
            ]
        )
    con.executemany("insert into t values (?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"copy (select * from t order by wiki_id, rev_id) to '{path}' (format parquet)")  # noqa: S608


def test_same_frozen_rows_give_an_identical_report(tmp_path: Path) -> None:
    # T-REPORT-REPRO: the report depends only on the frozen rows (and the fixed seed).
    path = tmp_path / "rows.parquet"
    frozen_rows(path)
    first = render(META, segments(load_rows(duckdb.connect(), path), resamples=300), False)
    second = render(META, segments(load_rows(duckdb.connect(), path), resamples=300), False)
    assert first == second
    assert sha256(path) == sha256(path)


def test_segments_follow_the_protocol(tmp_path: Path) -> None:
    path = tmp_path / "rows.parquet"
    frozen_rows(path)
    english, indian, bengali = segments(load_rows(duckdb.connect(), path), resamples=300)
    assert english.scored == 6000 and english.covered < english.scored  # coverage below 100%
    assert english.comparison is not None and english.damaging >= 100
    assert english.comparison.low <= english.comparison.difference <= english.comparison.high
    assert indian.comparison is None  # under 100 damaging: "insufficient data" (ADR 0006)
    assert bengali.covered == indian.covered


def test_rendering_marks_development_reports_and_insufficient_segments() -> None:
    rows = Rows(
        wiki_id=np.array(["enwiki"] * 4, dtype=object),
        hour=np.zeros(4, dtype=np.int64),
        language_group=np.array(["english"] * 4, dtype=object),
        damaging=np.array([True, False, False, False]),
        rules_score=np.array([1.0, 0.0, 0.0, 0.0]),
        wikimedia=np.array([0.9, 0.1, np.nan, 0.2]),
    )
    text = render(META, segments(rows, resamples=10), development=True)
    assert text.startswith("> **Development report: not the result.**")
    assert "| English, bot-miss | 3 | 1 | insufficient data (< 100 damaging) | | |" in text
    assert "75.0%" in text  # English coverage: 3 of 4 edits have a Wikimedia score


def test_test_report_keeps_later_sections_and_replaces_label_quality() -> None:
    results_md = Path("docs/results.md").read_text()
    merged = keep_later_sections(
        "# Results\n\nnew headline\n\n## Label quality\n\nours\n", results_md
    )
    assert merged.startswith("# Results\n\nnew headline\n")
    assert merged.count("## Label quality") == 1 and "ours" in merged  # the template's is dropped
    assert "## Ablation" in merged and "## SLOs" in merged and "## Cost" in merged
    assert merged.endswith(results_md[results_md.index("\n## SLOs") :])
