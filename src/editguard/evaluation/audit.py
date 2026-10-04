"""Hand audits of the labels (ADR 0005: "50 cases audited by hand"; design: 200-edit noise audit).

The samples come from development days only (never the test window, ADR 0013), from the wikis
the auditor reads, ordered by a seeded hash so the same command gives the same sample. Each
sample is a CSV under docs/audits/ with Wikipedia links and empty verdict columns; the auditor
fills the verdicts in Numbers or Excel; summarize() turns the filled files into the numbers in
docs/results.md. The CSVs hold public Wikipedia IDs and page titles only, never usernames.
"""

import csv
from collections import Counter
from pathlib import Path
from urllib.parse import quote

AUDIT_WIKIS = {"enwiki": "en", "hiwiki": "hi", "knwiki": "kn"}  # read by the auditor
NOISE_CATEGORIES = ("vandalism", "honest mistake", "content dispute", "fine")
SEED = "20261004"


def wiki_url(wiki_id: str, **params: str | int) -> str:
    query = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params.items())
    return f"https://{AUDIT_WIKIS[wiki_id]}.wikipedia.org/w/index.php?{query}"


def links(row: dict[str, str]) -> dict[str, str]:
    """Diff of the edit, diff of the revert (if any) and the page history."""
    out = {
        "diff_url": wiki_url(row["wiki_id"], diff=row["rev_id"]),
        "history_url": wiki_url(row["wiki_id"], title=row["page_title"], action="history"),
    }
    out["reverting_diff_url"] = (
        wiki_url(row["wiki_id"], diff=row["reverting_rev_id"])
        if row.get("reverting_rev_id")
        else ""
    )
    return out


def write_sample(path: Path, rows: list[dict[str, str]], verdicts: list[str]) -> None:
    """The audit sheet: one row per edit, links, and empty columns for the auditor."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["sample_id", *rows[0].keys(), "diff_url", "reverting_diff_url", "history_url",
              *verdicts]  # fmt: skip
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for i, row in enumerate(rows, 1):
            writer.writerow({"sample_id": i, **row, **links(row), **dict.fromkeys(verdicts, "")})


def summarize_logic(path: Path) -> tuple[int, int]:
    """(correct, audited): rows whose `correct` column says yes, out of rows with any answer."""
    answers = [r["correct"].strip().lower() for r in csv.DictReader(path.open(encoding="utf-8"))]
    audited = [a for a in answers if a]
    if any(a not in ("yes", "no") for a in audited):
        raise ValueError(f"{path}: `correct` must be yes or no")
    return sum(a == "yes" for a in audited), len(audited)


def summarize_noise(path: Path) -> tuple[Counter, int, list[str]]:
    """(category counts, audited, languages audited) from a filled noise sheet."""
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    audited = [r for r in rows if r["category"].strip()]
    counts = Counter(r["category"].strip().lower() for r in audited)
    if unknown := set(counts) - set(NOISE_CATEGORIES):
        raise ValueError(f"{path}: unknown categories {sorted(unknown)}; use {NOISE_CATEGORIES}")
    languages = sorted({AUDIT_WIKIS[r["wiki_id"]] for r in audited})
    return counts, len(audited), languages


def label_quality(audits: Path) -> list[str]:
    """The report's Label quality table, from the filled sheets under docs/audits/."""
    rows = ["## Label quality", "", "| Check | Result |", "| --- | --- |"]
    for path in sorted(audits.glob("logic-*.csv")):
        correct, audited = summarize_logic(path)
        wrong = [
            f"#{r['sample_id']}: {r['note'].strip()}"
            for r in csv.DictReader(path.open(encoding="utf-8"))
            if r["correct"].strip().lower() == "no"
        ]
        total = sum(1 for _ in csv.DictReader(path.open(encoding="utf-8")))
        detail = f" (auditor disagreed: {'; '.join(wrong)})" if wrong else ""
        rows.append(
            f"| Revert-logic audit ({total} cases, `{path.name}`) "
            f"| {correct}/{audited} correct{detail} |"
        )
    for path in sorted(audits.glob("noise-*.csv")):
        counts, audited, languages = summarize_noise(path)
        if not audited:
            continue
        shares = ", ".join(f"{c} {100 * counts[c] / audited:.0f}%" for c in NOISE_CATEGORIES)
        rows.append(
            f"| Noise audit ({audited} damaging edits audited, languages: "
            f"{', '.join(languages)}, `{path.name}`) | {shares} |"
        )
    return [*rows, ""]
