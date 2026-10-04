"""make audit-sample KIND=logic|noise: draw a label-audit sheet from prod (read-only).

Usage: make audit-sample KIND=logic|noise   (sample from prod, read-only)
       make audit-summary                  (count the filled-in verdicts)
Development days only (ADR 0013), audited wikis only (English, Hindi, Kannada), deterministic.
"""

import argparse
from pathlib import Path

from editguard.common.config import get_settings
from editguard.evaluation.audit import (
    AUDIT_WIKIS,
    NOISE_CATEGORIES,
    SEED,
    summarize_logic,
    summarize_noise,
    write_sample,
)
from editguard.tools.report import Athena

AUDITS = Path("docs/audits")
DEV_SINCE, DEV_UNTIL = "2026-09-26", "2026-10-02"  # before the test window (ADR 0013)
WIKIS = ", ".join(f"'{w}'" for w in AUDIT_WIKIS)
BASE = f"""
    select l.wiki_id, e.page_title, cast(l.rev_id as varchar) as rev_id,
        cast(l.event_time as varchar) as event_time, l.label,
        coalesce(cast(l.reverting_rev_id as varchar), '') as reverting_rev_id,
        coalesce(cast(round(l.minutes_to_revert, 1) as varchar), '') as minutes_to_revert
    from prod_silver.labels as l
    join prod_silver.edits as e on e.wiki_id = l.wiki_id and e.rev_id = l.rev_id
    where l.wiki_id in ({WIKIS}) and e.page_change_kind = 'edit'
      and l.event_time >= timestamp '{DEV_SINCE}' and l.event_time < timestamp '{DEV_UNTIL}'
"""  # noqa: S608 - fixed constants only
ORDER = f"to_hex(md5(to_utf8(concat('{SEED}', l.wiki_id, cast(l.rev_id as varchar)))))"


def pick(where: str, n: int) -> str:
    return f"{BASE} and {where} order by {ORDER} limit {n}"  # noqa: S608 - constants only


def logic_sql() -> str:
    """50 cases: 20 damaging, 10 bot_caught, 10 ok, 5 multi-edit reverts and 5 `ok` edits that
    a revert did cover (self-reverts, reverts that were undone, or reverts after 48 hours)."""
    multi = "exists (select 1 from prod_silver.edits r where r.wiki_id = l.wiki_id \
and r.rev_id = l.reverting_rev_id and r.rev_reverted_newest_id > r.rev_reverted_oldest_id)"
    covered_ok = "exists (select 1 from prod_silver.edits r where r.wiki_id = l.wiki_id \
and r.page_id = e.page_id and r.revert_method is not null and r.event_time > l.event_time \
and l.rev_id between r.rev_reverted_oldest_id and r.rev_reverted_newest_id)"
    parts = [
        ("damaging", pick(f"l.label = 'damaging' and l.wiki_id = 'enwiki' and not {multi}", 20)),
        ("bot_caught", pick("l.label = 'bot_caught'", 10)),
        ("ok", pick(f"l.label = 'ok' and not {covered_ok}", 10)),
        ("multi-edit revert", pick(f"l.label = 'damaging' and {multi}", 5)),
        ("covered but ok", pick(f"l.label = 'ok' and {covered_ok}", 5)),
    ]
    return " union all ".join(
        f"(select '{name}' as stratum, * from ({sql}))"  # noqa: S608 - constants only
        for name, sql in parts
    )


def noise_sql() -> str:
    """200 damaging edits: Kannada and Hindi first (they are rare), English for the rest."""
    parts = [pick(f"l.label = 'damaging' and l.wiki_id = '{w}'", n)
             for w, n in (("knwiki", 10), ("hiwiki", 25), ("enwiki", 165))]  # fmt: skip
    return " union all ".join(f"(select * from ({sql}))" for sql in parts)  # noqa: S608


def sample(kind: str) -> None:
    import boto3

    settings = get_settings()
    athena = Athena(
        boto3.Session(profile_name=settings.aws_profile, region_name=settings.aws_region)
    )
    sql = logic_sql() if kind == "logic" else noise_sql()
    columns = ["stratum"] if kind == "logic" else []
    columns += ["wiki_id", "page_title", "rev_id", "event_time", "label", "reverting_rev_id",
                "minutes_to_revert"]  # fmt: skip
    rows = [dict(zip(columns, r, strict=True)) for r in athena.run(sql)]
    verdicts = ["correct", "note"] if kind == "logic" else ["category", "note"]
    path = AUDITS / f"{kind}-{DEV_SINCE}-{DEV_UNTIL}.csv"
    write_sample(path, rows, verdicts)
    print(f"{len(rows)} edits written to {path}")
    if kind == "logic":
        print("Fill `correct` with yes or no (does the label follow the rule?) and a note if no.")
    else:
        print(f"Fill `category` with one of: {', '.join(NOISE_CATEGORIES)}.")


def summary() -> None:
    for path in sorted(AUDITS.glob("logic-*.csv")):
        correct, audited = summarize_logic(path)
        if not audited:
            print(f"{path.name}: not audited yet")
            continue
        print(f"{path.name}: {correct}/{audited} correct")
    for path in sorted(AUDITS.glob("noise-*.csv")):
        counts, audited, languages = summarize_noise(path)
        if not audited:
            print(f"{path.name}: not audited yet")
            continue
        shares = ", ".join(f"{c} {100 * counts[c] / audited:.0f}%" for c in NOISE_CATEGORIES)
        print(f"{path.name}: {audited} audited ({', '.join(languages)}): {shares}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("sample").add_argument("--kind", choices=["logic", "noise"], required=True)
    sub.add_parser("summary")
    args = parser.parse_args()
    sample(args.kind) if args.command == "sample" else summary()


if __name__ == "__main__":
    main()
