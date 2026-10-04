"""make report: the evaluation report from prod, pinned to a gold.fact_label snapshot (ADR 0013).

Usage: uv run --group transform --group evaluation python -m editguard.tools.report \
         --snapshot latest|<id> [--window test | --since 2026-09-26 --until 2026-10-02]
First run for a (snapshot, window): one Athena UNLOAD of the evaluated edits as of the snapshot
(raw rows land under athena-results/, which expires after 7 days), scored offline with the live
rules code, saved as frozen rows (no usernames, no edit text) with a SHA-256 checksum under
s3://<bucket>/prod/reports/<snapshot>/<window>/. Later runs read the frozen rows and check the
checksum, so the same command always gives the same report.
The test window writes docs/results.md (only once its labels are final); any other window is a
development report under data/reports/.
"""

import argparse
import hashlib
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

from editguard.common.config import get_settings
from editguard.evaluation.protocol import TEST_WINDOW, TEST_WINDOW_FINAL
from editguard.evaluation.report import Rows, render, segments
from editguard.evaluation.scoring import SCORE_VERSION, rules_scores

WORKGROUP = "editguard-prod"
RESULTS = Path("docs/results.md")
DEV_REPORTS = Path("data/reports")
# The fields compute_features reads (features.POINT_IN_TIME_FIELDS). SQL below is built only
# from these constants, integers and parsed dates, never from free text (hence the S608 noqas).
FEATURES = (
    "event_time", "rev_size", "prior_rev_size", "comment", "is_comment_visible",
    "performer_is_temp", "performer_registration_dt", "performer_edit_count",
    "performer_groups", "revert_method",
)  # fmt: skip


class Athena:
    def __init__(self, session: Any) -> None:
        self.client = session.client("athena")

    def run(self, sql: str) -> list[list[str]]:
        qid = self.client.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)[
            "QueryExecutionId"
        ]
        while True:
            state = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            if state["Status"]["State"] not in ("QUEUED", "RUNNING"):
                break
            time.sleep(1)
        if state["Status"]["State"] != "SUCCEEDED":
            raise SystemExit(f"Athena query failed: {state['Status'].get('StateChangeReason')}")
        rows = self.client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
        return [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows]


def snapshot(athena: Athena, wanted: str) -> tuple[str, str]:
    """(snapshot id, committed_at) of gold.fact_label: the newest, or the one asked for."""
    where = "" if wanted == "latest" else f"where snapshot_id = {int(wanted)}"
    sql = (
        "select cast(snapshot_id as varchar), cast(committed_at as varchar) "  # noqa: S608
        f'from "prod_gold"."fact_label$snapshots" {where} order by committed_at desc limit 1'
    )
    rows = athena.run(sql)
    if not rows:
        raise SystemExit(f"no gold.fact_label snapshot {wanted}")
    return rows[0][0], rows[0][1]


# UNLOAD writes only plain timestamps, at millisecond precision. Every timestamp here is UTC and
# Wikimedia's are millisecond-precise, so the cast to timestamp(3) changes no value.
UTC_TIMESTAMPS = frozenset({"event_time", "performer_registration_dt"})


def unload_column(name: str) -> str:
    if name in UTC_TIMESTAMPS:
        return f"cast(e.{name} as timestamp(3)) as {name}"
    return f"e.{name}"


def unload_sql(snap: str, committed: str, since: datetime, until: datetime, target: str) -> str:
    as_of = f"FOR TIMESTAMP AS OF TIMESTAMP '{committed}'"
    return f"""
        UNLOAD (
            select l.wiki_id, l.rev_id,
                cast(date_trunc('hour', l.event_time) as timestamp(3)) as hour,
                w.language_group, l.label, {", ".join(unload_column(c) for c in FEATURES)},
                b.probability_true as wikimedia_probability
            from prod_gold.fact_label FOR VERSION AS OF {int(snap)} as l
            join prod_silver.edits {as_of} as e
              on e.wiki_id = l.wiki_id and e.rev_id = l.rev_id and e.page_change_kind = 'edit'
            join prod_gold.dim_wiki as w on w.wiki_id = l.wiki_id
            left join prod_silver.baseline_scores {as_of} as b
              on b.wiki_id = l.wiki_id and b.rev_id = l.rev_id
            where l.label in ('damaging', 'ok')
              and l.event_time >= timestamp '{since:%Y-%m-%d %H:%M:%S}'
              and l.event_time < timestamp '{until:%Y-%m-%d %H:%M:%S}'
        ) TO '{target}' WITH (format = 'PARQUET')
    """  # noqa: S608 - every value is an int, a fixed name or a parsed date


def freeze(con: duckdb.DuckDBPyConnection, raw_glob: str, out: Path) -> None:
    """Score the unloaded rows with the live rules code and write the frozen rows."""
    con.execute(f"create table raw as select * from read_parquet('{raw_glob}')")  # noqa: S608
    select = f"select wiki_id, rev_id, {', '.join(FEATURES)} from raw"  # noqa: S608
    records = [
        {**dict(zip(FEATURES, rec[2:], strict=True)), "_key": (rec[0], rec[1])}
        for rec in con.execute(select).fetchall()
    ]
    for r in records:
        r["performer_groups"] = r["performer_groups"] or []
    con.execute("create table score (wiki_id varchar, rev_id bigint, rules_score double)")
    con.executemany(
        "insert into score values (?, ?, ?)",
        [[*r["_key"], s] for r, s in zip(records, rules_scores(records), strict=True)],
    )
    con.execute(
        f"""copy (
            select r.wiki_id, r.rev_id, r.hour, r.language_group, r.label, s.rules_score,
                r.wikimedia_probability
            from raw as r join score as s using (wiki_id, rev_id)
            order by r.wiki_id, r.rev_id
        ) to '{out}' (format parquet)"""  # noqa: S608
    )


def load_rows(con: duckdb.DuckDBPyConnection, path: Path) -> Rows:
    cols = con.execute(
        "select wiki_id, hour, language_group, label, rules_score, wikimedia_probability "  # noqa: S608
        f"from read_parquet('{path}') order by wiki_id, rev_id"
    ).fetchnumpy()
    return Rows(
        wiki_id=np.asarray(cols["wiki_id"], dtype=object),
        hour=np.asarray(cols["hour"]).astype("datetime64[s]").astype(np.int64),
        language_group=np.asarray(cols["language_group"], dtype=object),
        damaging=np.asarray(cols["label"], dtype=object) == "damaging",
        rules_score=np.asarray(cols["rules_score"], dtype=float),
        wikimedia=np.ma.filled(np.ma.asarray(cols["wikimedia_probability"], dtype=float), np.nan),
    )


def keep_later_sections(report: str, results_md: str) -> str:
    """The new header and headline, then results.md's later sections (ablation, SLOs, ...)."""
    marker = "\n## Ablation"
    return report.rstrip("\n") + "\n" + results_md[results_md.index(marker) :]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--snapshot", required=True, help="gold.fact_label snapshot id, or latest")
    parser.add_argument("--window", choices=["test", "dev"], default="dev")
    parser.add_argument("--since", default="2026-09-26", help="dev window start, UTC date")
    parser.add_argument("--until", default="2026-10-02", help="dev window end (exclusive)")
    args = parser.parse_args()

    if args.window == "test":
        since, until = TEST_WINDOW
        if datetime.now(UTC) < TEST_WINDOW_FINAL:
            sys.exit(
                f"the test window's labels are final from {TEST_WINDOW_FINAL:%Y-%m-%d %H:%M} UTC"
            )
    else:
        since = datetime.fromisoformat(args.since).replace(tzinfo=UTC)
        until = datetime.fromisoformat(args.until).replace(tzinfo=UTC)
        if since < TEST_WINDOW[1] and until > TEST_WINDOW[0]:
            sys.exit("a development window must not overlap the test window (ADR 0013)")
    label = f"{since:%Y%m%d}-{until:%Y%m%d}"

    import boto3

    settings = get_settings()
    session = boto3.Session(profile_name=settings.aws_profile, region_name=settings.aws_region)
    s3 = session.client("s3")
    athena = Athena(session)
    bucket = settings.data_bucket or sys.exit("DATA_BUCKET must be set in .env")
    snap, committed = snapshot(athena, args.snapshot)
    key = f"prod/reports/{snap}/{label}/rows.parquet"
    rows_path = f"s3://{bucket}/{key}"

    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "rows.parquet"
        con = duckdb.connect()
        stored = s3.list_objects_v2(Bucket=bucket, Prefix=key).get("KeyCount", 0) > 0
        if stored:
            s3.download_file(bucket, key, str(local))
            expected = s3.get_object(Bucket=bucket, Key=key + ".sha256")["Body"].read().decode()
            if sha256(local) != expected.strip():
                sys.exit(f"checksum mismatch for {key}: the frozen rows changed")
            print(f"frozen rows: {rows_path.replace(bucket, '<bucket>')} (checksum OK)")
        else:
            prefix = f"athena-results/prod/report-unload/{snap}/{label}/{int(time.time())}/"
            athena.run(unload_sql(snap, committed, since, until, f"s3://{bucket}/{prefix}"))
            raw = Path(tmp) / "raw"
            raw.mkdir()
            for obj in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", []):
                s3.download_file(bucket, obj["Key"], str(raw / Path(obj["Key"]).name))
            freeze(con, f"{raw}/*", local)
            digest = sha256(local)
            s3.upload_file(str(local), bucket, key)
            s3.put_object(Bucket=bucket, Key=key + ".sha256", Body=digest.encode())
            print(f"frozen rows saved: {rows_path.replace(bucket, '<bucket>')} (sha256 {digest})")
        rows = load_rows(con, local)
        digest = sha256(local)

    meta = {
        "snapshot": snap,
        "committed_at": committed,
        "window": f"{since:%Y-%m-%d} to {until:%Y-%m-%d} (end exclusive)",
        "score_version": SCORE_VERSION,
        "rows_path": rows_path.replace(bucket, "<bucket>"),
        "rows_sha256": digest,
    }
    text = render(meta, segments(rows), development=args.window != "test")
    if args.window == "test":
        out = RESULTS
        text = keep_later_sections(text, RESULTS.read_text())
    else:
        out = DEV_REPORTS / f"dev-{snap}-{label}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    print(text)
    print(f"written: {out}")


if __name__ == "__main__":
    main()
