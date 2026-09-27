import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastavro import parse_schema
from fastavro.validation import validate

from editguard.producer.parse import to_edit_event
from editguard.streaming.flags import (
    FLAGGED_SCHEMA,
    NO_HISTORY_SNAPSHOT,
    flag_key,
    flag_records,
    utc_rows,
)

ROOT = Path(__file__).parents[1]
SCHEMA = parse_schema(json.loads(FLAGGED_SCHEMA.read_text()))
NOW = datetime(2026, 9, 27, 4, 0, tzinfo=UTC)


@pytest.fixture
def edit() -> dict[str, Any]:
    raw = json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text())
    record = to_edit_event(raw)
    record["revert_method"] = None
    return record


def risky(edit: dict[str, Any]) -> dict[str, Any]:
    """A temporary account blanking most of a page without a summary."""
    return {
        **edit,
        "performer_is_temp": True,
        "performer_groups": ["*"],
        "rev_size": 100,
        "prior_rev_size": 6000,
        "comment": "",
    }


def test_only_scored_edits_over_the_threshold_are_flagged(edit: dict[str, Any]) -> None:
    calm = {**edit, "performer_edit_count": 5000}
    bot = {**risky(edit), "performer_is_bot": True}
    flags = flag_records([calm, bot, risky(edit)], NOW)
    assert len(flags) == 1
    assert flags[0]["score"] == 1.0


def test_flag_matches_the_contract(edit: dict[str, Any]) -> None:
    [flag] = flag_records([risky(edit)], NOW)
    assert validate(flag, SCHEMA)
    assert flag["score_version"] == "rules-v0"
    assert flag["history_snapshot_id"] == NO_HISTORY_SNAPSHOT
    assert flag["scored_at"] == NOW


def test_flag_key_is_wiki_and_revision(edit: dict[str, Any]) -> None:
    [flag] = flag_records([risky(edit)], NOW)
    assert flag_key(flag) == b"enwiki:2549252841"


def test_utc_rows_parses_spark_strings_and_marks_naive_times_utc() -> None:
    [row] = utc_rows(
        [
            {
                "event_time": "2026-09-27T03:59:58.000Z",
                "performer_registration_dt": datetime(2020, 1, 1),
            }
        ]
    )
    assert row["event_time"] == datetime(2026, 9, 27, 3, 59, 58, tzinfo=UTC)
    assert row["performer_registration_dt"].tzinfo is UTC
