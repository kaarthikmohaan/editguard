import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastavro import parse_schema
from fastavro.validation import validate

from editguard.producer.parse import is_target, parse_ts, to_edit_event

ROOT = Path(__file__).parents[1]
EDIT_SCHEMA = parse_schema(json.loads((ROOT / "contracts/generated/edits.avsc").read_text()))


@pytest.fixture
def raw_edit() -> dict[str, Any]:
    return json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text())


def test_is_target_keeps_only_the_eight_wikis(raw_edit: dict[str, Any]) -> None:
    assert is_target(raw_edit)
    assert not is_target({**raw_edit, "wiki_id": "frwiki"})


def test_parse_ts_handles_z_suffix() -> None:
    assert parse_ts("2026-09-25T12:53:02Z") == datetime(2026, 9, 25, 12, 53, 2, tzinfo=UTC)
    assert parse_ts(None) is None


def test_edit_matches_avro_contract(raw_edit: dict[str, Any]) -> None:
    record = to_edit_event(raw_edit)
    assert validate(record, EDIT_SCHEMA)


def test_maps_key_fields(raw_edit: dict[str, Any]) -> None:
    record = to_edit_event(raw_edit)
    assert record["event_id"] == "3cd2f8fa-7de6-4d1d-bbac-6b250168ca6e"
    assert record["event_time"] == datetime(2026, 9, 25, 12, 53, 2, tzinfo=UTC)
    assert record["upstream_offset"] == 1105850936
    assert record["prior_rev_size"] == 35600
    assert record["revert_method"] == "undo"
    assert record["rev_reverted_newest_id"] == 2473630467


def test_page_creation_has_no_prior_size(raw_edit: dict[str, Any]) -> None:
    create = copy.deepcopy(raw_edit)
    create["page_change_kind"] = "create"
    del create["prior_state"]
    record = to_edit_event(create)
    assert record["prior_rev_size"] is None
    assert validate(record, EDIT_SCHEMA)


def test_non_revert_has_null_revert_fields(raw_edit: dict[str, Any]) -> None:
    plain = copy.deepcopy(raw_edit)
    del plain["revision"]["revert"]
    record = to_edit_event(plain)
    assert record["revert_method"] is None
    assert record["rev_original_id"] is None


def test_raw_json_keeps_full_event_and_unicode(raw_edit: dict[str, Any]) -> None:
    hindi = copy.deepcopy(raw_edit)
    hindi["page"]["page_title"] = "अभय_सिंह"
    record = to_edit_event(hindi)
    assert json.loads(record["raw_json"]) == hindi
    assert "अभय_सिंह" in record["raw_json"]


def test_missing_required_field_raises(raw_edit: dict[str, Any]) -> None:
    broken = copy.deepcopy(raw_edit)
    del broken["revision"]["rev_id"]
    with pytest.raises(KeyError):
        to_edit_event(broken)


def test_delete_event_time_is_when_the_page_was_deleted(raw_edit: dict[str, Any]) -> None:
    delete = copy.deepcopy(raw_edit)
    delete["page_change_kind"] = "delete"
    delete["dt"] = "2026-09-26T10:00:00Z"
    delete["revision"]["rev_dt"] = "2012-05-12T08:34:10Z"  # the page's last edit, years ago
    record = to_edit_event(delete)
    assert record["event_time"] == datetime(2026, 9, 26, 10, 0, 0, tzinfo=UTC)


def test_suppressed_delete_without_rev_size_parses_to_null(raw_edit: dict[str, Any]) -> None:
    """Contract 1.2.0: deletes of suppressed revisions have no rev_size; they used to go to
    the dead letter queue with KeyError: 'rev_size'. The record must still match the contract."""
    raw_edit["page_change_kind"] = "delete"
    del raw_edit["revision"]["rev_size"]
    record = to_edit_event(raw_edit)
    assert record["rev_size"] is None
    assert validate(record, EDIT_SCHEMA)
