"""T-LEAK-01: features use only what was known at the moment of the edit."""

import json
from pathlib import Path
from typing import Any

import pytest

from editguard.features.features import POINT_IN_TIME_FIELDS, compute_features
from editguard.producer.parse import to_edit_event

ROOT = Path(__file__).parents[1]


class ReadRecorder(dict):
    """A record that remembers which fields were read."""

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self.read: set[str] = set()

    def __getitem__(self, key: str) -> Any:
        self.read.add(key)
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        self.read.add(key)
        return super().get(key, default)


@pytest.fixture
def record() -> dict[str, Any]:
    return to_edit_event(json.loads((ROOT / "tests/fixtures/page_change_edit.json").read_text()))


def test_features_read_only_point_in_time_fields(record: dict[str, Any]) -> None:
    recorder = ReadRecorder(record)
    compute_features(recorder)
    assert recorder.read <= POINT_IN_TIME_FIELDS, recorder.read - POINT_IN_TIME_FIELDS


def test_point_in_time_fields_are_contract_fields_of_the_event(record: dict[str, Any]) -> None:
    """Every allowed field arrives with the event itself, so it cannot come from the future."""
    assert set(record) >= POINT_IN_TIME_FIELDS


def test_what_happens_after_the_edit_cannot_change_its_features(record: dict[str, Any]) -> None:
    """Later knowledge (this edit was reverted, the editor made 500 more edits, a label) is
    ignored: only fields outside POINT_IN_TIME_FIELDS change, so the features stay the same."""
    later = {
        **record,
        "ingested_at": record["ingested_at"],
        "label": "damaging",
        "reverted_by_rev_id": 999,
        "performer_edit_count_now": (record["performer_edit_count"] or 0) + 500,
    }
    assert compute_features(later) == compute_features(record)
