import copy
import json
from pathlib import Path
from typing import Any

import pytest
from fastavro import parse_schema
from fastavro.validation import validate

from editguard.producer.parse import to_baseline_score
from editguard.producer.streams import STREAMS

ROOT = Path(__file__).parents[1]
SCHEMA = parse_schema(json.loads(STREAMS["baseline"].schema_path.read_text()))


@pytest.fixture
def raw_prediction() -> dict[str, Any]:
    return json.loads((ROOT / "tests/fixtures/revert_risk_prediction.json").read_text())


def test_maps_prediction_to_contract(raw_prediction: dict[str, Any]) -> None:
    record = to_baseline_score(raw_prediction)
    assert record == {
        "wiki_id": "enwiki",
        "rev_id": 2549252841,
        "model_name": "revertrisk-language-agnostic",
        "model_version": "3",
        "probability_true": 0.11478381603956223,
    }
    assert validate(record, SCHEMA)


def test_baseline_key_is_wiki_and_revision(raw_prediction: dict[str, Any]) -> None:
    record = to_baseline_score(raw_prediction)
    assert STREAMS["baseline"].key(record) == b"enwiki:2549252841"


def test_missing_prediction_raises(raw_prediction: dict[str, Any]) -> None:
    broken = copy.deepcopy(raw_prediction)
    del broken["predicted_classification"]
    with pytest.raises(KeyError):
        to_baseline_score(broken)
