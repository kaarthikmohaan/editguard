"""The two upstream streams EditGuard records, and where each one goes (design section 9)."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from editguard.producer.parse import to_baseline_score, to_edit_event

CONTRACTS = Path(__file__).parents[3] / "contracts/generated"


@dataclass(frozen=True)
class StreamSpec:
    """How to read one EventStreams stream and write it to one Kafka topic."""

    name: str
    topic: str
    schema_path: Path
    parse: Callable[[dict[str, Any]], dict[str, Any]]
    key_fields: tuple[str, str]

    def key(self, record: dict[str, Any]) -> bytes:
        """Kafka message key, e.g. enwiki:12345."""
        first, second = self.key_fields
        return f"{record[first]}:{record[second]}".encode()


STREAMS = {
    "edits": StreamSpec(
        name="mediawiki.page_change.v1",
        topic="edits.raw.v1",
        schema_path=CONTRACTS / "edits.avsc",
        parse=to_edit_event,
        key_fields=("wiki_id", "page_id"),
    ),
    "baseline": StreamSpec(
        name="mediawiki.page_revert_risk_prediction_change.v1",
        topic="baseline.raw.v1",
        schema_path=CONTRACTS / "baseline_scores.avsc",
        parse=to_baseline_score,
        key_fields=("wiki_id", "rev_id"),
    ),
}
