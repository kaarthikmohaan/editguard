"""Turn a raw page_change event into an EditEvent record matching contracts/edits.odcs.yaml.

Pure functions only, so they are unit-testable without Kafka or the network.
"""

import json
from datetime import UTC, datetime
from typing import Any

from editguard.common.config import TARGET_WIKIS


def is_target(event: dict[str, Any]) -> bool:
    """True if the event belongs to one of the 8 target wikis."""
    return event.get("wiki_id") in TARGET_WIKIS


def parse_ts(value: str | None) -> datetime | None:
    """Parse an ISO 8601 UTC timestamp such as 2026-09-25T12:53:02Z."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_edit_event(event: dict[str, Any], ingested_at: datetime | None = None) -> dict[str, Any]:
    """Map one upstream event to an EditEvent record. Raises KeyError on missing fields."""
    meta = event["meta"]
    page = event["page"]
    rev = event["revision"]
    performer = event.get("performer") or {}
    revert = rev.get("revert") or {}
    prior_rev = (event.get("prior_state") or {}).get("revision") or {}

    return {
        "event_id": meta["id"],
        "event_time": parse_ts(rev["rev_dt"]),
        "emitted_at": parse_ts(meta["dt"]),
        "ingested_at": ingested_at or datetime.now(UTC),
        "wiki_id": event["wiki_id"],
        "page_id": page["page_id"],
        "page_title": page["page_title"],
        "namespace_id": page["namespace_id"],
        "page_change_kind": event["page_change_kind"],
        "rev_id": rev["rev_id"],
        "rev_parent_id": rev.get("rev_parent_id"),
        "rev_size": rev["rev_size"],
        "prior_rev_size": prior_rev.get("rev_size"),
        "is_minor_edit": rev["is_minor_edit"],
        "comment": rev.get("comment"),
        "is_content_visible": rev["is_content_visible"],
        "is_comment_visible": rev["is_comment_visible"],
        "is_editor_visible": rev["is_editor_visible"],
        "performer_user_text": performer.get("user_text"),
        "performer_is_bot": performer.get("is_bot", False),
        "performer_is_temp": performer.get("is_temp", False),
        "performer_groups": performer.get("groups"),
        "performer_edit_count": performer.get("edit_count"),
        "performer_registration_dt": parse_ts(performer.get("registration_dt")),
        "revert_method": revert.get("method"),
        "rev_reverted_oldest_id": revert.get("rev_reverted_oldest_id"),
        "rev_reverted_newest_id": revert.get("rev_reverted_newest_id"),
        "rev_original_id": revert.get("rev_original_id"),
        "upstream_topic": meta["topic"],
        "upstream_partition": meta["partition"],
        "upstream_offset": meta["offset"],
        "schema_version": event["$schema"],
        "raw_json": json.dumps(event, ensure_ascii=False, separators=(",", ":")),
    }
