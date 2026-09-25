"""Minimal client for Wikimedia EventStreams (Server-Sent Events)."""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import httpx
from httpx_sse import connect_sse

STREAM_BASE_URL = "https://stream.wikimedia.org/v2/stream"


@dataclass(frozen=True)
class StreamEvent:
    """One event from the stream: its resume ID and parsed JSON body."""

    id: str
    data: dict[str, Any]


def iter_events(
    client: httpx.Client, stream: str, last_event_id: str | None = None
) -> Iterator[StreamEvent]:
    """Yield events from one stream, resuming after last_event_id if given."""
    headers = {"Last-Event-ID": last_event_id} if last_event_id else {}
    url = f"{STREAM_BASE_URL}/{stream}"
    with connect_sse(client, "GET", url, headers=headers) as source:
        source.response.raise_for_status()
        for sse in source.iter_sse():
            if sse.event != "message" or not sse.data:
                continue
            yield StreamEvent(id=sse.id, data=json.loads(sse.data))
