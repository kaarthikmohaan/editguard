import httpx

from editguard.producer.sse import iter_events

SSE_BODY = (
    ":ok\n\n"
    'event: message\nid: [{"offset":1}]\ndata: {"wiki_id": "enwiki"}\n\n'
    'event: message\nid: [{"offset":2}]\ndata: {"wiki_id": "hiwiki"}\n\n'
)


def make_client(seen_headers: dict[str, str]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.update(request.headers)
        return httpx.Response(200, text=SSE_BODY, headers={"content-type": "text/event-stream"})

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_yields_parsed_events_with_ids() -> None:
    events = list(iter_events(make_client({}), "mediawiki.page_change.v1"))
    assert [e.data["wiki_id"] for e in events] == ["enwiki", "hiwiki"]
    assert events[1].id == '[{"offset":2}]'


def test_sends_last_event_id_when_resuming() -> None:
    headers: dict[str, str] = {}
    list(iter_events(make_client(headers), "mediawiki.page_change.v1", '[{"offset":1}]'))
    assert headers["last-event-id"] == '[{"offset":1}]'


def test_sends_since_for_a_replay() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, text=SSE_BODY, headers={"content-type": "text/event-stream"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    list(iter_events(client, "mediawiki.page_change.v1", since="2026-09-26T10:00:00Z"))
    assert seen["url"].endswith("/mediawiki.page_change.v1?since=2026-09-26T10%3A00%3A00Z")
