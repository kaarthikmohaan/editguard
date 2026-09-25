import json

import pytest

from editguard.common.logs import configure_logging


def test_logs_are_json_with_service_name(capsys: pytest.CaptureFixture[str]) -> None:
    log = configure_logging("producer")
    log.info("connected", wiki_id="enwiki")
    line = json.loads(capsys.readouterr().out.strip())
    assert line["service"] == "producer"
    assert line["event"] == "connected"
    assert line["wiki_id"] == "enwiki"
    assert line["level"] == "info"
