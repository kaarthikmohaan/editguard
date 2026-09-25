import pytest
from pydantic import ValidationError

from editguard.common.config import TARGET_WIKIS, Settings


def test_target_wikis_are_the_eight_from_the_design() -> None:
    assert len(TARGET_WIKIS) == 8
    assert "enwiki" in TARGET_WIKIS


def test_env_vars_override_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTACT_EMAIL", "dev@example.com")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://registry:8081")
    settings = Settings(_env_file=None)
    assert settings.schema_registry_url == "http://registry:8081"


def test_user_agent_names_tool_and_contact(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTACT_EMAIL", "dev@example.com")
    ua = Settings(_env_file=None).user_agent
    assert ua.startswith("EditGuard/")
    assert "dev@example.com" in ua


def test_missing_contact_email_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONTACT_EMAIL", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
