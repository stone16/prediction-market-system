from __future__ import annotations

import pytest
from pydantic import ValidationError

from pms.config import LLMSettings


def test_llm_settings_disabled_skips_validation() -> None:
    """Disabled config accepts any shape; preserves default-off semantics."""
    settings = LLMSettings(enabled=False)
    assert settings.enabled is False
    assert settings.provider is None
    assert settings.api_key is None
    assert settings.base_url is None


def test_llm_settings_requires_provider_when_enabled() -> None:
    with pytest.raises(ValidationError, match="provider"):
        LLMSettings(enabled=True, api_key="sk-x")


def test_llm_settings_requires_api_key_when_enabled() -> None:
    with pytest.raises(ValidationError, match="api_key"):
        LLMSettings(enabled=True, provider="anthropic")


def test_llm_settings_openai_allows_default_base_url() -> None:
    """OpenAI uses the SDK default endpoint unless a gateway URL is provided."""
    settings = LLMSettings(enabled=True, provider="openai", api_key="sk-x")
    assert settings.base_url is None
    settings_with_base = LLMSettings(
        enabled=True,
        provider="openai",
        api_key="sk-x",
        base_url="https://gateway.example/v1",
    )
    assert settings_with_base.base_url == "https://gateway.example/v1"


def test_llm_settings_anthropic_optional_base_url() -> None:
    """Anthropic provider does not require base_url (SDK default)."""
    settings = LLMSettings(enabled=True, provider="anthropic", api_key="sk-x")
    assert settings.base_url is None
    settings_with_base = LLMSettings(
        enabled=True,
        provider="anthropic",
        api_key="sk-x",
        base_url="https://gateway.example/v1",
    )
    assert settings_with_base.base_url == "https://gateway.example/v1"
