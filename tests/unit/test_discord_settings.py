from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from pms.config import DiscordSettings, PMSSettings


def test_discord_settings_loads_nested_secret_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PMS_DISCORD__WEBHOOK_URL",
        "https://discord.example/webhooks/abc/secret-token",
    )

    settings = PMSSettings()

    assert settings.discord.require_webhook_url().get_secret_value().endswith(
        "/secret-token"
    )
    assert "secret-token" not in repr(settings.discord)
    assert "**********" in repr(settings.discord)


def test_discord_settings_missing_webhook_fails_when_required() -> None:
    settings = DiscordSettings()

    with pytest.raises(ValidationError, match="webhook_url is required"):
        settings.require_webhook_url()


def test_discord_settings_rejects_non_url() -> None:
    with pytest.raises(ValidationError):
        DiscordSettings.model_validate({"webhook_url": "not-a-url"})
