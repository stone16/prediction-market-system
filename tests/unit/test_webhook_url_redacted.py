from __future__ import annotations

import logging
from pathlib import Path

import httpx
import pytest

from pms.alerting.discord import DiscordWebhookClient


@pytest.mark.asyncio
async def test_webhook_url_redacted(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    marker = "secret-marker-12345"

    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("failed with raw marker should not leak", request=request)

    client = DiscordWebhookClient(
        f"https://discord.example/webhooks/app/token?token={marker}",
        alert_dir=tmp_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )

    with caplog.at_level(logging.ERROR):
        assert await client.send("payload") is False

    assert marker not in caplog.text
    assert "webhooks/app/token" not in caplog.text
    for dropped in tmp_path.glob("dropped-*.json"):
        content = dropped.read_text()
        assert marker not in content
        assert "webhooks/app/token" not in content
