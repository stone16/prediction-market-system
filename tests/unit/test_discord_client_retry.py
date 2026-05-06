from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx
import pytest

from pms.alerting.discord import DiscordWebhookClient


@pytest.mark.asyncio
async def test_discord_client_posts_successfully(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    client = DiscordWebhookClient(
        "https://discord.example/webhooks/app/token",
        alert_dir=tmp_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    assert await client.send("hello") is True
    assert len(requests) == 1
    assert json.loads(requests[0].content) == {"content": "hello"}


@pytest.mark.asyncio
async def test_discord_client_retries_then_writes_fallback(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    attempts = 0
    sleeps: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        del request
        attempts += 1
        return httpx.Response(500)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    client = DiscordWebhookClient(
        "https://discord.example/webhooks/app/token",
        alert_dir=tmp_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        sleep=sleep,
    )

    with caplog.at_level(logging.ERROR):
        assert await client.send("payload") is False

    assert attempts == 3
    assert sleeps == [1.0, 2.0]
    dropped = list(tmp_path.glob("dropped-*.json"))
    assert len(dropped) == 1
    assert json.loads(dropped[0].read_text())["content"] == "payload"
    assert "webhooks/app/token" not in caplog.text


@pytest.mark.asyncio
async def test_discord_client_respects_short_retry_after(tmp_path: Path) -> None:
    attempts = 0
    sleeps: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        del request
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(204)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    client = DiscordWebhookClient(
        "https://discord.example/webhooks/app/token",
        alert_dir=tmp_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        sleep=sleep,
    )

    assert await client.send("payload") is True
    assert attempts == 2
    assert sleeps == [5.0]


@pytest.mark.asyncio
async def test_discord_client_long_retry_after_falls_back(tmp_path: Path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(429, headers={"Retry-After": "120"})

    client = DiscordWebhookClient(
        "https://discord.example/webhooks/app/token",
        alert_dir=tmp_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    assert await client.send("payload") is False
    assert len(list(tmp_path.glob("dropped-*.json"))) == 1


@pytest.mark.asyncio
async def test_discord_client_network_partition_falls_back(tmp_path: Path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("network partition", request=request)

    client = DiscordWebhookClient(
        "https://discord.example/webhooks/app/token",
        alert_dir=tmp_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )

    assert await client.send("payload") is False
    assert len(list(tmp_path.glob("dropped-*.json"))) == 1
