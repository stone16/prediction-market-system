from __future__ import annotations

import json
import logging
import os
import stat
from datetime import UTC, datetime, tzinfo
from pathlib import Path
from types import TracebackType
from typing import IO, cast

import httpx
import pytest

from pms.alerting import discord
from pms.alerting.discord import DiscordWebhookClient


class _FailingTextWriter:
    def __init__(self, wrapped: IO[str]) -> None:
        self._wrapped = wrapped

    def __enter__(self) -> "_FailingTextWriter":
        self._wrapped.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._wrapped.__exit__(exc_type, exc, traceback)

    def write(self, content: str) -> int:
        self._wrapped.write(content)
        raise OSError("simulated fallback write failure")


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
async def test_discord_client_ignores_negative_retry_after(tmp_path: Path) -> None:
    attempts = 0
    sleeps: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        del request
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "-1"})
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
    assert sleeps == [1.0]


@pytest.mark.asyncio
async def test_discord_client_redacts_nested_fallback_values(tmp_path: Path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(500)

    client = DiscordWebhookClient(
        "https://discord.example/webhooks/app/token",
        alert_dir=tmp_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )

    assert await client.send(
        "payload",
        embed={
            "description": "see https://discord.example/webhooks/app/token",
            "fields": [
                {"value": "http://discord.example/webhooks/app/token"},
            ],
        },
    ) is False

    [dropped] = list(tmp_path.glob("dropped-*.json"))
    content = dropped.read_text(encoding="utf-8")
    assert "webhooks/app/token" not in content
    assert "<redacted-url>" in content


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


def test_discord_fallback_refuses_preexisting_symlink_tmp_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FixedDatetime:
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            del cls, tz
            return datetime(2026, 5, 27, 12, 0, tzinfo=UTC)

    target_path = tmp_path / "target-alert.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    tmp_path_for_fallback = tmp_path / "dropped-2026-05-27T12-00-00+00-00.tmp"
    tmp_path_for_fallback.symlink_to(target_path)
    monkeypatch.setattr(discord, "datetime", FixedDatetime)

    with pytest.raises(OSError, match="temporary path"):
        discord._write_fallback(tmp_path, {"content": "payload"}, prefix="dropped")

    assert target_path.read_text(encoding="utf-8") == (
        "target must not be overwritten\n"
    )
    assert os.readlink(tmp_path_for_fallback) == str(target_path)


def test_discord_fallback_refuses_symlink_alert_directory(tmp_path: Path) -> None:
    real_alert_dir = tmp_path / "real-alerts"
    real_alert_dir.mkdir()
    alert_dir = tmp_path / "alerts"
    alert_dir.symlink_to(real_alert_dir)

    with pytest.raises(OSError, match="alert fallback directory"):
        discord._write_fallback(alert_dir, {"content": "payload"}, prefix="dropped")

    assert list(real_alert_dir.iterdir()) == []


def test_discord_fallback_rejects_permissive_alert_directory(tmp_path: Path) -> None:
    alert_dir = tmp_path / "alerts"
    alert_dir.mkdir()
    alert_dir.chmod(0o777)

    with pytest.raises(OSError, match="too permissive"):
        discord._write_fallback(alert_dir, {"content": "payload"}, prefix="dropped")


def test_discord_fallback_creates_private_alert_directory(tmp_path: Path) -> None:
    alert_dir = tmp_path / "alerts"

    discord._write_fallback(alert_dir, {"content": "payload"}, prefix="dropped")

    assert stat.S_IMODE(alert_dir.stat().st_mode) == 0o700


def test_discord_fallback_fsyncs_file_and_parent_before_return(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_fsync_targets: list[str] = []

    def recording_fsync(fd: int) -> None:
        fd_stat = os.fstat(fd)
        if stat.S_ISDIR(fd_stat.st_mode):
            observed_fsync_targets.append("directory")
        elif stat.S_ISREG(fd_stat.st_mode):
            observed_fsync_targets.append("file")
        else:
            observed_fsync_targets.append("other")

    monkeypatch.setattr(os, "fsync", recording_fsync)

    path = discord._write_fallback(tmp_path, {"content": "payload"}, prefix="dropped")

    assert path.exists()
    assert observed_fsync_targets == ["file", "directory"]


def test_discord_fallback_removes_temp_file_when_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FixedDatetime:
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            del cls, tz
            return datetime(2026, 5, 27, 12, 0, tzinfo=UTC)

    real_fdopen = os.fdopen

    def failing_fdopen(
        fd: int,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        closefd: bool = True,
        opener: object | None = None,
    ) -> object:
        del opener
        file = real_fdopen(fd, mode, buffering, encoding, errors, newline, closefd)
        if "w" in mode:
            return _FailingTextWriter(cast(IO[str], file))
        return file

    monkeypatch.setattr(discord, "datetime", FixedDatetime)
    monkeypatch.setattr(os, "fdopen", failing_fdopen)

    with pytest.raises(OSError, match="simulated fallback write failure"):
        discord._write_fallback(tmp_path, {"content": "payload"}, prefix="dropped")

    assert not (tmp_path / "dropped-2026-05-27T12-00-00+00-00.json").exists()
    assert not (tmp_path / "dropped-2026-05-27T12-00-00+00-00.tmp").exists()
