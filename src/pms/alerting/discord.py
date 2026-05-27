"""DiscordWebhookClient - frozen public API for v1+.

`send(content: str, *, embed: dict | None = None) -> bool` is the stable
entrypoint. Family E (`pms-funding-runbook-v1`) imports this signature for
balance-monitor alerts; breaking changes require a v2 spec and coordination
with all consumers.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import stat
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import SecretStr


logger = logging.getLogger(__name__)
SleepFunc = Callable[[float], Awaitable[None] | None]


class DiscordWebhookClient:
    def __init__(
        self,
        webhook_url: str | SecretStr,
        *,
        alert_dir: str | Path = ".alerts",
        http_client: httpx.AsyncClient | None = None,
        sleep: SleepFunc = asyncio.sleep,
        max_retry_after_s: float = 60.0,
    ) -> None:
        self._webhook_url = (
            webhook_url.get_secret_value()
            if isinstance(webhook_url, SecretStr)
            else webhook_url
        )
        self._alert_dir = Path(alert_dir)
        self._client = http_client
        self._owns_client = http_client is None
        self._sleep = sleep
        self._max_retry_after_s = max_retry_after_s

    async def send(
        self,
        content: str,
        *,
        embed: dict[str, object] | None = None,
    ) -> bool:
        payload: dict[str, Any] = {"content": content}
        if embed is not None:
            payload["embeds"] = [embed]

        delays = [1.0, 2.0]
        client = self._client or httpx.AsyncClient(timeout=10.0)
        try:
            for attempt in range(3):
                try:
                    response = await client.post(self._webhook_url, json=payload)
                except httpx.HTTPError as exc:
                    if attempt == 2:
                        return await self._drop(payload, exc)
                    await self._sleep_for(delays[attempt])
                    continue

                if 200 <= response.status_code < 300:
                    return True

                retry_after = _retry_after(response)
                if response.status_code == 429 and retry_after is not None:
                    if retry_after > self._max_retry_after_s:
                        return await self._drop(
                            payload,
                            RuntimeError("discord retry-after exceeded max wait"),
                        )
                    if attempt == 2:
                        return await self._drop(
                            payload,
                            RuntimeError("discord 429 retry exhausted"),
                        )
                    await self._sleep_for(retry_after)
                    continue

                if attempt == 2:
                    return await self._drop(
                        payload,
                        RuntimeError(f"discord webhook returned {response.status_code}"),
                    )
                await self._sleep_for(delays[attempt])
        finally:
            if self._owns_client:
                await client.aclose()
        return False

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def write_fallback(
        self,
        payload: dict[str, Any],
        *,
        prefix: str = "dropped",
    ) -> Path:
        return _write_fallback(self._alert_dir, payload, prefix=prefix)

    async def _sleep_for(self, delay: float) -> None:
        slept = self._sleep(delay)
        if slept is not None:
            await slept

    async def _drop(self, payload: dict[str, Any], exc: BaseException) -> bool:
        path = await self.write_fallback(payload)
        logger.error(
            "Discord webhook send failed; wrote fallback=%s error_type=%s",
            path,
            type(exc).__name__,
        )
        return False


def _write_fallback(alert_dir: Path, payload: dict[str, Any], *, prefix: str) -> Path:
    _prepare_alert_dir(alert_dir)
    timestamp = datetime.now(tz=UTC).isoformat().replace(":", "-")
    target = alert_dir / f"{prefix}-{timestamp}.json"
    tmp = target.with_suffix(".tmp")
    sanitized = _sanitize_payload(payload)
    published = False
    try:
        _write_tmp_text_no_follow(tmp, json.dumps(sanitized, sort_keys=True))
        tmp.replace(target)
        published = True
        _fsync_parent_directory(alert_dir)
    finally:
        if not published:
            _unlink_regular_single_link_file_if_present(tmp)
    return target


def _prepare_alert_dir(alert_dir: Path) -> None:
    try:
        path_stat = alert_dir.lstat()
    except FileNotFoundError:
        alert_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        os.chmod(alert_dir, 0o700)
        return
    if not stat.S_ISDIR(path_stat.st_mode):
        raise OSError(f"alert fallback directory is not a directory: {alert_dir}")
    permissions = stat.S_IMODE(path_stat.st_mode)
    if permissions & 0o077:
        raise OSError(
            f"alert fallback directory {alert_dir} is too permissive; "
            "run `chmod 700`."
        )
    if not permissions & stat.S_IWUSR:
        raise OSError(
            f"alert fallback directory {alert_dir} is not owner-writable; "
            "run `chmod 700`."
        )


def _write_tmp_text_no_follow(path: Path, content: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise OSError(f"alert fallback temporary path already exists: {path}") from exc
    try:
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(f"alert fallback temporary path is not a regular file: {path}")
        if path_stat.st_nlink != 1:
            raise OSError(
                f"alert fallback temporary path is not a single-link file: {path}"
            )
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            fd = -1
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
    finally:
        if fd >= 0:
            os.close(fd)


def _unlink_regular_single_link_file_if_present(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_nlink != 1:
        return
    path.unlink()


def _fsync_parent_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        return
    finally:
        os.close(fd)


def _sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    def sanitize(value: Any) -> Any:
        if isinstance(value, str):
            return _redact_url_like(value)
        if isinstance(value, dict):
            return {key: sanitize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [sanitize(item) for item in value]
        return value

    return {key: sanitize(value) for key, value in payload.items()}


def _redact_url_like(value: str) -> str:
    if "http://" not in value and "https://" not in value:
        return value
    return value.replace(value, "<redacted-url>")


def _retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        parsed = float(value.strip())
    except ValueError:
        return None
    if parsed < 0:
        return None
    return parsed


def redact_webhook_url(url: str) -> str:
    split = urlsplit(url)
    return urlunsplit((split.scheme, split.netloc, "/webhooks/<redacted>", "", ""))
