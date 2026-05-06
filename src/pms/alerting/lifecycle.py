from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from pms.alerting.discord import _write_fallback


logger = logging.getLogger(__name__)


class AlertClient(Protocol):
    async def send(
        self,
        content: str,
        *,
        embed: dict[str, object] | None = None,
    ) -> bool: ...


async def emit_shutdown_alert(
    client: AlertClient,
    *,
    reason: str,
    alert_dir: str | Path = ".alerts",
    timeout_s: float = 10.0,
) -> bool:
    content = f"pms-api exiting ({reason})"
    try:
        return await asyncio.wait_for(client.send(content), timeout=timeout_s)
    except TimeoutError:
        _write_shutdown_fallback(alert_dir, reason=reason, error="timeout")
        logger.error("Shutdown alert timed out; wrote dropped shutdown fallback")
        return False
    except Exception as exc:
        _write_shutdown_fallback(alert_dir, reason=reason, error=str(exc))
        logger.exception("Shutdown alert failed; wrote dropped shutdown fallback")
        return False


async def flush_alerts(client: object) -> None:
    close = getattr(client, "aclose", None)
    if callable(close):
        await close()


def _write_shutdown_fallback(
    alert_dir: str | Path,
    *,
    reason: str,
    error: str,
) -> Path:
    payload = {
        "reason": reason,
        "error": error,
        "created_at": datetime.now(tz=UTC).isoformat(),
    }
    return _write_fallback(Path(alert_dir), payload, prefix="dropped-shutdown")
