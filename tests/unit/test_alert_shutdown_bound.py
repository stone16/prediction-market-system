from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest

from pms.alerting.lifecycle import emit_shutdown_alert


class HangingClient:
    async def send(
        self,
        content: str,
        *,
        embed: dict[str, object] | None = None,
    ) -> bool:
        del content, embed
        await asyncio.sleep(60)
        return True


@pytest.mark.asyncio
async def test_alert_shutdown_bound(tmp_path: Path) -> None:
    started = time.monotonic()

    delivered = await emit_shutdown_alert(
        HangingClient(),
        reason="SIGTERM",
        alert_dir=tmp_path,
        timeout_s=0.05,
    )

    assert delivered is False
    assert time.monotonic() - started < 0.5
    dropped = list(tmp_path.glob("dropped-shutdown-*.json"))
    assert len(dropped) == 1
    payload = json.loads(dropped[0].read_text())
    assert payload["reason"] == "SIGTERM"
