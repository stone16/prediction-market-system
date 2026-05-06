from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from pms.alerting.scheduler import run_eod_report_once


class RecordingClient:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(
        self,
        content: str,
        *,
        embed: dict[str, object] | None = None,
    ) -> bool:
        del embed
        self.messages.append(content)
        return True


@pytest.mark.asyncio
async def test_eod_subprocess_failure_posts_warning_and_reraises(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = RecordingClient()

    async def failing_runner(report_date: str) -> None:
        del report_date
        raise RuntimeError("paper-report exploded")

    with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
        await run_eod_report_once(
            client,
            now=datetime(2026, 5, 6, 22, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            report_root=tmp_path,
            run_report=failing_runner,
        )

    assert "EOD report generation failed for 2026-05-06" in client.messages[0]
    assert "warning" in client.messages[0].lower()
    assert "paper-report exploded" in caplog.text
