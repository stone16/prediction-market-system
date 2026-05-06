from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from pms.alerting.scheduler import EODScheduler, run_eod_report_once


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


class StopAfterSendClient(RecordingClient):
    def __init__(self, stop_event: asyncio.Event) -> None:
        super().__init__()
        self._stop_event = stop_event

    async def send(
        self,
        content: str,
        *,
        embed: dict[str, object] | None = None,
    ) -> bool:
        result = await super().send(content, embed=embed)
        self._stop_event.set()
        return result


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


@pytest.mark.asyncio
async def test_eod_scheduler_continues_after_report_failure(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    stop_event = asyncio.Event()
    client = StopAfterSendClient(stop_event)
    attempts = 0

    async def failing_runner(report_date: str) -> None:
        nonlocal attempts
        attempts += 1
        assert report_date == "2026-05-06"
        raise RuntimeError("transient report failure")

    scheduler = EODScheduler(
        client,
        clock=lambda: datetime(2026, 5, 6, 22, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        report_root=tmp_path,
        run_report=failing_runner,
        next_trigger_fn=lambda tz, now: now.astimezone(tz),
    )

    with caplog.at_level(logging.ERROR):
        await asyncio.wait_for(scheduler.run(stop_event), timeout=1.0)

    assert attempts == 1
    assert "EOD report generation failed for 2026-05-06" in client.messages[0]
    assert "EOD scheduler iteration failed; continuing" in caplog.text


@pytest.mark.asyncio
async def test_paper_report_subprocess_uses_no_dev(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: list[str] = []

    class _Process:
        async def wait(self) -> int:
            return 0

    async def fake_create_subprocess_exec(*args: str) -> _Process:
        captured.extend(args)
        return _Process()

    monkeypatch.setattr(
        asyncio,
        "create_subprocess_exec",
        fake_create_subprocess_exec,
    )
    (tmp_path / "2026-05-06.md").write_text("report", encoding="utf-8")

    await run_eod_report_once(
        RecordingClient(),
        now=datetime(2026, 5, 6, 22, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        report_root=tmp_path,
        run_report=None,
    )

    assert captured[:3] == ["uv", "run", "--no-dev"]
