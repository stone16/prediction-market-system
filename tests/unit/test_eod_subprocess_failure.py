from __future__ import annotations

import asyncio
import logging
import os
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
async def test_eod_report_rejects_symlink_report_file(tmp_path: Path) -> None:
    report_date = "2026-05-06"
    target_path = tmp_path / "sensitive.md"
    target_path.write_text("sensitive report content", encoding="utf-8")
    report_path = tmp_path / f"{report_date}.md"
    report_path.symlink_to(target_path)
    client = RecordingClient()

    async def successful_runner(candidate_date: str) -> None:
        assert candidate_date == report_date

    with pytest.raises(OSError, match="cannot be read safely"):
        await run_eod_report_once(
            client,
            now=datetime(2026, 5, 6, 22, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            report_root=tmp_path,
            run_report=successful_runner,
        )

    assert len(client.messages) == 1
    assert "EOD report generation failed for 2026-05-06" in client.messages[0]
    assert "sensitive report content" not in client.messages[0]


@pytest.mark.asyncio
async def test_eod_report_opens_report_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    report_date = "2026-05-06"
    report_path = tmp_path / f"{report_date}.md"
    report_path.write_text("daily report", encoding="utf-8")
    observed: list[tuple[Path, int]] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        observed.append((Path(os.fsdecode(os.fspath(path_arg))), flags))
        return real_open(path_arg, flags, mode)

    async def successful_runner(candidate_date: str) -> None:
        assert candidate_date == report_date

    monkeypatch.setattr(os, "open", recording_open)
    client = RecordingClient()

    await run_eod_report_once(
        client,
        now=datetime(2026, 5, 6, 22, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        report_root=tmp_path,
        run_report=successful_runner,
    )

    observed_by_path = {path: flags for path, flags in observed}
    assert observed_by_path[report_path] & no_follow_flag
    assert "daily report" in client.messages[0]


@pytest.mark.asyncio
async def test_eod_report_rejects_hardlink_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_date = "2026-05-06"
    report_path = tmp_path / f"{report_date}.md"
    report_path.write_text("daily report", encoding="utf-8")
    replacement_source = tmp_path / "replacement.md"
    replacement_source.write_text("replacement report content", encoding="utf-8")
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == report_path and not swapped:
            swapped = True
            report_path.unlink()
            os.link(replacement_source, report_path)
        return real_open(path_arg, flags, mode)

    async def successful_runner(candidate_date: str) -> None:
        assert candidate_date == report_date

    monkeypatch.setattr(os, "open", swapping_open)
    client = RecordingClient()

    with pytest.raises(OSError, match="cannot be read safely"):
        await run_eod_report_once(
            client,
            now=datetime(2026, 5, 6, 22, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            report_root=tmp_path,
            run_report=successful_runner,
        )

    assert swapped is True
    assert len(client.messages) == 1
    assert "replacement report content" not in client.messages[0]


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


@pytest.mark.asyncio
async def test_eod_report_pages_reserve_space_for_discord_header(tmp_path: Path) -> None:
    report_date = "2026-05-06"
    (tmp_path / f"{report_date}.md").write_text("x" * 4500, encoding="utf-8")
    client = RecordingClient()

    async def successful_runner(candidate_date: str) -> None:
        assert candidate_date == report_date

    await run_eod_report_once(
        client,
        now=datetime(2026, 5, 6, 22, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        report_root=tmp_path,
        run_report=successful_runner,
    )

    assert len(client.messages) > 1
    assert all(len(message) <= 2000 for message in client.messages)
    assert client.messages[0].startswith(
        "PMS Daily Report - 2026-05-06 22:00 CST (1/"
    )
