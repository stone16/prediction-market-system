from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo


logger = logging.getLogger(__name__)
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DISCORD_MESSAGE_LIMIT = 2000


class EODClient(Protocol):
    async def send(
        self,
        content: str,
        *,
        embed: dict[str, object] | None = None,
    ) -> bool: ...


RunReport = Callable[[str], Awaitable[None]]
Clock = Callable[[], datetime]


def next_trigger(tz: ZoneInfo, now: datetime) -> datetime:
    local_now = now.astimezone(tz)
    candidate = datetime.combine(local_now.date(), time(22, 0), tzinfo=tz)
    if local_now >= candidate:
        candidate = candidate + timedelta(days=1)
    return candidate


class EODScheduler:
    def __init__(
        self,
        client: EODClient,
        *,
        clock: Clock | None = None,
        report_root: str | Path = "docs/paper-reports",
    ) -> None:
        self._client = client
        self._clock = clock or (lambda: datetime.now(tz=SHANGHAI_TZ))
        self._report_root = Path(report_root)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            now = self._clock()
            trigger = next_trigger(SHANGHAI_TZ, now)
            delay = max((trigger - now.astimezone(SHANGHAI_TZ)).total_seconds(), 0.0)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except TimeoutError:
                await run_eod_report_once(
                    self._client,
                    now=self._clock(),
                    report_root=self._report_root,
                )


async def run_eod_report_once(
    client: EODClient,
    *,
    now: datetime,
    report_root: str | Path = "docs/paper-reports",
    run_report: RunReport | None = None,
) -> None:
    report_date = now.astimezone(SHANGHAI_TZ).date().isoformat()
    runner = run_report or _run_paper_report
    try:
        await runner(report_date)
        report_path = Path(report_root) / f"{report_date}.md"
        if not report_path.exists():
            raise FileNotFoundError(str(report_path))
        content = report_path.read_text(encoding="utf-8")
        for index, chunk in enumerate(_paginate(content), start=1):
            total = len(_paginate(content))
            await client.send(
                f"PMS Daily Report - {report_date} 22:00 CST ({index}/{total})\n\n"
                f"{chunk}"
            )
    except Exception as exc:
        message = (
            f"EOD report generation failed for {report_date} - {exc}.\n"
            "Severity: warning.\n"
            "Operator action: investigate manually."
        )
        await client.send(message)
        logger.error("EOD report generation failed for %s: %s", report_date, exc)
        raise


async def _run_paper_report(report_date: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "uv",
        "run",
        "python",
        "scripts/paper-report.py",
        "--date",
        report_date,
    )
    return_code = await process.wait()
    if return_code != 0:
        raise RuntimeError(f"paper-report.py exited {return_code}")


def _paginate(content: str) -> list[str]:
    if len(content) <= DISCORD_MESSAGE_LIMIT:
        return [content]
    return [
        content[index : index + DISCORD_MESSAGE_LIMIT]
        for index in range(0, len(content), DISCORD_MESSAGE_LIMIT)
    ]
