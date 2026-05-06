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
NextTrigger = Callable[[ZoneInfo, datetime], datetime]


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
        run_report: RunReport | None = None,
        next_trigger_fn: NextTrigger = next_trigger,
    ) -> None:
        self._client = client
        self._clock = clock or (lambda: datetime.now(tz=SHANGHAI_TZ))
        self._report_root = Path(report_root)
        self._run_report = run_report
        self._next_trigger = next_trigger_fn

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            now = self._clock()
            trigger = self._next_trigger(SHANGHAI_TZ, now)
            delay = max((trigger - now.astimezone(SHANGHAI_TZ)).total_seconds(), 0.0)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except TimeoutError:
                try:
                    await run_eod_report_once(
                        self._client,
                        now=self._clock(),
                        report_root=self._report_root,
                        run_report=self._run_report,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("EOD scheduler iteration failed; continuing")


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
        chunks = _paginate_report(content, report_date)
        total = len(chunks)
        for index, chunk in enumerate(chunks, start=1):
            header = _report_header(report_date, index=index, total=total)
            await client.send(
                f"{header}\n\n{chunk}"
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
        "--no-dev",
        "python",
        "scripts/paper-report.py",
        "--date",
        report_date,
    )
    return_code = await process.wait()
    if return_code != 0:
        raise RuntimeError(f"paper-report.py exited {return_code}")


def _report_header(report_date: str, *, index: int, total: int) -> str:
    return f"PMS Daily Report - {report_date} 22:00 CST ({index}/{total})"


def _paginate_report(content: str, report_date: str) -> list[str]:
    total = 1
    while True:
        max_header = _report_header(report_date, index=total, total=total)
        limit = DISCORD_MESSAGE_LIMIT - len(max_header) - 2
        if limit <= 0:
            raise ValueError("Discord report header exceeds message limit")
        chunks = _paginate(content, limit=limit)
        if len(chunks) == total:
            return chunks
        total = len(chunks)


def _paginate(content: str, *, limit: int = DISCORD_MESSAGE_LIMIT) -> list[str]:
    if len(content) <= limit:
        return [content]
    return [
        content[index : index + limit]
        for index in range(0, len(content), limit)
    ]
