from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

Fallback = Callable[[], Awaitable[None] | None]


class SensorWatchdog:
    def __init__(self, *, timeout_s: float = 120.0, fallback: Fallback) -> None:
        self.timeout_s = timeout_s
        self._fallback = fallback
        self._reset_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._fallback_started = False

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    def notify_message(self) -> None:
        self._reset_event.set()

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._reset_event.wait(), timeout=self.timeout_s)
                self._reset_event.clear()
            except TimeoutError:
                if not self._fallback_started:
                    self._fallback_started = True
                    result = self._fallback()
                    if result is not None:
                        await result
