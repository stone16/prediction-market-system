from __future__ import annotations

import asyncio
from collections.abc import Iterable

from pms.core.interfaces import ISensor
from pms.core.models import MarketSignal


class SensorStream:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[MarketSignal] = asyncio.Queue()
        self._tasks: tuple[asyncio.Task[None], ...] = ()

    @property
    def tasks(self) -> tuple[asyncio.Task[None], ...]:
        return self._tasks

    async def start(self, sensors: Iterable[ISensor]) -> None:
        if self._tasks:
            msg = "SensorStream is already started"
            raise RuntimeError(msg)
        self._tasks = tuple(
            asyncio.create_task(self._consume(sensor)) for sensor in sensors
        )

    async def stop(self, *, timeout: float = 5.0) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                timeout=timeout,
            )
        self._tasks = ()

    async def _consume(self, sensor: ISensor) -> None:
        async for signal in sensor:
            await self.queue.put(signal)
