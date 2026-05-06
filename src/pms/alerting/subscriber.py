from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Protocol

from pms.alerting.events import alert_from_halt, halt_event_from_runtime
from pms.event_stream import RuntimeEvent, RuntimeEventBus


class AlertSender(Protocol):
    async def send(
        self,
        content: str,
        *,
        embed: dict[str, object] | None = None,
    ) -> bool: ...


async def run_alerting_subscription(
    event_bus: RuntimeEventBus,
    client: AlertSender,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    replay, queue = await event_bus.subscribe()
    stop = stop_event or asyncio.Event()
    try:
        for event in replay:
            await _deliver_if_halt(event, client)
        while not stop.is_set():
            get_task = asyncio.create_task(queue.get())
            stop_task = asyncio.create_task(stop.wait())
            done, pending = await asyncio.wait(
                {get_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            if stop_task in done:
                return
            event = get_task.result()
            await _deliver_if_halt(event, client)
    finally:
        await event_bus.unsubscribe(queue)


async def _deliver_if_halt(event: RuntimeEvent, client: AlertSender) -> None:
    halt = halt_event_from_runtime(event)
    if halt is None:
        return
    alert = alert_from_halt(halt)
    await client.send(alert.message)
