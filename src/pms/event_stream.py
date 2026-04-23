from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class RuntimeEvent:
    event_id: int
    event_type: str
    created_at: datetime
    summary: str
    market_id: str | None = None
    decision_id: str | None = None
    fill_id: str | None = None


class RuntimeEventBus:
    def __init__(
        self,
        *,
        buffer_limit: int = 200,
        subscriber_queue_limit: int = 200,
    ) -> None:
        self._buffer_limit = buffer_limit
        self._subscriber_queue_limit = subscriber_queue_limit
        self._events: list[RuntimeEvent] = []
        self._subscribers: set[asyncio.Queue[RuntimeEvent]] = set()
        self._next_event_id = 1
        self._lock = asyncio.Lock()

    async def publish(
        self,
        event_type: str,
        summary: str,
        *,
        created_at: datetime | None = None,
        market_id: str | None = None,
        decision_id: str | None = None,
        fill_id: str | None = None,
    ) -> RuntimeEvent:
        created = created_at or datetime.now(tz=UTC)
        async with self._lock:
            event = RuntimeEvent(
                event_id=self._next_event_id,
                event_type=event_type,
                created_at=created,
                summary=summary,
                market_id=market_id,
                decision_id=decision_id,
                fill_id=fill_id,
            )
            self._next_event_id += 1
            self._events.append(event)
            overflow = len(self._events) - self._buffer_limit
            if overflow > 0:
                del self._events[:overflow]
            subscribers = tuple(self._subscribers)

        for subscriber in subscribers:
            _offer_event(subscriber, event)
        return event

    async def subscribe(
        self,
        *,
        last_event_id: int | None = None,
    ) -> tuple[list[RuntimeEvent], asyncio.Queue[RuntimeEvent]]:
        subscriber: asyncio.Queue[RuntimeEvent] = asyncio.Queue(
            maxsize=self._subscriber_queue_limit
        )
        async with self._lock:
            replay = [
                event
                for event in self._events
                if last_event_id is not None and event.event_id > last_event_id
            ]
            self._subscribers.add(subscriber)
        return replay, subscriber

    async def unsubscribe(self, subscriber: asyncio.Queue[RuntimeEvent]) -> None:
        async with self._lock:
            self._subscribers.discard(subscriber)


def _offer_event(
    subscriber: asyncio.Queue[RuntimeEvent],
    event: RuntimeEvent,
) -> None:
    if subscriber.full():
        with suppress(asyncio.QueueEmpty):
            subscriber.get_nowait()
    subscriber.put_nowait(event)
