from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Protocol


class SubscriptionSink(Protocol):
    async def update_subscription(self, asset_ids: list[str]) -> None: ...


class SensorSubscriptionController:
    def __init__(self, sink: SubscriptionSink) -> None:
        self._sink = sink
        self._lock = asyncio.Lock()
        self._current_asset_ids: frozenset[str] = frozenset()
        self._last_updated_at: datetime | None = None

    @property
    def current_asset_ids(self) -> frozenset[str]:
        return self._current_asset_ids

    @property
    def last_updated_at(self) -> datetime | None:
        return self._last_updated_at

    async def update(self, asset_ids: list[str]) -> bool:
        async with self._lock:
            next_asset_ids = frozenset(asset_ids)
            if next_asset_ids == self._current_asset_ids:
                return False

            await self._sink.update_subscription(asset_ids)
            self._current_asset_ids = next_asset_ids
            self._last_updated_at = datetime.now(tz=UTC)
            return True
