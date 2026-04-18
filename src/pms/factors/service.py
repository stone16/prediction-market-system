from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable
from dataclasses import dataclass, field
import logging

import asyncpg

from pms.core.models import MarketSignal
from pms.factors.base import FactorDefinition, FactorValueRow
from pms.storage.market_data_store import PostgresMarketDataStore


logger = logging.getLogger(__name__)


async def persist_factor_value(pool: asyncpg.Pool, row: FactorValueRow) -> None:
    query = """
    INSERT INTO factor_values (
        factor_id,
        param,
        market_id,
        ts,
        value
    ) VALUES ($1, $2, $3, $4, $5)
    """
    async with pool.acquire() as connection:
        await connection.execute(
            query,
            row.factor_id,
            row.param,
            row.market_id,
            row.ts,
            row.value,
        )


@dataclass
class FactorService:
    pool: asyncpg.Pool
    store: PostgresMarketDataStore
    cadence_s: float
    factors: tuple[type[FactorDefinition], ...]
    signal_stream: AsyncIterable[MarketSignal]
    _latest_signals: dict[str, MarketSignal] = field(init=False, default_factory=dict)
    _signal_queue: asyncio.Queue[MarketSignal | None] = field(
        init=False,
        default_factory=asyncio.Queue,
    )
    _stream_exhausted: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        if self.cadence_s <= 0.0:
            msg = "cadence_s must be positive"
            raise ValueError(msg)

    async def run(self) -> None:
        stream_task = asyncio.create_task(self._forward_signals())
        try:
            while True:
                signals = self._drain_signal_buffer()
                if signals:
                    for signal in signals:
                        self._latest_signals[signal.market_id] = signal
                if self._latest_signals:
                    await self.compute_once(list(self._latest_signals.values()))
                if self._stream_exhausted:
                    self._raise_stream_error(stream_task)
                    return
                await asyncio.sleep(self.cadence_s)
        except asyncio.CancelledError:
            logger.info("factor service cancelled")
            raise
        finally:
            if not stream_task.done():
                stream_task.cancel()
            await asyncio.gather(stream_task, return_exceptions=True)

    async def compute_once(self, signals: list[MarketSignal]) -> int:
        persisted = 0
        for signal in signals:
            for factor_cls in self.factors:
                row = factor_cls().compute(signal, self.store)
                if row is None:
                    continue
                await persist_factor_value(self.pool, row)
                persisted += 1
        return persisted

    async def _forward_signals(self) -> None:
        try:
            async for signal in self.signal_stream:
                await self._signal_queue.put(signal)
        finally:
            self._stream_exhausted = True
            await self._signal_queue.put(None)

    def _drain_signal_buffer(self) -> list[MarketSignal]:
        signals: list[MarketSignal] = []
        while True:
            try:
                item = self._signal_queue.get_nowait()
            except asyncio.QueueEmpty:
                return signals
            if item is None:
                self._stream_exhausted = True
                return signals
            signals.append(item)

    def _raise_stream_error(self, stream_task: asyncio.Task[None]) -> None:
        if not stream_task.done():
            return
        exception = stream_task.exception()
        if exception is not None:
            raise exception
