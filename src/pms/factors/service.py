from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import Any

import asyncpg

from pms.core.models import Market, MarketSignal
from pms.factors.base import FactorDefinition, FactorValueRow
from pms.factors.catalog import ensure_factor_catalog
from pms.research.cache import FactorPanel, load_factor_panel
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
    ON CONFLICT (factor_id, param, market_id, ts) DO UPDATE
    SET value = EXCLUDED.value
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
    _last_persisted_ts: dict[tuple[str, str, str], datetime] = field(
        init=False,
        default_factory=dict,
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
            await self._ensure_market_shell(signal)
            for factor_cls in self.factors:
                row = factor_cls().compute(signal, self.store)
                if row is None:
                    continue
                dedupe_key = (row.factor_id, row.market_id, row.param)
                if self._last_persisted_ts.get(dedupe_key) == row.ts:
                    continue
                await self._persist_factor_value(row, signal)
                self._last_persisted_ts[dedupe_key] = row.ts
                persisted += 1
        return persisted

    async def _persist_factor_value(
        self,
        row: FactorValueRow,
        signal: MarketSignal,
    ) -> None:
        try:
            await persist_factor_value(self.pool, row)
        except asyncpg.ForeignKeyViolationError as exc:
            if _is_constraint_violation(exc, "factor_values_factor_id_fkey"):
                await ensure_factor_catalog(self.pool, factor_ids=(row.factor_id,))
            elif _is_constraint_violation(exc, "factor_values_market_id_fkey"):
                await self._ensure_market_shell(signal)
            else:
                raise
            await persist_factor_value(self.pool, row)

    async def get_panel(
        self,
        factor_id: str,
        param: str | Mapping[str, Any] | None,
        market_ids: Sequence[str],
        ts_start: datetime,
        ts_end: datetime,
    ) -> FactorPanel:
        return await load_factor_panel(
            self.pool,
            factor_id=factor_id,
            param=param,
            market_ids=market_ids,
            ts_start=ts_start,
            ts_end=ts_end,
        )

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
            # None is the terminal FIFO sentinel from _forward_signals().
            # Producers must never enqueue more items after it, or they will
            # be orphaned once the service marks the stream exhausted here.
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

    async def _ensure_market_shell(self, signal: MarketSignal) -> None:
        if await self.store.read_market(signal.market_id) is not None:
            return
        await self.store.write_market(
            Market(
                condition_id=signal.market_id,
                slug=signal.market_id,
                question=signal.title,
                venue=signal.venue,
                resolves_at=signal.resolves_at,
                created_at=signal.timestamp,
                last_seen_at=signal.timestamp,
            )
        )


def _is_constraint_violation(
    exc: asyncpg.ForeignKeyViolationError,
    constraint_name: str,
) -> bool:
    actual_constraint = getattr(exc, "constraint_name", None)
    return actual_constraint == constraint_name or constraint_name in str(exc)
