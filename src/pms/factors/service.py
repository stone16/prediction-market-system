from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import Any, cast

import asyncpg

from pms.core.models import Market, MarketSignal
from pms.factors.base import FactorDefinition, FactorValueRow
from pms.research.cache import FactorPanel
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
                await persist_factor_value(self.pool, row)
                self._last_persisted_ts[dedupe_key] = row.ts
                persisted += 1
        return persisted

    async def get_panel(
        self,
        factor_id: str,
        param: str | Mapping[str, Any] | None,
        market_ids: Sequence[str],
        ts_start: datetime,
        ts_end: datetime,
    ) -> FactorPanel:
        ordered_market_ids = tuple(dict.fromkeys(str(market_id) for market_id in market_ids))
        if not ordered_market_ids:
            return {}

        query = """
        SELECT factor_id, param, market_id, ts, value
        FROM factor_values
        WHERE factor_id = $1
          AND param = $2
          AND market_id = ANY($3::text[])
          AND ts >= $4
          AND ts <= $5
        ORDER BY ts ASC, id ASC
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                query,
                factor_id,
                _panel_param_text(param),
                list(ordered_market_ids),
                ts_start,
                ts_end,
            )

        grouped: dict[str, list[FactorValueRow]] = {
            market_id: [] for market_id in ordered_market_ids
        }
        for row in rows:
            market_id = cast(str, row["market_id"])
            grouped.setdefault(market_id, []).append(
                FactorValueRow(
                    factor_id=cast(str, row["factor_id"]),
                    param=cast(str, row["param"]),
                    market_id=market_id,
                    ts=cast(datetime, row["ts"]),
                    value=cast(float, row["value"]),
                )
            )
        return {
            market_id: tuple(grouped[market_id]) for market_id in ordered_market_ids
        }

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


def _panel_param_text(param: str | Mapping[str, Any] | None) -> str:
    if param is None or param == "":
        return ""
    if isinstance(param, str):
        return param
    items = sorted((str(key), param[key]) for key in param)
    return "&".join(f"{key}={value}" for key, value in items)
