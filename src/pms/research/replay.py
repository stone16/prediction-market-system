"""Market-universe replay engine for research backtests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, TypeAlias, cast

import asyncpg

from pms.core.enums import MarketStatus
from pms.core.models import (
    BookLevel,
    BookSide,
    BookSnapshot,
    BookSource,
    MarketSignal,
    PriceChange,
    Trade,
    Venue,
)
from pms.research.specs import BacktestExecutionConfig, BacktestSpec


ChunkObserver: TypeAlias = Callable[[datetime, datetime], None]
WriteProbe: TypeAlias = Callable[[asyncpg.Connection], Awaitable[None]]

_SNAPSHOT_PRIORITY = 0
_PRICE_CHANGE_PRIORITY = 1
_TRADE_PRIORITY = 2


class ReplayEngineInvariantError(RuntimeError):
    """Raised when the replay engine violates the outer-ring read-only contract."""


@dataclass(frozen=True, slots=True)
class _ChunkWindow:
    start: datetime
    end: datetime
    is_final: bool


@dataclass(frozen=True, slots=True)
class _MarketMetadata:
    title: str
    venue: Venue
    resolves_at: datetime | None


@dataclass(slots=True)
class _LastBookState:
    market_id: str
    token_id: str
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_trade_price: float | None = None
    last_hash: str | None = None


@dataclass(frozen=True, slots=True)
class _SnapshotPayload:
    snapshot: BookSnapshot
    levels: tuple[BookLevel, ...]


@dataclass(frozen=True, slots=True)
class _ReplayEvent:
    ts: datetime
    priority: int
    sequence: int
    payload: _SnapshotPayload | PriceChange | Trade


@dataclass(slots=True)
class MarketUniverseReplayEngine:
    pool: asyncpg.Pool
    write_probe: WriteProbe | None = None
    chunk_observer: ChunkObserver | None = None

    async def stream(
        self,
        spec: BacktestSpec,
        exec_config: BacktestExecutionConfig,
    ) -> AsyncIterator[MarketSignal]:
        if exec_config.chunk_days <= 0:
            msg = "BacktestExecutionConfig.chunk_days must be positive"
            raise ValueError(msg)

        market_metadata = await self._load_market_metadata(spec.dataset.market_universe_filter)
        if not market_metadata:
            return

        market_ids = tuple(sorted(market_metadata))
        states: dict[tuple[str, str], _LastBookState] = {}
        for window in _chunk_windows(
            start=spec.date_range_start,
            end=spec.date_range_end,
            chunk_days=exec_config.chunk_days,
        ):
            if self.chunk_observer is not None:
                self.chunk_observer(window.start, window.end)
            events = await self._load_chunk_events(window=window, market_ids=market_ids)
            for event in events:
                yield _event_to_signal(
                    event=event,
                    states=states,
                    market_metadata=market_metadata,
                )

    async def _load_market_metadata(
        self,
        market_universe_filter: Mapping[str, Any],
    ) -> dict[str, _MarketMetadata]:
        connection = await self.pool.acquire()
        try:
            explicit_market_ids = _market_ids_from_filter(
                market_universe_filter.get("market_ids")
            ) or _market_ids_from_filter(market_universe_filter.get("condition_ids"))
            venue = _optional_str(market_universe_filter.get("venue"))
            if explicit_market_ids:
                rows = await connection.fetch(
                    """
                    SELECT condition_id, question, venue, resolves_at
                    FROM markets
                    WHERE condition_id = ANY($1::text[])
                      AND ($2::text IS NULL OR venue = $2)
                    ORDER BY condition_id ASC
                    """,
                    list(explicit_market_ids),
                    venue,
                )
            else:
                rows = await connection.fetch(
                    """
                    SELECT condition_id, question, venue, resolves_at
                    FROM markets
                    WHERE ($1::text IS NULL OR venue = $1)
                    ORDER BY condition_id ASC
                    """,
                    venue,
                )
        finally:
            await self.pool.release(connection)

        return {
            cast(str, row["condition_id"]): _MarketMetadata(
                title=cast(str, row["question"]),
                venue=cast(Venue, row["venue"]),
                resolves_at=cast(datetime | None, row["resolves_at"]),
            )
            for row in rows
        }

    async def _load_chunk_events(
        self,
        *,
        window: _ChunkWindow,
        market_ids: tuple[str, ...],
    ) -> list[_ReplayEvent]:
        connection = await self.pool.acquire()
        try:
            if self.write_probe is not None:
                await self._run_write_probe(connection)
            snapshots = await _fetch_snapshots(
                connection=connection,
                window=window,
                market_ids=market_ids,
            )
            levels_by_snapshot = await _fetch_levels(
                connection=connection,
                snapshot_ids=tuple(snapshot.id for snapshot in snapshots),
            )
            price_changes = await _fetch_price_changes(
                connection=connection,
                window=window,
                market_ids=market_ids,
            )
            trades = await _fetch_trades(
                connection=connection,
                window=window,
                market_ids=market_ids,
            )
        finally:
            await self.pool.release(connection)

        events: list[_ReplayEvent] = []
        for snapshot in snapshots:
            events.append(
                _ReplayEvent(
                    ts=snapshot.ts,
                    priority=_SNAPSHOT_PRIORITY,
                    sequence=snapshot.id,
                    payload=_SnapshotPayload(
                        snapshot=snapshot,
                        levels=levels_by_snapshot.get(snapshot.id, ()),
                    ),
                )
            )
        for price_change in price_changes:
            events.append(
                _ReplayEvent(
                    ts=price_change.ts,
                    priority=_PRICE_CHANGE_PRIORITY,
                    sequence=price_change.id,
                    payload=price_change,
                )
            )
        for trade in trades:
            events.append(
                _ReplayEvent(
                    ts=trade.ts,
                    priority=_TRADE_PRIORITY,
                    sequence=trade.id,
                    payload=trade,
                )
            )
        return sorted(events, key=lambda event: (event.ts, event.priority, event.sequence))

    async def _run_write_probe(self, connection: asyncpg.Connection) -> None:
        assert self.write_probe is not None
        try:
            await self.write_probe(connection)
        except asyncpg.PostgresError as exc:
            sqlstate = getattr(exc, "sqlstate", "")
            lowered = str(exc).lower()
            if sqlstate == "42501" or "permission denied" in lowered:
                probe_name = getattr(self.write_probe, "__qualname__", repr(self.write_probe))
                msg = f"engine attempted WRITE on outer ring: {probe_name}: {exc}"
                raise ReplayEngineInvariantError(msg) from exc
            raise


def _chunk_windows(
    *,
    start: datetime,
    end: datetime,
    chunk_days: int,
) -> list[_ChunkWindow]:
    if start > end:
        msg = "BacktestSpec.date_range_start must not exceed date_range_end"
        raise ValueError(msg)

    if start == end:
        return [_ChunkWindow(start=start, end=end, is_final=True)]

    windows: list[_ChunkWindow] = []
    cursor = start
    while cursor < end:
        next_cursor = min(cursor + timedelta(days=chunk_days), end)
        windows.append(
            _ChunkWindow(
                start=cursor,
                end=next_cursor,
                is_final=next_cursor == end,
            )
        )
        cursor = next_cursor
    return windows


async def _fetch_snapshots(
    *,
    connection: asyncpg.Connection,
    window: _ChunkWindow,
    market_ids: Sequence[str],
) -> list[BookSnapshot]:
    if not market_ids:
        return []
    comparator = "<=" if window.is_final else "<"
    rows = await connection.fetch(
        f"""
        SELECT id, market_id, token_id, ts, hash, source
        FROM book_snapshots
        WHERE ts >= $1
          AND ts {comparator} $2
          AND market_id = ANY($3::text[])
        ORDER BY ts ASC, id ASC
        """,
        window.start,
        window.end,
        list(market_ids),
    )
    return [
        BookSnapshot(
            id=cast(int, row["id"]),
            market_id=cast(str, row["market_id"]),
            token_id=cast(str, row["token_id"]),
            ts=cast(datetime, row["ts"]),
            hash=cast(str | None, row["hash"]),
            source=cast(BookSource, row["source"]),
        )
        for row in rows
    ]


async def _fetch_levels(
    *,
    connection: asyncpg.Connection,
    snapshot_ids: tuple[int, ...],
) -> dict[int, tuple[BookLevel, ...]]:
    if not snapshot_ids:
        return {}
    rows = await connection.fetch(
        """
        SELECT snapshot_id, market_id, side, price, size
        FROM book_levels
        WHERE snapshot_id = ANY($1::bigint[])
        ORDER BY
            snapshot_id ASC,
            CASE side WHEN 'BUY' THEN 0 ELSE 1 END,
            CASE side WHEN 'BUY' THEN price END DESC,
            CASE side WHEN 'SELL' THEN price END ASC
        """,
        list(snapshot_ids),
    )
    grouped: dict[int, list[BookLevel]] = {}
    for row in rows:
        snapshot_id = cast(int, row["snapshot_id"])
        grouped.setdefault(snapshot_id, []).append(
            BookLevel(
                snapshot_id=snapshot_id,
                market_id=cast(str, row["market_id"]),
                side=cast(BookSide, row["side"]),
                price=cast(float, row["price"]),
                size=cast(float, row["size"]),
            )
        )
    return {
        snapshot_id: tuple(levels) for snapshot_id, levels in grouped.items()
    }


async def _fetch_price_changes(
    *,
    connection: asyncpg.Connection,
    window: _ChunkWindow,
    market_ids: Sequence[str],
) -> list[PriceChange]:
    if not market_ids:
        return []
    comparator = "<=" if window.is_final else "<"
    rows = await connection.fetch(
        f"""
        SELECT id, market_id, token_id, ts, side, price, size, best_bid, best_ask, hash
        FROM price_changes
        WHERE ts >= $1
          AND ts {comparator} $2
          AND market_id = ANY($3::text[])
        ORDER BY ts ASC, id ASC
        """,
        window.start,
        window.end,
        list(market_ids),
    )
    return [
        PriceChange(
            id=cast(int, row["id"]),
            market_id=cast(str, row["market_id"]),
            token_id=cast(str, row["token_id"]),
            ts=cast(datetime, row["ts"]),
            side=cast(BookSide, row["side"]),
            price=cast(float, row["price"]),
            size=cast(float, row["size"]),
            best_bid=cast(float | None, row["best_bid"]),
            best_ask=cast(float | None, row["best_ask"]),
            hash=cast(str | None, row["hash"]),
        )
        for row in rows
    ]


async def _fetch_trades(
    *,
    connection: asyncpg.Connection,
    window: _ChunkWindow,
    market_ids: Sequence[str],
) -> list[Trade]:
    if not market_ids:
        return []
    comparator = "<=" if window.is_final else "<"
    rows = await connection.fetch(
        f"""
        SELECT id, market_id, token_id, ts, price
        FROM trades
        WHERE ts >= $1
          AND ts {comparator} $2
          AND market_id = ANY($3::text[])
        ORDER BY ts ASC, id ASC
        """,
        window.start,
        window.end,
        list(market_ids),
    )
    return [
        Trade(
            id=cast(int, row["id"]),
            market_id=cast(str, row["market_id"]),
            token_id=cast(str, row["token_id"]),
            ts=cast(datetime, row["ts"]),
            price=cast(float, row["price"]),
        )
        for row in rows
    ]


def _event_to_signal(
    *,
    event: _ReplayEvent,
    states: dict[tuple[str, str], _LastBookState],
    market_metadata: Mapping[str, _MarketMetadata],
) -> MarketSignal:
    payload = event.payload
    if isinstance(payload, _SnapshotPayload):
        snapshot = payload.snapshot
        state = _state_for(states, market_id=snapshot.market_id, token_id=snapshot.token_id)
        state.bids = {
            level.price: level.size
            for level in payload.levels
            if level.side == "BUY"
        }
        state.asks = {
            level.price: level.size
            for level in payload.levels
            if level.side == "SELL"
        }
        state.last_hash = snapshot.hash
        return _market_signal(
            state=state,
            metadata=market_metadata[snapshot.market_id],
            timestamp=snapshot.ts,
            price=state.last_trade_price,
            event_type="book",
        )
    if isinstance(payload, PriceChange):
        state = _state_for(states, market_id=payload.market_id, token_id=payload.token_id)
        _apply_price_change(state, change=payload)
        return _market_signal(
            state=state,
            metadata=market_metadata[payload.market_id],
            timestamp=payload.ts,
            price=_signal_price(
                price=payload.price,
                best_bid=payload.best_bid,
                best_ask=payload.best_ask,
            ),
            event_type="price_change",
            extra={
                "best_bid": payload.best_bid,
                "best_ask": payload.best_ask,
                "side": payload.side,
            },
        )

    state = _state_for(states, market_id=payload.market_id, token_id=payload.token_id)
    state.last_trade_price = payload.price
    return _market_signal(
        state=state,
        metadata=market_metadata[payload.market_id],
        timestamp=payload.ts,
        price=payload.price,
        event_type="last_trade_price",
    )


def _state_for(
    states: dict[tuple[str, str], _LastBookState],
    *,
    market_id: str,
    token_id: str,
) -> _LastBookState:
    key = (market_id, token_id)
    state = states.get(key)
    if state is None:
        state = _LastBookState(market_id=market_id, token_id=token_id)
        states[key] = state
    return state


def _apply_price_change(state: _LastBookState, *, change: PriceChange) -> None:
    levels = state.bids if change.side == "BUY" else state.asks
    if change.size <= 0:
        levels.pop(change.price, None)
    else:
        levels[change.price] = change.size
    state.last_hash = change.hash


def _market_signal(
    *,
    state: _LastBookState,
    metadata: _MarketMetadata,
    timestamp: datetime,
    price: float | None,
    event_type: str,
    extra: dict[str, Any] | None = None,
) -> MarketSignal:
    external_signal = {"raw_event_type": event_type}
    if extra is not None:
        external_signal.update(extra)
    return MarketSignal(
        market_id=state.market_id,
        token_id=state.token_id,
        venue=metadata.venue,
        title=metadata.title,
        yes_price=price if price is not None else 0.0,
        volume_24h=None,
        resolves_at=metadata.resolves_at,
        orderbook=_orderbook_from_state(state),
        external_signal=external_signal,
        fetched_at=timestamp,
        market_status=_market_status(timestamp=timestamp, resolves_at=metadata.resolves_at),
    )


def _orderbook_from_state(state: _LastBookState) -> dict[str, list[dict[str, float]]]:
    return {
        "bids": [
            {"price": price, "size": size}
            for price, size in sorted(state.bids.items(), key=lambda item: item[0], reverse=True)
        ],
        "asks": [
            {"price": price, "size": size}
            for price, size in sorted(state.asks.items(), key=lambda item: item[0])
        ],
    }


def _signal_price(
    *,
    price: float,
    best_bid: float | None,
    best_ask: float | None,
) -> float:
    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0:
        return (best_bid + best_ask) / 2.0
    return price


def _market_status(*, timestamp: datetime, resolves_at: datetime | None) -> str:
    if resolves_at is not None and timestamp >= resolves_at:
        return MarketStatus.CLOSED.value
    return MarketStatus.OPEN.value


def _market_ids_from_filter(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return tuple(sorted(str(item) for item in value))
    if isinstance(value, (set, frozenset)):
        return tuple(sorted(str(item) for item in value))
    return ()


def _optional_str(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)
