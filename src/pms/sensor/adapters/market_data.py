from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

from websockets.asyncio.client import connect

from pms.core.enums import MarketStatus
from pms.core.models import BookLevel, BookSide, BookSnapshot, MarketSignal, PriceChange, Trade
from pms.storage.market_data_store import PostgresMarketDataStore


logger = logging.getLogger(__name__)


@dataclass
class _BookState:
    market_id: str
    asset_id: str
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_trade_price: float | None = None
    last_hash: str | None = None


class MarketDataSensor:
    _INITIAL_BACKOFF_S = 1.0
    _MAX_BACKOFF_S = 30.0
    _POOL_SATURATION_S = 0.1
    _POOL_RETRY_BACKOFF_S = 0.05

    def __init__(
        self,
        store: PostgresMarketDataStore,
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        asset_ids: list[str] | None = None,
    ) -> None:
        self.store = store
        self.ws_url = ws_url
        self._asset_ids: list[str] = list(asset_ids) if asset_ids else []
        self._books: dict[str, _BookState] = {}
        self._websocket: Any = None
        self._send_lock = asyncio.Lock()

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        backoff = self._INITIAL_BACKOFF_S
        while True:
            try:
                async with connect(self.ws_url) as websocket:
                    self._websocket = websocket
                    try:
                        await self._send_subscription()
                        backoff = self._INITIAL_BACKOFF_S
                        async for raw_message in websocket:
                            for signal in await self._handle_raw_message(raw_message):
                                yield signal
                    finally:
                        self._websocket = None
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.error("market data sensor receive loop failed: %s", error)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self._MAX_BACKOFF_S)

    async def update_subscription(self, asset_ids: list[str]) -> None:
        self._asset_ids = list(asset_ids)
        if self._websocket is None:
            return

        latency = await self._probe_pool_latency()
        if latency > self._POOL_SATURATION_S:
            logger.warning("subscription update delayed: pool saturated")
            await asyncio.sleep(self._POOL_RETRY_BACKOFF_S)
            await self._probe_pool_latency()

        await self._send_subscription()

    async def aclose(self) -> None:
        websocket = self._websocket
        self._websocket = None
        if websocket is not None:
            await websocket.close()

    async def _handle_raw_message(self, raw_message: str | bytes) -> list[MarketSignal]:
        loaded = json.loads(raw_message)
        if isinstance(loaded, list):
            signals: list[MarketSignal] = []
            for item in loaded:
                if isinstance(item, dict):
                    signals.extend(await self._handle_message(item))
            return signals
        if isinstance(loaded, dict):
            return await self._handle_message(loaded)
        logger.warning("unrecognised market-data payload: %s", loaded)
        return []

    async def _handle_message(self, message: dict[str, Any]) -> list[MarketSignal]:
        event_type = str(message.get("event_type", ""))
        if event_type in {"keepalive", "pong", "tick_size_change"}:
            return []
        if event_type == "book":
            return [await self._handle_book(message)]
        if event_type == "price_change":
            return await self._handle_price_change(message)
        if event_type == "last_trade_price":
            return [await self._handle_last_trade_price(message)]
        logger.warning(
            "unrecognised market-data payload: %s",
            json.dumps(message, sort_keys=True),
        )
        return []

    async def _handle_book(self, message: dict[str, Any]) -> MarketSignal:
        market_id = _required_str(message, "market")
        asset_id = _required_str(message, "asset_id")
        timestamp = _message_timestamp(message.get("timestamp"))
        state = self._book_state(market_id, asset_id)
        state.bids = _levels_to_map(message.get("bids"))
        state.asks = _levels_to_map(message.get("asks"))
        state.last_hash = _optional_str(message.get("hash"))
        state.last_trade_price = _optional_float(message.get("last_trade_price"))

        snapshot = BookSnapshot(
            id=0,
            market_id=market_id,
            token_id=asset_id,
            ts=timestamp,
            hash=state.last_hash,
            source="subscribe",
        )
        await self.store.write_book_snapshot(snapshot, _book_levels_from_state(state))
        return _signal_from_state(
            state=state,
            timestamp=timestamp,
            price=state.last_trade_price,
            event_type="book",
        )

    async def _handle_price_change(self, message: dict[str, Any]) -> list[MarketSignal]:
        market_id = _required_str(message, "market")
        timestamp = _message_timestamp(message.get("timestamp"))
        changes = message.get("price_changes")
        if not isinstance(changes, list):
            msg = "price_changes payload must be a list"
            raise ValueError(msg)

        signals: list[MarketSignal] = []
        for change in changes:
            if not isinstance(change, dict):
                continue
            asset_id = _required_str(change, "asset_id")
            state = self._book_state(market_id, asset_id)
            price = _required_float(change, "price")
            size = _required_float(change, "size")
            side = _required_side(change.get("side"))
            _apply_price_change(state, side=side, price=price, size=size)
            best_bid = _optional_float(change.get("best_bid"))
            best_ask = _optional_float(change.get("best_ask"))
            state.last_hash = _optional_str(change.get("hash"))

            await self.store.write_price_change(
                PriceChange(
                    id=0,
                    market_id=market_id,
                    token_id=asset_id,
                    ts=timestamp,
                    side=side,
                    price=price,
                    size=size,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    hash=state.last_hash,
                )
            )
            signals.append(
                _signal_from_state(
                    state=state,
                    timestamp=timestamp,
                    price=_signal_price(price=price, best_bid=best_bid, best_ask=best_ask),
                    event_type="price_change",
                    extra={
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "side": side,
                    },
                )
            )
        return signals

    async def _handle_last_trade_price(self, message: dict[str, Any]) -> MarketSignal:
        market_id = _required_str(message, "market")
        asset_id = _required_str(message, "asset_id")
        price = _required_float(message, "price")
        timestamp = _message_timestamp(message.get("timestamp"))
        state = self._book_state(market_id, asset_id)
        state.last_trade_price = price
        await self.store.write_trade(
            Trade(
                id=0,
                market_id=market_id,
                token_id=asset_id,
                ts=timestamp,
                price=price,
            )
        )
        return _signal_from_state(
            state=state,
            timestamp=timestamp,
            price=price,
            event_type="last_trade_price",
            extra={
                "side": _optional_str(message.get("side")),
                "size": _optional_float(message.get("size")),
                "fee_rate_bps": _optional_float(message.get("fee_rate_bps")),
            },
        )

    def _book_state(self, market_id: str, asset_id: str) -> _BookState:
        state = self._books.get(asset_id)
        if state is None:
            state = _BookState(market_id=market_id, asset_id=asset_id)
            self._books[asset_id] = state
        else:
            state.market_id = market_id
        return state

    async def _send_subscription(self) -> None:
        websocket = self._websocket
        if websocket is None:
            return
        async with self._send_lock:
            await websocket.send(json.dumps(_subscription_payload(self._asset_ids)))

    async def _probe_pool_latency(self) -> float:
        started = time.perf_counter()
        async with self.store.pool.acquire():
            pass
        return time.perf_counter() - started


def _subscription_payload(asset_ids: list[str]) -> dict[str, Any]:
    return {
        "assets_ids": asset_ids,
        "type": "market",
        "initial_dump": True,
        "level": 2,
    }


def _levels_to_map(raw_levels: object) -> dict[float, float]:
    if not isinstance(raw_levels, list):
        msg = "book levels must be a list"
        raise ValueError(msg)
    levels: dict[float, float] = {}
    for level in raw_levels:
        if not isinstance(level, dict):
            continue
        price = _required_float(level, "price")
        size = _required_float(level, "size")
        levels[price] = size
    return levels


def _book_levels_from_state(state: _BookState) -> list[BookLevel]:
    return [
        *[
            BookLevel(
                snapshot_id=0,
                market_id=state.market_id,
                side="BUY",
                price=price,
                size=size,
            )
            for price, size in sorted(state.bids.items(), key=lambda item: item[0], reverse=True)
        ],
        *[
            BookLevel(
                snapshot_id=0,
                market_id=state.market_id,
                side="SELL",
                price=price,
                size=size,
            )
            for price, size in sorted(state.asks.items(), key=lambda item: item[0])
        ],
    ]


def _signal_from_state(
    *,
    state: _BookState,
    timestamp: datetime,
    price: float | None,
    event_type: str,
    extra: dict[str, Any] | None = None,
) -> MarketSignal:
    orderbook = {
        "bids": [
            {"price": level_price, "size": level_size}
            for level_price, level_size in sorted(
                state.bids.items(), key=lambda item: item[0], reverse=True
            )
        ],
        "asks": [
            {"price": level_price, "size": level_size}
            for level_price, level_size in sorted(state.asks.items(), key=lambda item: item[0])
        ],
    }
    external_signal = {"raw_event_type": event_type}
    if extra is not None:
        external_signal.update(extra)
    return MarketSignal(
        market_id=state.market_id,
        token_id=state.asset_id,
        venue="polymarket",
        title="",
        yes_price=price if price is not None else 0.0,
        volume_24h=None,
        resolves_at=None,
        orderbook=orderbook,
        external_signal=external_signal,
        fetched_at=timestamp,
        market_status=MarketStatus.OPEN.value,
    )


def _signal_price(
    *,
    price: float,
    best_bid: float | None,
    best_ask: float | None,
) -> float:
    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0:
        return (best_bid + best_ask) / 2.0
    return price


def _required_str(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if value is None or value == "":
        msg = f"{key} is required"
        raise KeyError(msg)
    return str(value)


def _required_float(mapping: dict[str, Any], key: str) -> float:
    value = mapping.get(key)
    if value is None or value == "":
        msg = f"{key} is required"
        raise KeyError(msg)
    return float(value)


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str | bytes | bytearray | int | float):
        msg = f"unsupported float payload: {value!r}"
        raise TypeError(msg)
    return float(value)


def _optional_str(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _required_side(value: object) -> BookSide:
    side = _optional_str(value)
    if side not in {"BUY", "SELL"}:
        msg = f"unsupported side: {value!r}"
        raise ValueError(msg)
    return cast(BookSide, side)


def _message_timestamp(value: object) -> datetime:
    if value is None or value == "":
        return datetime.now(tz=UTC)
    if isinstance(value, int | float):
        return _timestamp_number(float(value))
    text = str(value)
    if text.isdigit():
        return _timestamp_number(float(text))
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _timestamp_number(value: float) -> datetime:
    if value > 10_000_000_000:
        return datetime.fromtimestamp(value / 1000.0, tz=UTC)
    return datetime.fromtimestamp(value, tz=UTC)


def _apply_price_change(
    state: _BookState,
    *,
    side: str,
    price: float,
    size: float,
) -> None:
    """Apply a price-level delta to the in-memory book state.

    Break points: size > 0 (level add/update), size == 0 (level removal).
    """

    side_levels = state.bids if side == "BUY" else state.asks
    if size == 0:
        side_levels.pop(price, None)
        return
    side_levels[price] = size
