from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from pms.core.enums import MarketStatus
from pms.core.models import (
    BookLevel,
    BookSide,
    BookSnapshot,
    BookSource,
    MarketSignal,
    PriceChange,
    Trade,
)
from pms.sensor.watchdog import SensorWatchdog
from pms.storage.market_data_store import PostgresMarketDataStore


logger = logging.getLogger(__name__)
_POLYMARKET_MILLISECONDS_EPOCH_THRESHOLD = 10_000_000_000


@dataclass
class _BookState:
    market_id: str
    asset_id: str
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_trade_price: float | None = None
    last_hash: str | None = None


class MarketDataSensor:
    # Polymarket's observed market-channel idle timeout is ~60 s. Keep heartbeat
    # comfortably below that so missed pongs trigger reconnect before the server
    # silently drops the subscription.
    _INITIAL_BACKOFF_S = 1.0
    _DEFAULT_MAX_RECONNECT_INTERVAL_S = 60.0
    _POOL_SATURATION_S = 0.1
    _POOL_RETRY_BACKOFF_S = 0.05
    _HEARTBEAT_INTERVAL_S = 10.0
    _PONG_TIMEOUT_S = 15.0
    _WATCHDOG_TIMEOUT_S = 120.0

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
        self._max_reconnect_interval_s = self._DEFAULT_MAX_RECONNECT_INTERVAL_S
        self._heartbeat_interval_s = self._HEARTBEAT_INTERVAL_S
        self._pong_timeout_s = self._PONG_TIMEOUT_S
        self._pending_book_source: BookSource | None = None
        self._connected_once = False
        self._watchdog_timeout_count = 0
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._watchdog = SensorWatchdog(
            timeout_s=self._WATCHDOG_TIMEOUT_S,
            fallback=self._handle_watchdog_timeout,
        )

    @property
    def max_reconnect_interval_s(self) -> float:
        return self._max_reconnect_interval_s

    @max_reconnect_interval_s.setter
    def max_reconnect_interval_s(self, value: float) -> None:
        self._max_reconnect_interval_s = value

    @property
    def watchdog_timeout_count(self) -> int:
        return self._watchdog_timeout_count

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        backoff = self._INITIAL_BACKOFF_S
        await self._watchdog.start()
        try:
            while True:
                connection_book_source: BookSource = (
                    "reconnect" if self._connected_once else "subscribe"
                )
                try:
                    async with connect(self.ws_url) as websocket:
                        self._reset_book_state()
                        self._websocket = websocket
                        self._pending_book_source = connection_book_source
                        heartbeat_task = asyncio.create_task(
                            self._heartbeat_loop(websocket)
                        )
                        self._heartbeat_task = heartbeat_task
                        try:
                            await self._send_subscription()
                            self._connected_once = True
                            backoff = self._INITIAL_BACKOFF_S
                            async for raw_message in websocket:
                                for signal in await self._handle_raw_message(raw_message):
                                    yield signal
                        finally:
                            heartbeat_task.cancel()
                            with suppress(asyncio.CancelledError):
                                await heartbeat_task
                            self._heartbeat_task = None
                            self._websocket = None
                            self._pending_book_source = None
                            self._reset_book_state()
                except asyncio.CancelledError:
                    raise
                except (ConnectionClosed, ConnectionError, OSError, TimeoutError) as error:
                    logger.error("market data sensor receive loop failed: %s", error)
                    await self._sleep(backoff)
                    backoff = min(backoff * 2.0, self._max_reconnect_interval_s)
        finally:
            self._heartbeat_task = None
            await self._watchdog.stop()
            self._websocket = None
            self._pending_book_source = None
            self._reset_book_state()

    async def update_subscription(self, asset_ids: list[str]) -> None:
        self._asset_ids = list(asset_ids)
        if self._websocket is None:
            return

        latency = await self._probe_pool_latency()
        if latency > self._POOL_SATURATION_S:
            logger.warning("subscription update delayed: pool saturated")
            await self._sleep(self._POOL_RETRY_BACKOFF_S)
            await self._probe_pool_latency()

        await self._send_subscription()

    async def aclose(self) -> None:
        heartbeat_task = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

        websocket = self._websocket
        self._websocket = None
        if websocket is not None:
            await websocket.close()
        await self._watchdog.stop()

    async def _handle_raw_message(self, raw_message: str | bytes) -> list[MarketSignal]:
        loaded = json.loads(raw_message)
        book_source = self._consume_book_source(loaded)
        if isinstance(loaded, list):
            signals: list[MarketSignal] = []
            for item in loaded:
                if isinstance(item, dict):
                    signals.extend(
                        await self._handle_message(item, book_source=book_source)
                    )
            return signals
        if isinstance(loaded, dict):
            return await self._handle_message(loaded, book_source=book_source)
        logger.warning("unrecognised market-data payload: %s", loaded)
        return []

    async def _handle_message(
        self,
        message: dict[str, Any],
        *,
        book_source: BookSource | None = None,
    ) -> list[MarketSignal]:
        event_type = str(message.get("event_type", ""))
        if event_type in {"keepalive", "pong", "tick_size_change"}:
            return []
        if event_type == "book":
            self._watchdog.notify_message()
            return [await self._handle_book(message, source=book_source)]
        if event_type == "price_change":
            self._watchdog.notify_message()
            return await self._handle_price_change(message)
        if event_type == "last_trade_price":
            self._watchdog.notify_message()
            return [await self._handle_last_trade_price(message)]
        logger.warning(
            "unrecognised market-data payload: %s",
            json.dumps(message, sort_keys=True),
        )
        return []

    async def _handle_book(
        self,
        message: dict[str, Any],
        *,
        source: BookSource | None = None,
    ) -> MarketSignal:
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
            source=source or "subscribe",
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

    async def _sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def _heartbeat_loop(self, websocket: Any) -> None:
        while True:
            await self._sleep(self._heartbeat_interval_s)
            try:
                pong_waiter = await websocket.ping()
                await asyncio.wait_for(pong_waiter, timeout=self._pong_timeout_s)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                logger.warning("market data sensor missed pong; reconnecting")
                await websocket.close()
                return
            except Exception as error:
                logger.error("market data sensor heartbeat failed: %s", error)
                await websocket.close()
                return

    async def _handle_watchdog_timeout(self) -> None:
        self._watchdog_timeout_count += 1
        logger.warning(
            "market data sensor silent for %.1fs",
            self._watchdog.timeout_s,
        )

    def _consume_book_source(self, payload: object) -> BookSource | None:
        if self._pending_book_source is None:
            return None
        if _payload_contains_book(payload):
            source = self._pending_book_source
            self._pending_book_source = None
            return source
        return None

    def _reset_book_state(self) -> None:
        self._books.clear()


def _subscription_payload(asset_ids: list[str]) -> dict[str, Any]:
    return {
        "assets_ids": asset_ids,
        "type": "market",
        "initial_dump": True,
        "level": 2,
    }


def _payload_contains_book(payload: object) -> bool:
    if isinstance(payload, dict):
        return str(payload.get("event_type", "")) == "book"
    if isinstance(payload, list):
        return any(
            isinstance(item, dict) and str(item.get("event_type", "")) == "book"
            for item in payload
        )
    return False


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
    if value > _POLYMARKET_MILLISECONDS_EPOCH_THRESHOLD:
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
