from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

from websockets.asyncio.client import connect

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal

MessageCallback = Callable[[], Awaitable[None] | None]


@dataclass(frozen=True)
class PolymarketStreamSensor:
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/"
    market_ids: Sequence[str] = field(default_factory=tuple)
    on_message: MessageCallback | None = None
    initial_backoff_s: float = 1.0
    max_backoff_s: float = 30.0

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        backoff = self.initial_backoff_s
        while True:
            try:
                async with connect(self.ws_url) as websocket:
                    await websocket.send(
                        json.dumps({"type": "subscribe", "markets": list(self.market_ids)})
                    )
                    backoff = self.initial_backoff_s
                    async for raw_message in websocket:
                        await self._notify_message()
                        signal = _message_to_signal(raw_message)
                        if signal is not None:
                            yield signal
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self.max_backoff_s)

    async def _notify_message(self) -> None:
        if self.on_message is None:
            return
        result = self.on_message()
        if inspect.isawaitable(result):
            await result


def _message_to_signal(raw_message: str | bytes) -> MarketSignal | None:
    loaded = json.loads(raw_message)
    if isinstance(loaded, list):
        for item in loaded:
            signal = _message_dict_to_signal(item)
            if signal is not None:
                return signal
        return None
    if isinstance(loaded, dict):
        return _message_dict_to_signal(loaded)
    return None


def _message_dict_to_signal(message: dict[str, Any]) -> MarketSignal | None:
    if message.get("event_type") in {"keepalive", "pong"}:
        return None
    price = message.get("price") or message.get("yes_price")
    market_id = message.get("market") or message.get("market_id")
    if price is None or market_id is None:
        return None
    return MarketSignal(
        market_id=str(market_id),
        token_id=_optional_str(message.get("asset_id") or message.get("token_id")),
        venue="polymarket",
        title=str(message.get("title", "")),
        yes_price=float(cast(str | int | float, price)),
        volume_24h=None,
        resolves_at=None,
        orderbook={"bids": [], "asks": []},
        external_signal={"raw_event_type": message.get("event_type")},
        fetched_at=_message_timestamp(message),
        market_status=MarketStatus.OPEN.value,
    )


def _optional_str(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _message_timestamp(message: dict[str, Any]) -> datetime:
    timestamp = message.get("timestamp")
    if timestamp is None:
        return datetime.now(tz=UTC)
    if isinstance(timestamp, int | float):
        return datetime.fromtimestamp(float(timestamp), tz=UTC)
    return datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
