from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import httpx

from pms.config import PMSSettings
from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal


logger = logging.getLogger(__name__)


@dataclass
class PolymarketRestSensor:
    client: httpx.AsyncClient | None = None
    poll_interval_s: float | None = None
    initial_backoff_s: float = 1.0
    max_backoff_s: float = 30.0

    def __post_init__(self) -> None:
        settings = PMSSettings()
        if self.poll_interval_s is None:
            self.poll_interval_s = settings.sensor.poll_interval_s
        if self.client is None:
            self.client = httpx.AsyncClient(base_url="https://gamma-api.polymarket.com")

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        backoff = self.initial_backoff_s
        while True:
            try:
                for signal in await self.poll_once():
                    yield signal
                backoff = self.initial_backoff_s
                await asyncio.sleep(cast(float, self.poll_interval_s))
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 429:
                    raise
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self.max_backoff_s)

    async def poll_once(self) -> list[MarketSignal]:
        client = self.client
        if client is None:
            msg = "PolymarketRestSensor client is not initialized"
            raise RuntimeError(msg)
        response = await client.get("/markets")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            msg = "Expected Gamma API /markets response to be a list"
            raise ValueError(msg)
        signals: list[MarketSignal] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            try:
                signals.append(_gamma_market_to_signal(row))
            except (KeyError, TypeError, ValueError) as error:
                logger.warning("skipping malformed Gamma market row: %s", error)
        return signals

    async def aclose(self) -> None:
        if self.client is not None:
            await self.client.aclose()


def _gamma_market_to_signal(row: dict[str, Any]) -> MarketSignal:
    yes_token, no_token = _parse_token_ids(row.get("clobTokenIds"))
    yes_price = _first_price(row.get("outcomePrices"))
    fetched_at = datetime.now(tz=UTC)
    return MarketSignal(
        market_id=str(row.get("conditionId") or row.get("condition_id") or row["id"]),
        token_id=yes_token,
        venue="polymarket",
        title=str(row.get("question", "")),
        yes_price=yes_price,
        volume_24h=_optional_float(row.get("volume24hr")),
        resolves_at=_optional_datetime(row.get("endDateIso")),
        orderbook=_simulated_paper_orderbook(row, yes_price),
        external_signal={
            "no_token_id": no_token,
            "accepting_orders": bool(row.get("acceptingOrders", False)),
            "liquidity": _optional_float(row.get("liquidity")),
        },
        fetched_at=fetched_at,
        market_status=_market_status(row),
    )


def _parse_token_ids(value: object) -> tuple[str | None, str | None]:
    if value is None or value == "":
        return None, None
    loaded = json.loads(value) if isinstance(value, str) else value
    if not isinstance(loaded, list):
        return None, None
    yes_token = str(loaded[0]) if len(loaded) > 0 else None
    no_token = str(loaded[1]) if len(loaded) > 1 else None
    return yes_token, no_token


def _first_price(value: object) -> float:
    if value is None or value == "":
        msg = "outcomePrices is missing"
        raise ValueError(msg)
    loaded = json.loads(value) if isinstance(value, str) else value
    if isinstance(loaded, list) and loaded:
        return float(loaded[0])
    if isinstance(loaded, list):
        msg = "outcomePrices is empty"
        raise ValueError(msg)
    return float(cast(str | int | float, value))


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(cast(str | int | float, value))


def _simulated_paper_orderbook(
    row: dict[str, Any],
    yes_price: float,
) -> dict[str, list[dict[str, float]]]:
    if not bool(row.get("acceptingOrders", False)):
        return {"bids": [], "asks": []}
    liquidity = _optional_float(row.get("liquidity"))
    if liquidity is None or liquidity <= 0.0:
        return {"bids": [], "asks": []}

    ask_price = _clamp_probability(yes_price)
    bid_price = _clamp_probability(ask_price - 0.01)
    return {
        "bids": [{"price": bid_price, "size": liquidity}],
        "asks": [{"price": ask_price, "size": liquidity}],
    }


def _clamp_probability(value: float) -> float:
    return round(min(max(value, 0.01), 0.99), 4)


def _optional_datetime(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _market_status(row: dict[str, Any]) -> str:
    if bool(row.get("closed", False)):
        return MarketStatus.CLOSED.value
    if bool(row.get("active", False)):
        return MarketStatus.OPEN.value
    return MarketStatus.UNOPENED.value
