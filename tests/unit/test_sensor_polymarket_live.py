from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncGenerator
from datetime import UTC
from typing import Any, cast

import httpx
import pytest
from websockets.asyncio.server import serve

from pms.config import PMSSettings, SensorSettings
from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.sensor.adapters.polymarket_rest import PolymarketRestSensor
from pms.sensor.adapters.polymarket_stream import PolymarketStreamSensor
from pms.sensor.watchdog import SensorWatchdog


def _gamma_market_payload() -> list[dict[str, Any]]:
    return [
        {
            "conditionId": "pm-live-1",
            "clobTokenIds": json.dumps(["yes-token", "no-token"]),
            "question": "Will CP03 pass?",
            "outcomePrices": json.dumps(["0.42", "0.58"]),
            "volume24hr": 1200.0,
            "endDateIso": "2026-04-20T00:00:00Z",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
            "liquidity": 3000.0,
        }
    ]


@pytest.mark.asyncio
async def test_polymarket_rest_sensor_polls_gamma_once() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=_gamma_market_payload())

    sensor = PolymarketRestSensor(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=0.01,
    )

    signals = await sensor.poll_once()
    await sensor.aclose()

    assert len(signals) == 1
    assert signals[0].market_id == "pm-live-1"
    assert signals[0].token_id == "yes-token"
    assert signals[0].yes_price == 0.42
    assert signals[0].orderbook == {"bids": [], "asks": []}
    assert signals[0].market_status == MarketStatus.OPEN.value


@pytest.mark.asyncio
async def test_polymarket_rest_sensor_backoff_on_http_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    sleeps: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, json={"error": "rate limited"})
        return httpx.Response(200, json=_gamma_market_payload())

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    sensor = PolymarketRestSensor(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=0.01,
        initial_backoff_s=0.25,
    )

    iterator = cast(AsyncGenerator[MarketSignal, None], sensor.__aiter__())
    signal = await anext(iterator)
    await iterator.aclose()
    await sensor.aclose()

    assert signal.market_id == "pm-live-1"
    assert sleeps[0] == 0.25


@pytest.mark.asyncio
async def test_polymarket_stream_sensor_emits_price_updates() -> None:
    received_subscriptions: list[dict[str, Any]] = []

    async def handler(websocket: Any) -> None:
        received_subscriptions.append(json.loads(await websocket.recv()))
        await websocket.send(json.dumps({"event_type": "keepalive"}))
        await websocket.send(
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "pm-ws-1",
                    "asset_id": "yes-token",
                    "price": 0.47,
                    "title": "Will WS work?",
                    "timestamp": "2026-04-13T00:00:00Z",
                }
            )
        )

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        sensor = PolymarketStreamSensor(
            ws_url=f"ws://127.0.0.1:{port}", market_ids=["pm-ws-1"]
        )

        iterator = cast(AsyncGenerator[MarketSignal, None], sensor.__aiter__())
        signal = await asyncio.wait_for(anext(iterator), timeout=2.0)
        await iterator.aclose()

    assert received_subscriptions == [
        {"type": "subscribe", "markets": ["pm-ws-1"]}
    ]
    assert signal.market_id == "pm-ws-1"
    assert signal.token_id == "yes-token"
    assert signal.yes_price == 0.47
    assert signal.fetched_at.tzinfo is not None


@pytest.mark.asyncio
async def test_stream_keepalive_resets_watchdog_without_fallback() -> None:
    fallback_calls = 0

    async def fallback() -> None:
        nonlocal fallback_calls
        fallback_calls += 1

    watchdog = SensorWatchdog(timeout_s=0.05, fallback=fallback)

    async def handler(websocket: Any) -> None:
        await websocket.recv()
        await websocket.send(json.dumps({"event_type": "keepalive"}))
        await asyncio.sleep(0.02)
        await websocket.send(
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "pm-ws-1",
                    "asset_id": "yes-token",
                    "price": 0.51,
                    "timestamp": "2026-04-13T00:00:00Z",
                }
            )
        )

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        await watchdog.start()
        sensor = PolymarketStreamSensor(
            ws_url=f"ws://127.0.0.1:{port}",
            market_ids=["pm-ws-1"],
            on_message=watchdog.notify_message,
        )
        iterator = cast(AsyncGenerator[MarketSignal, None], sensor.__aiter__())
        signal = await asyncio.wait_for(anext(iterator), timeout=2.0)
        await iterator.aclose()
        await asyncio.wait_for(watchdog.stop(), timeout=5.0)

    assert signal.market_id == "pm-ws-1"
    assert fallback_calls == 0


@pytest.mark.asyncio
async def test_watchdog_fallback_starts_once_and_reset_is_idempotent() -> None:
    fallback_calls = 0

    async def fallback() -> None:
        nonlocal fallback_calls
        fallback_calls += 1

    watchdog = SensorWatchdog(timeout_s=0.01, fallback=fallback)
    await watchdog.start()

    await asyncio.sleep(0.03)
    watchdog.notify_message()
    await asyncio.sleep(0.03)
    await watchdog.stop()

    assert fallback_calls == 1


def test_config_exposes_sensor_poll_interval() -> None:
    settings = PMSSettings(sensor=SensorSettings(poll_interval_s=7.5))

    assert settings.sensor.poll_interval_s == 7.5


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("PMS_RUN_INTEGRATION") != "1",
    reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
)
@pytest.mark.asyncio
async def test_polymarket_rest_sensor_real_gamma_poll_returns_signal() -> None:
    sensor = PolymarketRestSensor()

    signals = await sensor.poll_once()
    await sensor.aclose()

    assert len(signals) >= 1
    assert all(isinstance(signal, MarketSignal) for signal in signals)
    assert all(signal.fetched_at.tzinfo is not None for signal in signals)
    assert all(signal.fetched_at.utcoffset() == UTC.utcoffset(None) for signal in signals)
