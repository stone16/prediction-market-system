from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from typing import Any, cast

import pytest
from websockets.asyncio.server import serve

from pms.config import PMSSettings, SensorSettings
from pms.core.models import MarketSignal
from pms.sensor.adapters.polymarket_stream import PolymarketStreamSensor
from pms.sensor.watchdog import SensorWatchdog


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

    # Allow local websocket handshake jitter so this test verifies that
    # keepalive traffic resets the watchdog instead of racing connection setup.
    watchdog = SensorWatchdog(timeout_s=0.2, fallback=fallback)

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
