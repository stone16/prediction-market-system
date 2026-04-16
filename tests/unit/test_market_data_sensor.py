from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from pms.config import PMSSettings
from pms.core.enums import RunMode
from pms.sensor.adapters.market_data import MarketDataSensor
from pms.storage.market_data_store import PostgresMarketDataStore


REPLAY_FIXTURE = Path("tests/fixtures/polymarket_ws_replay.jsonl")
_REAL_SLEEP = asyncio.sleep


class _AcquireContext:
    def __init__(self, pool: "ProbePool") -> None:
        self._pool = pool

    async def __aenter__(self) -> object:
        delay = self._pool.delays[min(self._pool.attempts, len(self._pool.delays) - 1)]
        self._pool.attempts += 1
        await _REAL_SLEEP(delay)
        return object()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


@dataclass
class ProbePool:
    delays: list[float] = field(default_factory=lambda: [0.0])
    attempts: int = 0

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(self)


@dataclass
class StoreMock:
    pool: ProbePool = field(default_factory=ProbePool)
    write_book_snapshot_mock: AsyncMock = field(default_factory=AsyncMock)
    write_price_change_mock: AsyncMock = field(default_factory=AsyncMock)
    write_trade_mock: AsyncMock = field(default_factory=AsyncMock)

    async def write_book_snapshot(self, snapshot: Any, levels: list[Any]) -> int:
        result = await self.write_book_snapshot_mock(snapshot, levels)
        return cast(int, result if result is not None else 1)

    async def write_price_change(self, price_change: Any) -> None:
        await self.write_price_change_mock(price_change)

    async def write_trade(self, trade: Any) -> None:
        await self.write_trade_mock(trade)


class FakeWebSocket:
    def __init__(self, messages: Sequence[str | bytes]) -> None:
        self._messages = list(messages)
        self.sent_messages: list[dict[str, Any]] = []
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent_messages.append(cast(dict[str, Any], json.loads(payload)))

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        return self

    async def __anext__(self) -> str | bytes:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


class FakeConnect:
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> FakeWebSocket:
        return self.websocket

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.websocket.closed = True


def _store_mock() -> PostgresMarketDataStore:
    return cast(PostgresMarketDataStore, StoreMock())


def _replay_messages() -> list[str]:
    return [line for line in REPLAY_FIXTURE.read_text().splitlines() if line.strip()]


@pytest.mark.asyncio
async def test_market_data_sensor_replays_fixture_losslessly_and_persists_piecewise_deltas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.side_effect = [1, 2]
    fake_websocket = FakeWebSocket(_replay_messages())
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        lambda url: FakeConnect(fake_websocket),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-yes"],
    )

    signals: list[Any] = []
    async for signal in sensor:
        signals.append(signal)
        if len(signals) == 7:
            break
    await sensor.aclose()

    assert fake_websocket.sent_messages == [
        {
            "assets_ids": ["asset-yes"],
            "type": "market",
            "initial_dump": True,
            "level": 2,
        }
    ]
    assert store_mock.write_book_snapshot_mock.await_count == 2
    assert store_mock.write_price_change_mock.await_count == 5
    delta_sizes = [
        call.args[0].size for call in store_mock.write_price_change_mock.await_args_list
    ]
    assert delta_sizes[:3] == [5.0, 0.0, 3.0]
    final_orderbook = signals[-1].orderbook
    assert [level["price"] for level in final_orderbook["bids"]] == [0.47, 0.45, 0.44]
    assert [level["size"] for level in final_orderbook["bids"]] == [3.0, 7.0, 2.0]
    assert [level["price"] for level in final_orderbook["asks"]] == [0.54, 0.55, 0.56]
    assert [level["size"] for level in final_orderbook["asks"]] == [6.0, 4.0, 1.0]


@pytest.mark.asyncio
async def test_market_data_sensor_persists_last_trade_price_and_emits_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.return_value = 1
    messages = [
        json.dumps(
            {
                "event_type": "book",
                "market": "m-trade",
                "asset_id": "asset-trade",
                "timestamp": "1757908892351",
                "hash": "book-hash",
                "bids": [{"price": "0.48", "size": "30"}],
                "asks": [{"price": "0.52", "size": "25"}],
                "last_trade_price": "0.50",
            }
        ),
        json.dumps(
            {
                "event_type": "last_trade_price",
                "market": "m-trade",
                "asset_id": "asset-trade",
                "price": "0.456",
                "side": "BUY",
                "size": "219.217767",
                "fee_rate_bps": "0",
                "timestamp": "1750428146322",
            }
        ),
    ]
    fake_websocket = FakeWebSocket(messages)
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        lambda url: FakeConnect(fake_websocket),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-trade"],
    )

    iterator = cast(Any, sensor.__aiter__())
    await anext(iterator)
    trade_signal = await anext(iterator)
    await iterator.aclose()
    await sensor.aclose()

    assert store_mock.write_trade_mock.await_count == 1
    assert store_mock.write_trade_mock.await_args is not None
    trade = store_mock.write_trade_mock.await_args.args[0]
    assert trade.market_id == "m-trade"
    assert trade.token_id == "asset-trade"
    assert trade.price == 0.456
    assert trade_signal.market_id == "m-trade"
    assert trade_signal.token_id == "asset-trade"
    assert trade_signal.yes_price == 0.456


@pytest.mark.asyncio
async def test_market_data_sensor_update_subscription_warns_and_retries_on_slow_pool(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.pool = ProbePool(delays=[0.12, 0.0])
    sensor = MarketDataSensor(store=store)
    fake_websocket = FakeWebSocket([])
    sensor._websocket = cast(Any, fake_websocket)

    real_sleep = asyncio.sleep
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with caplog.at_level(logging.WARN):
        await sensor.update_subscription(["asset-a", "asset-b"])

    assert store_mock.pool.attempts == 2
    assert sleeps == [0.05]
    assert "subscription update delayed: pool saturated" in caplog.text
    assert fake_websocket.sent_messages == [
        {
            "assets_ids": ["asset-a", "asset-b"],
            "type": "market",
            "initial_dump": True,
            "level": 2,
        }
    ]


@pytest.mark.asyncio
async def test_market_data_sensor_logs_unrecognized_payload_at_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _store_mock()
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-yes"],
    )

    with caplog.at_level(logging.WARN):
        signals = await sensor._handle_raw_message(
            json.dumps({"event_type": "mystery", "foo": "bar"})
        )
    await sensor.aclose()

    assert signals == []
    assert "mystery" in caplog.text
    assert '"foo": "bar"' in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [RunMode.PAPER, RunMode.LIVE])
async def test_runner_builds_market_discovery_and_market_data_sensors_for_non_backtest_modes(
    mode: RunMode,
) -> None:
    from pms.runner import Runner
    from pms.sensor.adapters.market_discovery import MarketDiscoverySensor

    runner = Runner(config=PMSSettings(mode=mode))
    runner._pg_pool = cast(Any, object())

    sensors = runner._build_sensors()

    assert len(sensors) == 2
    assert isinstance(sensors[0], MarketDiscoverySensor)
    assert isinstance(sensors[1], MarketDataSensor)
    await sensors[0].aclose()
    await sensors[1].aclose()
