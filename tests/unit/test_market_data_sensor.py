from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from pms.config import PMSSettings, SensorSettings
from pms.core.enums import RunMode
from pms.sensor.adapters.market_data import MarketDataSensor, _message_timestamp
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
    def __init__(self, websocket: Any) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> Any:
        return self.websocket

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.websocket.close()


class HeartbeatWebSocket:
    def __init__(
        self,
        messages: Sequence[str | bytes],
        *,
        ping_outcomes: Sequence[float | None] | None = None,
        message_delays_s: Sequence[float] | None = None,
    ) -> None:
        self._messages = list(messages)
        self._ping_outcomes = list(ping_outcomes or [0.0])
        self._message_delays_s = list(message_delays_s or [])
        self._message_index = 0
        self.sent_messages: list[dict[str, Any]] = []
        self.closed = False
        self.ping_count = 0
        self._closed_event = asyncio.Event()

    async def send(self, payload: str) -> None:
        self.sent_messages.append(cast(dict[str, Any], json.loads(payload)))

    async def ping(self) -> asyncio.Future[float]:
        outcome = self._ping_outcomes[
            min(self.ping_count, len(self._ping_outcomes) - 1)
        ]
        self.ping_count += 1
        future: asyncio.Future[float] = asyncio.get_running_loop().create_future()
        if outcome is not None:
            future.set_result(outcome)
        return future

    async def close(self) -> None:
        self.closed = True
        self._closed_event.set()

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        return self

    async def __anext__(self) -> str | bytes:
        if self._messages:
            if self._message_delays_s:
                delay = self._message_delays_s[
                    min(self._message_index, len(self._message_delays_s) - 1)
                ]
                if delay > 0:
                    await _REAL_SLEEP(delay)
            self._message_index += 1
            return self._messages.pop(0)
        await self._closed_event.wait()
        raise StopAsyncIteration


class ConnectSequence:
    def __init__(self, responses: Sequence[Any]) -> None:
        self._responses = list(responses)

    def __call__(self, url: str) -> FakeConnect:
        del url
        if not self._responses:
            msg = "connect called more times than planned"
            raise AssertionError(msg)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return FakeConnect(cast(Any, response))


@dataclass
class WatchdogProbe:
    notify_calls: int = 0

    def notify_message(self) -> None:
        self.notify_calls += 1


def _store_mock() -> PostgresMarketDataStore:
    return cast(PostgresMarketDataStore, StoreMock())


def _replay_messages() -> list[str]:
    return [line for line in REPLAY_FIXTURE.read_text().splitlines() if line.strip()]


@pytest.mark.parametrize(
    ("raw_timestamp", "expected"),
    [
        (1_757_908_892_351, datetime.fromtimestamp(1_757_908_892_351 / 1000.0, tz=UTC)),
        (1_757_908_892, datetime.fromtimestamp(1_757_908_892, tz=UTC)),
    ],
)
def test_message_timestamp_supports_millisecond_and_second_epoch_values(
    raw_timestamp: int,
    expected: datetime,
) -> None:
    assert _message_timestamp(raw_timestamp) == expected


@pytest.mark.asyncio
async def test_market_data_sensor_replays_fixture_losslessly_and_persists_piecewise_deltas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.side_effect = [1, 2]
    fake_websocket = HeartbeatWebSocket(_replay_messages(), ping_outcomes=[0.0])
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        lambda url: FakeConnect(fake_websocket),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-yes"],
    )
    sensor._heartbeat_interval_s = 1.0

    signals: list[Any] = []
    iterator = cast(Any, sensor.__aiter__())
    while len(signals) < 7:
        signals.append(await asyncio.wait_for(anext(iterator), timeout=0.5))
    await asyncio.wait_for(iterator.aclose(), timeout=0.5)
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
    fake_websocket = HeartbeatWebSocket(messages, ping_outcomes=[0.0])
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        lambda url: FakeConnect(fake_websocket),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-trade"],
    )
    sensor._heartbeat_interval_s = 1.0

    iterator = cast(Any, sensor.__aiter__())
    await asyncio.wait_for(anext(iterator), timeout=0.5)
    trade_signal = await asyncio.wait_for(anext(iterator), timeout=0.5)
    await asyncio.wait_for(iterator.aclose(), timeout=0.5)
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

    monkeypatch.setattr(sensor, "_sleep", fake_sleep)

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
async def test_market_data_sensor_propagates_programming_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.side_effect = TypeError("boom")
    fake_websocket = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "market": "m-broken",
                    "asset_id": "asset-broken",
                    "timestamp": "1757908892351",
                    "hash": "book-broken",
                    "bids": [{"price": "0.48", "size": "30"}],
                    "asks": [{"price": "0.52", "size": "25"}],
                    "last_trade_price": "0.50",
                }
            )
        ]
    )
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        lambda url: FakeConnect(fake_websocket),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-broken"],
    )
    sensor._heartbeat_interval_s = 1.0
    iterator = cast(Any, sensor.__aiter__())

    with pytest.raises(TypeError, match="boom"):
        await asyncio.wait_for(anext(iterator), timeout=0.5)

    await sensor.aclose()


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
async def test_market_data_sensor_reconnects_and_marks_next_snapshot_as_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.side_effect = [1, 2]
    first_websocket = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "market": "m-reconnect",
                    "asset_id": "asset-reconnect",
                    "timestamp": "1757908892351",
                    "hash": "book-1",
                    "bids": [{"price": "0.48", "size": "10"}],
                    "asks": [{"price": "0.52", "size": "8"}],
                    "last_trade_price": "0.50",
                }
            ),
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "m-reconnect",
                    "timestamp": "1757908892352",
                    "price_changes": [
                        {
                            "asset_id": "asset-reconnect",
                            "price": "0.49",
                            "size": "12",
                            "side": "BUY",
                            "hash": "delta-1",
                            "best_bid": "0.49",
                            "best_ask": "0.52",
                        }
                    ],
                }
            ),
        ]
    )
    second_websocket = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "market": "m-reconnect",
                    "asset_id": "asset-reconnect",
                    "timestamp": "1757908892451",
                    "hash": "book-2",
                    "bids": [{"price": "0.31", "size": "9"}],
                    "asks": [{"price": "0.69", "size": "6"}],
                    "last_trade_price": "0.32",
                }
            )
        ]
    )
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        ConnectSequence([first_websocket, second_websocket]),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-reconnect"],
    )
    iterator = cast(Any, sensor.__aiter__())

    await anext(iterator)
    await anext(iterator)
    reconnect_signal = await asyncio.wait_for(anext(iterator), timeout=0.2)
    await iterator.aclose()
    await sensor.aclose()

    snapshot_sources = [
        call.args[0].source for call in store_mock.write_book_snapshot_mock.await_args_list
    ]
    assert snapshot_sources == ["subscribe", "reconnect"]
    assert reconnect_signal.orderbook["bids"] == [{"price": 0.31, "size": 9.0}]
    assert reconnect_signal.orderbook["asks"] == [{"price": 0.69, "size": 6.0}]


@pytest.mark.asyncio
async def test_market_data_sensor_tags_every_asset_initial_dump_after_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.side_effect = [1, 2, 3, 4, 5, 6]
    asset_ids = ["asset-a", "asset-b", "asset-c"]

    def _book_event(asset_id: str, ts: str, hash_value: str) -> str:
        return json.dumps(
            {
                "event_type": "book",
                "market": f"m-{asset_id}",
                "asset_id": asset_id,
                "timestamp": ts,
                "hash": hash_value,
                "bids": [{"price": "0.40", "size": "5"}],
                "asks": [{"price": "0.60", "size": "5"}],
                "last_trade_price": "0.50",
            }
        )

    first_websocket = FakeWebSocket(
        [_book_event(asset, "1757908892351", f"first-{asset}") for asset in asset_ids]
    )
    second_websocket = FakeWebSocket(
        [_book_event(asset, "1757908892451", f"second-{asset}") for asset in asset_ids]
    )
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        ConnectSequence([first_websocket, second_websocket]),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=asset_ids,
    )
    sensor._INITIAL_BACKOFF_S = 0.0
    iterator = cast(Any, sensor.__aiter__())

    # Drain the first connection's three initial dumps (subscribe).
    for _ in range(3):
        await asyncio.wait_for(anext(iterator), timeout=0.5)

    # The first websocket exhausts its messages -> StopAsyncIteration ->
    # _iterate falls into the reconnect path. Drain the three reconnect dumps.
    for _ in range(3):
        await asyncio.wait_for(anext(iterator), timeout=0.5)

    await iterator.aclose()
    await sensor.aclose()

    snapshot_sources = [
        call.args[0].source for call in store_mock.write_book_snapshot_mock.await_args_list
    ]
    assert snapshot_sources == [
        "subscribe",
        "subscribe",
        "subscribe",
        "reconnect",
        "reconnect",
        "reconnect",
    ]


@pytest.mark.asyncio
async def test_market_data_sensor_retries_after_websocket_handshake_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from websockets.exceptions import InvalidHandshake

    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.return_value = 1

    handshake_error = InvalidHandshake("server returned 503")
    second_websocket = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "market": "m-handshake",
                    "asset_id": "asset-handshake",
                    "timestamp": "1757908892351",
                    "hash": "book-after-handshake",
                    "bids": [{"price": "0.40", "size": "5"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                    "last_trade_price": "0.50",
                }
            )
        ]
    )
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        ConnectSequence([handshake_error, second_websocket]),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-handshake"],
    )
    sensor._INITIAL_BACKOFF_S = 0.0
    iterator = cast(Any, sensor.__aiter__())

    signal = await asyncio.wait_for(anext(iterator), timeout=2.0)
    await iterator.aclose()
    await sensor.aclose()

    assert signal.market_id == "m-handshake"
    assert store_mock.write_book_snapshot_mock.await_count == 1


@pytest.mark.asyncio
async def test_market_data_sensor_reconnects_when_pong_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.return_value = 1
    first_websocket = HeartbeatWebSocket([], ping_outcomes=[None])
    second_websocket = HeartbeatWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "market": "m-pong",
                    "asset_id": "asset-pong",
                    "timestamp": "1757908892351",
                    "hash": "book-reconnect",
                    "bids": [{"price": "0.44", "size": "11"}],
                    "asks": [{"price": "0.56", "size": "7"}],
                    "last_trade_price": "0.45",
                }
            )
        ],
        ping_outcomes=[0.0],
    )
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        ConnectSequence([first_websocket, second_websocket]),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-pong"],
    )
    sensor._heartbeat_interval_s = 0.01
    sensor._pong_timeout_s = 0.02
    sensor._watchdog.timeout_s = 1.0
    iterator = cast(Any, sensor.__aiter__())

    reconnect_signal = await asyncio.wait_for(anext(iterator), timeout=0.5)
    await iterator.aclose()
    await sensor.aclose()

    assert first_websocket.ping_count >= 1
    assert first_websocket.closed is True
    assert store_mock.write_book_snapshot_mock.await_args is not None
    snapshot = store_mock.write_book_snapshot_mock.await_args.args[0]
    assert snapshot.source == "reconnect"
    assert reconnect_signal.market_id == "m-pong"


@pytest.mark.asyncio
async def test_market_data_sensor_book_resets_watchdog() -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.return_value = 1
    sensor = MarketDataSensor(store=store)
    probe = WatchdogProbe()
    sensor._watchdog = cast(Any, probe)

    await sensor._handle_message(
        {
            "event_type": "book",
            "market": "m-book",
            "asset_id": "asset-book",
            "timestamp": "1757908892351",
            "hash": "book-hash",
            "bids": [{"price": "0.48", "size": "30"}],
            "asks": [{"price": "0.52", "size": "25"}],
            "last_trade_price": "0.50",
        }
    )

    assert probe.notify_calls == 1


@pytest.mark.asyncio
async def test_market_data_sensor_price_change_resets_watchdog() -> None:
    store = _store_mock()
    sensor = MarketDataSensor(store=store)
    probe = WatchdogProbe()
    sensor._watchdog = cast(Any, probe)

    await sensor._handle_message(
        {
            "event_type": "price_change",
            "market": "m-delta",
            "timestamp": "1757908892352",
            "price_changes": [
                {
                    "asset_id": "asset-delta",
                    "price": "0.49",
                    "size": "12",
                    "side": "BUY",
                    "hash": "delta-hash",
                    "best_bid": "0.49",
                    "best_ask": "0.52",
                }
            ],
        }
    )

    assert probe.notify_calls == 1


@pytest.mark.asyncio
async def test_market_data_sensor_last_trade_price_resets_watchdog() -> None:
    store = _store_mock()
    sensor = MarketDataSensor(store=store)
    probe = WatchdogProbe()
    sensor._watchdog = cast(Any, probe)

    await sensor._handle_message(
        {
            "event_type": "last_trade_price",
            "market": "m-trade-watchdog",
            "asset_id": "asset-trade-watchdog",
            "price": "0.456",
            "side": "BUY",
            "size": "219.217767",
            "fee_rate_bps": "0",
            "timestamp": "1750428146322",
        }
    )

    assert probe.notify_calls == 1


@pytest.mark.asyncio
async def test_market_data_sensor_watchdog_warns_after_silence(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sensor = MarketDataSensor(store=_store_mock())
    sensor._watchdog.timeout_s = 0.01

    with caplog.at_level(logging.WARN):
        await sensor._watchdog.start()
        try:
            await asyncio.sleep(0.03)
        finally:
            await sensor._watchdog.stop()

    assert sensor.watchdog_timeout_count == 1
    assert "market data sensor silent for" in caplog.text


@pytest.mark.asyncio
async def test_market_data_sensor_watchdog_timeout_closes_websocket() -> None:
    sensor = MarketDataSensor(store=_store_mock())

    closed_event = asyncio.Event()

    class TrackingWebSocket:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True
            closed_event.set()

    websocket = TrackingWebSocket()
    sensor._websocket = websocket

    await sensor._handle_watchdog_timeout()

    assert websocket.closed is True
    assert closed_event.is_set()
    assert sensor.watchdog_timeout_count == 1


@pytest.mark.asyncio
async def test_market_data_sensor_fresh_connection_does_not_trigger_watchdog(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.return_value = 1
    websocket = HeartbeatWebSocket(
        [
            json.dumps({"event_type": "keepalive"}),
            json.dumps(
                {
                    "event_type": "book",
                    "market": "m-fresh",
                    "asset_id": "asset-fresh",
                    "timestamp": "1757908892351",
                    "hash": "book-fresh",
                    "bids": [{"price": "0.40", "size": "15"}],
                    "asks": [{"price": "0.60", "size": "9"}],
                    "last_trade_price": "0.41",
                }
            ),
        ],
        message_delays_s=[0.0, 0.01],
    )
    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        lambda url: FakeConnect(websocket),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-fresh"],
    )
    sensor._watchdog.timeout_s = 0.05
    sensor._heartbeat_interval_s = 1.0
    iterator = cast(Any, sensor.__aiter__())

    with caplog.at_level(logging.WARN):
        signal = await asyncio.wait_for(anext(iterator), timeout=0.2)
    await iterator.aclose()
    await sensor.aclose()

    assert signal.market_id == "m-fresh"
    assert sensor.watchdog_timeout_count == 0
    assert "market data sensor silent for" not in caplog.text


@pytest.mark.asyncio
async def test_market_data_sensor_reconnect_backoff_caps_and_resets_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    store_mock.write_book_snapshot_mock.side_effect = [1, 2]
    sleeps: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(seconds: float) -> None:
        if seconds >= 1.0:
            await real_sleep(0.05)
            return
        sleeps.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr(
        "pms.sensor.adapters.market_data.connect",
        ConnectSequence(
            [
                OSError("boom-1"),
                OSError("boom-2"),
                OSError("boom-3"),
                FakeWebSocket(
                    [
                        json.dumps(
                            {
                                "event_type": "book",
                                "market": "m-reset-1",
                                "asset_id": "asset-reset",
                                "timestamp": "1757908892351",
                                "hash": "book-reset-1",
                                "bids": [{"price": "0.48", "size": "10"}],
                                "asks": [{"price": "0.52", "size": "8"}],
                                "last_trade_price": "0.50",
                            }
                        )
                    ]
                ),
                OSError("boom-4"),
                FakeWebSocket(
                    [
                        json.dumps(
                            {
                                "event_type": "book",
                                "market": "m-reset-2",
                                "asset_id": "asset-reset",
                                "timestamp": "1757908892451",
                                "hash": "book-reset-2",
                                "bids": [{"price": "0.44", "size": "11"}],
                                "asks": [{"price": "0.56", "size": "7"}],
                                "last_trade_price": "0.45",
                            }
                        )
                    ]
                ),
            ]
        ),
    )
    sensor = MarketDataSensor(
        store=store,
        ws_url="ws://market-data.example.test",
        asset_ids=["asset-reset"],
    )
    monkeypatch.setattr(sensor, "_sleep", fake_sleep)
    sensor._INITIAL_BACKOFF_S = 0.01
    sensor.max_reconnect_interval_s = 0.02
    sensor._watchdog.timeout_s = 1.0
    iterator = cast(Any, sensor.__aiter__())

    await asyncio.wait_for(anext(iterator), timeout=0.2)
    await asyncio.wait_for(anext(iterator), timeout=0.2)
    await iterator.aclose()
    await sensor.aclose()

    assert sleeps[:4] == [0.01, 0.02, 0.02, 0.01]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [RunMode.PAPER, RunMode.LIVE])
async def test_runner_builds_market_discovery_and_market_data_sensors_for_non_backtest_modes(
    mode: RunMode,
) -> None:
    from pms.runner import Runner
    from pms.sensor.adapters.market_discovery import MarketDiscoverySensor

    runner = Runner(
        config=PMSSettings(
            mode=mode,
            sensor=SensorSettings(
                poll_interval_s=7.5,
                max_reconnect_interval_s=9.0,
            ),
        )
    )
    runner.bind_pg_pool(cast(Any, object()))

    sensors = runner._build_sensors()

    assert len(sensors) == 2
    assert isinstance(sensors[0], MarketDiscoverySensor)
    assert isinstance(sensors[1], MarketDataSensor)
    assert sensors[1].max_reconnect_interval_s == 9.0
    await sensors[0].aclose()
    await sensors[1].aclose()
