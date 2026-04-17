from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, cast
from unittest.mock import AsyncMock

import httpx
import pytest

from pms.config import PMSSettings
from pms.core.enums import RunMode
from pms.sensor.adapters.market_discovery import MarketDiscoverySensor
from pms.sensor.stream import SensorStream
from pms.storage.market_data_store import PostgresMarketDataStore


def _gamma_market(
    condition_id: str,
    *,
    token_ids: list[str] | None = None,
    outcomes: list[str] | None = None,
    include_condition_id: bool = True,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": condition_id,
        "slug": f"market-{condition_id}",
        "question": f"Will {condition_id} settle?",
        "endDateIso": "2026-07-31",
        "createdAt": "2025-05-02T15:03:10.397014Z",
        "active": True,
        "closed": False,
    }
    if include_condition_id:
        payload["conditionId"] = condition_id
    if token_ids is not None:
        payload["clobTokenIds"] = json.dumps(token_ids)
    if outcomes is not None:
        payload["outcomes"] = json.dumps(outcomes)
    return payload


@dataclass
class StoreMock:
    write_market_mock: AsyncMock = field(default_factory=AsyncMock)
    write_token_mock: AsyncMock = field(default_factory=AsyncMock)

    async def write_market(self, market: Any) -> None:
        await self.write_market_mock(market)

    async def write_token(self, token: Any) -> None:
        await self.write_token_mock(token)


def _store_mock() -> PostgresMarketDataStore:
    return cast(PostgresMarketDataStore, StoreMock())


@pytest.mark.asyncio
async def test_market_discovery_sensor_polls_gamma_once_and_writes_markets_and_tokens() -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    payload = [
        _gamma_market(
            f"pm-live-{index}",
            token_ids=[f"yes-token-{index}", f"no-token-{index}"],
            outcomes=["Yes", "No"],
        )
        for index in range(10)
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=payload)

    sensor = MarketDiscoverySensor(
        store=store,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    await sensor.poll_once()
    await sensor.aclose()

    assert store_mock.write_market_mock.await_count == 10
    assert store_mock.write_token_mock.await_count == 20
    first_market = store_mock.write_market_mock.await_args_list[0].args[0]
    assert first_market.condition_id == "pm-live-0"
    assert first_market.venue == "polymarket"
    first_token = store_mock.write_token_mock.await_args_list[0].args[0]
    assert first_token.condition_id == "pm-live-0"
    assert first_token.outcome == "YES"


@pytest.mark.asyncio
async def test_market_discovery_sensor_pairs_token_ids_with_gamma_outcomes() -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    payload = [
        _gamma_market(
            "pm-live-ordered",
            token_ids=["token-no", "token-yes"],
            outcomes=["No", "Yes"],
        )
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=payload)

    sensor = MarketDiscoverySensor(
        store=store,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    await sensor.poll_once()
    await sensor.aclose()

    written_tokens = [call.args[0] for call in store_mock.write_token_mock.await_args_list]
    assert [(token.token_id, token.outcome) for token in written_tokens] == [
        ("token-no", "NO"),
        ("token-yes", "YES"),
    ]


@pytest.mark.asyncio
async def test_market_discovery_sensor_logs_and_skips_missing_token_ids(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    payload = [
        _gamma_market("missing-tokens", token_ids=None),
        _gamma_market(
            "valid-market",
            token_ids=["yes-token", "no-token"],
            outcomes=["Yes", "No"],
        ),
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=payload)

    sensor = MarketDiscoverySensor(
        store=store,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    with caplog.at_level(logging.INFO):
        await sensor.poll_once()
    await sensor.aclose()

    assert store_mock.write_market_mock.await_count == 2
    assert store_mock.write_token_mock.await_count == 2
    assert "missing-tokens" in caplog.text
    assert "clobTokenIds" in caplog.text


@pytest.mark.asyncio
async def test_market_discovery_sensor_skips_rows_missing_condition_id() -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    payload = [
        _gamma_market(
            "gamma-primary-key-only",
            token_ids=["yes-token", "no-token"],
            outcomes=["Yes", "No"],
            include_condition_id=False,
        )
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=payload)

    sensor = MarketDiscoverySensor(
        store=store,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    await sensor.poll_once()
    await sensor.aclose()

    assert store_mock.write_market_mock.await_count == 0
    assert store_mock.write_token_mock.await_count == 0


@pytest.mark.asyncio
async def test_market_discovery_sensor_backoff_on_http_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    attempts = 0
    sleeps: list[float] = []
    wrote_market = asyncio.Event()
    real_sleep = asyncio.sleep

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, json={"error": "rate limited"})
        return httpx.Response(
            200,
            json=[
                _gamma_market(
                    "pm-live-1",
                    token_ids=["yes-token", "no-token"],
                    outcomes=["Yes", "No"],
                )
            ],
        )

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await real_sleep(0)

    async def tracked_write_market(*args: Any, **kwargs: Any) -> None:
        wrote_market.set()
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    store_mock.write_market_mock.side_effect = tracked_write_market
    sensor = MarketDiscoverySensor(
        store=store,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    async def consume() -> None:
        async for _ in cast(AsyncGenerator[object, None], sensor.__aiter__()):
            pass

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(wrote_market.wait(), timeout=2.0)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await sensor.aclose()

    assert sleeps[0] == 1.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transport_error",
    [
        httpx.ConnectError("connection refused"),
        httpx.ReadTimeout("read timeout"),
        httpx.RemoteProtocolError("server hung up"),
    ],
)
async def test_market_discovery_sensor_recovers_from_transient_transport_errors(
    monkeypatch: pytest.MonkeyPatch,
    transport_error: httpx.HTTPError,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    attempts = 0
    sleeps: list[float] = []
    wrote_market = asyncio.Event()
    real_sleep = asyncio.sleep

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise transport_error
        return httpx.Response(
            200,
            json=[
                _gamma_market(
                    "pm-live-after-transient",
                    token_ids=["yes-token", "no-token"],
                    outcomes=["Yes", "No"],
                )
            ],
        )

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await real_sleep(0)

    async def tracked_write_market(*args: Any, **kwargs: Any) -> None:
        wrote_market.set()
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    store_mock.write_market_mock.side_effect = tracked_write_market
    sensor = MarketDiscoverySensor(
        store=store,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    async def consume() -> None:
        async for _ in cast(AsyncGenerator[object, None], sensor.__aiter__()):
            pass

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(wrote_market.wait(), timeout=2.0)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await sensor.aclose()

    assert attempts >= 2
    assert sleeps[0] == 1.0


@pytest.mark.asyncio
async def test_market_discovery_sensor_continues_when_token_write_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _store_mock()
    store_mock = cast(StoreMock, store)
    payload = [
        _gamma_market(
            "pm-live-write-fail",
            token_ids=["yes-token", "no-token"],
            outcomes=["Yes", "No"],
        )
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    import asyncpg as _asyncpg

    write_token_calls = 0

    async def flaky_write_token(*args: Any, **kwargs: Any) -> None:
        nonlocal write_token_calls
        write_token_calls += 1
        if write_token_calls == 1:
            raise _asyncpg.PostgresError("transient db error")

    store_mock.write_token_mock.side_effect = flaky_write_token

    sensor = MarketDiscoverySensor(
        store=store,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    with caplog.at_level(logging.WARNING):
        await sensor.poll_once()
    await sensor.aclose()

    assert store_mock.write_market_mock.await_count == 1
    assert write_token_calls == 2
    assert "write_token failed" in caplog.text


@pytest.mark.asyncio
async def test_sensor_stream_accepts_market_discovery_sensor() -> None:
    store = _store_mock()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=[])

    sensor = MarketDiscoverySensor(
        store=store,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )
    stream = SensorStream()

    await stream.start([sensor])
    await asyncio.sleep(0)
    await stream.stop()
    await sensor.aclose()

    assert stream.tasks == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [RunMode.PAPER, RunMode.LIVE])
async def test_runner_builds_market_discovery_sensor_for_non_backtest_modes(
    mode: RunMode,
) -> None:
    from pms.runner import Runner
    from pms.sensor.adapters.market_data import MarketDataSensor

    runner = Runner(config=PMSSettings(mode=mode))
    runner.bind_pg_pool(cast(Any, object()))

    sensors = runner._build_sensors()

    assert len(sensors) == 2
    assert isinstance(sensors[0], MarketDiscoverySensor)
    assert isinstance(sensors[1], MarketDataSensor)
    await sensors[0].aclose()
    await sensors[1].aclose()
