from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg
import httpx
import pytest

from pms.actuator.adapters.polymarket import (
    PolymarketOrderRequest,
    PolymarketOrderResult,
)
from pms.api.app import create_app
from pms.config import DatabaseSettings, PMSSettings, PolymarketSettings
from pms.core.enums import RunMode
from pms.core.models import Market, MarketSignal, Token, VenueCredentials
from pms.runner import Runner
from pms.storage.market_data_store import PostgresMarketDataStore


PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
    pytest.mark.skipif(
        PMS_TEST_DATABASE_URL is None,
        reason="set PMS_TEST_DATABASE_URL to the compose-backed PostgreSQL URI",
    ),
]


@dataclass
class _DiscoverySensorDouble:
    on_poll_complete: Callable[[], Awaitable[None]] | None = None
    poll_complete: asyncio.Event = field(default_factory=asyncio.Event)
    close_calls: int = 0

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        if self.on_poll_complete is not None:
            await self.on_poll_complete()
        self.poll_complete.set()
        while True:
            await asyncio.sleep(60.0)
            yield _signal()

    async def aclose(self) -> None:
        self.close_calls += 1


@dataclass
class _MarketDataSensorDouble:
    updates: list[list[str]] = field(default_factory=list)
    close_calls: int = 0

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()

    async def update_subscription(self, asset_ids: list[str]) -> None:
        self.updates.append(list(asset_ids))

    async def aclose(self) -> None:
        self.close_calls += 1


@dataclass
class _SensorDoubles:
    discoveries: list[_DiscoverySensorDouble] = field(default_factory=list)
    market_data_sensors: list[_MarketDataSensorDouble] = field(default_factory=list)

    def discovery_factory(self, **kwargs: object) -> _DiscoverySensorDouble:
        del kwargs
        discovery = _DiscoverySensorDouble()
        self.discoveries.append(discovery)
        return discovery

    def market_data_factory(self, **kwargs: object) -> _MarketDataSensorDouble:
        del kwargs
        market_data = _MarketDataSensorDouble()
        self.market_data_sensors.append(market_data)
        return market_data


class _NoopFactorService:
    def __init__(self, **kwargs: object) -> None:
        del kwargs

    async def run(self) -> None:
        return None


class _NoopPolymarketClient:
    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> PolymarketOrderResult:
        del order, credentials
        raise AssertionError("subscription reselection tests must not submit live orders")


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.LIVE,
        live_trading_enabled=True,
        auto_migrate_default_v2=False,
        polymarket=PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0xabc",
        ),
        database=DatabaseSettings(
            dsn=cast(str, PMS_TEST_DATABASE_URL),
            pool_min_size=1,
            pool_max_size=3,
        ),
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="cp07-user-subscription",
        token_id="cp07-user-token",
        venue="polymarket",
        title="Will CP07 user subscriptions select?",
        yes_price=0.55,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 5, 1, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={},
        fetched_at=datetime(2026, 4, 24, 9, 0, tzinfo=UTC),
        market_status="open",
    )


async def _seed_market_with_token(
    pg_pool: asyncpg.Pool,
    *,
    market_id: str,
    token_id: str,
) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = datetime(2026, 4, 24, 9, 0, tzinfo=UTC)
    await store.write_market(
        Market(
            condition_id=market_id,
            slug=market_id,
            question=f"Will {market_id} be selectable?",
            venue="polymarket",
            resolves_at=now + timedelta(days=7),
            created_at=now,
            last_seen_at=now,
            volume_24h=1_000.0,
        )
    )
    await store.write_token(
        Token(
            token_id=token_id,
            condition_id=market_id,
            outcome="YES",
        )
    )


def _install_runner_sensor_doubles(
    monkeypatch: pytest.MonkeyPatch,
) -> _SensorDoubles:
    sensors = _SensorDoubles()

    async def _noop_ensure_factor_catalog(
        pool: object,
        *,
        factor_ids: object = None,
    ) -> None:
        del pool, factor_ids

    monkeypatch.setattr("pms.runner.ensure_factor_catalog", _noop_ensure_factor_catalog)
    monkeypatch.setattr("pms.runner.FactorService", _NoopFactorService)
    monkeypatch.setattr("pms.runner.MarketDiscoverySensor", sensors.discovery_factory)
    monkeypatch.setattr("pms.runner.MarketDataSensor", sensors.market_data_factory)
    monkeypatch.setattr("pms.runner.PolymarketSDKClient", _NoopPolymarketClient)
    return sensors


async def _wait_for_subscription_update(
    sensor: _MarketDataSensorDouble,
    token_id: str,
) -> None:
    for _ in range(60):
        if any(token_id in update for update in sensor.updates):
            return
        await asyncio.sleep(0.05)
    pytest.fail(f"{token_id} never appeared in subscription updates: {sensor.updates}")


async def _wait_for_discovery_poll(sensor: _DiscoverySensorDouble) -> None:
    await asyncio.wait_for(sensor.poll_complete.wait(), timeout=2.0)


@pytest.mark.asyncio(loop_scope="session")
async def test_subscription_survives_runner_restart(
    pg_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensors = _install_runner_sensor_doubles(monkeypatch)
    token_id = "cp07-restart-token"
    await _seed_market_with_token(
        pg_pool,
        market_id="cp07-restart-market",
        token_id=token_id,
    )
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        started = await client.post("/run/start")
        assert started.status_code == 200
        await _wait_for_discovery_poll(sensors.discoveries[-1])

        subscribed = await client.post(f"/markets/{token_id}/subscribe")
        assert subscribed.status_code == 200

        stopped = await client.post("/run/stop")
        assert stopped.status_code == 200

        restarted = await client.post("/run/start")
        assert restarted.status_code == 200
        await _wait_for_discovery_poll(sensors.discoveries[-1])
        await _wait_for_subscription_update(sensors.market_data_sensors[-1], token_id)

        markets = await client.get("/markets?limit=20&offset=0")
        await client.post("/run/stop")

    assert markets.status_code == 200
    row = markets.json()["markets"][0]
    assert row["market_id"] == "cp07-restart-market"
    assert row["subscription_source"] == "user"
    assert row["subscribed"] is True


@pytest.mark.asyncio(loop_scope="session")
async def test_live_reselection_reads_user_subscriptions_without_restart(
    pg_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensors = _install_runner_sensor_doubles(monkeypatch)
    token_id = "cp07-live-token"
    await _seed_market_with_token(
        pg_pool,
        market_id="cp07-live-market",
        token_id=token_id,
    )
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        started = await client.post("/run/start")
        assert started.status_code == 200
        await _wait_for_discovery_poll(sensors.discoveries[-1])

        subscribed = await client.post(f"/markets/{token_id}/subscribe")
        assert subscribed.status_code == 200

        await runner._request_reselection()  # noqa: SLF001
        await _wait_for_subscription_update(sensors.market_data_sensors[-1], token_id)

        markets = await client.get("/markets?limit=20&offset=0")
        await client.post("/run/stop")

    assert markets.status_code == 200
    row = markets.json()["markets"][0]
    assert row["market_id"] == "cp07-live-market"
    assert row["subscription_source"] == "user"
    assert row["subscribed"] is True
