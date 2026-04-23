from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg
import httpx
import pytest

from pms.api.app import create_app
from pms.config import DatabaseSettings, PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, RunMode, Side, TimeInForce
from pms.core.models import Market, MarketSignal, Token, TradeDecision
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


class OneShotSensor:
    def __init__(self, signal: MarketSignal) -> None:
        self.signal = signal

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        yield self.signal


class SingleDecisionController:
    async def decide(
        self,
        signal: MarketSignal,
        portfolio: object,
    ) -> TradeDecision:
        del portfolio
        return TradeDecision(
            decision_id="decision-cp06",
            market_id=signal.market_id,
            token_id=signal.token_id,
            venue=signal.venue,
            side=Side.BUY.value,
            notional_usdc=20.5,
            order_type="limit",
            max_slippage_bps=50,
            stop_conditions=["cp06"],
            prob_estimate=0.72,
            expected_edge=0.18,
            time_in_force=TimeInForce.GTC,
            opportunity_id="opportunity-cp06",
            strategy_id="default",
            strategy_version_id="default-v1",
            limit_price=0.41,
            action=Side.BUY.value,
            model_id="model-a",
        )


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.PAPER,
        auto_migrate_default_v2=False,
        api_token="expected-token",
        database=DatabaseSettings(dsn=cast(str, PMS_TEST_DATABASE_URL)),
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="market-cp06",
        token_id="market-cp06-yes",
        venue="polymarket",
        title="Will CP06 route persisted fills?",
        yes_price=0.41,
        volume_24h=1200.0,
        resolves_at=datetime(2026, 5, 1, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.40, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal={"resolved_outcome": 1.0},
        fetched_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


async def _seed_market_data(pg_pool: asyncpg.Pool) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    await store.write_market(
        Market(
            condition_id="market-cp06",
            slug="market-cp06",
            question="Will CP06 route persisted fills?",
            venue="polymarket",
            resolves_at=now + timedelta(days=7),
            created_at=now - timedelta(days=1),
            last_seen_at=now,
            volume_24h=1200.0,
        )
    )
    await store.write_token(
        Token(
            token_id="market-cp06-yes",
            condition_id="market-cp06",
            outcome="YES",
        )
    )
    await store.write_token(
        Token(
            token_id="market-cp06-no",
            condition_id="market-cp06",
            outcome="NO",
        )
    )


async def _wait_for(predicate: Any, *, timeout_s: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition did not become true before timeout")


def _client(runner: Runner) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(runner, auto_start=False)),
        base_url="http://test",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_positions_and_trades_routes_reflect_persisted_paper_fill(
    pg_pool: asyncpg.Pool,
) -> None:
    await _seed_market_data(pg_pool)

    runner = Runner(
        config=_settings(),
        sensors=[OneShotSensor(_signal())],
        controller=SingleDecisionController(),
    )
    runner.bind_pg_pool(pg_pool)

    await runner.start()
    await _wait_for(lambda: len(runner.state.fills) == 1)
    await _wait_for(
        lambda: runner.controller_task is not None
        and runner.controller_task.done()
        and runner.actuator_task is not None
        and runner.actuator_task.done()
    )

    async with pg_pool.acquire() as connection:
        fill_row = await connection.fetchrow(
            """
            SELECT fill_id, market_id, fill_notional_usdc, fill_quantity
            FROM fills
            WHERE market_id = 'market-cp06'
            """
        )

    assert fill_row is not None
    assert fill_row["market_id"] == "market-cp06"
    assert fill_row["fill_notional_usdc"] == pytest.approx(20.5)
    assert fill_row["fill_quantity"] == pytest.approx(50.0)

    async with _client(runner) as client:
        positions = await client.get(
            "/positions",
            headers={"Authorization": "Bearer expected-token"},
        )
        trades = await client.get(
            "/trades?limit=10",
            headers={"Authorization": "Bearer expected-token"},
        )

    assert positions.status_code == 200
    assert positions.json() == {
        "positions": [
            {
                "market_id": "market-cp06",
                "token_id": "market-cp06-yes",
                "venue": "polymarket",
                "side": "BUY",
                "shares_held": 50.0,
                "avg_entry_price": 0.41,
                "unrealized_pnl": 0.0,
                "locked_usdc": 20.5,
            }
        ]
    }
    assert trades.status_code == 200
    assert trades.json() == {
        "trades": [
            {
                "trade_id": "order-market-cp06",
                "fill_id": "order-market-cp06",
                "order_id": "order-market-cp06",
                "decision_id": "decision-cp06",
                "market_id": "market-cp06",
                "question": "Will CP06 route persisted fills?",
                "token_id": "market-cp06-yes",
                "venue": "polymarket",
                "side": "BUY",
                "fill_price": 0.41,
                "fill_notional_usdc": 20.5,
                "fill_quantity": 50.0,
                "executed_at": "2026-04-23T10:00:00+00:00",
                "filled_at": "2026-04-23T10:00:00+00:00",
                "status": "matched",
                "strategy_id": "default",
                "strategy_version_id": "default-v1",
            }
        ],
        "limit": 10,
    }
