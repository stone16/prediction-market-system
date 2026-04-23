from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg
import pytest

from pms.config import DatabaseSettings, PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import MarketSignal, Opportunity, OrderState, Portfolio, TradeDecision
from pms.runner import Runner


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


class SingleOpportunityController:
    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[Opportunity, TradeDecision] | None:
        del portfolio
        created_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
        opportunity = Opportunity(
            opportunity_id="opportunity-cp07",
            market_id=signal.market_id,
            token_id=cast(str, signal.token_id),
            side="yes",
            selected_factor_values={"edge": 0.22},
            expected_edge=0.22,
            rationale="persist decisions before accept",
            target_size_usdc=25.0,
            expiry=created_at + timedelta(minutes=15),
            staleness_policy="cp07",
            strategy_id="default",
            strategy_version_id="default-v1",
            created_at=created_at,
            factor_snapshot_hash="snapshot-cp07",
        )
        decision = TradeDecision(
            decision_id="decision-cp07",
            market_id=signal.market_id,
            token_id=signal.token_id,
            venue=signal.venue,
            side=Side.BUY.value,
            notional_usdc=25.0,
            order_type="limit",
            max_slippage_bps=50,
            stop_conditions=["cp07"],
            prob_estimate=0.68,
            expected_edge=0.22,
            time_in_force=TimeInForce.GTC,
            opportunity_id=opportunity.opportunity_id,
            strategy_id="default",
            strategy_version_id="default-v1",
            limit_price=0.41,
            action=Side.BUY.value,
            model_id="model-cp07",
        )
        return opportunity, decision


class RejectingExecutor:
    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio,
    ) -> OrderState:
        del portfolio
        now = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
        return OrderState(
            order_id=f"order-{decision.decision_id}",
            decision_id=decision.decision_id,
            status=OrderStatus.INVALID.value,
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            requested_notional_usdc=decision.notional_usdc,
            filled_notional_usdc=0.0,
            remaining_notional_usdc=decision.notional_usdc,
            fill_price=None,
            submitted_at=now,
            last_updated_at=now,
            raw_status="invalid",
            strategy_id=decision.strategy_id,
            strategy_version_id=decision.strategy_version_id,
            filled_quantity=0.0,
        )


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.BACKTEST,
        auto_migrate_default_v2=False,
        database=DatabaseSettings(dsn=cast(str, PMS_TEST_DATABASE_URL)),
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="market-cp07",
        token_id="token-cp07-yes",
        venue="polymarket",
        title="Will CP07 persist emitted decisions?",
        yes_price=0.41,
        volume_24h=1200.0,
        resolves_at=datetime(2026, 5, 1, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"resolved_outcome": 1.0},
        fetched_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_runner_persists_pending_decision_rows_on_emission(
    pg_pool: asyncpg.Pool,
) -> None:
    runner = Runner(
        config=_settings(),
        sensors=[OneShotSensor(_signal())],
        controller=SingleOpportunityController(),
    )
    runner.bind_pg_pool(pg_pool)
    runner.actuator_executor = cast(Any, RejectingExecutor())

    try:
        await runner.start()
        await asyncio.wait_for(runner.wait_until_idle(), timeout=10.0)

        async with pg_pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT
                    decision_id,
                    opportunity_id,
                    status,
                    factor_snapshot_hash,
                    strategy_id,
                    strategy_version_id
                FROM decisions
                WHERE decision_id = 'decision-cp07'
                """
            )

        assert row is not None
        assert row["decision_id"] == "decision-cp07"
        assert row["opportunity_id"] == "opportunity-cp07"
        assert row["status"] == "pending"
        assert row["factor_snapshot_hash"] == "snapshot-cp07"
        assert row["strategy_id"] == "default"
        assert row["strategy_version_id"] == "default-v1"
    finally:
        await runner.stop()


@pytest.mark.asyncio(loop_scope="session")
async def test_runner_sweep_expires_stale_pending_decisions(
    pg_pool: asyncpg.Pool,
) -> None:
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    now = datetime(2026, 4, 23, 11, 0, tzinfo=UTC)

    async with pg_pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO decisions (
                decision_id,
                opportunity_id,
                strategy_id,
                strategy_version_id,
                status,
                factor_snapshot_hash,
                created_at,
                updated_at,
                expires_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9
            )
            """,
            "decision-expired-cp07",
            "opportunity-expired-cp07",
            "default",
            "default-v1",
            "pending",
            "snapshot-expired",
            now - timedelta(minutes=10),
            now - timedelta(minutes=10),
            now - timedelta(seconds=1),
        )

    try:
        expired = await runner._sweep_expired_decisions_once(now=now)  # noqa: SLF001

        async with pg_pool.acquire() as connection:
            status = await connection.fetchval(
                """
                SELECT status
                FROM decisions
                WHERE decision_id = 'decision-expired-cp07'
                """
            )
    finally:
        await runner.close_pg_pool()

    assert expired == 1
    assert status == "expired"
