from __future__ import annotations

import os
from datetime import datetime
from typing import cast

import asyncpg
import pytest

from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import RiskSettings
from pms.core.enums import OrderStatus
from pms.core.models import Portfolio, TradeDecision
from pms.runner import Runner
from pms.storage.dedup_store import InMemoryDedupStore, PgDedupStore
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryFeedbackStore


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


def _decision(decision_id: str = "decision-executor-persistent-dedup") -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id="market-executor-persistent-dedup",
        token_id="token-executor-persistent-dedup",
        venue="polymarket",
        side="BUY",
        notional_usdc=10.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["executor-persistent-dedup"],
        prob_estimate=0.6,
        expected_edge=0.2,
        time_in_force="GTC",
        opportunity_id=f"op-{decision_id}",
        strategy_id="strategy-a",
        strategy_version_id="strategy-a-v1",
        action="BUY",
        limit_price=0.4,
        outcome="YES",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


async def _row_for(
    pool: asyncpg.Pool,
    decision_id: str,
) -> asyncpg.Record | None:
    async with pool.acquire() as connection:
        return await connection.fetchrow(
            """
            SELECT decision_id, released_at, outcome
            FROM order_intents
            WHERE decision_id = $1
            """,
            decision_id,
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_executor_pg_dedup_store_soft_releases_rejected_outcome_and_blocks_retry(
    pg_pool: asyncpg.Pool,
) -> None:
    dedup_store = PgDedupStore(pg_pool)
    feedback_store = cast(FeedbackStore, InMemoryFeedbackStore())
    first_executor = ActuatorExecutor(
        adapter=PaperActuator(
            orderbooks={
                "market-executor-persistent-dedup": {"bids": [], "asks": []}
            }
        ),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1_000.0)
        ),
        feedback=ActuatorFeedback(feedback_store),
        dedup_store=dedup_store,
    )
    decision = _decision()

    first = await first_executor.execute(decision, _portfolio())

    assert first.status == OrderStatus.INVALID.value
    assert first.raw_status == "insufficient_liquidity"

    row = await _row_for(pg_pool, decision.decision_id)
    assert row is not None
    assert row["outcome"] == "rejected"
    assert isinstance(row["released_at"], datetime)

    second_executor = ActuatorExecutor(
        adapter=PaperActuator(
            orderbooks={
                "market-executor-persistent-dedup": {
                    "bids": [{"price": 0.39, "size": 100.0}],
                    "asks": [{"price": 0.41, "size": 100.0}],
                }
            }
        ),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1_000.0)
        ),
        feedback=ActuatorFeedback(feedback_store),
        dedup_store=PgDedupStore(pg_pool),
    )

    second = await second_executor.execute(decision, _portfolio())

    assert second.status == OrderStatus.INVALID.value
    assert second.raw_status == "duplicate_decision"


@pytest.mark.asyncio(loop_scope="session")
async def test_runner_binds_pg_backed_dedup_store_when_pool_is_available(
    pg_pool: asyncpg.Pool,
) -> None:
    runner = Runner()

    initial_dedup_store = runner.actuator_executor.dedup_store
    assert isinstance(initial_dedup_store, InMemoryDedupStore)

    runner.bind_pg_pool(pg_pool)
    bound_dedup_store = runner.actuator_executor.dedup_store
    assert isinstance(bound_dedup_store, PgDedupStore)

    await runner.close_pg_pool()
    assert runner.pg_pool is None
    closed_dedup_store = runner.actuator_executor.dedup_store
    assert isinstance(closed_dedup_store, InMemoryDedupStore)
