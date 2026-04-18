from __future__ import annotations

import os
from datetime import UTC, datetime

import asyncpg
import pytest

from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.core.models import EvalRecord, Feedback
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.storage.strategy_registry import PostgresStrategyRegistry


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


def _strategy(*, drawdown_pct: float) -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id="default",
            factor_composition=(
                FactorCompositionStep(
                    factor_id="factor-a",
                    role="weighted",
                    param="",
                    weight=0.6,
                    threshold=None,
                ),
                FactorCompositionStep(
                    factor_id="factor-b",
                    role="weighted",
                    param="",
                    weight=0.4,
                    threshold=None,
                ),
            ),
            metadata=(("owner", "system"), ("tier", "default")),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=drawdown_pct,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        forecaster=ForecasterSpec(
            forecasters=(
                ("rules", (("threshold", "0.55"),)),
                ("stats", (("window", "15m"),)),
            )
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_feedback_and_eval_stores_use_active_strategy_version_by_default(
    pg_pool: asyncpg.Pool,
) -> None:
    active_version = await PostgresStrategyRegistry(pg_pool).create_version(
        _strategy(drawdown_pct=3.5)
    )
    feedback_store = FeedbackStore(pg_pool)
    eval_store = EvalStore(pg_pool)
    created_at = datetime(2026, 4, 17, tzinfo=UTC)

    await feedback_store.append(
        Feedback(
            feedback_id="reservation-feedback",
            target="controller",
            source="evaluator",
            message="verify null strategy columns",
            severity="warning",
            created_at=created_at,
            category="reservation",
        )
    )
    await eval_store.append(
        EvalRecord(
            market_id="reservation-market",
            decision_id="reservation-decision",
            prob_estimate=0.63,
            resolved_outcome=1.0,
            brier_score=0.1369,
            fill_status="matched",
            recorded_at=created_at,
            citations=["reservation-trade"],
            category="reservation",
            model_id="reservation-model",
            pnl=4.5,
            slippage_bps=6.0,
            filled=True,
        )
    )

    async with pg_pool.acquire() as connection:
        feedback_row = await connection.fetchrow(
            """
            SELECT strategy_id, strategy_version_id
            FROM feedback
            WHERE feedback_id = $1
            """,
            "reservation-feedback",
        )
        eval_row = await connection.fetchrow(
            """
            SELECT strategy_id, strategy_version_id
            FROM eval_records
            WHERE decision_id = $1
            """,
            "reservation-decision",
    )

    assert feedback_row is not None
    assert feedback_row["strategy_id"] == "default"
    assert feedback_row["strategy_version_id"] == active_version.strategy_version_id
    assert eval_row is not None
    assert eval_row["strategy_id"] == "default"
    assert eval_row["strategy_version_id"] == active_version.strategy_version_id


@pytest.mark.asyncio(loop_scope="session")
async def test_orders_and_fills_accept_null_strategy_columns(
    pg_pool: asyncpg.Pool,
) -> None:
    # S5-MIGRATION-MARKER: flip NULL asserts after NOT NULL upgrade lands
    async with pg_pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO orders (
                order_id,
                market_id,
                ts,
                strategy_id,
                strategy_version_id
            ) VALUES ($1, $2, $3, $4, $5)
            """,
            "reservation-order",
            "reservation-market",
            datetime(2026, 4, 17, tzinfo=UTC),
            None,
            None,
        )
        await connection.execute(
            """
            INSERT INTO fills (
                fill_id,
                order_id,
                market_id,
                ts,
                strategy_id,
                strategy_version_id
            ) VALUES ($1, $2, $3, $4, $5, $6)
            """,
            "reservation-fill",
            "reservation-order",
            "reservation-market",
            datetime(2026, 4, 17, 0, 1, tzinfo=UTC),
            None,
            None,
        )
        order_row = await connection.fetchrow(
            """
            SELECT strategy_id, strategy_version_id
            FROM orders
            WHERE order_id = $1
            """,
            "reservation-order",
        )
        fill_row = await connection.fetchrow(
            """
            SELECT strategy_id, strategy_version_id
            FROM fills
            WHERE fill_id = $1
            """,
            "reservation-fill",
        )

    assert order_row is not None
    assert order_row["strategy_id"] is None
    assert order_row["strategy_version_id"] is None
    assert fill_row is not None
    assert fill_row["strategy_id"] is None
    assert fill_row["strategy_version_id"] is None
