from __future__ import annotations

import os
from datetime import UTC, datetime

import asyncpg
import pytest

from pms.core.models import FillRecord, OrderState
from pms.storage.fill_store import FillStore
from pms.storage.order_store import OrderStore


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


def _order_state() -> OrderState:
    return OrderState(
        order_id="order-cp10-1",
        decision_id="decision-cp10-1",
        status="matched",
        market_id="market-cp10-1",
        token_id="token-cp10-1",
        venue="polymarket",
        requested_notional_usdc=125.0,
        filled_notional_usdc=80.0,
        remaining_notional_usdc=45.0,
        fill_price=0.25,
        submitted_at=datetime(2026, 4, 21, 9, 0, tzinfo=UTC),
        last_updated_at=datetime(2026, 4, 21, 9, 1, tzinfo=UTC),
        raw_status="partially_filled",
        strategy_id="default",
        strategy_version_id="default-v2",
        filled_quantity=320.0,
    )


def _fill_record() -> FillRecord:
    return FillRecord(
        trade_id="trade-cp10-1",
        fill_id="fill-cp10-1",
        order_id="order-cp10-1",
        decision_id="decision-cp10-1",
        market_id="market-cp10-1",
        token_id="token-cp10-1",
        venue="polymarket",
        side="yes",
        fill_price=0.25,
        fill_notional_usdc=80.0,
        fill_quantity=320.0,
        executed_at=datetime(2026, 4, 21, 9, 1, tzinfo=UTC),
        filled_at=datetime(2026, 4, 21, 9, 1, tzinfo=UTC),
        status="filled",
        anomaly_flags=["partial_fill", "slippage_checked"],
        strategy_id="default",
        strategy_version_id="default-v2",
        fee_bps=10,
        fees=0.8,
        liquidity_side="taker",
        transaction_ref="tx-cp10-1",
        resolved_outcome=1.0,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_order_store_round_trips_order_state(pg_pool: asyncpg.Pool) -> None:
    store = OrderStore(pg_pool)
    expected = _order_state()

    await store.insert(expected)
    actual = await store.get(expected.order_id)

    assert actual == expected

    async with pg_pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT
                order_id,
                market_id,
                requested_notional_usdc,
                filled_notional_usdc,
                remaining_notional_usdc,
                filled_quantity,
                strategy_id,
                strategy_version_id
            FROM orders
            WHERE order_id = $1
            """,
            expected.order_id,
        )

    assert row is not None
    assert row["order_id"] == expected.order_id
    assert row["market_id"] == expected.market_id
    assert row["requested_notional_usdc"] == expected.requested_notional_usdc
    assert row["filled_notional_usdc"] == expected.filled_notional_usdc
    assert row["remaining_notional_usdc"] == expected.remaining_notional_usdc
    assert row["filled_quantity"] == expected.filled_quantity
    assert row["strategy_id"] == expected.strategy_id
    assert row["strategy_version_id"] == expected.strategy_version_id


@pytest.mark.asyncio(loop_scope="session")
async def test_fill_store_round_trips_fill_record(pg_pool: asyncpg.Pool) -> None:
    order_store = OrderStore(pg_pool)
    fill_store = FillStore(pg_pool)
    order = _order_state()
    expected = _fill_record()

    await order_store.insert(order)
    await fill_store.insert(expected)
    actual = await fill_store.get(expected.fill_id)

    assert actual == expected

    async with pg_pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT
                fill_id,
                order_id,
                market_id,
                fill_notional_usdc,
                fill_quantity,
                strategy_id,
                strategy_version_id
            FROM fills
            WHERE fill_id = $1
            """,
            expected.fill_id,
        )

    assert row is not None
    assert row["fill_id"] == expected.fill_id
    assert row["order_id"] == expected.order_id
    assert row["market_id"] == expected.market_id
    assert row["fill_notional_usdc"] == expected.fill_notional_usdc
    assert row["fill_quantity"] == expected.fill_quantity
    assert row["strategy_id"] == expected.strategy_id
    assert row["strategy_version_id"] == expected.strategy_version_id
