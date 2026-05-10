from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from pms.core.models import BookLevel, BookSnapshot, FillRecord, Market, OrderState, Token
from pms.storage.fill_store import FillStore
from pms.storage.market_data_store import PostgresMarketDataStore
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


@pytest.mark.asyncio(loop_scope="session")
async def test_fill_store_read_positions_prefers_clob_bid_over_stale_market_price(
    pg_pool: asyncpg.Pool,
) -> None:
    market_store = PostgresMarketDataStore(pg_pool)
    fill_store = FillStore(pg_pool)
    order_store = OrderStore(pg_pool)
    now = datetime(2026, 5, 9, 12, 50, 34, tzinfo=UTC)
    market_id = "market-cp10-mtm"
    token_id = "token-cp10-mtm-yes"

    await market_store.write_market(
        Market(
            condition_id=market_id,
            slug=market_id,
            question="Will MtM prefer the live CLOB bid?",
            venue="polymarket",
            resolves_at=now + timedelta(days=7),
            created_at=now - timedelta(days=1),
            last_seen_at=now,
            yes_price=0.6306,
            no_price=0.3694,
            price_updated_at=now - timedelta(hours=3),
        )
    )
    await market_store.write_token(
        Token(token_id=token_id, condition_id=market_id, outcome="YES")
    )
    await market_store.write_book_snapshot(
        BookSnapshot(
            id=0,
            market_id=market_id,
            token_id=token_id,
            ts=now - timedelta(minutes=5),
            hash="old-book",
            source="subscribe",
        ),
        [
            BookLevel(
                snapshot_id=0,
                market_id=market_id,
                side="BUY",
                price=0.55,
                size=50.0,
            )
        ],
    )
    await market_store.write_book_snapshot(
        BookSnapshot(
            id=0,
            market_id=market_id,
            token_id=token_id,
            ts=now,
            hash="latest-book",
            source="subscribe",
        ),
        [
            BookLevel(
                snapshot_id=0,
                market_id=market_id,
                side="BUY",
                price=0.25,
                size=20.0,
            ),
            BookLevel(
                snapshot_id=0,
                market_id=market_id,
                side="BUY",
                price=0.261,
                size=20.0,
            ),
            BookLevel(
                snapshot_id=0,
                market_id=market_id,
                side="SELL",
                price=0.29,
                size=20.0,
            ),
        ],
    )
    await order_store.insert(
        OrderState(
            order_id="order-cp10-mtm-1",
            decision_id="decision-cp10-mtm-1",
            status="matched",
            market_id=market_id,
            token_id=token_id,
            venue="polymarket",
            requested_notional_usdc=1.99923,
            filled_notional_usdc=1.99923,
            remaining_notional_usdc=0.0,
            fill_price=0.309,
            submitted_at=now - timedelta(minutes=1),
            last_updated_at=now - timedelta(minutes=1),
            raw_status="matched",
            strategy_id="default",
            strategy_version_id="default-v2",
            filled_quantity=6.47,
            action="BUY",
            outcome="YES",
        )
    )
    await fill_store.insert(
        FillRecord(
            trade_id="trade-cp10-mtm-1",
            fill_id="fill-cp10-mtm-1",
            order_id="order-cp10-mtm-1",
            decision_id="decision-cp10-mtm-1",
            market_id=market_id,
            token_id=token_id,
            venue="polymarket",
            side="BUY",
            fill_price=0.309,
            fill_notional_usdc=1.99923,
            fill_quantity=6.47,
            executed_at=now - timedelta(minutes=1),
            filled_at=now - timedelta(minutes=1),
            status="filled",
            anomaly_flags=[],
            strategy_id="default",
            strategy_version_id="default-v2",
        )
    )

    positions = await fill_store.read_positions()

    assert len(positions) == 1
    assert positions[0].avg_entry_price == pytest.approx(0.309)
    assert positions[0].unrealized_pnl == pytest.approx((0.261 - 0.309) * 6.47)
