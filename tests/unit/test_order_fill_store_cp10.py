from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import asyncpg
import pytest

from pms.core.models import FillRecord, OrderState
from pms.storage.fill_store import FillStore, _json_object as fill_json_object
from pms.storage.fill_store import _string_list
from pms.storage.order_store import OrderStore, _json_object as order_json_object


def _order_state() -> OrderState:
    return OrderState(
        order_id="order-unit-cp10-1",
        decision_id="decision-unit-cp10-1",
        status="matched",
        market_id="market-unit-cp10-1",
        token_id="token-unit-cp10-1",
        venue="polymarket",
        requested_notional_usdc=100.0,
        filled_notional_usdc=100.0,
        remaining_notional_usdc=0.0,
        fill_price=0.25,
        submitted_at=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        last_updated_at=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        raw_status="matched",
        strategy_id="default",
        strategy_version_id="default-v2",
        filled_quantity=400.0,
    )


def _fill_record() -> FillRecord:
    return FillRecord(
        trade_id="trade-unit-cp10-1",
        fill_id="fill-unit-cp10-1",
        order_id="order-unit-cp10-1",
        decision_id="decision-unit-cp10-1",
        market_id="market-unit-cp10-1",
        token_id="token-unit-cp10-1",
        venue="polymarket",
        side="yes",
        fill_price=0.25,
        fill_notional_usdc=100.0,
        fill_quantity=400.0,
        executed_at=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        filled_at=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        status="filled",
        anomaly_flags=["checked"],
        strategy_id="default",
        strategy_version_id="default-v2",
    )


def test_bind_pool_sets_store_pool_reference() -> None:
    pool = cast(asyncpg.Pool, object())

    order_store = OrderStore()
    fill_store = FillStore()

    order_store.bind_pool(pool)
    fill_store.bind_pool(pool)

    assert order_store.pool is pool
    assert fill_store.pool is pool


@pytest.mark.asyncio
async def test_order_store_requires_bound_pool_for_insert() -> None:
    with pytest.raises(RuntimeError, match="OrderStore pool is not bound"):
        await OrderStore().insert(_order_state())


@pytest.mark.asyncio
async def test_fill_store_requires_bound_pool_for_insert() -> None:
    with pytest.raises(RuntimeError, match="FillStore pool is not bound"):
        await FillStore().insert(_fill_record())


@pytest.mark.asyncio
async def test_store_get_short_circuits_without_pool_or_fill_id() -> None:
    assert await OrderStore().get("missing") is None
    assert await FillStore().get(None) is None


def test_payload_helpers_cover_dict_and_error_branches() -> None:
    assert order_json_object({"status": "matched"}) == {"status": "matched"}
    assert fill_json_object({"status": "filled"}) == {"status": "filled"}
    assert _string_list(["a", 2]) == ["a", "2"]
    assert _string_list(None) == []

    with pytest.raises(RuntimeError, match="order payload must be a JSON object"):
        order_json_object(123)

    with pytest.raises(RuntimeError, match="fill payload must be a JSON object"):
        fill_json_object(123)
