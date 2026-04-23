from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import asyncpg
import pytest

from pms.core.models import FillRecord, OrderState, Position
from pms.storage.fill_store import FillStore, _json_object as fill_json_object
from pms.storage.fill_store import StoredTradeRow, _string_list
from pms.storage.order_store import OrderStore, _json_object as order_json_object


class _TransactionRecorder:
    def __init__(self, connection: "_RecordingConnection") -> None:
        self._connection = connection

    async def __aenter__(self) -> "_TransactionRecorder":
        self._connection.in_transaction = True
        self._connection.transaction_entries += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb
        self._connection.in_transaction = False


class _RecordingConnection:
    def __init__(self) -> None:
        self.in_transaction = False
        self.transaction_entries = 0
        self.execute_flags: list[bool] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_rows: list[object] = []

    async def execute(self, query: str, *args: object) -> str:
        del query, args
        self.execute_flags.append(self.in_transaction)
        return "OK"

    async def fetch(self, query: str, *args: object) -> list[object]:
        self.fetch_calls.append((query, args))
        return list(self.fetch_rows)

    def transaction(self) -> _TransactionRecorder:
        return _TransactionRecorder(self)


class _AcquireContext:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _RecordingConnection:
        return self._connection

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb


class _RecordingPool:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(self._connection)


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


@pytest.mark.asyncio
async def test_order_store_insert_wraps_shell_and_payload_writes_in_transaction() -> None:
    connection = _RecordingConnection()
    store = OrderStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    await store.insert(_order_state())

    assert connection.transaction_entries == 1
    assert connection.execute_flags == [False, True, True]


@pytest.mark.asyncio
async def test_fill_store_insert_wraps_shell_and_payload_writes_in_transaction() -> None:
    connection = _RecordingConnection()
    store = FillStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    await store.insert(_fill_record())

    assert connection.transaction_entries == 1
    assert connection.execute_flags == [False, True, True]


@pytest.mark.asyncio
async def test_fill_store_read_positions_maps_aggregated_rows() -> None:
    connection = _RecordingConnection()
    connection.fetch_rows = [
        {
            "market_id": "market-unit-cp10-1",
            "token_id": "token-unit-cp10-1",
            "venue": "polymarket",
            "side": "yes",
            "shares_held": 400.0,
            "avg_entry_price": 0.25,
            "locked_usdc": 100.0,
        }
    ]
    store = FillStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    positions = await store.read_positions()

    assert positions == [
        Position(
            market_id="market-unit-cp10-1",
            token_id="token-unit-cp10-1",
            venue="polymarket",
            side="yes",
            shares_held=400.0,
            avg_entry_price=0.25,
            unrealized_pnl=0.0,
            locked_usdc=100.0,
        )
    ]
    assert "GROUP BY" in connection.fetch_calls[0][0]


@pytest.mark.asyncio
async def test_fill_store_read_trades_maps_joined_market_rows_and_skips_missing_payloads() -> None:
    connection = _RecordingConnection()
    connection.fetch_rows = [
        {
            "fill_id": "fill-unit-cp10-1",
            "order_id": "order-unit-cp10-1",
            "market_id": "market-unit-cp10-1",
            "ts": datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
            "fill_notional_usdc": 100.0,
            "fill_quantity": 400.0,
            "strategy_id": "default",
            "strategy_version_id": "default-v2",
            "question": "Will CP10 fill rows persist?",
            "payload": {
                "trade_id": "trade-unit-cp10-1",
                "decision_id": "decision-unit-cp10-1",
                "token_id": "token-unit-cp10-1",
                "venue": "polymarket",
                "side": "yes",
                "fill_price": 0.25,
                "executed_at": "2026-04-21T10:00:00+00:00",
                "status": "filled",
            },
        },
        {
            "fill_id": "fill-unit-cp10-2",
            "order_id": "order-unit-cp10-2",
            "market_id": "market-unit-cp10-2",
            "ts": datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
            "fill_notional_usdc": 50.0,
            "fill_quantity": 200.0,
            "strategy_id": "default",
            "strategy_version_id": "default-v2",
            "question": "skip me",
            "payload": None,
        },
    ]
    store = FillStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    trades = await store.read_trades(limit=10)

    assert trades == [
        StoredTradeRow(
            trade_id="trade-unit-cp10-1",
            fill_id="fill-unit-cp10-1",
            order_id="order-unit-cp10-1",
            decision_id="decision-unit-cp10-1",
            market_id="market-unit-cp10-1",
            question="Will CP10 fill rows persist?",
            token_id="token-unit-cp10-1",
            venue="polymarket",
            side="yes",
            fill_price=0.25,
            fill_notional_usdc=100.0,
            fill_quantity=400.0,
            executed_at=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
            filled_at=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
            status="filled",
            strategy_id="default",
            strategy_version_id="default-v2",
        )
    ]
    assert connection.fetch_calls[0][1] == (10,)
