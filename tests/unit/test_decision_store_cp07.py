from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from typing import cast

import asyncpg
import pytest

from pms.core.enums import TimeInForce
from pms.core.models import TradeDecision
from pms.storage.decision_store import DecisionStore, validate_decision_status_transition


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
        self.execute_calls: list[tuple[str, tuple[object, ...], bool]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_rows: list[object] = []
        self.fetchrow_row: object | None = None

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args, self.in_transaction))
        return "OK"

    async def fetch(self, query: str, *args: object) -> list[object]:
        self.fetch_calls.append((query, args))
        return list(self.fetch_rows)

    async def fetchrow(self, query: str, *args: object) -> object | None:
        self.fetchrow_calls.append((query, args))
        return self.fetchrow_row

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


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="decision-cp07",
        market_id="market-cp07",
        token_id="token-cp07-yes",
        venue="polymarket",
        side="BUY",
        notional_usdc=25.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["cp07"],
        prob_estimate=0.68,
        expected_edge=0.21,
        time_in_force=TimeInForce.GTC,
        opportunity_id="opportunity-cp07",
        strategy_id="default",
        strategy_version_id="default-v1",
        limit_price=0.41,
        action="BUY",
        model_id="model-cp07",
    )


def _decision_db_row() -> dict[str, object]:
    created_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    expires_at = created_at + timedelta(minutes=15)
    return {
        "decision_id": "decision-cp07",
        "opportunity_id": "opportunity-cp07",
        "strategy_id": "default",
        "strategy_version_id": "default-v1",
        "status": "pending",
        "factor_snapshot_hash": "snapshot-cp07",
        "created_at": created_at,
        "updated_at": created_at,
        "expires_at": expires_at,
        "payload": json.dumps(
            {
                "decision_id": "decision-cp07",
                "market_id": "market-cp07",
                "token_id": "token-cp07-yes",
                "venue": "polymarket",
                "side": "BUY",
                "notional_usdc": 25.0,
                "order_type": "limit",
                "max_slippage_bps": 50,
                "stop_conditions": ["cp07"],
                "prob_estimate": 0.68,
                "expected_edge": 0.21,
                "time_in_force": "GTC",
                "opportunity_id": "opportunity-cp07",
                "strategy_id": "default",
                "strategy_version_id": "default-v1",
                "limit_price": 0.41,
                "action": "BUY",
                "outcome": "YES",
                "model_id": "model-cp07",
            }
        ),
        "opportunity_row_id": "opportunity-cp07",
        "opportunity_market_id": "market-cp07",
        "opportunity_token_id": "token-cp07-yes",
        "opportunity_side": "yes",
        "selected_factor_values": {"edge": 0.21, "liquidity": 0.04},
        "opportunity_expected_edge": 0.21,
        "rationale": "cp07 rationale",
        "target_size_usdc": 25.0,
        "opportunity_expiry": expires_at,
        "staleness_policy": "cp07",
        "opportunity_strategy_id": "default",
        "opportunity_strategy_version_id": "default-v1",
        "opportunity_created_at": created_at,
        "opportunity_factor_snapshot_hash": "snapshot-cp07",
        "composition_trace": {"kind": "unit"},
    }


@pytest.mark.parametrize(
    ("current_status", "next_status"),
    [
        ("accepted", "pending"),
        ("expired", "accepted"),
        ("rejected", "accepted"),
    ],
)
def test_validate_decision_status_transition_rejects_invalid_paths(
    current_status: str,
    next_status: str,
) -> None:
    with pytest.raises(ValueError, match=rf"{current_status} -> {next_status}"):
        validate_decision_status_transition(current_status, next_status)


@pytest.mark.asyncio
async def test_decision_store_insert_wraps_shell_and_payload_writes_in_transaction() -> None:
    connection = _RecordingConnection()
    store = DecisionStore(cast(asyncpg.Pool, _RecordingPool(connection)))
    created_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    expires_at = created_at + timedelta(minutes=15)

    await store.insert(
        _decision(),
        factor_snapshot_hash="snapshot-cp07",
        created_at=created_at,
        expires_at=expires_at,
    )

    assert connection.transaction_entries == 1
    assert [in_transaction for _, _, in_transaction in connection.execute_calls] == [
        False,
        True,
        True,
    ]

    decision_query, decision_args, _ = connection.execute_calls[1]
    assert "INSERT INTO decisions" in decision_query
    assert decision_args == (
        "decision-cp07",
        "opportunity-cp07",
        "default",
        "default-v1",
        "pending",
        "snapshot-cp07",
        created_at,
        created_at,
        expires_at,
    )

    payload_query, payload_args, _ = connection.execute_calls[2]
    assert "INSERT INTO decision_payloads" in payload_query
    assert payload_args[0] == "decision-cp07"
    payload = json.loads(cast(str, payload_args[1]))
    assert payload["decision_id"] == "decision-cp07"
    assert payload["market_id"] == "market-cp07"
    assert payload["opportunity_id"] == "opportunity-cp07"
    assert payload["strategy_version_id"] == "default-v1"
    assert payload["max_slippage_bps"] == 50


@pytest.mark.asyncio
async def test_decision_store_read_decisions_rehydrates_payload_and_opportunity() -> None:
    connection = _RecordingConnection()
    connection.fetch_rows = [_decision_db_row()]
    store = DecisionStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    rows = await store.read_decisions(
        limit=5,
        status="pending",
        include_opportunity=True,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.decision.decision_id == "decision-cp07"
    assert row.decision.time_in_force is TimeInForce.GTC
    assert row.status == "pending"
    assert row.factor_snapshot_hash == "snapshot-cp07"
    assert row.opportunity is not None
    assert row.opportunity.selected_factor_values == {
        "edge": 0.21,
        "liquidity": 0.04,
    }
    assert row.opportunity.composition_trace == {"kind": "unit"}
    query, args = connection.fetch_calls[0]
    assert "FROM decisions" in query
    assert "LEFT JOIN opportunities" in query
    assert args == ("pending", 5)


@pytest.mark.asyncio
async def test_decision_store_get_decision_can_omit_opportunity_payload() -> None:
    connection = _RecordingConnection()
    connection.fetchrow_row = _decision_db_row()
    store = DecisionStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    row = await store.get_decision(
        "decision-cp07",
        include_opportunity=False,
    )

    assert row is not None
    assert row.decision.market_id == "market-cp07"
    assert row.opportunity is None
    query, args = connection.fetchrow_calls[0]
    assert "WHERE decisions.decision_id = $1" in query
    assert args == ("decision-cp07",)


@pytest.mark.asyncio
async def test_decision_store_update_status_uses_expected_current_state() -> None:
    connection = _RecordingConnection()
    connection.fetchrow_row = {"decision_id": "decision-cp07"}
    store = DecisionStore(cast(asyncpg.Pool, _RecordingPool(connection)))
    updated_at = datetime(2026, 4, 23, 10, 30, tzinfo=UTC)

    updated = await store.update_status(
        "decision-cp07",
        current_status="pending",
        next_status="accepted",
        updated_at=updated_at,
    )

    assert updated is True
    query, args = connection.fetchrow_calls[0]
    assert "UPDATE decisions" in query
    assert "AND status = $2" in query
    assert args == ("decision-cp07", "pending", "accepted", updated_at)


@pytest.mark.asyncio
async def test_decision_store_expire_pending_updates_matching_rows() -> None:
    connection = _RecordingConnection()
    connection.fetch_rows = [
        {"decision_id": "decision-a"},
        {"decision_id": "decision-b"},
    ]
    store = DecisionStore(cast(asyncpg.Pool, _RecordingPool(connection)))
    cutoff = datetime(2026, 4, 23, 10, 30, tzinfo=UTC)

    expired = await store.expire_pending(before=cutoff)

    assert expired == 2
    assert len(connection.fetch_calls) == 1
    query, args = connection.fetch_calls[0]
    assert "UPDATE decisions" in query
    assert "status IN ('pending', 'accepted', 'queued')" in query
    assert "RETURNING decision_id" in query
    assert args == (cutoff,)
