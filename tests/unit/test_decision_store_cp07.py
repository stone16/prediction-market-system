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
        self.fetch_rows: list[object] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args, self.in_transaction))
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
    assert payload["market_id"] == "market-cp07"
    assert payload["opportunity_id"] == "opportunity-cp07"
    assert payload["strategy_version_id"] == "default-v1"
    assert payload["max_slippage_bps"] == 50


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
    assert "status = 'pending'" in query
    assert "RETURNING decision_id" in query
    assert args == (cutoff,)
