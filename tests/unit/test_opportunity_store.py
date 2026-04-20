from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import asyncpg
import pytest

from pms.core.models import Opportunity
from pms.storage.opportunity_store import OpportunityStore, _opportunity_from_row


class RaisingConnection:
    async def execute(self, query: str, *args: object) -> str:
        del query, args
        raise asyncpg.UndefinedTableError("opportunities missing")


class RaisingAcquire:
    async def __aenter__(self) -> RaisingConnection:
        return RaisingConnection()

    async def __aexit__(self, *_: object) -> None:
        return None


class RaisingPool:
    def acquire(self) -> RaisingAcquire:
        return RaisingAcquire()


class RecordingConnection:
    def __init__(self) -> None:
        self.query: str | None = None
        self.args: tuple[object, ...] | None = None

    async def execute(self, query: str, *args: object) -> str:
        self.query = query
        self.args = args
        return "INSERT 0 1"


class RecordingAcquire:
    def __init__(self, connection: RecordingConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> RecordingConnection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        return None


class RecordingPool:
    def __init__(self) -> None:
        self.connection = RecordingConnection()

    def acquire(self) -> RecordingAcquire:
        return RecordingAcquire(self.connection)


def _opportunity() -> Opportunity:
    return Opportunity(
        opportunity_id="opp-1",
        market_id="market-1",
        token_id="token-1",
        side="yes",
        selected_factor_values={"edge": 0.42},
        expected_edge=0.42,
        rationale="edge available",
        target_size_usdc=25.0,
        expiry=None,
        staleness_policy="strict",
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        created_at=datetime(2026, 4, 19, 15, 0, tzinfo=UTC),
        factor_snapshot_hash="snapshot-1",
        composition_trace={
            "selected_probability": 0.61,
            "branch_probabilities": {"rules": 0.61},
        },
    )


@pytest.mark.asyncio
async def test_opportunity_store_insert_propagates_missing_table_errors() -> None:
    store = OpportunityStore(pool=cast(asyncpg.Pool, RaisingPool()))

    with pytest.raises(asyncpg.UndefinedTableError, match="opportunities missing"):
        await store.insert(_opportunity())


@pytest.mark.asyncio
async def test_opportunity_store_insert_serializes_trace_fields() -> None:
    pool = RecordingPool()
    store = OpportunityStore(pool=cast(asyncpg.Pool, pool))

    await store.insert(_opportunity())

    assert pool.connection.query is not None
    assert "factor_snapshot_hash" in pool.connection.query
    assert "composition_trace" in pool.connection.query
    assert pool.connection.args is not None
    assert pool.connection.args[13] == "snapshot-1"
    assert pool.connection.args[14] == '{"selected_probability": 0.61, "branch_probabilities": {"rules": 0.61}}'


def test_opportunity_from_row_skips_boolean_factor_values() -> None:
    row = {
        "opportunity_id": "opp-1",
        "market_id": "market-1",
        "token_id": "token-1",
        "side": "yes",
        "selected_factor_values": {"numeric": 0.42, "flag": True, "count": 2},
        "expected_edge": 0.42,
        "rationale": "edge available",
        "target_size_usdc": 25.0,
        "expiry": None,
        "staleness_policy": "strict",
        "strategy_id": "alpha",
        "strategy_version_id": "alpha-v1",
        "created_at": datetime(2026, 4, 19, 15, 0, tzinfo=UTC),
        "factor_snapshot_hash": "snapshot-1",
        "composition_trace": {
            "selected_probability": 0.61,
            "branch_probabilities": {"rules": 0.61},
        },
    }

    opportunity = _opportunity_from_row(cast(Any, row))

    assert opportunity.selected_factor_values == {"numeric": 0.42, "count": 2.0}
    assert opportunity.factor_snapshot_hash == "snapshot-1"
    assert opportunity.composition_trace == {
        "selected_probability": 0.61,
        "branch_probabilities": {"rules": 0.61},
    }
