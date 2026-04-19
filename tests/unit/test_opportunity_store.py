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
    )


@pytest.mark.asyncio
async def test_opportunity_store_insert_propagates_missing_table_errors() -> None:
    store = OpportunityStore(pool=cast(asyncpg.Pool, RaisingPool()))

    with pytest.raises(asyncpg.UndefinedTableError, match="opportunities missing"):
        await store.insert(_opportunity())


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
    }

    opportunity = _opportunity_from_row(cast(Any, row))

    assert opportunity.selected_factor_values == {"numeric": 0.42, "count": 2.0}
