from __future__ import annotations

import os
from dataclasses import replace
from datetime import UTC, datetime

import asyncpg
import pytest

from pms.core.models import EvalRecord, FillRecord, OrderState
from pms.storage.eval_store import EvalStore
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


def _order_state(order_id: str, market_id: str) -> OrderState:
    return OrderState(
        order_id=order_id,
        decision_id=f"decision-{order_id}",
        status="matched",
        market_id=market_id,
        token_id="token-res-1",
        venue="polymarket",
        requested_notional_usdc=10.0,
        filled_notional_usdc=10.0,
        remaining_notional_usdc=0.0,
        fill_price=0.4,
        submitted_at=datetime(2026, 6, 1, 9, 0, tzinfo=UTC),
        last_updated_at=datetime(2026, 6, 1, 9, 1, tzinfo=UTC),
        raw_status="matched",
        strategy_id="default",
        strategy_version_id="default-v2",
        filled_quantity=25.0,
    )


def _fill_record(
    fill_id: str,
    *,
    market_id: str,
    resolved_outcome: float | None,
) -> FillRecord:
    return FillRecord(
        trade_id=f"trade-{fill_id}",
        fill_id=fill_id,
        order_id=f"order-{fill_id}",
        decision_id=f"decision-order-{fill_id}",
        market_id=market_id,
        token_id="token-res-1",
        venue="polymarket",
        side="BUY",
        fill_price=0.4,
        fill_notional_usdc=10.0,
        fill_quantity=25.0,
        executed_at=datetime(2026, 6, 1, 9, 1, tzinfo=UTC),
        filled_at=datetime(2026, 6, 1, 9, 1, tzinfo=UTC),
        status="MATCHED",
        anomaly_flags=[],
        strategy_id="default",
        strategy_version_id="default-v2",
        resolved_outcome=resolved_outcome,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_resolve_fill_transitions_null_outcome_exactly_once(
    pg_pool: asyncpg.Pool,
) -> None:
    order_store = OrderStore(pg_pool)
    fill_store = FillStore(pg_pool)
    unresolved = _fill_record(
        "fill-res-int-1",
        market_id="0xmarket-res-1",
        resolved_outcome=None,
    )
    already_resolved = _fill_record(
        "fill-res-int-2",
        market_id="0xmarket-res-2",
        resolved_outcome=0.0,
    )
    await order_store.insert(_order_state("order-fill-res-int-1", "0xmarket-res-1"))
    await order_store.insert(_order_state("order-fill-res-int-2", "0xmarket-res-2"))
    await fill_store.insert(unresolved)
    await fill_store.insert(already_resolved)

    pending = await fill_store.read_unresolved_fills()
    assert [fill.fill_id for fill in pending] == ["fill-res-int-1"]
    assert pending[0].resolved_outcome is None

    first_update = await fill_store.resolve_fill(
        "fill-res-int-1",
        resolved_outcome=1.0,
    )
    second_update = await fill_store.resolve_fill(
        "fill-res-int-1",
        resolved_outcome=1.0,
    )

    assert first_update is True
    assert second_update is False

    stored = await fill_store.get("fill-res-int-1")
    assert stored is not None
    assert stored.resolved_outcome == 1.0
    # Resolution only touches resolved_outcome; the rest of the payload
    # survives the jsonb update untouched.
    assert stored.decision_id == unresolved.decision_id
    assert stored.fill_price == unresolved.fill_price
    assert await fill_store.read_unresolved_fills() == []


@pytest.mark.asyncio(loop_scope="session")
async def test_eval_store_append_keeps_first_record_on_decision_id_conflict(
    pg_pool: asyncpg.Pool,
) -> None:
    """Retried sweeps (enqueue-first ordering) and LIVE partial fills both
    re-append the same decision_id; the PK conflict must drop the duplicate
    silently instead of raising UniqueViolation through the spool."""
    eval_store = EvalStore(pool=pg_pool)
    record = EvalRecord(
        market_id="0xmarket-res-3",
        decision_id="decision-res-conflict-1",
        strategy_id="default",
        strategy_version_id="default-v2",
        prob_estimate=0.7,
        resolved_outcome=1.0,
        brier_score=0.09,
        fill_status="MATCHED",
        recorded_at=datetime(2026, 6, 1, 9, 5, tzinfo=UTC),
        citations=["trade-res-conflict-1"],
    )

    await eval_store.append(record)
    await eval_store.append(replace(record, prob_estimate=0.9, brier_score=0.01))

    stored = [
        row
        for row in await eval_store.all()
        if row.decision_id == "decision-res-conflict-1"
    ]
    assert len(stored) == 1
    assert stored[0].prob_estimate == 0.7
    assert stored[0].brier_score == 0.09
