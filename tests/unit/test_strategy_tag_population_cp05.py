from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.executor import _rejected_order_state
from pms.core.enums import Side, TimeInForce
from pms.core.models import MarketSignal, Portfolio, TradeDecision
from pms.evaluation.adapters.scoring import Scorer
from pms.runner import _fill_from_order


def _decision(
    *,
    decision_id: str = "d-cp05",
    strategy_id: str = "alpha",
    strategy_version_id: str = "alpha-v1",
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id="m-cp05",
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        limit_price=0.4,
        notional_usdc=10.0,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["min_volume:100.00"],
        prob_estimate=0.7,
        expected_edge=0.3,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"op-{decision_id}",
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="m-cp05",
        token_id="t-yes",
        venue="polymarket",
        title="Will CP05 preserve strategy tags?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal={"fair_value": 0.7, "resolved_outcome": 1.0},
        fetched_at=datetime(2026, 4, 19, tzinfo=UTC),
        market_status="open",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


@pytest.mark.asyncio
async def test_order_state_emit_paths_copy_strategy_tags_from_trade_decision() -> None:
    decision = _decision()
    paper = PaperActuator(orderbooks=_signal().orderbook | {"m-cp05": _signal().orderbook})

    matched = await paper.execute(decision, _portfolio())
    rejected = _rejected_order_state(decision, "duplicate_decision")

    assert matched.strategy_id == decision.strategy_id
    assert matched.strategy_version_id == decision.strategy_version_id
    assert rejected.strategy_id == decision.strategy_id
    assert rejected.strategy_version_id == decision.strategy_version_id


@pytest.mark.asyncio
async def test_fill_and_eval_emit_paths_preserve_strategy_tags_end_to_end() -> None:
    decision = _decision()
    paper = PaperActuator(orderbooks={"m-cp05": _signal().orderbook})
    matched = await paper.execute(decision, _portfolio())

    fill = _fill_from_order(matched, decision, _signal())

    assert fill is not None
    assert fill.strategy_id == decision.strategy_id
    assert fill.strategy_version_id == decision.strategy_version_id

    updated_fill = replace(fill, fill_id="fill-cp05")
    assert updated_fill.strategy_id == decision.strategy_id
    assert updated_fill.strategy_version_id == decision.strategy_version_id

    record = Scorer().score(fill, decision)
    assert record.strategy_id == decision.strategy_id
    assert record.strategy_version_id == decision.strategy_version_id
