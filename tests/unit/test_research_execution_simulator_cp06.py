from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

import pytest

from pms.core.enums import OrderStatus
from pms.core.models import MarketSignal, Portfolio, TradeDecision
from pms.research.execution import BacktestExecutionSimulator
from pms.research.specs import ExecutionModel


def _signal(
    *,
    yes_price: float = 0.4,
    resolves_at: datetime | None = None,
) -> MarketSignal:
    return MarketSignal(
        market_id="sim-market",
        token_id="yes-token",
        venue="polymarket",
        title="Will the simulator work?",
        yes_price=yes_price,
        volume_24h=1_000.0,
        resolves_at=resolves_at or datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal={},
        fetched_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
        market_status="open",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _decision(
    *,
    action: Literal["BUY", "SELL"] = "BUY",
    outcome: Literal["YES", "NO"] = "YES",
    limit_price: float = 0.4,
    notional_usdc: float = 10.0,
) -> TradeDecision:
    token_id = "no-token" if outcome == "NO" else "yes-token"
    return TradeDecision(
        decision_id="decision-sim",
        market_id="sim-market",
        token_id=token_id,
        venue="polymarket",
        side=action,
        notional_usdc=notional_usdc,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=[],
        prob_estimate=0.6,
        expected_edge=0.2,
        time_in_force="GTC",
        opportunity_id="opp-sim",
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        action=action,
        limit_price=limit_price,
        outcome=outcome,
        model_id="rules",
    )


@pytest.mark.asyncio
async def test_simulator_ioc_matches_using_book_price_with_latency_applied() -> None:
    simulator = BacktestExecutionSimulator()

    order_state = await simulator.execute(
        signal=_signal(),
        decision=_decision(limit_price=0.4),
        portfolio=_portfolio(),
        execution_model=ExecutionModel(
            fee_rate=0.0,
            slippage_bps=0.0,
            latency_ms=250.0,
            staleness_ms=1_000.0,
            fill_policy="immediate_or_cancel",
        ),
    )

    assert order_state.status == OrderStatus.MATCHED.value
    assert order_state.fill_price == pytest.approx(0.41)
    assert order_state.submitted_at == datetime(2026, 4, 20, 12, 0, tzinfo=UTC) + timedelta(
        milliseconds=250
    )
    assert order_state.last_updated_at == order_state.submitted_at


@pytest.mark.asyncio
async def test_simulator_applies_slippage_bps_to_fill_price() -> None:
    simulator = BacktestExecutionSimulator()

    order_state = await simulator.execute(
        signal=_signal(),
        decision=_decision(limit_price=0.4),
        portfolio=_portfolio(),
        execution_model=ExecutionModel(
            fee_rate=0.0,
            slippage_bps=100.0,
            latency_ms=0.0,
            staleness_ms=1_000.0,
            fill_policy="immediate_or_cancel",
        ),
    )

    assert order_state.status == OrderStatus.MATCHED.value
    assert order_state.fill_price == pytest.approx(0.41 * 1.01)


@pytest.mark.asyncio
async def test_simulator_rejects_when_latency_exceeds_staleness_budget() -> None:
    simulator = BacktestExecutionSimulator()

    order_state = await simulator.execute(
        signal=_signal(),
        decision=_decision(limit_price=0.4),
        portfolio=_portfolio(),
        execution_model=ExecutionModel(
            fee_rate=0.0,
            slippage_bps=0.0,
            latency_ms=250.0,
            staleness_ms=100.0,
            fill_policy="immediate_or_cancel",
        ),
    )

    assert order_state.status == OrderStatus.CANCELED.value
    assert order_state.fill_price is None
    assert order_state.raw_status == "stale_signal"


@pytest.mark.asyncio
async def test_simulator_limit_if_touched_leaves_order_unmatched_when_book_never_touches_limit() -> None:
    simulator = BacktestExecutionSimulator()

    order_state = await simulator.execute(
        signal=_signal(),
        decision=_decision(limit_price=0.4),
        portfolio=_portfolio(),
        execution_model=ExecutionModel(
            fee_rate=0.0,
            slippage_bps=0.0,
            latency_ms=0.0,
            staleness_ms=1_000.0,
            fill_policy="limit_if_touched",
        ),
    )

    assert order_state.status == OrderStatus.UNMATCHED.value
    assert order_state.fill_price is None
    assert order_state.raw_status == "limit_not_touched"


@pytest.mark.asyncio
async def test_simulator_cancels_when_latency_pushes_execution_past_resolution() -> None:
    simulator = BacktestExecutionSimulator()

    order_state = await simulator.execute(
        signal=_signal(
            resolves_at=datetime(2026, 4, 20, 12, 0, 0, 100_000, tzinfo=UTC)
        ),
        decision=_decision(limit_price=0.4),
        portfolio=_portfolio(),
        execution_model=ExecutionModel(
            fee_rate=0.0,
            slippage_bps=0.0,
            latency_ms=250.0,
            staleness_ms=1_000.0,
            fill_policy="immediate_or_cancel",
        ),
    )

    assert order_state.status == OrderStatus.CANCELED_MARKET_RESOLVED.value
    assert order_state.fill_price is None
    assert order_state.raw_status == "market_resolved_before_execution"
