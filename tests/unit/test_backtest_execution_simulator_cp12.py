from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest

from pms.actuator.risk import InsufficientLiquidityError
from pms.core.models import MarketSignal, Portfolio, TradeDecision
from pms.research.execution import BacktestExecutionSimulator
from pms.research.specs import ExecutionModel


def _signal(*, price: float = 0.25, size: float = 1_000.0) -> MarketSignal:
    return MarketSignal(
        market_id="market-cp12",
        token_id="token-yes",
        venue="polymarket",
        title="Will CP12 migrate simulator units?",
        yes_price=price,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.24, "size": size}],
            "asks": [{"price": price, "size": size}],
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


def _decision(*, notional_usdc: float = 100.0, limit_price: float = 0.25) -> TradeDecision:
    return TradeDecision(
        decision_id="decision-cp12",
        market_id="market-cp12",
        token_id="token-yes",
        venue="polymarket",
        side="BUY",
        notional_usdc=notional_usdc,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["cp12"],
        prob_estimate=0.7,
        expected_edge=0.2,
        time_in_force="GTC",
        opportunity_id="opportunity-cp12",
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        action="BUY",
        limit_price=limit_price,
        outcome="YES",
        model_id="rules",
    )


def _invalid_decision(*, notional_usdc: float) -> TradeDecision:
    return cast(
        TradeDecision,
        SimpleNamespace(
            decision_id="invalid-decision",
            market_id="market-cp12",
            token_id="token-yes",
            venue="polymarket",
            side="BUY",
            notional_usdc=notional_usdc,
            order_type="limit",
            max_slippage_bps=100,
            stop_conditions=["cp12"],
            prob_estimate=0.7,
            expected_edge=0.2,
            time_in_force="GTC",
            opportunity_id="opportunity-invalid",
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
            action="BUY",
            limit_price=0.25,
            outcome="YES",
            model_id="rules",
        ),
    )


def _execution_model() -> ExecutionModel:
    return ExecutionModel(
        fee_rate=0.0,
        slippage_bps=0.0,
        latency_ms=0.0,
        staleness_ms=1_000.0,
        fill_policy="immediate_or_cancel",
    )


@pytest.mark.asyncio
async def test_simulator_matches_notional_and_quantity() -> None:
    simulator = BacktestExecutionSimulator()

    state = await simulator.execute(
        signal=_signal(),
        decision=_decision(),
        portfolio=_portfolio(),
        execution_model=_execution_model(),
    )

    assert state.fill_price == pytest.approx(0.25)
    assert state.requested_notional_usdc == pytest.approx(100.0)
    assert state.filled_notional_usdc == pytest.approx(100.0)
    assert state.remaining_notional_usdc == pytest.approx(0.0)
    assert state.filled_quantity == pytest.approx(400.0)


@pytest.mark.asyncio
async def test_simulator_rejects_zero_notional_bypass() -> None:
    simulator = BacktestExecutionSimulator()

    with pytest.raises(InsufficientLiquidityError):
        await simulator.execute(
            signal=_signal(),
            decision=_invalid_decision(notional_usdc=0.0),
            portfolio=_portfolio(),
            execution_model=_execution_model(),
        )


@pytest.mark.asyncio
async def test_simulator_rejects_zero_fill_price() -> None:
    simulator = BacktestExecutionSimulator()

    with pytest.raises(InsufficientLiquidityError):
        await simulator.execute(
            signal=_signal(price=0.0),
            decision=_decision(limit_price=0.01),
            portfolio=_portfolio(),
            execution_model=_execution_model(),
        )
