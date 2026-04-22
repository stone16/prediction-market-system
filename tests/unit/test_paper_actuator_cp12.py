from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.risk import InsufficientLiquidityError
from pms.core.enums import TimeInForce
from pms.core.models import Portfolio, TradeDecision


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _decision(
    *,
    market_id: str = "market-cp12",
    notional_usdc: float = 100.0,
    limit_price: float = 0.25,
) -> TradeDecision:
    return TradeDecision(
        decision_id=f"decision-{market_id}",
        market_id=market_id,
        token_id="token-yes",
        venue="polymarket",
        side="BUY",
        notional_usdc=notional_usdc,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["cp12"],
        prob_estimate=0.7,
        expected_edge=0.2,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"opportunity-{market_id}",
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


@pytest.mark.asyncio
async def test_paper_actuator_matches_notional_and_quantity() -> None:
    actuator = PaperActuator(
        orderbooks={
            "market-cp12": {
                "bids": [{"price": 0.24, "size": 1_000.0}],
                "asks": [{"price": 0.25, "size": 1_000.0}],
            }
        }
    )

    state = await actuator.execute(_decision(), _portfolio())

    assert state.fill_price == pytest.approx(0.25)
    assert state.requested_notional_usdc == pytest.approx(100.0)
    assert state.filled_notional_usdc == pytest.approx(100.0)
    assert state.remaining_notional_usdc == pytest.approx(0.0)
    assert state.filled_quantity == pytest.approx(400.0)


@pytest.mark.asyncio
async def test_paper_actuator_rejects_insufficient_notional_depth() -> None:
    actuator = PaperActuator(
        orderbooks={
            "market-cp12": {
                "bids": [{"price": 0.24, "size": 300.0}],
                "asks": [{"price": 0.25, "size": 300.0}],
            }
        }
    )

    with pytest.raises(InsufficientLiquidityError):
        await actuator.execute(_decision(), _portfolio())


@pytest.mark.asyncio
async def test_paper_actuator_rejects_zero_notional_bypass() -> None:
    actuator = PaperActuator(
        orderbooks={
            "market-cp12": {
                "bids": [{"price": 0.24, "size": 1_000.0}],
                "asks": [{"price": 0.25, "size": 1_000.0}],
            }
        }
    )

    with pytest.raises(InsufficientLiquidityError):
        await actuator.execute(_invalid_decision(notional_usdc=0.0), _portfolio())


@pytest.mark.asyncio
async def test_paper_actuator_rejects_zero_fill_price() -> None:
    actuator = PaperActuator(
        orderbooks={
            "market-cp12": {
                "bids": [{"price": 0.24, "size": 1_000.0}],
                "asks": [{"price": 0.0, "size": 1_000.0}],
            }
        }
    )

    with pytest.raises(InsufficientLiquidityError):
        await actuator.execute(_decision(limit_price=0.01), _portfolio())
