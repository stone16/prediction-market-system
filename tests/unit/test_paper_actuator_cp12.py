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
async def test_paper_buy_no_uses_no_token_asks_not_yes_complement() -> None:
    decision = _decision(limit_price=0.45)
    object.__setattr__(decision, "token_id", "token-no")
    object.__setattr__(decision, "outcome", "NO")
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.62, "size": 1_000.0}],
                "asks": [{"price": 0.64, "size": 1_000.0}],
            },
            "token-no": {
                "bids": [{"price": 0.40, "size": 1_000.0}],
                "asks": [{"price": 0.44, "size": 1_000.0}],
            },
        }
    )

    state = await actuator.execute(decision, _portfolio())

    assert state.fill_price == pytest.approx(0.44)
    assert state.filled_quantity == pytest.approx(100.0 / 0.44)


@pytest.mark.asyncio
async def test_paper_buy_no_requires_no_token_orderbook() -> None:
    decision = _decision(limit_price=0.45)
    object.__setattr__(decision, "token_id", "token-no")
    object.__setattr__(decision, "outcome", "NO")
    actuator = PaperActuator(
        orderbooks={
            "market-cp12": {
                "bids": [{"price": 0.62, "size": 1_000.0}],
                "asks": [{"price": 0.64, "size": 1_000.0}],
            },
        }
    )

    with pytest.raises(InsufficientLiquidityError, match="missing paper orderbook"):
        await actuator.execute(decision, _portfolio())


@pytest.mark.asyncio
async def test_paper_buy_respects_limit_price() -> None:
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.39, "size": 100.0}],
                "asks": [{"price": 0.46, "size": 100.0}],
            }
        }
    )

    with pytest.raises(InsufficientLiquidityError, match="executable depth"):
        await actuator.execute(_decision(limit_price=0.45), _portfolio())


@pytest.mark.asyncio
async def test_paper_fill_consumes_multiple_levels_vwap() -> None:
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.39, "size": 100.0}],
                "asks": [
                    {"price": 0.40, "size": 10.0},
                    {"price": 0.50, "size": 20.0},
                ],
            }
        }
    )

    state = await actuator.execute(
        _decision(notional_usdc=10.0, limit_price=0.55),
        _portfolio(),
    )

    assert state.fill_price == pytest.approx(10.0 / 22.0)
    assert state.filled_quantity == pytest.approx(22.0)


@pytest.mark.asyncio
async def test_paper_sell_preserves_requested_notional_when_fill_price_differs() -> None:
    decision = _decision(notional_usdc=10.0, limit_price=0.45)
    object.__setattr__(decision, "side", "SELL")
    object.__setattr__(decision, "action", "SELL")
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.50, "size": 30.0}],
                "asks": [{"price": 0.52, "size": 30.0}],
            }
        }
    )

    state = await actuator.execute(decision, _portfolio())

    assert state.requested_notional_usdc == pytest.approx(10.0)
    assert state.filled_quantity == pytest.approx(10.0 / 0.45)
    assert state.filled_notional_usdc == pytest.approx((10.0 / 0.45) * 0.50)


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "level",
    [
        {"size": 1_000.0},
        {"price": "not-a-price", "size": 1_000.0},
        {"price": 0.25, "size": "not-a-size"},
    ],
)
async def test_paper_actuator_rejects_malformed_depth_rows(
    level: dict[str, object],
) -> None:
    actuator = PaperActuator(
        orderbooks={
            "market-cp12": {
                "bids": [{"price": 0.24, "size": 1_000.0}],
                "asks": [level],
            }
        }
    )

    with pytest.raises(InsufficientLiquidityError, match="asks depth is invalid"):
        await actuator.execute(_decision(), _portfolio())


def _decision_with_slippage(
    *,
    limit_price: float,
    max_slippage_bps: int,
    side: str = "BUY",
    outcome: str = "YES",
    notional_usdc: float = 100.0,
) -> TradeDecision:
    decision = _decision(limit_price=limit_price, notional_usdc=notional_usdc)
    object.__setattr__(decision, "max_slippage_bps", max_slippage_bps)
    object.__setattr__(decision, "side", side)
    object.__setattr__(decision, "action", side)
    object.__setattr__(decision, "outcome", outcome)
    return decision


@pytest.mark.asyncio
async def test_paper_buy_never_fills_above_limit_even_with_slippage_budget() -> None:
    # Live parity: the Polymarket pre-submit guard rejects a BUY whenever the
    # best executable price exceeds decision.limit_price, regardless of
    # max_slippage_bps (polymarket.py _validate_pre_submit_quote). Paper must
    # not book fills the live adapter would refuse — best_ask 0.305 > 0.30
    # rejects even with a 200bps slippage budget.
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.295, "size": 1_000.0}],
                "asks": [{"price": 0.305, "size": 1_000.0}],
            }
        }
    )

    with pytest.raises(InsufficientLiquidityError, match="executable depth"):
        await actuator.execute(
            _decision_with_slippage(limit_price=0.30, max_slippage_bps=200),
            _portfolio(),
        )


@pytest.mark.asyncio
async def test_paper_buy_with_slippage_budget_fills_at_or_below_limit() -> None:
    # The slippage budget never relaxes the limit: a book whose best ask sits
    # exactly at the limit fills at the limit, never above it.
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.295, "size": 1_000.0}],
                "asks": [{"price": 0.30, "size": 1_000.0}],
            }
        }
    )

    state = await actuator.execute(
        _decision_with_slippage(limit_price=0.30, max_slippage_bps=200),
        _portfolio(),
    )

    assert state.fill_price is not None
    assert state.fill_price == pytest.approx(0.30)
    assert state.fill_price <= 0.30
    assert state.filled_notional_usdc == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_paper_zero_slippage_keeps_strict_limit() -> None:
    # max_slippage_bps=0: ask above limit rejects (limit is always strict)
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.295, "size": 1_000.0}],
                "asks": [{"price": 0.305, "size": 1_000.0}],
            }
        }
    )

    with pytest.raises(InsufficientLiquidityError, match="executable depth"):
        await actuator.execute(
            _decision_with_slippage(limit_price=0.30, max_slippage_bps=0),
            _portfolio(),
        )


@pytest.mark.asyncio
async def test_paper_sell_never_fills_below_limit_even_with_slippage_budget() -> None:
    # SELL mirror of the live guard: best executable below the limit rejects
    # at submit regardless of max_slippage_bps — best_bid 0.298 < 0.30 rejects
    # even with a 100bps slippage budget.
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.298, "size": 1_000.0}],
                "asks": [{"price": 0.305, "size": 1_000.0}],
            }
        }
    )

    with pytest.raises(InsufficientLiquidityError, match="executable depth"):
        await actuator.execute(
            _decision_with_slippage(
                limit_price=0.30,
                max_slippage_bps=100,
                side="SELL",
            ),
            _portfolio(),
        )


@pytest.mark.asyncio
async def test_paper_sell_notional_represents_limit_valued_target_quantity() -> None:
    target_quantity = 4.0
    limit_price = 0.125
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.40, "size": 1_000.0}],
                "asks": [{"price": 0.41, "size": 1_000.0}],
            }
        }
    )

    state = await actuator.execute(
        _decision_with_slippage(
            limit_price=limit_price,
            max_slippage_bps=0,
            side="SELL",
            notional_usdc=target_quantity * limit_price,
        ),
        _portfolio(),
    )

    assert state.fill_price == pytest.approx(0.40)
    assert state.filled_quantity == pytest.approx(target_quantity)
    assert state.filled_notional_usdc == pytest.approx(target_quantity * 0.40)
