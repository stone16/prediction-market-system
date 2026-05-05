from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.actuator.adapters.paper import PaperActuator
from pms.config import PMSSettings
from pms.controller.factory import ControllerPipelineFactory
from pms.core.enums import RunMode
from pms.core.models import MarketSignal, Portfolio
from pms.strategies.aggregate import Strategy
from pms.strategies.paper_canary import PAPER_CANARY_STRATEGY_ID, build_paper_canary_strategy
from pms.strategies.projections import ForecasterSpec


def _signal(*, yes_price: float = 0.50, best_ask: float = 0.52) -> MarketSignal:
    return MarketSignal(
        market_id="canary-market-8",
        token_id="canary-token-8",  # noqa: S106 - market token fixture, not a secret.
        venue="polymarket",
        title="Can the canary prove paper execution?",
        yes_price=yes_price,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 6, 1, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.48, "size": 100.0}],
            "asks": [{"price": best_ask, "size": 100.0}],
        },
        external_signal={"raw_event_type": "book", "best_ask": best_ask},
        fetched_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
        market_status="open",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _unsampled_canary_strategy() -> Strategy:
    strategy = build_paper_canary_strategy()
    return Strategy(
        config=strategy.config,
        risk=strategy.risk,
        eval_spec=strategy.eval_spec,
        forecaster=ForecasterSpec(
            forecasters=(
                (
                    "paper_canary",
                    tuple(
                        ("sample_modulus", "1")
                        if key == "sample_modulus"
                        else (key, value)
                        for key, value in strategy.forecaster.forecasters[0][1]
                    ),
                ),
            )
        ),
        market_selection=strategy.market_selection,
    )


@pytest.mark.asyncio
async def test_paper_canary_uses_executable_best_ask_for_decision_price() -> None:
    settings = PMSSettings(mode=RunMode.PAPER)
    strategy = _unsampled_canary_strategy()
    pipeline = ControllerPipelineFactory(settings=settings).build(
        strategy.to_active(strategy_version_id="paper-canary-test-v1")
    )

    decision = await pipeline.decide(_signal(), portfolio=_portfolio())

    assert decision is not None
    assert decision.strategy_id == PAPER_CANARY_STRATEGY_ID
    assert decision.outcome == "YES"
    assert decision.limit_price == pytest.approx(0.52)
    assert decision.prob_estimate > decision.limit_price
    assert decision.notional_usdc == pytest.approx(1.0)
    assert decision.model_id == "PaperCanaryForecaster"


@pytest.mark.asyncio
async def test_paper_canary_decision_matches_paper_orderbook() -> None:
    settings = PMSSettings(mode=RunMode.PAPER)
    signal = _signal()
    strategy = _unsampled_canary_strategy()
    pipeline = ControllerPipelineFactory(settings=settings).build(
        strategy.to_active(strategy_version_id="paper-canary-test-v1")
    )

    decision = await pipeline.decide(signal, portfolio=_portfolio())
    assert decision is not None
    assert signal.token_id is not None

    order_state = await PaperActuator(orderbooks={signal.token_id: signal.orderbook}).execute(
        decision,
        _portfolio(),
    )

    assert order_state.status == "matched"
    assert order_state.raw_status == "matched"
    assert order_state.fill_price == pytest.approx(0.52)
    assert order_state.filled_notional_usdc == pytest.approx(1.0)
    assert order_state.strategy_id == PAPER_CANARY_STRATEGY_ID


def test_paper_canary_strategy_is_small_paper_probe_with_no_factor_gate() -> None:
    strategy = build_paper_canary_strategy()

    assert strategy.config.strategy_id == PAPER_CANARY_STRATEGY_ID
    assert strategy.config.factor_composition == ()
    assert dict(strategy.config.metadata)["purpose"] == "paper_e2e_canary"
    assert dict(strategy.config.metadata)["price_reference"] == "best_ask"
    assert dict(strategy.config.metadata)["event_filter"] == "book"
    assert dict(strategy.config.metadata)["sample"] == "0/25"
    assert strategy.risk.max_position_notional_usdc == pytest.approx(1.0)
    assert strategy.risk.min_order_size_usdc == pytest.approx(1.0)
    assert strategy.forecaster.forecasters == (
        (
            "paper_canary",
            (
                ("edge_bps", "1000"),
                ("max_probability", "0.97"),
                ("min_price", "0.05"),
                ("max_price", "0.90"),
                ("sample_modulus", "25"),
                ("sample_remainder", "0"),
            ),
        ),
    )


def test_paper_canary_forecaster_is_rejected_outside_paper_mode() -> None:
    settings = PMSSettings(mode=RunMode.LIVE)
    strategy = build_paper_canary_strategy()

    with pytest.raises(ValueError, match="paper_canary_v1 is PAPER-only"):
        ControllerPipelineFactory(settings=settings).build(
            strategy.to_active(strategy_version_id="paper-canary-test-v1")
        )
