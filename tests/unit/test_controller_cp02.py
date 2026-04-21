from __future__ import annotations

from decimal import Decimal
from datetime import UTC, datetime

import pytest

from pms.config import ControllerSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.models import MarketSignal, Portfolio


class StaticForecaster:
    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return (0.67, 0.9, "factor-value edge")

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return 0.67


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="market-cp02",
        token_id="token-cp02",
        venue="polymarket",
        title="Will CP02 emit opportunities?",
        yes_price=0.4,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"fair_value": 0.61, "confidence": 0.8, "label": "skip"},
        fetched_at=datetime(2026, 4, 19, tzinfo=UTC),
        market_status="open",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _expected_kelly_notional() -> float:
    probability = Decimal("0.67")
    market_price = Decimal("0.4")
    payout_multiple = (Decimal("1.0") - market_price) / market_price
    kelly_fraction = (
        (probability * payout_multiple) - (Decimal("1.0") - probability)
    ) / payout_multiple
    scaled_fraction = kelly_fraction * Decimal("0.25")
    return float(Decimal("1000.0") * scaled_fraction)


@pytest.mark.asyncio
async def test_controller_pipeline_on_signal_emits_opportunity_and_linked_decision() -> None:
    pipeline = ControllerPipeline(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        forecasters=[StaticForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is not None
    opportunity, decision = emission
    assert opportunity.strategy_id == "alpha"
    assert opportunity.strategy_version_id == "alpha-v1"
    assert opportunity.side == "yes"
    assert opportunity.market_id == "market-cp02"
    assert opportunity.token_id == "token-cp02"
    assert opportunity.selected_factor_values == {
        "fair_value": 0.61,
        "confidence": 0.8,
        "yes_price": 0.4,
    }
    assert opportunity.rationale == "StaticForecaster:factor-value edge"
    assert decision.notional_usdc == pytest.approx(_expected_kelly_notional())
    assert opportunity.target_size_usdc == pytest.approx(decision.notional_usdc)
    assert opportunity.expiry == datetime(2026, 4, 30, tzinfo=UTC)
    assert opportunity.staleness_policy == "market_signal_freshness"
    assert decision.opportunity_id == opportunity.opportunity_id
    assert decision.model_id == "StaticForecaster"
    assert decision.strategy_id == "alpha"
    assert decision.strategy_version_id == "alpha-v1"
    assert decision.limit_price == pytest.approx(0.4)
    assert decision.stop_conditions


@pytest.mark.asyncio
async def test_controller_pipeline_decide_returns_notional_decision() -> None:
    pipeline = ControllerPipeline(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        forecasters=[StaticForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    decision = await pipeline.decide(_signal(), portfolio=_portfolio())

    assert decision is not None
    assert decision.notional_usdc == pytest.approx(_expected_kelly_notional())
    assert decision.limit_price == pytest.approx(0.4)
