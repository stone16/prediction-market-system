from __future__ import annotations

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
    assert opportunity.target_size_usdc == decision.size
    assert opportunity.expiry == datetime(2026, 4, 30, tzinfo=UTC)
    assert opportunity.staleness_policy == "market_signal_freshness"
    assert decision.opportunity_id == opportunity.opportunity_id
    assert decision.model_id == "StaticForecaster"
    assert decision.strategy_id == "alpha"
    assert decision.strategy_version_id == "alpha-v1"
    assert decision.stop_conditions
