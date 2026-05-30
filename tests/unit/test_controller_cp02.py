from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from datetime import UTC, datetime, timedelta

import pytest

from pms.config import ControllerSettings, PMSSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.enums import RunMode
from pms.core.models import MarketSignal, Portfolio


class StaticForecaster:
    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return (0.67, 0.9, "factor-value edge")

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return 0.67


class NullForecaster:
    """Forecaster that always returns None — used to exercise the
    no-forecaster-output branch of ``ControllerPipeline.on_signal``."""

    def predict(self, signal: MarketSignal) -> tuple[float, float, str] | None:
        del signal
        return None

    async def forecast(self, signal: MarketSignal) -> float:
        return signal.yes_price


def _signal(*, fetched_at: datetime | None = None) -> MarketSignal:
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
        fetched_at=fetched_at or datetime(2026, 4, 19, tzinfo=UTC),
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
    assert opportunity.selected_factor_values == {"yes_price": 0.4}
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


@pytest.mark.asyncio
async def test_controller_pipeline_suppresses_duplicate_paper_decisions_within_cooldown() -> None:
    first_ts = datetime(2026, 4, 19, tzinfo=UTC)
    pipeline = ControllerPipeline(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        forecasters=[StaticForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
        settings=PMSSettings(
            mode=RunMode.PAPER,
            controller=ControllerSettings(
                min_volume=100.0,
                decision_cooldown_s=60.0,
            ),
        ),
    )

    first = await pipeline.on_signal(_signal(fetched_at=first_ts), portfolio=_portfolio())
    duplicate = await pipeline.on_signal(
        _signal(fetched_at=first_ts + timedelta(seconds=30)),
        portfolio=_portfolio(),
    )
    after_cooldown = await pipeline.on_signal(
        _signal(fetched_at=first_ts + timedelta(seconds=61)),
        portfolio=_portfolio(),
    )

    assert first is not None
    assert duplicate is None
    assert after_cooldown is not None


@pytest.mark.asyncio
async def test_on_signal_emits_diagnostic_when_router_gate_rejects_signal() -> None:
    pipeline = ControllerPipeline(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        forecasters=[StaticForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0, max_spread_bps=100.0)),
    )

    emission = await pipeline.on_signal(
        replace(_signal(), external_signal={"spread_bps": 250.0}),
        portfolio=_portfolio(),
    )

    assert emission is None
    diagnostic = pipeline.last_diagnostic
    assert diagnostic is not None, "router-gate rejections must surface as a diagnostic"
    assert diagnostic.code == "router_gate:spread_too_wide"
    assert diagnostic.market_id == "market-cp02"
    assert diagnostic.strategy_id == "alpha"
    assert diagnostic.strategy_version_id == "alpha-v1"
    assert diagnostic.metadata.get("gate_reason") == "spread_too_wide"


@pytest.mark.asyncio
async def test_on_signal_emits_diagnostic_when_signal_lacks_token_id() -> None:
    pipeline = ControllerPipeline(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        forecasters=[StaticForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(
        replace(_signal(), token_id=None),
        portfolio=_portfolio(),
    )

    assert emission is None
    diagnostic = pipeline.last_diagnostic
    assert diagnostic is not None, "missing-token-id rejections must surface as a diagnostic"
    assert diagnostic.code == "missing_token_id"
    assert diagnostic.market_id == "market-cp02"
    assert diagnostic.token_id is None


@pytest.mark.asyncio
async def test_on_signal_emits_diagnostic_when_no_forecaster_output_and_no_factor_composition() -> None:
    pipeline = ControllerPipeline(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        forecasters=[NullForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    diagnostic = pipeline.last_diagnostic
    assert diagnostic is not None, (
        "no-forecaster-output rejections must surface as a diagnostic so operators "
        "can see that signals reached the pipeline but no forecaster produced a probability"
    )
    assert diagnostic.code == "no_forecaster_output"
    assert diagnostic.market_id == "market-cp02"
    assert diagnostic.token_id == "token-cp02"


class _EqualToMarketForecaster:
    """Returns exactly the market YES price so the resulting edge is zero,
    exercising the decision_edge <= 0 silent-return branch."""

    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return (0.4, 0.9, "no-edge")

    async def forecast(self, signal: MarketSignal) -> float:
        return 0.4


@pytest.mark.asyncio
async def test_on_signal_emits_diagnostic_when_decision_edge_not_positive() -> None:
    pipeline = ControllerPipeline(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        forecasters=[_EqualToMarketForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    # _signal() has yes_price=0.4; a forecast of 0.4 yields zero edge.
    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    diagnostic = pipeline.last_diagnostic
    assert diagnostic is not None, (
        "a zero/negative-edge drop must surface as a diagnostic so operators can "
        "distinguish 'no opportunity' from 'controller idle'"
    )
    assert diagnostic.code == "decision_edge_not_positive"
    assert diagnostic.severity == "info"
    assert diagnostic.market_id == "market-cp02"
    assert diagnostic.metadata.get("decision_edge") == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_on_signal_emits_diagnostic_when_order_size_below_minimum() -> None:
    pipeline = ControllerPipeline(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        forecasters=[StaticForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
        settings=PMSSettings(
            mode=RunMode.PAPER,
            controller=ControllerSettings(min_volume=100.0),
            # Force the computed Kelly size (~$137) below the floor.
            risk=RiskSettings(min_order_usdc=500.0),
        ),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    diagnostic = pipeline.last_diagnostic
    assert diagnostic is not None, (
        "a sub-minimum order-size drop must surface as a diagnostic"
    )
    assert diagnostic.code == "order_size_below_minimum"
    assert diagnostic.severity == "info"
    assert diagnostic.metadata.get("min_order_usdc") == pytest.approx(500.0)


@pytest.mark.asyncio
async def test_on_signal_emits_diagnostic_when_within_decision_cooldown() -> None:
    first_ts = datetime(2026, 4, 19, tzinfo=UTC)
    pipeline = ControllerPipeline(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        forecasters=[StaticForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
        settings=PMSSettings(
            mode=RunMode.PAPER,
            controller=ControllerSettings(min_volume=100.0, decision_cooldown_s=60.0),
        ),
    )

    first = await pipeline.on_signal(_signal(fetched_at=first_ts), portfolio=_portfolio())
    duplicate = await pipeline.on_signal(
        _signal(fetched_at=first_ts + timedelta(seconds=30)),
        portfolio=_portfolio(),
    )

    assert first is not None
    assert duplicate is None
    diagnostic = pipeline.last_diagnostic
    assert diagnostic is not None, (
        "a cooldown-suppressed duplicate must surface as a diagnostic so the "
        "suppression is observable, not silent"
    )
    assert diagnostic.code == "within_decision_cooldown"
    assert diagnostic.severity == "info"
