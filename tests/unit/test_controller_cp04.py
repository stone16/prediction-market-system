from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.config import RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.forecasters.rules import RulesForecaster
from pms.controller.forecasters.statistical import StatisticalForecaster
from pms.controller.pipeline import ControllerPipeline
from pms.controller.sizers.kelly import KellySizer
from pms.core.enums import MarketStatus
from pms.core.models import EvalRecord, MarketSignal, Portfolio


def _signal(
    *,
    yes_price: float = 0.4,
    external_signal: dict[str, object] | None = None,
) -> MarketSignal:
    return MarketSignal(
        market_id="m1",
        token_id="t1",
        venue="polymarket",
        title="Will CP04 pass?",
        yes_price=yes_price,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 20, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal=external_signal or {},
        fetched_at=datetime(2026, 4, 13, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _portfolio() -> Portfolio:
    return Portfolio(total_usdc=1000.0, free_usdc=1000.0, locked_usdc=0.0, open_positions=[])


def _records(count: int) -> list[EvalRecord]:
    return [
        EvalRecord(
            market_id=f"m{index}",
            decision_id=f"d{index}",
            prob_estimate=0.2 if index < count // 2 else 0.8,
            resolved_outcome=0.0 if index < count // 2 else 1.0,
            brier_score=0.04,
            fill_status="matched",
            recorded_at=datetime(2026, 4, 13, tzinfo=UTC),
            citations=["fixture"],
        )
        for index in range(count)
    ]


def test_rules_forecaster_detects_price_spread_opportunity() -> None:
    result = RulesForecaster(min_edge=0.05).predict(
        _signal(yes_price=0.4, external_signal={"fair_value": 0.55})
    )

    assert result == pytest.approx((0.4, 0.0, "pre-s5-neutral"))


def test_rules_forecaster_detects_subset_pricing_violation() -> None:
    result = RulesForecaster().predict(
        _signal(
            external_signal={
                "subset_price": 0.75,
                "superset_price": 0.60,
                "subset_label": "A and B",
                "superset_label": "A",
            }
        )
    )

    assert result == pytest.approx((0.4, 0.0, "pre-s5-neutral"))


def test_rules_forecaster_returns_neutral_without_opportunity() -> None:
    assert RulesForecaster(min_edge=0.1).predict(_signal(yes_price=0.5)) == pytest.approx(
        (0.5, 0.0, "pre-s5-neutral")
    )


def test_statistical_forecaster_uses_uniform_prior_without_metaculus() -> None:
    result = StatisticalForecaster().predict(
        _signal(external_signal={"yes_count": 3, "no_count": 1})
    )

    assert result == pytest.approx((0.4, 0.0, "pre-s5-neutral"))


def test_statistical_forecaster_uses_metaculus_prior() -> None:
    result = StatisticalForecaster(prior_strength=10.0).predict(
        _signal(external_signal={"metaculus_prob": 0.7, "yes_count": 3, "no_count": 7})
    )

    assert result == pytest.approx((0.4, 0.0, "pre-s5-neutral"))


def test_statistical_forecaster_rejects_non_positive_prior_strength() -> None:
    with pytest.raises(ValueError, match="prior_strength"):
        StatisticalForecaster(prior_strength=0.0)


@pytest.mark.asyncio
async def test_controller_pipeline_reports_uninitialized_dependencies() -> None:
    pipeline = ControllerPipeline()
    pipeline.router = None

    with pytest.raises(RuntimeError, match="router"):
        await pipeline.decide(_signal(), portfolio=_portfolio())


def test_netcal_calibrator_identity_boundary_at_99_samples() -> None:
    calibrator = NetcalCalibrator()
    calibrator.add_samples("model-a", _records(99))

    assert calibrator.calibrate(0.8, model_id="model-a") == 0.8


def test_netcal_calibrator_applies_isotonic_at_100_samples() -> None:
    calibrator = NetcalCalibrator()
    calibrator.add_samples("model-a", _records(100))

    assert calibrator.calibrate(0.8, model_id="model-a") == pytest.approx(1.0)
    assert calibrator.calibrate(0.2, model_id="model-a") == pytest.approx(0.0)


def test_kelly_sizer_even_odds_fractional_bet() -> None:
    sizer = KellySizer(risk=RiskSettings(max_position_per_market=1000.0))

    assert sizer.size(prob=0.6, market_price=0.5, portfolio=_portfolio()) == pytest.approx(50.0)


def test_kelly_sizer_negative_edge_returns_zero() -> None:
    sizer = KellySizer(risk=RiskSettings(max_position_per_market=1000.0))

    assert sizer.size(prob=0.4, market_price=0.5, portfolio=_portfolio()) == 0.0


def test_kelly_sizer_caps_at_max_position_per_market() -> None:
    sizer = KellySizer(risk=RiskSettings(max_position_per_market=10.0))

    assert sizer.size(prob=0.8, market_price=0.5, portfolio=_portfolio()) == 10.0


def test_kelly_sizer_reports_uninitialized_risk() -> None:
    sizer = KellySizer()
    object.__setattr__(sizer, "risk", None)

    with pytest.raises(RuntimeError, match="risk"):
        sizer.size(prob=0.8, market_price=0.5, portfolio=_portfolio())
