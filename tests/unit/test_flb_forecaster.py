from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.controller.forecasters.flb import FLB_CALIBRATED_MODEL_ID, FlbForecaster
from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.strategies.flb.source import (
    FlbCalibrationModel,
    FlbSignalCalibration,
)


def _calibration_model() -> FlbCalibrationModel:
    return FlbCalibrationModel(
        calibrations=(
            FlbSignalCalibration(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=0.99,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbSignalCalibration(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=0.97,
                sample_count=151,
                source_label="warehouse-flb-v1",
            ),
        )
    )


def _signal(*, yes_price: float) -> MarketSignal:
    return MarketSignal(
        market_id="market-flb-forecast",
        token_id="token-yes",
        venue="polymarket",
        title="Will calibrated H1 FLB produce the correct YES coordinate?",
        yes_price=yes_price,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 6, 1, tzinfo=UTC),
        orderbook={
            "bids": [{"price": max(0.01, yes_price - 0.01), "size": 100.0}],
            "asks": [{"price": min(0.99, yes_price + 0.01), "size": 100.0}],
        },
        external_signal={},
        fetched_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def test_flb_forecaster_maps_longshot_buy_no_calibration_to_yes_probability() -> None:
    forecaster = FlbForecaster(calibration_model=_calibration_model())

    result = forecaster.predict(_signal(yes_price=0.05))

    assert result is not None
    prob_estimate, confidence, model_id = result
    assert prob_estimate == pytest.approx(0.01)
    assert confidence == pytest.approx(0.65)
    assert model_id == FLB_CALIBRATED_MODEL_ID


def test_flb_forecaster_maps_favorite_buy_yes_calibration_to_yes_probability() -> None:
    forecaster = FlbForecaster(calibration_model=_calibration_model())

    result = forecaster.predict(_signal(yes_price=0.95))

    assert result is not None
    prob_estimate, confidence, model_id = result
    assert prob_estimate == pytest.approx(0.97)
    assert confidence == pytest.approx(0.65)
    assert model_id == FLB_CALIBRATED_MODEL_ID


def test_flb_forecaster_suppresses_middle_deciles() -> None:
    forecaster = FlbForecaster(calibration_model=_calibration_model())

    assert forecaster.predict(_signal(yes_price=0.50)) is None


@pytest.mark.asyncio
async def test_flb_forecaster_implements_legacy_forecast_protocol() -> None:
    forecaster = FlbForecaster(calibration_model=_calibration_model())

    probability = await forecaster.forecast(_signal(yes_price=0.95))

    assert probability == pytest.approx(0.97)
