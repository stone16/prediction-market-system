from __future__ import annotations

from dataclasses import dataclass

from pms.core.models import MarketSignal
from pms.strategies.flb.source import (
    DEFAULT_FLB_CONFIDENCE,
    FAVORITE_YES_THRESHOLD,
    LONGSHOT_YES_THRESHOLD,
    FlbCalibrationModel,
)

ForecastResult = tuple[float, float, str]
FLB_CALIBRATED_MODEL_ID = "flb-calibrated-v1"


@dataclass(frozen=True)
class FlbForecaster:
    calibration_model: FlbCalibrationModel
    confidence: float = DEFAULT_FLB_CONFIDENCE

    def predict(self, signal: MarketSignal) -> ForecastResult | None:
        yes_price = signal.yes_price
        if yes_price < float(LONGSHOT_YES_THRESHOLD):
            calibration = self.calibration_model.calibration_for(
                "longshot_yes_overpriced_buy_no"
            )
            yes_probability = 1.0 - calibration.probability_estimate
        elif yes_price > float(FAVORITE_YES_THRESHOLD):
            calibration = self.calibration_model.calibration_for(
                "favorite_yes_underpriced_buy_yes"
            )
            yes_probability = calibration.probability_estimate
        else:
            return None
        return (
            _bounded_probability(yes_probability),
            self.confidence,
            FLB_CALIBRATED_MODEL_ID,
        )

    async def forecast(self, signal: MarketSignal) -> float:
        result = self.predict(signal)
        return signal.yes_price if result is None else result[0]


def _bounded_probability(value: float) -> float:
    return max(0.01, min(0.99, value))
