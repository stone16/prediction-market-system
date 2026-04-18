from __future__ import annotations

from dataclasses import dataclass

from pms.core.models import MarketSignal

ForecastResult = tuple[float, float, str]


@dataclass(frozen=True)
class StatisticalForecaster:
    prior_strength: float = 2.0

    def __post_init__(self) -> None:
        if self.prior_strength <= 0.0:
            msg = "prior_strength must be positive"
            raise ValueError(msg)

    def predict(self, signal: MarketSignal) -> ForecastResult:
        return signal.yes_price, 0.0, "pre-s5-neutral"

    async def forecast(self, signal: MarketSignal) -> float:
        return self.predict(signal)[0]
