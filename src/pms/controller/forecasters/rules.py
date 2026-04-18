from __future__ import annotations

from dataclasses import dataclass

from pms.core.models import MarketSignal

ForecastResult = tuple[float, float, str]


@dataclass(frozen=True)
class RulesForecaster:
    min_edge: float = 0.02

    def predict(self, signal: MarketSignal) -> ForecastResult | None:
        return (signal.yes_price, 0.0, "pre-s5-neutral")

    async def forecast(self, signal: MarketSignal) -> float:
        result = self.predict(signal)
        return signal.yes_price if result is None else result[0]
