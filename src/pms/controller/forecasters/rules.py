from __future__ import annotations

from dataclasses import dataclass

from pms.core.models import MarketSignal

ForecastResult = tuple[float, float, str]


@dataclass(frozen=True)
class RulesForecaster:
    min_edge: float = 0.02

    def predict(self, signal: MarketSignal) -> ForecastResult | None:
        spread = self._price_spread(signal)
        if spread is not None:
            return spread
        return self._subset_violation(signal)

    async def forecast(self, signal: MarketSignal) -> float:
        result = self.predict(signal)
        return signal.yes_price if result is None else result[0]

    def _price_spread(self, signal: MarketSignal) -> ForecastResult | None:
        raw_fair_value = signal.external_signal.get("fair_value")
        if raw_fair_value is None:
            return None
        fair_value = float(raw_fair_value)
        edge = fair_value - signal.yes_price
        if abs(edge) < self.min_edge:
            return None
        confidence = min(abs(edge), 1.0)
        rationale = (
            f"price_spread: fair_value={fair_value:.4f} "
            f"yes_price={signal.yes_price:.4f}"
        )
        return fair_value, confidence, rationale

    def _subset_violation(self, signal: MarketSignal) -> ForecastResult | None:
        raw_subset = signal.external_signal.get("subset_price")
        raw_superset = signal.external_signal.get("superset_price")
        if raw_subset is None or raw_superset is None:
            return None
        subset_price = float(raw_subset)
        superset_price = float(raw_superset)
        violation = subset_price - superset_price
        if violation < self.min_edge:
            return None
        subset_label = signal.external_signal.get("subset_label", "subset")
        superset_label = signal.external_signal.get("superset_label", "superset")
        rationale = (
            "subset_pricing_violation: "
            f"{subset_label}={subset_price:.4f} > "
            f"{superset_label}={superset_price:.4f}"
        )
        return superset_price, min(violation, 1.0), rationale
