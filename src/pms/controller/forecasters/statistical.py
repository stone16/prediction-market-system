from __future__ import annotations

from dataclasses import dataclass

from pms.core.models import MarketSignal

ForecastResult = tuple[float, float, str]


@dataclass(frozen=True)
class StatisticalForecaster:
    prior_strength: float = 2.0

    def predict(self, signal: MarketSignal) -> ForecastResult:
        metaculus = signal.external_signal.get("metaculus_prob")
        if metaculus is None:
            alpha = 1.0
            beta = 1.0
            prior_note = "Beta(1.00, 1.00)"
        else:
            metaculus_prob = float(metaculus)
            alpha = metaculus_prob * self.prior_strength
            beta = (1.0 - metaculus_prob) * self.prior_strength
            prior_note = f"Metaculus prior p={metaculus_prob:.4f}"

        yes_count = float(signal.external_signal.get("yes_count", 0.0))
        no_count = float(signal.external_signal.get("no_count", 0.0))
        posterior_alpha = alpha + yes_count
        posterior_beta = beta + no_count
        total = posterior_alpha + posterior_beta
        probability = posterior_alpha / total
        observations = yes_count + no_count
        confidence = observations / (observations + alpha + beta)
        rationale = (
            f"{prior_note}; yes_count={yes_count:.0f}; no_count={no_count:.0f}; "
            f"posterior=Beta({posterior_alpha:.2f}, {posterior_beta:.2f})"
        )
        return probability, confidence, rationale

    async def forecast(self, signal: MarketSignal) -> float:
        return self.predict(signal)[0]
