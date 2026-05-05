from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256

from pms.controller._price_utils import best_ask
from pms.core.models import MarketSignal

ForecastResult = tuple[float, float, str]


@dataclass(frozen=True)
class PaperCanaryForecaster:
    edge_bps: float = 1000.0
    max_probability: float = 0.97
    min_price: float = 0.05
    max_price: float = 0.90
    sample_modulus: int = 25
    sample_remainder: int = 0

    def __post_init__(self) -> None:
        if self.edge_bps <= 0.0:
            msg = "edge_bps must be positive"
            raise ValueError(msg)
        if not 0.0 < self.max_probability < 1.0:
            msg = "max_probability must be between 0 and 1"
            raise ValueError(msg)
        if not 0.0 < self.min_price < self.max_price < 1.0:
            msg = "min_price and max_price must satisfy 0 < min < max < 1"
            raise ValueError(msg)
        if self.sample_modulus <= 0:
            msg = "sample_modulus must be positive"
            raise ValueError(msg)
        if not 0 <= self.sample_remainder < self.sample_modulus:
            msg = "sample_remainder must satisfy 0 <= remainder < modulus"
            raise ValueError(msg)

    def predict(self, signal: MarketSignal) -> ForecastResult | None:
        if signal.external_signal.get("raw_event_type") != "book":
            return None
        if not _matches_sample(
            signal,
            modulus=self.sample_modulus,
            remainder=self.sample_remainder,
        ):
            return None
        executable_price = best_ask(signal)
        if executable_price is None:
            return None
        if executable_price < self.min_price or executable_price > self.max_price:
            return None
        edge = Decimal(str(self.edge_bps)) / Decimal("10000")
        executable_price_dec = Decimal(str(executable_price))
        max_probability_dec = Decimal(str(self.max_probability))
        probability_dec = min(executable_price_dec + edge, max_probability_dec)
        if probability_dec <= executable_price_dec:
            return None
        probability = float(probability_dec)
        return (
            probability,
            float(min(edge, Decimal("1"))),
            (
                "paper_canary_v1_e2e_probe:"
                f"best_ask={executable_price:.4f},edge_bps={self.edge_bps:.1f},"
                f"sample={self.sample_remainder}/{self.sample_modulus}"
            ),
        )

    async def forecast(self, signal: MarketSignal) -> float:
        result = self.predict(signal)
        return signal.yes_price if result is None else result[0]


def _matches_sample(
    signal: MarketSignal,
    *,
    modulus: int,
    remainder: int,
) -> bool:
    key = f"{signal.market_id}:{signal.token_id or ''}"
    bucket = int(sha256(key.encode("utf-8")).hexdigest(), 16) % modulus
    return bucket == remainder
