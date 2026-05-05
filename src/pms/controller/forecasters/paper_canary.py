from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from math import isfinite
from typing import Any

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
        executable_price = _best_ask(signal)
        if executable_price is None:
            return None
        if executable_price < self.min_price or executable_price > self.max_price:
            return None
        edge = self.edge_bps / 10_000.0
        probability = min(executable_price + edge, self.max_probability)
        if probability <= executable_price:
            return None
        return (
            probability,
            min(edge, 1.0),
            (
                "paper_canary_v1_e2e_probe:"
                f"best_ask={executable_price:.4f},edge_bps={self.edge_bps:.1f},"
                f"sample={self.sample_remainder}/{self.sample_modulus}"
            ),
        )

    async def forecast(self, signal: MarketSignal) -> float:
        result = self.predict(signal)
        return signal.yes_price if result is None else result[0]


def _best_ask(signal: MarketSignal) -> float | None:
    raw_external_ask = signal.external_signal.get("best_ask")
    external_ask = _open_probability_or_none(raw_external_ask)
    if external_ask is not None:
        return external_ask

    raw_asks = signal.orderbook.get("asks")
    if not isinstance(raw_asks, list):
        return None
    asks: list[float] = []
    for raw_level in raw_asks:
        if not isinstance(raw_level, dict):
            continue
        price = _open_probability_or_none(raw_level.get("price"))
        size = _positive_float_or_none(raw_level.get("size"))
        if price is not None and size is not None:
            asks.append(price)
    if not asks:
        return None
    return min(asks)


def _matches_sample(
    signal: MarketSignal,
    *,
    modulus: int,
    remainder: int,
) -> bool:
    key = f"{signal.market_id}:{signal.token_id or ''}"
    bucket = int(sha256(key.encode("utf-8")).hexdigest(), 16) % modulus
    return bucket == remainder


def _open_probability_or_none(value: Any) -> float | None:
    parsed = _positive_float_or_none(value)
    if parsed is None:
        return None
    if parsed >= 1.0:
        return None
    return parsed


def _positive_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed <= 0.0:
        return None
    return parsed
