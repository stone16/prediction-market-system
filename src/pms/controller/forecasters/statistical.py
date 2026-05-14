from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from pms.controller.factor_snapshot import (
    FactorSnapshotReader,
    NullFactorSnapshotReader,
)
from pms.core.models import MarketSignal
from pms.strategies.projections import FactorCompositionStep

ForecastResult = tuple[float, float, str]
MIN_STATISTICAL_PROBABILITY = 0.01
MAX_STATISTICAL_PROBABILITY = 0.99
STATISTICAL_MODEL_ID = "statistical-v1"
STATISTICAL_ROLES = frozenset(
    {
        "posterior_prior",
        "posterior_success",
        "posterior_failure",
        "weighted",
    }
)
EDGE_FACTOR_IDS = frozenset(
    {
        "anchoring_lag_divergence",
        "fair_value_spread",
        "favorite_longshot_bias",
        "orderbook_imbalance",
    }
)
PROBABILITY_FACTOR_IDS = frozenset(
    {
        "metaculus_prior",
        "rules",
        "statistical",
        "llm",
    }
)


@dataclass(frozen=True)
class StatisticalForecaster:
    factor_reader: FactorSnapshotReader = field(
        default_factory=NullFactorSnapshotReader
    )
    composition: Sequence[FactorCompositionStep] = ()
    strategy_id: str = "default"
    strategy_version_id: str = "default-v1"
    prior_strength: float = 2.0

    def __post_init__(self) -> None:
        if self.prior_strength <= 0.0:
            msg = "prior_strength must be positive"
            raise ValueError(msg)

    def predict(self, signal: MarketSignal) -> ForecastResult | None:
        if not _enabled_statistical_steps(self.composition):
            return None
        return asyncio.run(self.apredict(signal))

    async def apredict(self, signal: MarketSignal) -> ForecastResult | None:
        statistical_steps = _enabled_statistical_steps(self.composition)
        if not statistical_steps:
            return None
        snapshot = await self.factor_reader.snapshot(
            market_id=signal.market_id,
            as_of=signal.timestamp,
            required=statistical_steps,
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
        )
        if _missing_required_keys(
            statistical_steps,
            snapshot.values,
            snapshot.missing_factors,
        ):
            return None
        return _forecast_from_factors(
            signal=signal,
            statistical_steps=statistical_steps,
            factor_values=snapshot.values,
            prior_strength=self.prior_strength,
        )

    async def forecast(self, signal: MarketSignal) -> float:
        result = await self.apredict(signal)
        return signal.yes_price if result is None else result[0]


def _enabled_statistical_steps(
    composition: Sequence[FactorCompositionStep],
) -> tuple[FactorCompositionStep, ...]:
    return tuple(
        step for step in composition if step.enabled and step.role in STATISTICAL_ROLES
    )


def _missing_required_keys(
    steps: Sequence[FactorCompositionStep],
    factor_values: Mapping[tuple[str, str], float],
    reported_missing: Sequence[tuple[str, str]],
) -> bool:
    reported = set(reported_missing)
    return any(
        step.required
        and (
            (step.factor_id, step.param) in reported
            or (step.factor_id, step.param) not in factor_values
        )
        for step in steps
    )


def _forecast_from_factors(
    *,
    signal: MarketSignal,
    statistical_steps: Sequence[FactorCompositionStep],
    factor_values: Mapping[tuple[str, str], float],
    prior_strength: float,
) -> ForecastResult | None:
    candidates: list[tuple[float, float]] = []
    posterior = _posterior_candidate(
        statistical_steps,
        factor_values,
        prior_strength=prior_strength,
    )
    if posterior is not None:
        candidates.append(posterior)
    for step in statistical_steps:
        if step.role not in {"posterior_prior", "weighted"}:
            continue
        if posterior is not None and step.role == "posterior_prior":
            continue
        factor_value = factor_values.get((step.factor_id, step.param))
        if factor_value is None:
            continue
        probability = _factor_probability(
            factor_id=step.factor_id,
            value=factor_value,
            market_price=signal.yes_price,
        )
        if probability is None:
            continue
        candidates.append((_bounded_probability(probability), step.weight))
    if not candidates:
        return None
    probability = _weighted_average(candidates)
    probabilities = tuple(candidate[0] for candidate in candidates)
    weights = tuple(candidate[1] for candidate in candidates)
    return probability, _confidence(probabilities, weights), STATISTICAL_MODEL_ID


def _posterior_candidate(
    steps: Sequence[FactorCompositionStep],
    factor_values: Mapping[tuple[str, str], float],
    *,
    prior_strength: float,
) -> tuple[float, float] | None:
    success_steps = tuple(step for step in steps if step.role == "posterior_success")
    failure_steps = tuple(step for step in steps if step.role == "posterior_failure")
    if not success_steps and not failure_steps:
        return None
    successes = sum(
        step.weight * factor_values.get((step.factor_id, step.param), 0.0)
        for step in success_steps
    )
    failures = sum(
        step.weight * factor_values.get((step.factor_id, step.param), 0.0)
        for step in failure_steps
    )
    if successes == 0.0 and failures == 0.0:
        return None
    prior_steps = tuple(step for step in steps if step.role == "posterior_prior")
    prior_probability = 0.5
    prior_weight = prior_strength
    if prior_steps:
        prior_step = prior_steps[0]
        prior_probability = factor_values.get(
            (prior_step.factor_id, prior_step.param),
            0.5,
        )
        prior_weight = prior_step.weight
    alpha = prior_probability * prior_weight + successes
    beta = (1.0 - prior_probability) * prior_weight + failures
    total = alpha + beta
    if total == 0.0:
        return None
    return _bounded_probability(alpha / total), max(1.0, successes + failures)


def _factor_probability(
    *,
    factor_id: str,
    value: float,
    market_price: float,
) -> float | None:
    if factor_id in PROBABILITY_FACTOR_IDS:
        return value
    if factor_id in EDGE_FACTOR_IDS:
        return market_price + value
    return None


def _weighted_average(candidates: Sequence[tuple[float, float]]) -> float:
    total_weight = sum(weight for _, weight in candidates)
    if total_weight == 0.0:
        return sum(probability for probability, _ in candidates) / len(candidates)
    return sum(probability * weight for probability, weight in candidates) / total_weight


def _confidence(probabilities: Sequence[float], weights: Sequence[float]) -> float:
    if len(probabilities) == 1:
        return 0.5
    total_weight = sum(weights)
    if total_weight == 0.0:
        weights = tuple(1.0 for _ in probabilities)
        total_weight = float(len(probabilities))
    mean = (
        sum(
            probability * weight
            for probability, weight in zip(probabilities, weights)
        )
        / total_weight
    )
    mean_abs_deviation = (
        sum(
            abs(probability - mean) * weight
            for probability, weight in zip(probabilities, weights)
        )
        / total_weight
    )
    agreement = max(0.0, 1.0 - (mean_abs_deviation / 0.5))
    return min(0.95, 0.35 + 0.60 * agreement)


def _bounded_probability(value: float) -> float:
    return max(MIN_STATISTICAL_PROBABILITY, min(MAX_STATISTICAL_PROBABILITY, value))
