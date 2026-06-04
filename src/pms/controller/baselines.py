from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Literal

from pms.core.category_prior import (
    CategoryPriorObservation as CategoryPriorObservation,
    CategoryPriorObservationLoad as CategoryPriorObservationLoad,
    aware_utc_datetime as _aware_utc,
    load_category_prior_observations_csv as load_category_prior_observations_csv,
    normalize_category as _normalize_category,
)
from pms.core.models import MarketSignal


CategoryPriorSource = Literal["category", "global"]


@dataclass(frozen=True, slots=True)
class CategoryPriorEstimate:
    probability: float
    source: CategoryPriorSource
    category: str
    sample_count: int


@dataclass(frozen=True, slots=True)
class CategoryPriorBaselineEstimator:
    observations: Iterable[CategoryPriorObservation]
    min_category_samples: int = 20
    min_global_samples: int = 100
    smoothing_alpha: float = 1.0
    smoothing_beta: float = 1.0

    def __post_init__(self) -> None:
        if self.min_category_samples <= 0:
            msg = "min_category_samples must be positive"
            raise ValueError(msg)
        if self.min_global_samples <= 0:
            msg = "min_global_samples must be positive"
            raise ValueError(msg)
        if self.smoothing_alpha <= 0.0 or self.smoothing_beta <= 0.0:
            msg = "smoothing_alpha and smoothing_beta must be positive"
            raise ValueError(msg)
        object.__setattr__(self, "observations", tuple(self.observations))

    def estimate(self, signal: MarketSignal) -> CategoryPriorEstimate | None:
        category = _signal_category(signal)
        if category is None:
            return None

        as_of = _aware_utc(signal.fetched_at)
        eligible = [
            observation
            for observation in self.observations
            if observation.resolved_at < as_of
        ]
        category_observations = [
            observation
            for observation in eligible
            if observation.category == category
        ]
        if len(category_observations) >= self.min_category_samples:
            return CategoryPriorEstimate(
                probability=self._smoothed_rate(category_observations),
                source="category",
                category=category,
                sample_count=len(category_observations),
            )
        if len(eligible) >= self.min_global_samples:
            return CategoryPriorEstimate(
                probability=self._smoothed_rate(eligible),
                source="global",
                category=category,
                sample_count=len(eligible),
            )
        return None

    def _smoothed_rate(
        self,
        observations: list[CategoryPriorObservation],
    ) -> float:
        positive_outcomes = sum(observation.resolved_outcome for observation in observations)
        denominator = len(observations) + self.smoothing_alpha + self.smoothing_beta
        return (positive_outcomes + self.smoothing_alpha) / denominator


def enrich_signal_with_category_prior(
    signal: MarketSignal,
    estimator: CategoryPriorBaselineEstimator,
) -> MarketSignal:
    if "category_prior_baseline_prob_estimate" in signal.external_signal:
        return signal

    estimate = estimator.estimate(signal)
    if estimate is None:
        return signal

    external_signal = dict(signal.external_signal)
    external_signal.update(
        {
            "category_prior_baseline_prob_estimate": estimate.probability,
            "category_prior_baseline_source": estimate.source,
            "category_prior_baseline_category": estimate.category,
            "category_prior_baseline_sample_count": estimate.sample_count,
        }
    )
    return replace(signal, external_signal=external_signal)


def _signal_category(signal: MarketSignal) -> str | None:
    for key in ("category", "market_category"):
        value = signal.external_signal.get(key)
        if isinstance(value, str):
            category = _normalize_category(value)
            if category is not None:
                return category
    risk_group_id = signal.external_signal.get("risk_group_id")
    if isinstance(risk_group_id, str):
        category = _normalize_category(risk_group_id)
        if category is not None:
            return category
    event_id = signal.external_signal.get("event_id")
    if isinstance(event_id, str):
        category = _normalize_category(event_id)
        if category is not None:
            return f"event:{category}"
    return None
