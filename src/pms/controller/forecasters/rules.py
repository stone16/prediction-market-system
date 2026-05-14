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
MIN_RULE_PROBABILITY = 0.01
MAX_RULE_PROBABILITY = 0.99
RULES_MODEL_ID = "rules-v1"


@dataclass(frozen=True)
class RulesForecaster:
    factor_reader: FactorSnapshotReader = field(
        default_factory=NullFactorSnapshotReader
    )
    composition: Sequence[FactorCompositionStep] = ()
    strategy_id: str = "default"
    strategy_version_id: str = "default-v1"
    min_edge: float = 0.02

    def predict(self, signal: MarketSignal) -> ForecastResult | None:
        if not _enabled_rule_steps(self.composition):
            return None
        return asyncio.run(self.apredict(signal))

    async def apredict(self, signal: MarketSignal) -> ForecastResult | None:
        rule_steps = _enabled_rule_steps(self.composition)
        if not rule_steps:
            return None
        snapshot = await self.factor_reader.snapshot(
            market_id=signal.market_id,
            as_of=signal.timestamp,
            required=rule_steps,
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
        )
        return _forecast_from_rules(
            signal=signal,
            rule_steps=rule_steps,
            factor_values=snapshot.values,
        )

    async def forecast(self, signal: MarketSignal) -> float:
        result = await self.apredict(signal)
        return signal.yes_price if result is None else result[0]


def _enabled_rule_steps(
    composition: Sequence[FactorCompositionStep],
) -> tuple[FactorCompositionStep, ...]:
    return tuple(
        step for step in composition if step.role == "rule_delta" and step.enabled
    )


def _forecast_from_rules(
    *,
    signal: MarketSignal,
    rule_steps: Sequence[FactorCompositionStep],
    factor_values: Mapping[tuple[str, str], float],
) -> ForecastResult | None:
    probability = signal.yes_price
    max_abs_contribution = 0.0
    for step in rule_steps:
        factor_value = factor_values.get((step.factor_id, step.param))
        if factor_value is None:
            if step.required:
                return None
            continue
        delta = _rule_delta(step, factor_value, market_price=signal.yes_price)
        if step.threshold is not None and abs(delta) < step.threshold:
            continue
        contribution = delta * step.weight
        probability += contribution
        max_abs_contribution = max(max_abs_contribution, abs(contribution))
    return (
        _bounded_probability(probability),
        min(max_abs_contribution * 5.0, 0.95),
        RULES_MODEL_ID,
    )


def _rule_delta(
    step: FactorCompositionStep,
    factor_value: float,
    *,
    market_price: float,
) -> float:
    if step.factor_id == "metaculus_prior":
        return factor_value - market_price
    if step.factor_id == "subset_pricing_violation":
        return -factor_value
    return factor_value


def _bounded_probability(value: float) -> float:
    return max(MIN_RULE_PROBABILITY, min(MAX_RULE_PROBABILITY, value))
