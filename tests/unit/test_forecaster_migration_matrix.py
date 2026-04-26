from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.factors.base import EMPTY_OUTER_RING
from pms.factors.composition import apply_composition, evaluate_branch_probabilities
from pms.factors.definitions import REGISTERED
from pms.strategies.defaults import DEFAULT_STRATEGY_COMPOSITION


ForecastResult = tuple[float, float, str]


@dataclass(frozen=True)
class PreMigrationRules:
    min_edge: float = 0.02

    def predict(self, signal: MarketSignal) -> ForecastResult | None:
        spread = self._price_spread(signal)
        if spread is not None:
            return spread
        return self._subset_violation(signal)

    def _price_spread(self, signal: MarketSignal) -> ForecastResult | None:
        raw_fair_value = signal.external_signal.get("fair_value")
        if raw_fair_value is None:
            return None
        fair_value = float(raw_fair_value)
        edge = fair_value - signal.yes_price
        if abs(edge) < self.min_edge:
            return None
        return fair_value, min(abs(edge), 1.0), "price_spread"

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
        return superset_price, min(violation, 1.0), "subset_pricing_violation"


@dataclass(frozen=True)
class PreMigrationStatistical:
    prior_strength: float = 2.0

    def predict(self, signal: MarketSignal) -> ForecastResult | None:
        metaculus = signal.external_signal.get("metaculus_prob")
        has_counts = (
            "yes_count" in signal.external_signal or "no_count" in signal.external_signal
        )
        if metaculus is None and not has_counts:
            return None
        if metaculus is None:
            alpha = 1.0
            beta = 1.0
        else:
            metaculus_prob = float(metaculus)
            alpha = metaculus_prob * self.prior_strength
            beta = (1.0 - metaculus_prob) * self.prior_strength

        yes_count = float(signal.external_signal.get("yes_count", 0.0))
        no_count = float(signal.external_signal.get("no_count", 0.0))
        posterior_alpha = alpha + yes_count
        posterior_beta = beta + no_count
        total = posterior_alpha + posterior_beta
        probability = posterior_alpha / total
        return probability, 0.0, "statistical"


PreMigrationForecaster = PreMigrationRules | PreMigrationStatistical


def _signal(
    *,
    yes_price: float = 0.5,
    external_signal: dict[str, object] | None = None,
) -> MarketSignal:
    return MarketSignal(
        market_id="matrix-market",
        token_id="matrix-token",
        venue="polymarket",
        title="Will the migration matrix stay equivalent?",
        yes_price=yes_price,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal=external_signal or {},
        fetched_at=datetime(2026, 4, 18, 4, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _pre_branch_probability(
    forecaster: PreMigrationForecaster,
    signal: MarketSignal,
) -> float | None:
    result = forecaster.predict(signal)
    if result is None:
        return None
    return result[0]


def _factor_values(signal: MarketSignal) -> dict[tuple[str, str], float]:
    values: dict[tuple[str, str], float] = {
        ("yes_price", ""): signal.yes_price,
    }
    raw_subset = signal.external_signal.get("subset_price")
    if raw_subset is not None:
        values[("subset_price", "")] = float(raw_subset)
    for factor_cls in REGISTERED:
        row = factor_cls().compute(signal, EMPTY_OUTER_RING)
        if row is not None:
            values[(row.factor_id, row.param)] = row.value
    return values


def _branch_name(forecaster: PreMigrationForecaster) -> str:
    if isinstance(forecaster, PreMigrationRules):
        return "rules"
    return "statistical"


@pytest.mark.parametrize(
    ("case_name", "forecaster", "signal"),
    [
        (
            "spread_below_min_edge",
            PreMigrationRules(),
            _signal(external_signal={"fair_value": 0.51}),
        ),
        (
            "spread_at_min_edge",
            PreMigrationRules(),
            _signal(external_signal={"fair_value": 0.52}),
        ),
        (
            "spread_above_min_edge",
            PreMigrationRules(),
            _signal(external_signal={"fair_value": 0.60}),
        ),
        (
            "fair_value_absent_subset_runs",
            PreMigrationRules(),
            _signal(external_signal={"subset_price": 0.31, "superset_price": 0.28}),
        ),
        (
            "subset_below_min_edge",
            PreMigrationRules(),
            _signal(external_signal={"subset_price": 0.31, "superset_price": 0.30}),
        ),
        (
            "subset_at_min_edge",
            PreMigrationRules(),
            _signal(external_signal={"subset_price": 0.32, "superset_price": 0.30}),
        ),
        (
            "subset_above_min_edge",
            PreMigrationRules(),
            _signal(external_signal={"subset_price": 0.33, "superset_price": 0.30}),
        ),
        (
            "subset_absent",
            PreMigrationRules(),
            _signal(),
        ),
        (
            "both_rules_present",
            PreMigrationRules(),
            _signal(
                yes_price=0.40,
                external_signal={
                    "fair_value": 0.55,
                    "subset_price": 0.80,
                    "superset_price": 0.60,
                },
            ),
        ),
        (
            "only_spread_present",
            PreMigrationRules(),
            _signal(yes_price=0.40, external_signal={"fair_value": 0.55}),
        ),
        (
            "only_subset_present",
            PreMigrationRules(),
            _signal(external_signal={"subset_price": 0.80, "superset_price": 0.60}),
        ),
        (
            "neither_rule_present_yes_price_fallback",
            PreMigrationRules(),
            _signal(),
        ),
        (
            "metaculus_absent_zero_counts",
            PreMigrationStatistical(),
            _signal(),
        ),
        (
            "metaculus_absent_non_zero_counts",
            PreMigrationStatistical(),
            _signal(external_signal={"yes_count": 3, "no_count": 1}),
        ),
        (
            "metaculus_present_zero_counts",
            PreMigrationStatistical(),
            _signal(external_signal={"metaculus_prob": 0.7}),
        ),
        (
            "metaculus_present_non_zero_counts",
            PreMigrationStatistical(),
            _signal(external_signal={"metaculus_prob": 0.7, "yes_count": 3, "no_count": 7}),
        ),
    ],
)
def test_forecaster_migration_matrix_matches_pre_migration_branch_output(
    case_name: str,
    forecaster: PreMigrationForecaster,
    signal: MarketSignal,
) -> None:
    del case_name

    expected_probability = _pre_branch_probability(forecaster, signal)
    branch_probabilities = evaluate_branch_probabilities(
        DEFAULT_STRATEGY_COMPOSITION,
        _factor_values(signal),
    )

    observed_probability = branch_probabilities.get(_branch_name(forecaster))
    if expected_probability is None:
        assert observed_probability is None
        return
    assert observed_probability == pytest.approx(expected_probability, abs=1e-9)


@pytest.mark.parametrize(
    ("case_name", "signal", "llm_probability"),
    [
        (
            "rules_and_statistical_average_without_llm",
            _signal(yes_price=0.40, external_signal={"fair_value": 0.55}),
            None,
        ),
        (
            "rules_statistical_and_llm_average",
            _signal(yes_price=0.40, external_signal={"fair_value": 0.55}),
            0.80,
        ),
        (
            "statistical_and_llm_average_without_rules",
            _signal(external_signal={"metaculus_prob": 0.70, "yes_count": 3, "no_count": 7}),
            0.80,
        ),
    ],
)
def test_default_strategy_composition_matches_present_branch_average(
    case_name: str,
    signal: MarketSignal,
    llm_probability: float | None,
) -> None:
    del case_name

    expected_probabilities: list[float] = []
    rules_result = PreMigrationRules().predict(signal)
    if rules_result is not None:
        expected_probabilities.append(rules_result[0])
    statistical_result = PreMigrationStatistical().predict(signal)
    if statistical_result is not None:
        expected_probabilities.append(statistical_result[0])
    factor_values = _factor_values(signal)
    if llm_probability is not None:
        expected_probabilities.append(llm_probability)
        factor_values[("llm", "")] = llm_probability

    observed_probability = apply_composition(
        DEFAULT_STRATEGY_COMPOSITION,
        factor_values,
    )

    assert observed_probability == pytest.approx(
        sum(expected_probabilities) / len(expected_probabilities),
        abs=1e-9,
    )
