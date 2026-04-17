from __future__ import annotations

import pytest

from pms.factors.composition import apply_composition
from pms.strategies.projections import FactorCompositionStep


def _step(
    factor_id: str,
    *,
    role: str,
    weight: float,
    threshold: float | None = None,
    param: str = "",
) -> FactorCompositionStep:
    return FactorCompositionStep(
        factor_id=factor_id,
        role=role,
        param=param,
        weight=weight,
        threshold=threshold,
    )


def test_apply_composition_supports_weighted_legacy_shape() -> None:
    result = apply_composition(
        (
            _step("factor-a", role="weighted", weight=0.6),
            _step("factor-b", role="weighted", weight=0.4),
        ),
        {
            ("factor-a", ""): 0.25,
            ("factor-b", ""): 0.75,
        },
    )

    assert result == pytest.approx(0.45)


def test_apply_composition_matches_statistical_posterior_with_metaculus_prior() -> None:
    result = apply_composition(
        (
            _step("metaculus_prior", role="posterior_prior", weight=2.0),
            _step("yes_count", role="posterior_success", weight=1.0),
            _step("no_count", role="posterior_failure", weight=1.0),
        ),
        {
            ("metaculus_prior", ""): 0.7,
            ("yes_count", ""): 3.0,
            ("no_count", ""): 7.0,
        },
    )

    assert result == pytest.approx(4.4 / 12.0)


def test_apply_composition_uses_rules_precedence_before_statistical_fallback() -> None:
    result = apply_composition(
        (
            _step("fair_value_spread", role="precedence_rank", weight=1.0),
            _step("subset_pricing_violation", role="precedence_rank", weight=2.0),
            _step("fair_value_spread", role="threshold_edge", weight=1.0, threshold=0.02),
            _step("subset_pricing_violation", role="threshold_edge", weight=1.0, threshold=0.02),
            _step("metaculus_prior", role="posterior_prior", weight=2.0),
            _step("yes_count", role="posterior_success", weight=1.0),
            _step("no_count", role="posterior_failure", weight=1.0),
        ),
        {
            ("fair_value_spread", ""): 0.15,
            ("subset_pricing_violation", ""): 0.20,
            ("yes_price", ""): 0.40,
            ("subset_price", ""): 0.80,
            ("yes_count", ""): 0.0,
            ("no_count", ""): 0.0,
        },
    )

    assert result == pytest.approx(0.55)
