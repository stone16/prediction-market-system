from __future__ import annotations

import pytest

from pms.factors.composition import apply_composition, evaluate_branch_probabilities
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


def _branching_composition() -> tuple[FactorCompositionStep, ...]:
    return (
        _step("fair_value_spread", role="precedence_rank", weight=1.0),
        _step("subset_pricing_violation", role="precedence_rank", weight=2.0),
        _step("fair_value_spread", role="threshold_edge", weight=1.0, threshold=0.02),
        _step("subset_pricing_violation", role="threshold_edge", weight=1.0, threshold=0.02),
        _step("metaculus_prior", role="posterior_prior", weight=2.0),
        _step("yes_count", role="posterior_success", weight=1.0),
        _step("no_count", role="posterior_failure", weight=1.0),
        _step("llm", role="runtime_probability", weight=1.0),
        _step("rules", role="blend_weighted", weight=1.0),
        _step("statistical", role="blend_weighted", weight=1.0),
        _step("llm", role="blend_weighted", weight=1.0),
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


def test_apply_composition_renormalizes_present_weighted_inputs() -> None:
    result = apply_composition(
        (
            _step("factor-a", role="weighted", weight=0.6),
            _step("factor-b", role="weighted", weight=0.4),
        ),
        {
            ("factor-a", ""): 0.25,
        },
    )

    assert result == pytest.approx(0.25)


def test_apply_composition_weighted_legacy_shape_raises_when_all_inputs_missing() -> None:
    with pytest.raises(ValueError, match="weighted composition is missing all factor inputs"):
        apply_composition(
            (
                _step("factor-a", role="weighted", weight=0.6),
                _step("factor-b", role="weighted", weight=0.4),
            ),
            {},
        )


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


def test_evaluate_branch_probabilities_exposes_rules_statistical_and_llm_inputs() -> None:
    branch_probabilities = evaluate_branch_probabilities(
        _branching_composition(),
        {
            ("fair_value_spread", ""): 0.15,
            ("subset_pricing_violation", ""): 0.20,
            ("yes_price", ""): 0.40,
            ("subset_price", ""): 0.80,
            ("metaculus_prior", ""): 0.70,
            ("yes_count", ""): 3.0,
            ("no_count", ""): 7.0,
            ("llm", ""): 0.80,
        },
    )

    assert branch_probabilities == {
        "rules": pytest.approx(0.55),
        "statistical": pytest.approx(4.4 / 12.0),
        "llm": pytest.approx(0.80),
    }


def test_apply_composition_averages_present_branches_when_blend_steps_exist() -> None:
    result = apply_composition(
        _branching_composition(),
        {
            ("fair_value_spread", ""): 0.15,
            ("subset_pricing_violation", ""): 0.20,
            ("yes_price", ""): 0.40,
            ("subset_price", ""): 0.80,
            ("metaculus_prior", ""): 0.70,
            ("yes_count", ""): 3.0,
            ("no_count", ""): 7.0,
            ("llm", ""): 0.80,
        },
    )

    assert result == pytest.approx((0.55 + (4.4 / 12.0) + 0.80) / 3.0)


def test_apply_composition_blend_skips_missing_llm_branch() -> None:
    result = apply_composition(
        _branching_composition(),
        {
            ("fair_value_spread", ""): 0.15,
            ("subset_pricing_violation", ""): 0.20,
            ("yes_price", ""): 0.40,
            ("subset_price", ""): 0.80,
            ("metaculus_prior", ""): 0.70,
            ("yes_count", ""): 3.0,
            ("no_count", ""): 7.0,
        },
    )

    assert result == pytest.approx((0.55 + (4.4 / 12.0)) / 2.0)


def test_apply_composition_returns_runtime_probability_without_rules_or_posterior() -> None:
    result = apply_composition(
        (
            _step("llm", role="runtime_probability", weight=1.0),
        ),
        {
            ("llm", ""): 0.80,
        },
    )

    assert result == pytest.approx(0.80)


def test_apply_composition_falls_back_to_yes_price_when_blend_has_no_present_branches() -> None:
    result = apply_composition(
        (
            _step("rules", role="blend_weighted", weight=1.0),
            _step("llm", role="blend_weighted", weight=1.0),
        ),
        {
            ("yes_price", ""): 0.41,
        },
    )

    assert result == pytest.approx(0.41)


def test_apply_composition_supports_generic_threshold_edge_steps() -> None:
    result = apply_composition(
        (
            _step("generic_signal", role="threshold_edge", weight=1.0, threshold=0.02),
        ),
        {
            ("generic_signal", ""): 0.07,
        },
    )

    assert result == pytest.approx(0.07)


def test_apply_composition_raises_when_required_rule_inputs_are_missing() -> None:
    with pytest.raises(KeyError, match="missing required factor input 'yes_price':''"):
        apply_composition(
            (
                _step("fair_value_spread", role="threshold_edge", weight=1.0, threshold=0.02),
            ),
            {
                ("fair_value_spread", ""): 0.10,
            },
        )


def test_apply_composition_raises_when_no_probability_can_be_resolved() -> None:
    with pytest.raises(KeyError, match="composition could not resolve a probability"):
        apply_composition((), {})


def test_posterior_branch_can_fall_back_to_yes_price_when_total_is_zero() -> None:
    result = apply_composition(
        (
            _step("metaculus_prior", role="posterior_prior", weight=0.0),
        ),
        {
            ("yes_price", ""): 0.63,
        },
    )

    assert result == pytest.approx(0.63)
