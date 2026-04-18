from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol


class FactorCompositionStep(Protocol):
    @property
    def factor_id(self) -> str: ...

    @property
    def role(self) -> str: ...

    @property
    def param(self) -> str: ...

    @property
    def weight(self) -> float: ...

    @property
    def threshold(self) -> float | None: ...


def apply_composition(
    composition: Sequence[FactorCompositionStep],
    factor_values: Mapping[tuple[str, str], float],
) -> float:
    weighted_steps = tuple(step for step in composition if step.role == "weighted")
    non_weighted_steps = tuple(step for step in composition if step.role != "weighted")
    if weighted_steps and not non_weighted_steps:
        return _apply_weighted(weighted_steps, factor_values)

    branch_probabilities = evaluate_branch_probabilities(composition, factor_values)
    blended_probability = _apply_blend_weighted(composition, branch_probabilities)
    if blended_probability is not None:
        return blended_probability

    precedence_probability = branch_probabilities.get("rules")
    if precedence_probability is not None:
        return precedence_probability

    posterior_probability = branch_probabilities.get("statistical")
    if posterior_probability is not None:
        return posterior_probability

    runtime_probability = _first_runtime_probability(composition, branch_probabilities)
    if runtime_probability is not None:
        return runtime_probability

    if weighted_steps:
        return _apply_weighted(weighted_steps, factor_values)

    fallback = factor_values.get(("yes_price", ""))
    if fallback is not None:
        return fallback

    msg = "composition could not resolve a probability"
    raise KeyError(msg)


def evaluate_branch_probabilities(
    composition: Sequence[FactorCompositionStep],
    factor_values: Mapping[tuple[str, str], float],
) -> dict[str, float]:
    branch_probabilities: dict[str, float] = {}

    rules_probability = _apply_threshold_precedence(composition, factor_values)
    if rules_probability is not None:
        branch_probabilities["rules"] = rules_probability

    posterior_probability = _apply_posterior(composition, factor_values)
    if posterior_probability is not None:
        branch_probabilities["statistical"] = posterior_probability

    for step in composition:
        if step.role != "runtime_probability":
            continue
        runtime_probability = factor_values.get((step.factor_id, step.param))
        if runtime_probability is not None:
            branch_probabilities[step.factor_id] = runtime_probability

    return branch_probabilities


def _apply_weighted(
    steps: tuple[FactorCompositionStep, ...],
    factor_values: Mapping[tuple[str, str], float],
) -> float:
    weighted_total = 0.0
    realized_weight = 0.0
    for step in steps:
        value = factor_values.get((step.factor_id, step.param))
        if value is None:
            continue
        weighted_total += step.weight * value
        realized_weight += step.weight
    if realized_weight == 0.0:
        msg = "weighted composition is missing all factor inputs"
        raise ValueError(msg)
    return weighted_total / realized_weight


def _apply_blend_weighted(
    composition: Sequence[FactorCompositionStep],
    branch_probabilities: Mapping[str, float],
) -> float | None:
    blend_steps = tuple(step for step in composition if step.role == "blend_weighted")
    if not blend_steps:
        return None

    total_weight = 0.0
    weighted_probability = 0.0
    for step in blend_steps:
        branch_probability = branch_probabilities.get(step.factor_id)
        if branch_probability is None:
            continue
        total_weight += step.weight
        weighted_probability += step.weight * branch_probability

    if total_weight == 0.0:
        return None
    return weighted_probability / total_weight


def _apply_threshold_precedence(
    composition: Sequence[FactorCompositionStep],
    factor_values: Mapping[tuple[str, str], float],
) -> float | None:
    ranks = {
        (step.factor_id, step.param): step.weight
        for step in composition
        if step.role == "precedence_rank"
    }
    threshold_steps = sorted(
        (step for step in composition if step.role == "threshold_edge"),
        key=lambda step: ranks.get((step.factor_id, step.param), step.weight),
    )

    for step in threshold_steps:
        edge = factor_values.get((step.factor_id, step.param))
        if edge is None:
            continue

        threshold = 0.0 if step.threshold is None else step.threshold
        if step.factor_id == "fair_value_spread":
            if abs(edge) < threshold:
                continue
            return _required_factor_value(factor_values, "yes_price", "") + edge
        if step.factor_id == "subset_pricing_violation":
            if edge < threshold:
                continue
            return _required_factor_value(factor_values, "subset_price", "") - edge
        if abs(edge) < threshold:
            continue
        return edge

    return None


def _first_runtime_probability(
    composition: Sequence[FactorCompositionStep],
    branch_probabilities: Mapping[str, float],
) -> float | None:
    for step in composition:
        if step.role != "runtime_probability":
            continue
        runtime_probability = branch_probabilities.get(step.factor_id)
        if runtime_probability is not None:
            return runtime_probability
    return None


def _apply_posterior(
    composition: Sequence[FactorCompositionStep],
    factor_values: Mapping[tuple[str, str], float],
) -> float | None:
    prior_steps = tuple(step for step in composition if step.role == "posterior_prior")
    success_steps = tuple(step for step in composition if step.role == "posterior_success")
    failure_steps = tuple(step for step in composition if step.role == "posterior_failure")
    if not prior_steps and not success_steps and not failure_steps:
        return None

    prior_strength = 0.0
    prior_alpha = 0.0
    prior_beta = 0.0
    if prior_steps:
        prior_step = prior_steps[0]
        prior_strength = prior_step.weight
        prior_prob = factor_values.get((prior_step.factor_id, prior_step.param))
        if prior_prob is None:
            prior_alpha = prior_strength / 2.0
            prior_beta = prior_strength / 2.0
        else:
            prior_alpha = prior_prob * prior_strength
            prior_beta = (1.0 - prior_prob) * prior_strength

    successes = sum(
        step.weight * factor_values.get((step.factor_id, step.param), 0.0)
        for step in success_steps
    )
    failures = sum(
        step.weight * factor_values.get((step.factor_id, step.param), 0.0)
        for step in failure_steps
    )
    total = prior_alpha + prior_beta + successes + failures
    if total == 0.0:
        return factor_values.get(("yes_price", ""))
    return (prior_alpha + successes) / total


def _required_factor_value(
    factor_values: Mapping[tuple[str, str], float],
    factor_id: str,
    param: str,
) -> float:
    value = factor_values.get((factor_id, param))
    if value is None:
        msg = f"missing required factor input {factor_id!r}:{param!r}"
        raise KeyError(msg)
    return value
