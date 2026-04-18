from __future__ import annotations

import importlib
from typing import cast

from .composition import FactorCompositionStep


def _step(
    factor_id: str,
    *,
    role: str,
    weight: float,
    threshold: float | None = None,
    param: str = "",
) -> FactorCompositionStep:
    projections_module = importlib.import_module("pms.strategies.projections")
    step_cls = getattr(projections_module, "FactorCompositionStep")
    return cast(
        FactorCompositionStep,
        step_cls(
            factor_id=factor_id,
            role=role,
            param=param,
            weight=weight,
            threshold=threshold,
        ),
    )


DEFAULT_STRATEGY_COMPOSITION: tuple[FactorCompositionStep, ...] = (
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
