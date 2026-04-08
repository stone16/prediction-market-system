"""Guardrail bounds for auto-adjustable risk parameters (CP08).

The :data:`GUARDRAILS` table captures the hard floor/ceiling for every risk
parameter that the :class:`~pms.execution.risk.RiskManager` is allowed to
mutate in response to evaluation feedback. ``apply_guardrail`` clamps a
candidate value into the corresponding bound and is a no-op for parameters
not listed here (keeping the helper safe for future additions).
"""

from __future__ import annotations

from decimal import Decimal
from typing import TypedDict


class GuardrailBounds(TypedDict):
    """Inclusive lower and upper bounds for a risk parameter."""

    floor: Decimal
    ceiling: Decimal


# Floor/ceiling bounds for every auto-adjustable risk parameter. Values used
# here are the CP08 spec defaults and are intentionally fixed: they are the
# "hard stops" that feedback loops cannot override.
GUARDRAILS: dict[str, GuardrailBounds] = {
    "max_position_per_market": {
        "floor": Decimal("10"),
        "ceiling": Decimal("5000"),
    },
    "max_total_exposure": {
        "floor": Decimal("100"),
        "ceiling": Decimal("50000"),
    },
    "max_drawdown_pct": {
        "floor": Decimal("0.01"),
        "ceiling": Decimal("0.50"),
    },
}


def apply_guardrail(name: str, value: Decimal) -> Decimal:
    """Clamp ``value`` to the guardrail bounds registered for ``name``.

    If ``name`` has no registered bounds the value passes through unchanged —
    this keeps the helper forward-compatible with new parameters that may be
    introduced before they are registered in :data:`GUARDRAILS`.
    """
    bounds = GUARDRAILS.get(name)
    if bounds is None:
        return value
    return max(bounds["floor"], min(bounds["ceiling"], value))
