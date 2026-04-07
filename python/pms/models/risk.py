"""Risk-management decision model."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RiskDecision:
    """Outcome of a risk check on a candidate order.

    ``adjusted_size`` is set when the risk manager approves the order but
    requires a smaller size; otherwise ``None``.
    """

    approved: bool
    reason: str
    adjusted_size: Decimal | None
