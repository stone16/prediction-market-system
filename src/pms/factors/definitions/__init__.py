from __future__ import annotations

from pms.factors.base import FactorDefinition

from .fair_value_spread import FairValueSpread
from .orderbook_imbalance import OrderbookImbalance
from .subset_pricing_violation import SubsetPricingViolation

REGISTERED: tuple[type[FactorDefinition], ...] = (
    OrderbookImbalance,
    FairValueSpread,
    SubsetPricingViolation,
)

__all__ = (
    "REGISTERED",
    "OrderbookImbalance",
    "FairValueSpread",
    "SubsetPricingViolation",
)
