from __future__ import annotations

from pms.factors.base import FactorDefinition

from .fair_value_spread import FairValueSpread
from .metaculus_prior import MetaculusPrior
from .no_count import NoCount
from .orderbook_imbalance import OrderbookImbalance
from .subset_pricing_violation import SubsetPricingViolation
from .yes_count import YesCount

REGISTERED: tuple[type[FactorDefinition], ...] = (
    OrderbookImbalance,
    FairValueSpread,
    SubsetPricingViolation,
    MetaculusPrior,
    YesCount,
    NoCount,
)

__all__ = (
    "REGISTERED",
    "OrderbookImbalance",
    "FairValueSpread",
    "SubsetPricingViolation",
    "MetaculusPrior",
    "YesCount",
    "NoCount",
)
