from __future__ import annotations

from pms.factors.base import FactorDefinition

from .anchoring_lag_divergence import AnchoringLagDivergence
from .favorite_longshot_bias import FavoriteLongshotBias
from .fair_value_spread import FairValueSpread
from .metaculus_prior import MetaculusPrior
from .no_count import NoCount
from .orderbook_imbalance import OrderbookImbalance
from .subset_pricing_violation import SubsetPricingViolation
from .yes_count import YesCount

REGISTERED: tuple[type[FactorDefinition], ...] = (
    AnchoringLagDivergence,
    FavoriteLongshotBias,
    FairValueSpread,
    SubsetPricingViolation,
    MetaculusPrior,
    NoCount,
    OrderbookImbalance,
    YesCount,
)

__all__ = (
    "REGISTERED",
    "AnchoringLagDivergence",
    "FavoriteLongshotBias",
    "FairValueSpread",
    "MetaculusPrior",
    "NoCount",
    "OrderbookImbalance",
    "SubsetPricingViolation",
    "YesCount",
)
