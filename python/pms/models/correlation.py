"""Correlation model — pairs of related markets and their relation type.

CorrelationPair lives in CP01 (not CP10) so that StrategyProtocol and
CorrelationDetectorProtocol can both reference it without circular imports.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from .market import Market

RelationType = Literal[
    "subset",
    "superset",
    "overlapping",
    "contradictory",
    "independent",
]


@dataclass(frozen=True)
class CorrelationPair:
    """A detected logical relationship between two markets.

    ``arbitrage_opportunity`` is the estimated edge (price units) of the
    associated arbitrage trade, or ``None`` when no actionable edge exists.
    """

    market_a: Market
    market_b: Market
    similarity_score: float
    relation_type: RelationType
    relation_detail: str
    arbitrage_opportunity: Decimal | None
