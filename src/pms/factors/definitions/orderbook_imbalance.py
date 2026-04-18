from __future__ import annotations

from pms.core.models import MarketSignal
from pms.factors.base import FactorDefinition, FactorValueRow, OuterRingReader


def _depth(levels: object) -> float:
    if not isinstance(levels, list):
        return 0.0

    total = 0.0
    for level in levels:
        if not isinstance(level, dict):
            continue
        size = level.get("size")
        if size is None:
            continue
        total += float(size)
    return total


class OrderbookImbalance(FactorDefinition):
    factor_id = "orderbook_imbalance"
    required_inputs = ("orderbook",)

    def compute(
        self,
        signal: MarketSignal,
        outer_ring: OuterRingReader,
    ) -> FactorValueRow | None:
        del outer_ring

        bid_depth = _depth(signal.orderbook.get("bids", []))
        ask_depth = _depth(signal.orderbook.get("asks", []))
        if bid_depth == 0.0 or ask_depth == 0.0:
            return None

        total_depth = bid_depth + ask_depth
        if total_depth == 0.0:
            return None

        return FactorValueRow(
            factor_id=self.factor_id,
            param="",
            market_id=signal.market_id,
            ts=signal.timestamp,
            value=(bid_depth - ask_depth) / total_depth,
        )
