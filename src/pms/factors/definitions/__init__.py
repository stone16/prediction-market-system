from __future__ import annotations

from pms.factors.base import FactorDefinition

from .orderbook_imbalance import OrderbookImbalance

REGISTERED: tuple[type[FactorDefinition], ...] = (OrderbookImbalance,)

__all__ = ("REGISTERED", "OrderbookImbalance")
