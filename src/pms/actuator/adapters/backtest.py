"""Internal backtest actuator.

License decision: `prediction-market-backtesting` currently includes
LGPL-3.0-or-later terms for `nautilus_pm/` and root-level derivatives, so CP06
does not import or depend on that library. This adapter uses internal replay
from fixture orderbook snapshots instead.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pms.actuator.adapters.paper import _best_fill_price, _matched_order_state
from pms.core.enums import Venue
from pms.core.exceptions import KalshiStubError
from pms.core.models import OrderState, Portfolio, TradeDecision
from pms.core.venue_support import kalshi_stub_error


@dataclass
class BacktestActuator:
    fixture_path: Path
    _orderbooks: dict[str, dict[str, Any]] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self._orderbooks = _load_orderbooks(self.fixture_path)

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        if decision.venue == Venue.KALSHI.value:
            raise kalshi_stub_error("BacktestActuator.execute")
        orderbook = self._orderbooks.get(decision.market_id, {"bids": [], "asks": []})
        fill_price = _best_fill_price(orderbook, decision)
        return _matched_order_state(decision, fill_price, "backtest")


def _load_orderbooks(path: Path) -> dict[str, dict[str, Any]]:
    orderbooks: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            continue
        market_id = row.get("market_id")
        orderbook = row.get("orderbook")
        if isinstance(market_id, str) and isinstance(orderbook, dict):
            orderbooks[market_id] = orderbook
    return orderbooks
