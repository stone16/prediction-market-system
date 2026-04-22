from __future__ import annotations

# License note: unlike `prediction-market-backtesting` (LGPL-3.0-or-later),
# this thin adapter keeps the internal replay implementation in-tree.
# Venue.KALSHI dispatch must still raise KalshiStubError via the shared stub.

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pms.core.enums import Venue
from pms.core.models import MarketSignal, OrderState, Portfolio, TradeDecision
from pms.core.venue_support import kalshi_stub_error
from pms.research.execution import BacktestExecutionSimulator
from pms.research.specs import ExecutionModel


@dataclass
class BacktestActuator:
    fixture_path: Path
    simulator: BacktestExecutionSimulator = field(default_factory=BacktestExecutionSimulator)
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
        signal = MarketSignal(
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            title=decision.market_id,
            yes_price=decision.limit_price,
            volume_24h=None,
            resolves_at=None,
            orderbook=self._orderbooks.get(decision.market_id, {"bids": [], "asks": []}),
            external_signal={},
            fetched_at=datetime.now(tz=UTC),
            market_status="open",
        )
        return await self.simulator.execute(
            signal=signal,
            decision=decision,
            portfolio=portfolio,
            execution_model=ExecutionModel.polymarket_paper(),
        )


def _load_orderbooks(path: Path) -> dict[str, dict[str, Any]]:
    return {
        row["market_id"]: row["orderbook"]
        for row in (
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
        if isinstance(row, dict)
        and isinstance(row.get("market_id"), str)
        and isinstance(row.get("orderbook"), dict)
    }
