from __future__ import annotations

# License note: unlike `prediction-market-backtesting` (LGPL-3.0-or-later),
# this thin adapter delegates to the in-tree internal replay implementation.

from dataclasses import dataclass, field
from pathlib import Path

from pms.actuator.adapters.backtest_fixtures import (
    DEFAULT_FIXTURE_TIMESTAMP,
    OrderbookSnapshot,
    load_orderbook_snapshots,
)
from pms.core.enums import Venue
from pms.core.exceptions import KalshiStubError
from pms.core.models import MarketSignal, OrderState, Portfolio, TradeDecision
from pms.core.venue_support import kalshi_stub_error
from pms.research.execution import BacktestExecutionSimulator
from pms.research.specs import ExecutionModel


@dataclass
class BacktestActuator:
    fixture_path: Path
    simulator: BacktestExecutionSimulator = field(default_factory=BacktestExecutionSimulator)
    _orderbooks: dict[tuple[str, str], OrderbookSnapshot] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self._orderbooks = load_orderbook_snapshots(self.fixture_path)

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        if decision.venue == Venue.KALSHI.value:
            error: KalshiStubError = kalshi_stub_error("BacktestActuator.execute")
            raise error
        token_id = decision.token_id or ""
        snapshot = self._orderbooks.get((decision.market_id, token_id)) or self._orderbooks.get(
            (decision.market_id, "")
        )
        signal = MarketSignal(
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            title=decision.market_id,
            yes_price=decision.limit_price,
            volume_24h=None,
            resolves_at=None,
            orderbook=snapshot.orderbook if snapshot is not None else {"bids": [], "asks": []},
            external_signal={},
            fetched_at=snapshot.fetched_at if snapshot is not None else DEFAULT_FIXTURE_TIMESTAMP,
            market_status="open",
        )
        return await self.simulator.execute(
            signal=signal,
            decision=decision,
            portfolio=portfolio,
            execution_model=ExecutionModel.polymarket_paper(),
        )
