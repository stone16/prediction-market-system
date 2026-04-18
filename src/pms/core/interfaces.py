from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from typing import Any, Protocol

from pms.core.models import (
    EvalRecord,
    FillRecord,
    Market,
    MarketSignal,
    OrderState,
    Portfolio,
    Token,
    TradeDecision,
)
from pms.strategies.projections import MarketSelectionSpec


class ISensor(Protocol):
    def __aiter__(self) -> AsyncIterator[MarketSignal]: ...


class DiscoveryPollCompleteSensor(Protocol):
    on_poll_complete: Callable[[], Awaitable[None]] | None


class SubscriptionManagedSensor(Protocol):
    async def update_subscription(self, asset_ids: list[str]) -> None: ...


class MarketDataStore(Protocol):
    async def read_eligible_markets(
        self,
        venue: str,
        max_horizon_days: int | None,
        min_volume_usdc: float,
    ) -> list[tuple[Market, list[Token]]]: ...


class StrategySelectionRegistry(Protocol):
    async def list_market_selections(
        self,
    ) -> list[tuple[str, str, MarketSelectionSpec]]: ...


class MarketSelectorLike(Protocol):
    async def select(self) -> Any: ...


class SubscriptionControllerLike(Protocol):
    async def update(self, asset_ids: list[str]) -> bool: ...

    @property
    def current_asset_ids(self) -> frozenset[str]: ...

    @property
    def last_updated_at(self) -> datetime | None: ...


class IController(Protocol):
    async def decide(
        self, signal: MarketSignal, portfolio: Portfolio | None = None
    ) -> TradeDecision | None: ...


class IActuator(Protocol):
    async def execute(
        self, decision: TradeDecision, portfolio: Portfolio | None = None
    ) -> OrderState: ...


class IEvaluator(Protocol):
    async def evaluate(self, fill: FillRecord, decision: TradeDecision) -> EvalRecord: ...


class IForecaster(Protocol):
    def predict(self, signal: MarketSignal) -> tuple[float, float, str] | None: ...

    async def forecast(self, signal: MarketSignal) -> float: ...


class ICalibrator(Protocol):
    def calibrate(self, probability: float, *, model_id: str) -> float: ...


class ISizer(Protocol):
    def size(self, *, prob: float, market_price: float, portfolio: Portfolio) -> float: ...
