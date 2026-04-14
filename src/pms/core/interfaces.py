from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from pms.core.models import (
    EvalRecord,
    FillRecord,
    MarketSignal,
    OrderState,
    Portfolio,
    TradeDecision,
)


class ISensor(Protocol):
    def __aiter__(self) -> AsyncIterator[MarketSignal]: ...


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
