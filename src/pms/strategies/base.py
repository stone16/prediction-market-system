"""Protocol surface for strategy plugins."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from pms.strategies.intents import (
    BasketIntent,
    StrategyCandidate,
    StrategyContext,
    StrategyJudgement,
    StrategyObservation,
    TradeIntent,
)


class StrategyObservationSource(Protocol):
    """plugin-local observation collection, not the core Sensor layer."""

    async def observe(self, context: StrategyContext) -> Sequence[StrategyObservation]: ...


class StrategyController(Protocol):
    async def propose(
        self,
        context: StrategyContext,
        observations: Sequence[StrategyObservation],
    ) -> Sequence[StrategyCandidate]: ...


class StrategyAgent(Protocol):
    async def judge(self, context: StrategyContext, candidate: StrategyCandidate) -> StrategyJudgement: ...

    async def build_intents(
        self,
        context: StrategyContext,
        candidate: StrategyCandidate,
        judgement: StrategyJudgement,
    ) -> Sequence[TradeIntent | BasketIntent]: ...


class StrategyModule(Protocol):
    @property
    def strategy_id(self) -> str: ...

    @property
    def strategy_version_id(self) -> str: ...

    async def run(self, context: StrategyContext) -> Sequence[TradeIntent | BasketIntent]: ...
