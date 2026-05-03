"""Composable H1 FLB strategy module."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pms.strategies.base import (
    StrategyAgent,
    StrategyController,
    StrategyObservationSource,
)
from pms.strategies.intents import BasketIntent, StrategyContext, TradeIntent
from pms.strategies.runtime_bridge import StrategyRunResult


@dataclass(frozen=True, slots=True)
class FlbStrategyModule:
    source: StrategyObservationSource
    controller: StrategyController
    agent: StrategyAgent
    strategy_id: str
    strategy_version_id: str

    async def run(
        self,
        context: StrategyContext,
    ) -> Sequence[TradeIntent | BasketIntent]:
        results = await self.run_with_artifacts(context)
        return tuple(intent for result in results for intent in result.intents)

    async def run_with_artifacts(
        self,
        context: StrategyContext,
    ) -> Sequence[StrategyRunResult]:
        if (
            context.strategy_id != self.strategy_id
            or context.strategy_version_id != self.strategy_version_id
        ):
            msg = "context strategy identity must match H1 FLB module"
            raise ValueError(msg)

        observations = await self.source.observe(context)
        candidates = await self.controller.propose(context, observations)
        results: list[StrategyRunResult] = []
        for candidate in candidates:
            judgement = await self.agent.judge(context, candidate)
            intents = await self.agent.build_intents(context, candidate, judgement)
            results.append(
                StrategyRunResult(
                    judgement=judgement,
                    intents=tuple(intents),
                )
            )
        return tuple(results)
