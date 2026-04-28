"""Composable ripple strategy module."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pms.strategies.base import (
    StrategyAgent,
    StrategyController,
    StrategyObservationSource,
)
from pms.strategies.intents import BasketIntent, StrategyContext, TradeIntent


@dataclass(frozen=True, slots=True)
class RippleStrategyModule:
    source: StrategyObservationSource
    controller: StrategyController
    agent: StrategyAgent
    strategy_id: str
    strategy_version_id: str

    async def run(
        self,
        context: StrategyContext,
    ) -> Sequence[TradeIntent | BasketIntent]:
        if (
            context.strategy_id != self.strategy_id
            or context.strategy_version_id != self.strategy_version_id
        ):
            msg = "context strategy identity must match ripple module"
            raise ValueError(msg)

        observations = await self.source.observe(context)
        candidates = await self.controller.propose(context, observations)
        intents: list[TradeIntent | BasketIntent] = []
        for candidate in candidates:
            judgement = await self.agent.judge(context, candidate)
            intents.extend(await self.agent.build_intents(context, candidate, judgement))
        return tuple(intents)
