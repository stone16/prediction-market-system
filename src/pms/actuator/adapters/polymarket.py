from __future__ import annotations

from dataclasses import dataclass, field

from pms.config import PMSSettings
from pms.core.models import (
    LiveTradingDisabledError,
    OrderState,
    Portfolio,
    TradeDecision,
)


@dataclass(frozen=True)
class PolymarketActuator:
    settings: PMSSettings = field(default_factory=PMSSettings)

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        if not self.settings.live_trading_enabled:
            raise LiveTradingDisabledError("Polymarket live trading is disabled")
        raise NotImplementedError("Polymarket live execution is gated for v2")
