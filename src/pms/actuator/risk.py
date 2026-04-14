from __future__ import annotations

from dataclasses import dataclass, field

from pms.config import RiskSettings
from pms.core.models import Portfolio, TradeDecision


class InsufficientLiquidityError(RuntimeError):
    """Raised when an actuator cannot fill a decision from available depth."""


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reason: str


@dataclass(frozen=True)
class RiskManager:
    risk: RiskSettings = field(default_factory=RiskSettings)

    def check(self, decision: TradeDecision, portfolio: Portfolio) -> RiskDecision:
        market_exposure = _market_exposure(portfolio, decision.market_id) + decision.size
        if market_exposure > self.risk.max_position_per_market:
            return RiskDecision(False, "max_position_per_market")

        total_exposure = portfolio.locked_usdc + decision.size
        if total_exposure > self.risk.max_total_exposure:
            return RiskDecision(False, "max_total_exposure")

        if (
            self.risk.max_drawdown_pct is not None
            and portfolio.max_drawdown_pct is not None
            and portfolio.max_drawdown_pct > self.risk.max_drawdown_pct
        ):
            return RiskDecision(False, "drawdown_circuit_breaker")

        return RiskDecision(True, "approved")


def _market_exposure(portfolio: Portfolio, market_id: str) -> float:
    return sum(
        position.locked_usdc
        for position in portfolio.open_positions
        if position.market_id == market_id
    )
