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
        notional = decision.notional_usdc
        if notional <= 0.0:
            return RiskDecision(False, "non_positive_size")

        if notional < self.risk.min_order_usdc:
            return RiskDecision(False, "min_order_usdc")

        market_exposure = _market_exposure(portfolio, decision.market_id) + notional
        if market_exposure > self.risk.max_position_per_market:
            return RiskDecision(False, "max_position_per_market")

        total_exposure = portfolio.locked_usdc + notional
        if total_exposure > self.risk.max_total_exposure:
            return RiskDecision(False, "max_total_exposure")

        if (
            self.risk.max_drawdown_pct is not None
            and portfolio.max_drawdown_pct is not None
            and portfolio.max_drawdown_pct > self.risk.max_drawdown_pct
        ):
            return RiskDecision(False, "drawdown_circuit_breaker")

        if (
            self.risk.max_open_positions is not None
            and len(portfolio.open_positions) >= self.risk.max_open_positions
        ):
            return RiskDecision(False, "max_open_positions")

        if decision.max_slippage_bps > self.risk.slippage_threshold_bps:
            return RiskDecision(False, "slippage_threshold_bps")

        if notional > portfolio.free_usdc:
            return RiskDecision(False, "insufficient_free_usdc")

        return RiskDecision(True, "approved")


def _market_exposure(portfolio: Portfolio, market_id: str) -> float:
    return sum(
        position.locked_usdc
        for position in portfolio.open_positions
        if position.market_id == market_id
    )
