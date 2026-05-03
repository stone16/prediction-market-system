from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal

from pms.config import RiskSettings
from pms.core.models import Portfolio, TradeDecision


class InsufficientLiquidityError(RuntimeError):
    """Raised when an actuator cannot fill a decision from available depth."""


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reason: str


HaltTriggerKind = Literal[
    "none",
    "consecutive_losses",
    "slippage_spike",
    "credential_failure",
    "order_without_fill",
    "rate_limit_exceeded",
    "drawdown_circuit_breaker",
]


@dataclass(frozen=True)
class HaltState:
    halted: bool
    reason: str
    triggered_at: datetime
    trigger_kind: HaltTriggerKind


@dataclass(frozen=True)
class HaltEvent:
    state: HaltState
    trace_id: str | None = None


@dataclass(frozen=True)
class RiskTradeResult:
    pnl: float
    slippage_bps: float
    filled_at: datetime
    trace_id: str | None = None


@dataclass
class RiskManager:
    risk: RiskSettings = field(default_factory=RiskSettings)
    _halt_state: HaltState | None = field(default=None, init=False)
    _halt_events: list[HaltEvent] = field(default_factory=list, init=False)
    _recent_trades: list[RiskTradeResult] = field(default_factory=list, init=False)
    _recent_rate_limits: list[tuple[datetime, str | None]] = field(
        default_factory=list,
        init=False,
    )
    _credential_failure: tuple[datetime, str | None] | None = field(
        default=None,
        init=False,
    )
    _open_order_submitted_at: dict[str, datetime] = field(
        default_factory=dict,
        init=False,
    )

    @property
    def halt_events(self) -> tuple[HaltEvent, ...]:
        return tuple(self._halt_events)

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
            and not _has_open_position(portfolio, decision)
            and len(portfolio.open_positions) >= self.risk.max_open_positions
        ):
            return RiskDecision(False, "max_open_positions")

        if decision.max_slippage_bps > self.risk.slippage_threshold_bps:
            return RiskDecision(False, "slippage_threshold_bps")

        if notional > portfolio.free_usdc:
            return RiskDecision(False, "insufficient_free_usdc")

        if self.risk.max_quantity_shares is not None and decision.limit_price > 0.0:
            estimated_quantity = notional / decision.limit_price
            if estimated_quantity > self.risk.max_quantity_shares:
                return RiskDecision(False, "max_quantity_shares")

        return RiskDecision(True, "approved")

    def check_auto_halt(
        self,
        portfolio: Portfolio,
        *,
        now: datetime | None = None,
        trace_id: str | None = None,
    ) -> HaltState:
        checked_at = _coerce_aware(now)
        if self._halt_state is not None and self._halt_state.halted:
            return self._halt_state

        if self._credential_failure is not None:
            _, credential_trace_id = self._credential_failure
            return self._halt(
                trigger_kind="credential_failure",
                reason="api_credential_failure",
                triggered_at=checked_at,
                trace_id=credential_trace_id or trace_id,
            )

        if (
            self.risk.max_drawdown_pct is not None
            and portfolio.max_drawdown_pct is not None
            and portfolio.max_drawdown_pct > self.risk.max_drawdown_pct
        ):
            return self._halt(
                trigger_kind="drawdown_circuit_breaker",
                reason="drawdown_circuit_breaker",
                triggered_at=checked_at,
                trace_id=trace_id,
            )

        if len(self._recent_trades) >= 5 and all(
            trade.pnl < 0.0 for trade in self._recent_trades[-5:]
        ):
            return self._halt(
                trigger_kind="consecutive_losses",
                reason="five_consecutive_losses",
                triggered_at=checked_at,
                trace_id=self._recent_trades[-1].trace_id or trace_id,
            )

        if len(self._recent_trades) >= 10:
            last_ten = self._recent_trades[-10:]
            avg_slippage = sum(trade.slippage_bps for trade in last_ten) / len(last_ten)
            if avg_slippage > 100.0:
                return self._halt(
                    trigger_kind="slippage_spike",
                    reason="avg_slippage_above_100bps",
                    triggered_at=checked_at,
                    trace_id=last_ten[-1].trace_id or trace_id,
                )

        self._prune_rate_limits(checked_at)
        if len(self._recent_rate_limits) >= 3:
            _, rate_limit_trace_id = self._recent_rate_limits[-1]
            return self._halt(
                trigger_kind="rate_limit_exceeded",
                reason="three_rate_limits_10m",
                triggered_at=checked_at,
                trace_id=rate_limit_trace_id or trace_id,
            )

        stale_order_id = self._stale_order_id(checked_at)
        if stale_order_id is not None:
            return self._halt(
                trigger_kind="order_without_fill",
                reason="order_without_fill_30m",
                triggered_at=checked_at,
                trace_id=trace_id,
            )

        return HaltState(
            halted=False,
            reason="ok",
            triggered_at=checked_at,
            trigger_kind="none",
        )

    def record_trade_result(self, result: RiskTradeResult) -> None:
        self._recent_trades.append(result)
        if len(self._recent_trades) > 100:
            del self._recent_trades[:-100]

    def record_api_error(
        self,
        status_code: int,
        *,
        at: datetime | None = None,
        trace_id: str | None = None,
    ) -> None:
        occurred_at = _coerce_aware(at)
        if status_code in {401, 403}:
            self._credential_failure = (occurred_at, trace_id)
        if status_code == 429:
            self._recent_rate_limits.append((occurred_at, trace_id))
            self._prune_rate_limits(occurred_at)

    def record_order_placed(self, order_id: str, *, at: datetime | None = None) -> None:
        self._open_order_submitted_at[order_id] = _coerce_aware(at)

    def record_order_filled(self, order_id: str) -> None:
        self._open_order_submitted_at.pop(order_id, None)

    def clear_halt(self) -> None:
        self._halt_state = None
        self._credential_failure = None

    def _halt(
        self,
        *,
        trigger_kind: HaltTriggerKind,
        reason: str,
        triggered_at: datetime,
        trace_id: str | None,
    ) -> HaltState:
        state = HaltState(
            halted=True,
            reason=reason,
            triggered_at=triggered_at,
            trigger_kind=trigger_kind,
        )
        self._halt_state = state
        self._halt_events.append(HaltEvent(state=state, trace_id=trace_id))
        return state

    def _prune_rate_limits(self, now: datetime) -> None:
        cutoff = now - timedelta(minutes=10)
        self._recent_rate_limits = [
            event for event in self._recent_rate_limits if event[0] >= cutoff
        ]

    def _stale_order_id(self, now: datetime) -> str | None:
        cutoff = now - timedelta(minutes=30)
        for order_id, submitted_at in self._open_order_submitted_at.items():
            if submitted_at <= cutoff:
                return order_id
        return None


def _market_exposure(portfolio: Portfolio, market_id: str) -> float:
    return sum(
        position.locked_usdc
        for position in portfolio.open_positions
        if position.market_id == market_id
    )


def _has_open_position(portfolio: Portfolio, decision: TradeDecision) -> bool:
    return any(
        position.market_id == decision.market_id
        and position.token_id == decision.token_id
        and position.venue == decision.venue
        and position.side == decision.side
        for position in portfolio.open_positions
    )


def _coerce_aware(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value
