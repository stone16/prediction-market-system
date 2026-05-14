from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Literal
from uuid import uuid4

from pms.config import PositionExitSettings
from pms.core.enums import TimeInForce
from pms.core.models import MarketSignal, Position, TradeDecision


ExitTrigger = Literal["stop_loss", "profit_take", "time_decay"]


@dataclass(frozen=True)
class PositionExitSignal:
    trigger: ExitTrigger
    position: Position
    pnl_pct: float
    held_days: float | None
    current_price: float


@dataclass(frozen=True)
class PositionExitMonitor:
    settings: PositionExitSettings

    def evaluate(
        self,
        position: Position,
        *,
        now: datetime,
    ) -> PositionExitSignal | None:
        if not self.settings.enabled:
            return None
        current_price = _position_current_price(position)
        if current_price is None:
            return None
        pnl_pct = _position_pnl_pct(position)
        held_days = _held_days(position, now=now)
        if (
            self.settings.stop_loss_pct is not None
            and pnl_pct <= -self.settings.stop_loss_pct
        ):
            return PositionExitSignal(
                trigger="stop_loss",
                position=position,
                pnl_pct=pnl_pct,
                held_days=held_days,
                current_price=current_price,
            )
        if (
            self.settings.profit_take_pct is not None
            and pnl_pct >= self.settings.profit_take_pct
        ):
            return PositionExitSignal(
                trigger="profit_take",
                position=position,
                pnl_pct=pnl_pct,
                held_days=held_days,
                current_price=current_price,
            )
        if (
            self.settings.max_holding_days is not None
            and held_days is not None
            and held_days > self.settings.max_holding_days
        ):
            return PositionExitSignal(
                trigger="time_decay",
                position=position,
                pnl_pct=pnl_pct,
                held_days=held_days,
                current_price=current_price,
            )
        return None


def mark_position_from_signal(
    position: Position,
    signal: MarketSignal,
) -> Position | None:
    if position.market_id != signal.market_id:
        return None
    if position.token_id is not None and signal.token_id != position.token_id:
        return None
    current_price = signal.yes_price
    pnl = _unrealized_pnl(
        side=position.side,
        shares_held=position.shares_held,
        avg_entry_price=position.avg_entry_price,
        current_price=current_price,
    )
    return replace(
        position,
        current_price=current_price,
        unrealized_pnl=pnl,
        mark_source="signal",
        mark_age_seconds=0.0,
    )


def build_exit_decision(
    signal: MarketSignal,
    exit_signal: PositionExitSignal,
    *,
    max_slippage_bps: int,
    time_in_force: TimeInForce,
) -> TradeDecision:
    position = exit_signal.position
    side = _opposing_side(position.side)
    notional = max(exit_signal.current_price * position.shares_held, 0.01)
    decision_id = f"exit-{exit_signal.trigger}-{uuid4().hex}"
    return TradeDecision(
        decision_id=decision_id,
        market_id=position.market_id,
        token_id=position.token_id,
        venue=position.venue,
        side=side,
        action=side,
        notional_usdc=notional,
        order_type="limit",
        max_slippage_bps=max_slippage_bps,
        stop_conditions=[f"position_exit:{exit_signal.trigger}"],
        prob_estimate=signal.yes_price,
        expected_edge=exit_signal.pnl_pct / 100.0,
        time_in_force=time_in_force,
        opportunity_id=decision_id,
        strategy_id=position.strategy_id,
        strategy_version_id=position.strategy_version_id,
        limit_price=exit_signal.current_price,
        model_id=f"position-exit:{exit_signal.trigger}",
    )


def exit_key(exit_signal: PositionExitSignal) -> tuple[str, str, str, str | None, str]:
    position = exit_signal.position
    return (
        position.strategy_id,
        position.strategy_version_id,
        position.market_id,
        position.token_id,
        exit_signal.trigger,
    )


def _position_current_price(position: Position) -> float | None:
    if position.current_price is not None:
        return position.current_price
    if position.shares_held <= 0.0:
        return None
    if position.side.upper() == "SELL":
        return position.avg_entry_price - (position.unrealized_pnl / position.shares_held)
    return position.avg_entry_price + (position.unrealized_pnl / position.shares_held)


def _position_pnl_pct(position: Position) -> float:
    if position.locked_usdc <= 0.0:
        return 0.0
    return (position.unrealized_pnl / position.locked_usdc) * 100.0


def _held_days(position: Position, *, now: datetime) -> float | None:
    if position.opened_at is None:
        return None
    return (now - position.opened_at).total_seconds() / 86_400.0


def _unrealized_pnl(
    *,
    side: str,
    shares_held: float,
    avg_entry_price: float,
    current_price: float,
) -> float:
    if side.upper() == "SELL":
        return (avg_entry_price - current_price) * shares_held
    return (current_price - avg_entry_price) * shares_held


def _opposing_side(side: str) -> Literal["BUY", "SELL"]:
    return "SELL" if side.upper() == "BUY" else "BUY"
