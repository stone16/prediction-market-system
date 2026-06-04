from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
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
    current_price = _mark_price_for_position(position, signal)
    if current_price is None:
        return None
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
    limit_price = _limit_order_price(exit_signal.current_price)
    notional = float(_decimal(limit_price) * _decimal(position.shares_held))
    decision_id = f"exit-{exit_signal.trigger}-{uuid4().hex}"
    outcome = _position_outcome(position, signal)
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
        prob_estimate=exit_signal.current_price,
        expected_edge=0.0,
        time_in_force=time_in_force,
        opportunity_id=decision_id,
        strategy_id=position.strategy_id,
        strategy_version_id=position.strategy_version_id,
        limit_price=limit_price,
        outcome=outcome,
        model_id=f"position-exit:{exit_signal.trigger}",
        risk_group_id=position.risk_group_id,
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
        return float(
            _decimal(position.avg_entry_price)
            - (_decimal(position.unrealized_pnl) / _decimal(position.shares_held))
        )
    return float(
        _decimal(position.avg_entry_price)
        + (_decimal(position.unrealized_pnl) / _decimal(position.shares_held))
    )


def _limit_order_price(current_price: float) -> float:
    price = _decimal(current_price)
    return float(min(max(price, Decimal("0.001")), Decimal("0.999")))


def _mark_price_for_position(position: Position, signal: MarketSignal) -> float | None:
    if position.token_id is not None and signal.token_id is None:
        return None
    bid = _best_book_price(signal.orderbook.get("bids"), side="bid")
    ask = _best_book_price(signal.orderbook.get("asks"), side="ask")
    if (
        position.token_id is not None
        and signal.token_id is not None
        and position.token_id != signal.token_id
    ):
        position_outcome = _position_outcome_from_signal(position, signal)
        signal_outcome = _signal_token_outcome(signal)
        if position_outcome is None or signal_outcome is None:
            return None
        if position_outcome == signal_outcome:
            return None
        if position.side.upper() == "SELL":
            price = None if bid is None else float(Decimal("1") - _decimal(bid))
        else:
            price = None if ask is None else float(Decimal("1") - _decimal(ask))
        if price is not None:
            return _open_probability_or_none(price)
        signal_price = _open_probability_or_none(signal.yes_price)
        return (
            None
            if signal_price is None
            else float(Decimal("1") - _decimal(signal_price))
        )
    if position.side.upper() == "SELL":
        if ask is not None:
            return ask
    elif bid is not None:
        return bid
    return _open_probability_or_none(signal.yes_price)


def _position_outcome(position: Position, signal: MarketSignal) -> Literal["YES", "NO"]:
    return _position_outcome_from_signal(position, signal) or "YES"


def _position_outcome_from_signal(
    position: Position,
    signal: MarketSignal,
) -> Literal["YES", "NO"] | None:
    yes_token_id = _string_or_none(signal.external_signal.get("yes_token_id"))
    no_token_id = _string_or_none(signal.external_signal.get("no_token_id"))
    if position.token_id is not None:
        if position.token_id == yes_token_id:
            return "YES"
        if position.token_id == no_token_id:
            return "NO"
    hinted = _token_id_outcome_hint(position.token_id)
    if hinted is not None:
        return hinted
    if position.token_id == signal.token_id:
        return _signal_token_outcome(signal) or "YES"
    signal_outcome = _signal_token_outcome(signal)
    return None if signal_outcome is None else _opposite_outcome(signal_outcome)


def _signal_token_outcome(signal: MarketSignal) -> Literal["YES", "NO"] | None:
    yes_token_id = _string_or_none(signal.external_signal.get("yes_token_id"))
    no_token_id = _string_or_none(signal.external_signal.get("no_token_id"))
    if signal.token_id is not None:
        if signal.token_id == yes_token_id:
            return "YES"
        if signal.token_id == no_token_id:
            return "NO"
    raw_outcome = signal.external_signal.get("signal_token_outcome")
    if raw_outcome is None:
        raw_outcome = signal.external_signal.get("token_outcome")
    normalized = str(raw_outcome).upper() if raw_outcome is not None else ""
    if normalized == "YES":
        return "YES"
    if normalized == "NO":
        return "NO"
    return _token_id_outcome_hint(signal.token_id)


def _token_id_outcome_hint(token_id: str | None) -> Literal["YES", "NO"] | None:
    if token_id is None:
        return None
    normalized = token_id.lower()
    parts = (
        normalized.replace("_", "-")
        .replace(":", "-")
        .replace("/", "-")
        .split("-")
    )
    if "yes" in parts:
        return "YES"
    if "no" in parts:
        return "NO"
    return None


def _opposite_outcome(outcome: Literal["YES", "NO"]) -> Literal["YES", "NO"]:
    return "NO" if outcome == "YES" else "YES"


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _position_pnl_pct(position: Position) -> float:
    if position.locked_usdc <= 0.0:
        return 0.0
    return float(
        (_decimal(position.unrealized_pnl) / _decimal(position.locked_usdc))
        * Decimal("100")
    )


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
        return float(
            (_decimal(avg_entry_price) - _decimal(current_price))
            * _decimal(shares_held)
        )
    return float(
        (_decimal(current_price) - _decimal(avg_entry_price))
        * _decimal(shares_held)
    )


def _opposing_side(side: str) -> Literal["BUY", "SELL"]:
    return "SELL" if side.upper() == "BUY" else "BUY"


def _best_book_price(raw_levels: object, *, side: Literal["bid", "ask"]) -> float | None:
    if not isinstance(raw_levels, list):
        return None
    prices: list[Decimal] = []
    for raw_level in raw_levels:
        if not isinstance(raw_level, dict):
            continue
        size = _positive_decimal_or_none(raw_level.get("size"))
        price = _open_probability_decimal_or_none(raw_level.get("price"))
        if size is None or price is None:
            continue
        prices.append(price)
    if not prices:
        return None
    return float(max(prices) if side == "bid" else min(prices))


def _open_probability_or_none(value: object) -> float | None:
    price = _open_probability_decimal_or_none(value)
    return None if price is None else float(price)


def _open_probability_decimal_or_none(value: object) -> Decimal | None:
    parsed = _decimal_or_none(value)
    if parsed is None or parsed <= 0 or parsed >= 1:
        return None
    return parsed


def _positive_decimal_or_none(value: object) -> Decimal | None:
    parsed = _decimal_or_none(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _decimal_or_none(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _decimal(value: object) -> Decimal:
    parsed = _decimal_or_none(value)
    if parsed is None:
        return Decimal("0")
    return parsed
