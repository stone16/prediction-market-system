from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast
from uuid import uuid4

from pms.actuator.risk import InsufficientLiquidityError
from pms.core.enums import OrderStatus, Side, Venue
from pms.core.exceptions import KalshiStubError
from pms.core.models import MarketSignal, OrderState, Portfolio, TradeDecision
from pms.core.venue_support import kalshi_stub_error
from pms.research.specs import ExecutionModel


@dataclass(frozen=True)
class BacktestExecutionSimulator:
    async def execute(
        self,
        *,
        signal: MarketSignal,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
        execution_model: ExecutionModel,
    ) -> OrderState:
        del portfolio
        if signal.venue == Venue.KALSHI.value:
            raise kalshi_stub_error("BacktestExecutionSimulator.execute(signal)")
        if decision.venue == Venue.KALSHI.value:
            raise kalshi_stub_error("BacktestExecutionSimulator.execute(decision)")
        submitted_at = signal.fetched_at + timedelta(milliseconds=execution_model.latency_ms)
        if signal.resolves_at is not None and submitted_at >= signal.resolves_at:
            return _unfilled_order_state(
                decision,
                submitted_at=submitted_at,
                status=OrderStatus.CANCELED_MARKET_RESOLVED.value,
                raw_status="market_resolved_before_execution",
            )
        if (
            not math.isinf(execution_model.staleness_ms)
            and execution_model.latency_ms > execution_model.staleness_ms
        ):
            return _unfilled_order_state(
                decision,
                submitted_at=submitted_at,
                status=OrderStatus.CANCELED.value,
                raw_status="stale_signal",
            )
        market_price = _best_market_price(signal.orderbook, decision)
        fill_price = _apply_slippage(
            market_price,
            action=_action(decision),
            slippage_bps=execution_model.slippage_bps,
        )
        if execution_model.fill_policy == "limit_if_touched" and not _is_touched(
            action=_action(decision),
            fill_price=fill_price,
            limit_price=_limit_price(decision),
        ):
            return _unfilled_order_state(
                decision,
                submitted_at=submitted_at,
                status=OrderStatus.UNMATCHED.value,
                raw_status="limit_not_touched",
            )
        return _matched_order_state(
            decision,
            fill_price=fill_price,
            submitted_at=submitted_at,
        )


def _best_market_price(orderbook: dict[str, Any], decision: TradeDecision) -> float:
    if decision.outcome == "NO":
        side_key = "bids" if _action(decision) == Side.BUY.value else "asks"
    else:
        side_key = "asks" if _action(decision) == Side.BUY.value else "bids"
    levels = orderbook.get(side_key)
    if not isinstance(levels, list) or not levels:
        raise InsufficientLiquidityError(f"{side_key} depth is empty")
    best = levels[0]
    if not isinstance(best, dict):
        raise InsufficientLiquidityError(f"{side_key} depth is invalid")
    available_size = float(cast(str | int | float, best.get("size", 0.0)))
    if available_size <= 0.0:
        raise InsufficientLiquidityError(f"{side_key} depth is empty")
    if available_size < decision.size:
        raise InsufficientLiquidityError(f"{side_key} depth is insufficient")
    best_price = float(cast(str | int | float, best["price"]))
    if decision.outcome == "NO":
        return 1.0 - best_price
    return best_price


def _apply_slippage(price: float, *, action: str, slippage_bps: float) -> float:
    multiplier = slippage_bps / 10_000.0
    if action == Side.SELL.value:
        return max(0.0, price * (1.0 - multiplier))
    return min(1.0, price * (1.0 + multiplier))


def _is_touched(*, action: str, fill_price: float, limit_price: float) -> bool:
    if action == Side.SELL.value:
        return fill_price >= limit_price
    return fill_price <= limit_price


def _matched_order_state(
    decision: TradeDecision,
    *,
    fill_price: float,
    submitted_at: datetime,
) -> OrderState:
    return OrderState(
        order_id=f"backtest-sim-{uuid4().hex}",
        decision_id=decision.decision_id,
        status=OrderStatus.MATCHED.value,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_size=decision.size,
        filled_size=decision.size,
        remaining_size=0.0,
        fill_price=fill_price,
        submitted_at=submitted_at,
        last_updated_at=submitted_at,
        raw_status="matched",
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
    )


def _unfilled_order_state(
    decision: TradeDecision,
    *,
    submitted_at: datetime,
    status: str,
    raw_status: str,
) -> OrderState:
    return OrderState(
        order_id=f"backtest-sim-{uuid4().hex}",
        decision_id=decision.decision_id,
        status=status,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_size=decision.size,
        filled_size=0.0,
        remaining_size=decision.size,
        fill_price=None,
        submitted_at=submitted_at,
        last_updated_at=submitted_at,
        raw_status=raw_status,
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
    )


def _action(decision: TradeDecision) -> str:
    return decision.action if decision.action is not None else decision.side


def _limit_price(decision: TradeDecision) -> float:
    return decision.limit_price if decision.limit_price is not None else decision.price
