"""Single-leg execution planner for strategy intents."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from pms.core.enums import Side, TimeInForce
from pms.core.models import BookSide, Outcome, Venue
from pms.execution.quotes import ExecutableQuote, QuoteProvider
from pms.strategies.intents import TradeIntent


@dataclass(frozen=True, slots=True)
class PlannedOrder:
    planned_order_id: str
    intent_id: str
    intent_key: str
    market_id: str
    token_id: str
    venue: Venue
    side: BookSide
    outcome: Outcome
    notional_usdc: float
    limit_price: float
    expected_edge: float
    max_slippage_bps: int
    time_in_force: TimeInForce
    strategy_id: str
    strategy_version_id: str
    quote_hash: str


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    plan_id: str
    intent_id: str
    strategy_id: str
    strategy_version_id: str
    quote_hash: str | None
    planned_orders: tuple[PlannedOrder, ...]
    rejection_reason: str | None
    audit_metadata: Mapping[str, Any]
    created_at: datetime
    execution_policy: str | None = None
    leg_rejection_reasons: Mapping[str, str] = field(default_factory=dict)
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.rejection_reason is None and not self.planned_orders:
            msg = "accepted ExecutionPlan requires planned_orders"
            raise ValueError(msg)
        if self.rejection_reason is not None and self.planned_orders:
            msg = "rejected ExecutionPlan must not include planned_orders"
            raise ValueError(msg)

    @classmethod
    def rejected(
        cls,
        *,
        intent: TradeIntent,
        reason: str,
        quote_hash: str | None,
        audit_metadata: Mapping[str, Any],
        created_at: datetime,
        evidence_refs: tuple[str, ...] = (),
    ) -> ExecutionPlan:
        return cls(
            plan_id=f"plan-{intent.intent_id}-rejected-{reason}",
            intent_id=intent.intent_id,
            strategy_id=intent.strategy_id,
            strategy_version_id=intent.strategy_version_id,
            quote_hash=quote_hash,
            planned_orders=(),
            rejection_reason=reason,
            audit_metadata=audit_metadata,
            created_at=created_at,
            evidence_refs=evidence_refs or intent.evidence_refs,
        )


@dataclass(frozen=True, slots=True)
class ExecutionPlanner:
    quote_provider: QuoteProvider
    max_book_age_s: float = 30.0

    async def plan(self, intent: TradeIntent, *, as_of: datetime) -> ExecutionPlan:
        quote = await self.quote_provider.quote_for_intent(intent)
        if quote is None:
            return self._reject(intent, "quote_unavailable", None, {}, as_of)

        reason = self._rejection_reason(intent, quote, as_of)
        if reason is not None:
            return self._reject(
                intent,
                reason,
                quote.quote_hash,
                self._audit_metadata(intent, quote, reason=reason),
                as_of,
            )

        edge_after_cost = _edge_after_cost(intent, quote)
        order = PlannedOrder(
            planned_order_id=f"planned-{intent.intent_id}-{quote.quote_hash[:12]}",
            intent_id=intent.intent_id,
            intent_key=intent.intent_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            venue=intent.venue,
            side=intent.side,
            outcome=intent.outcome,
            notional_usdc=intent.notional_usdc,
            limit_price=intent.limit_price,
            expected_edge=edge_after_cost,
            max_slippage_bps=intent.max_slippage_bps,
            time_in_force=intent.time_in_force,
            strategy_id=intent.strategy_id,
            strategy_version_id=intent.strategy_version_id,
            quote_hash=quote.quote_hash,
        )
        return ExecutionPlan(
            plan_id=f"plan-{intent.intent_id}-{quote.quote_hash[:12]}",
            intent_id=intent.intent_id,
            strategy_id=intent.strategy_id,
            strategy_version_id=intent.strategy_version_id,
            quote_hash=quote.quote_hash,
            planned_orders=(order,),
            rejection_reason=None,
            audit_metadata=self._audit_metadata(
                intent,
                quote,
                reason=None,
                edge_after_cost=edge_after_cost,
            ),
            created_at=as_of,
            evidence_refs=intent.evidence_refs,
        )

    def _rejection_reason(
        self,
        intent: TradeIntent,
        quote: ExecutableQuote,
        as_of: datetime,
    ) -> str | None:
        if (as_of - quote.book_timestamp).total_seconds() > self.max_book_age_s:
            return "stale_book"
        if not quote.token_id or quote.token_id != intent.token_id:
            return "missing_token_id"
        if quote.best_price <= 0.0 or quote.best_price >= 1.0:
            return "impossible_price"
        if quote.tick_size <= 0.0 or _decimal(quote.best_price) % _decimal(quote.tick_size) != 0:
            return "invalid_tick_size"
        if intent.notional_usdc < quote.min_order_size_usdc:
            return "min_size_violation"
        if quote.executable_notional_usdc < intent.notional_usdc:
            return "insufficient_executable_notional"
        if _violates_limit(intent, quote):
            return "limit_price_not_executable"
        if _edge_after_cost(intent, quote) <= 0.0:
            return "loss_of_edge_after_costs"
        return None

    def _reject(
        self,
        intent: TradeIntent,
        reason: str,
        quote_hash: str | None,
        audit_metadata: Mapping[str, Any],
        created_at: datetime,
    ) -> ExecutionPlan:
        return ExecutionPlan.rejected(
            intent=intent,
            reason=reason,
            quote_hash=quote_hash,
            audit_metadata=audit_metadata,
            created_at=created_at,
        )

    def _audit_metadata(
        self,
        intent: TradeIntent,
        quote: ExecutableQuote,
        *,
        reason: str | None,
        edge_after_cost: float | None = None,
    ) -> Mapping[str, Any]:
        return {
            "reason": reason,
            "quote_price": quote.best_price,
            "quote_available_size": quote.available_size,
            "executable_notional_usdc": quote.executable_notional_usdc,
            "min_order_size_usdc": quote.min_order_size_usdc,
            "tick_size": quote.tick_size,
            "max_slippage_bps": intent.max_slippage_bps,
            "fee_bps": quote.fee_bps,
            "edge_after_cost": edge_after_cost
            if edge_after_cost is not None
            else _edge_after_cost(intent, quote),
        }


def _decimal(value: float) -> Decimal:
    return Decimal(str(value))


def _edge_after_cost(intent: TradeIntent, quote: ExecutableQuote) -> float:
    bps = Decimal(str(intent.max_slippage_bps + quote.fee_bps)) / Decimal("10000")
    price = _decimal(quote.best_price)
    cost = price * bps
    if intent.side == Side.BUY.value:
        effective_price = price + cost
        edge = _decimal(intent.expected_price) - effective_price
    else:
        effective_price = price - cost
        edge = effective_price - _decimal(intent.expected_price)
    return float(edge)


def _violates_limit(intent: TradeIntent, quote: ExecutableQuote) -> bool:
    if intent.side == Side.BUY.value:
        return quote.best_price > intent.limit_price
    return quote.best_price < intent.limit_price
