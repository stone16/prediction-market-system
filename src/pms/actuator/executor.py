from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import InsufficientLiquidityError, RiskManager
from pms.core.enums import OrderStatus
from pms.core.models import OrderState, Portfolio, TradeDecision


class ActuatorAdapter(Protocol):
    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState: ...


@dataclass
class DedupTokenStore:
    _tokens: set[str] = field(default_factory=set)

    def acquire(self, token: str) -> bool:
        if token in self._tokens:
            return False
        self._tokens.add(token)
        return True

    def release(self, token: str) -> None:
        self._tokens.discard(token)

    def contains(self, token: str) -> bool:
        return token in self._tokens


@dataclass(frozen=True)
class ActuatorExecutor:
    adapter: ActuatorAdapter
    risk: RiskManager
    feedback: ActuatorFeedback
    dedup_tokens: DedupTokenStore = field(default_factory=DedupTokenStore)

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio,
    ) -> OrderState:
        acquired = self.dedup_tokens.acquire(decision.decision_id)
        if not acquired:
            state = _rejected_order_state(decision, "duplicate_decision")
            self.feedback.generate(state, reason="duplicate_decision")
            return state

        try:
            risk_decision = self.risk.check(decision, portfolio)
            if not risk_decision.approved:
                state = _rejected_order_state(decision, risk_decision.reason)
                self.feedback.generate(state, reason=risk_decision.reason)
                return state

            try:
                return await self.adapter.execute(decision, portfolio)
            except InsufficientLiquidityError:
                state = _rejected_order_state(decision, "insufficient_liquidity")
                self.feedback.generate(state, reason="insufficient_liquidity")
                raise
            except Exception:
                state = _rejected_order_state(decision, "venue_rejection")
                self.feedback.generate(state, reason="venue_rejection")
                raise
        finally:
            self.dedup_tokens.release(decision.decision_id)


def _rejected_order_state(decision: TradeDecision, reason: str) -> OrderState:
    now = datetime.now(tz=UTC)
    return OrderState(
        order_id=f"rejected-{uuid4().hex}",
        decision_id=decision.decision_id,
        status=OrderStatus.INVALID.value,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_size=decision.size,
        filled_size=0.0,
        remaining_size=decision.size,
        fill_price=None,
        submitted_at=now,
        last_updated_at=now,
        raw_status=reason,
    )
