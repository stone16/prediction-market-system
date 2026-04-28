"""Basket execution-policy planning."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from typing import Any

from pms.execution.planner import ExecutionPlan, ExecutionPlanner
from pms.strategies.intents import BasketIntent


EXECUTABLE_BASKET_POLICIES = frozenset({"manual_review", "all_or_none"})


@dataclass(frozen=True, slots=True)
class BasketExecutionPlanner:
    single_leg_planner: ExecutionPlanner
    executable_policies: frozenset[str] = field(
        default_factory=lambda: EXECUTABLE_BASKET_POLICIES
    )

    async def plan(self, basket: BasketIntent, *, as_of: datetime) -> ExecutionPlan:
        if basket.execution_policy not in self.executable_policies:
            return _rejected_basket_plan(
                basket,
                reason="unsupported_basket_policy",
                leg_reasons={"__basket__": "unsupported_basket_policy"},
                as_of=as_of,
                audit_metadata={"execution_policy": basket.execution_policy},
            )

        leg_plans = [
            await self.single_leg_planner.plan(leg, as_of=as_of) for leg in basket.legs
        ]
        leg_reasons = {
            plan.intent_id: plan.rejection_reason
            for plan in leg_plans
            if plan.rejection_reason is not None
        }
        if leg_reasons:
            return _rejected_basket_plan(
                basket,
                reason="basket_leg_rejected",
                leg_reasons=leg_reasons,
                as_of=as_of,
                audit_metadata={
                    "execution_policy": basket.execution_policy,
                    "leg_count": len(basket.legs),
                },
            )

        planned_orders = tuple(
            order for plan in leg_plans for order in plan.planned_orders
        )
        quote_hashes = tuple(
            plan.quote_hash for plan in leg_plans if plan.quote_hash is not None
        )
        quote_hash = _basket_quote_hash(quote_hashes)
        return ExecutionPlan(
            plan_id=f"plan-{basket.basket_id}-{quote_hash[:12]}",
            intent_id=basket.basket_id,
            strategy_id=basket.strategy_id,
            strategy_version_id=basket.strategy_version_id,
            quote_hash=quote_hash,
            planned_orders=planned_orders,
            rejection_reason=None,
            audit_metadata={
                "execution_policy": basket.execution_policy,
                "leg_count": len(basket.legs),
                "quote_hashes": quote_hashes,
            },
            created_at=as_of,
            execution_policy=basket.execution_policy,
            leg_rejection_reasons={},
            evidence_refs=basket.evidence_refs,
        )


def _rejected_basket_plan(
    basket: BasketIntent,
    *,
    reason: str,
    leg_reasons: dict[str, str],
    as_of: datetime,
    audit_metadata: dict[str, Any],
) -> ExecutionPlan:
    return ExecutionPlan(
        plan_id=f"plan-{basket.basket_id}-rejected-{reason}",
        intent_id=basket.basket_id,
        strategy_id=basket.strategy_id,
        strategy_version_id=basket.strategy_version_id,
        quote_hash=None,
        planned_orders=(),
        rejection_reason=reason,
        audit_metadata=audit_metadata,
        created_at=as_of,
        execution_policy=basket.execution_policy,
        leg_rejection_reasons=leg_reasons,
        evidence_refs=basket.evidence_refs,
    )


def _basket_quote_hash(quote_hashes: tuple[str, ...]) -> str:
    payload = "|".join(quote_hashes)
    return sha256(payload.encode("utf-8")).hexdigest()
