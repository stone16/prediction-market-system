"""Disabled-by-default bridge from strategy plugins to the decision queue."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, cast

from pms.artifacts.models import (
    JudgementArtifactType,
    StrategyExecutionArtifact,
    StrategyJudgementArtifact,
)
from pms.config import PMSSettings
from pms.core.models import TradeDecision
from pms.execution.planner import ExecutionPlan, PlannedOrder
from pms.strategies.base import StrategyModule
from pms.strategies.intents import (
    BasketIntent,
    StrategyContext,
    StrategyJudgement,
    TradeIntent,
)
from pms.strategies.registry import StrategyModuleRegistry


DecisionEnqueuer = Callable[[TradeDecision], Awaitable[None]]


class StrategyArtifactWriter(Protocol):
    async def insert_judgement_artifact(
        self,
        artifact: StrategyJudgementArtifact,
    ) -> None: ...

    async def insert_execution_artifact(
        self,
        artifact: StrategyExecutionArtifact,
    ) -> None: ...


class TradeIntentPlanner(Protocol):
    async def plan(self, intent: TradeIntent, *, as_of: datetime) -> ExecutionPlan: ...


@dataclass(frozen=True, slots=True)
class StrategyRunResult:
    judgement: StrategyJudgement | None
    intents: tuple[TradeIntent | BasketIntent, ...] = ()


@dataclass(frozen=True, slots=True)
class AgentStrategyBridgeReport:
    strategy_id: str
    strategy_version_id: str
    enqueued_decision_ids: tuple[str, ...]
    rejection_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AgentStrategyRuntimeBridge:
    settings: PMSSettings
    registry: StrategyModuleRegistry
    planner: TradeIntentPlanner
    artifact_store: StrategyArtifactWriter
    enqueue_decision: DecisionEnqueuer

    async def run_once(
        self,
        *,
        strategy_id: str,
        strategy_version_id: str,
        as_of: datetime,
    ) -> AgentStrategyBridgeReport:
        if not self.settings.agent_strategy_runtime_enabled:
            msg = "agent strategy runtime is disabled"
            raise RuntimeError(msg)

        module = self.registry.get(strategy_id, strategy_version_id)
        if module is None:
            await self.artifact_store.insert_execution_artifact(
                _unknown_strategy_artifact(
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    as_of=as_of,
                )
            )
            return AgentStrategyBridgeReport(
                strategy_id=strategy_id,
                strategy_version_id=strategy_version_id,
                enqueued_decision_ids=(),
                rejection_reasons=("unknown_strategy_version",),
            )

        context = StrategyContext(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            as_of=as_of,
        )
        enqueued_decision_ids: list[str] = []
        rejection_reasons: list[str] = []
        for result in await _run_module_with_artifacts(module, context):
            if result.judgement is not None:
                await self.artifact_store.insert_judgement_artifact(
                    _judgement_artifact(result.judgement, result.intents)
                )
                if not result.judgement.approved:
                    rejection_reasons.extend(result.judgement.failure_reasons)
                    continue

            for intent in result.intents:
                if isinstance(intent, BasketIntent):
                    await self.artifact_store.insert_execution_artifact(
                        _unsupported_basket_artifact(intent, as_of=as_of)
                    )
                    rejection_reasons.append("basket_runtime_not_supported")
                    continue

                plan = await self.planner.plan(intent, as_of=as_of)
                if plan.rejection_reason is not None:
                    await self.artifact_store.insert_execution_artifact(
                        _execution_artifact(intent, plan)
                    )
                    rejection_reasons.append(plan.rejection_reason)
                    continue
                if len(plan.planned_orders) > 1:
                    reason = "single_leg_plan_multiple_orders"
                    await self.artifact_store.insert_execution_artifact(
                        _bridge_rejection_artifact(
                            intent,
                            plan,
                            rejection_reason=reason,
                            as_of=as_of,
                        )
                    )
                    rejection_reasons.append(reason)
                    continue
                await self.artifact_store.insert_execution_artifact(
                    _execution_artifact(intent, plan)
                )
                for order in plan.planned_orders:
                    decision = _decision_from_order(order, intent)
                    await self.enqueue_decision(decision)
                    enqueued_decision_ids.append(decision.decision_id)

        return AgentStrategyBridgeReport(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            enqueued_decision_ids=tuple(enqueued_decision_ids),
            rejection_reasons=tuple(rejection_reasons),
        )


async def _run_module_with_artifacts(
    module: StrategyModule,
    context: StrategyContext,
) -> Sequence[StrategyRunResult]:
    run_with_artifacts = getattr(module, "run_with_artifacts", None)
    if callable(run_with_artifacts):
        runner = cast(
            Callable[[StrategyContext], Awaitable[Sequence[StrategyRunResult]]],
            run_with_artifacts,
        )
        return tuple(await runner(context))
    intents = await module.run(context)
    return (StrategyRunResult(judgement=None, intents=tuple(intents)),)


def _judgement_artifact(
    judgement: StrategyJudgement,
    intents: tuple[TradeIntent | BasketIntent, ...],
) -> StrategyJudgementArtifact:
    executable_intents = tuple(
        intent for intent in intents if isinstance(intent, TradeIntent)
    )
    unsupported_baskets = tuple(
        intent for intent in intents if isinstance(intent, BasketIntent)
    )
    artifact_type: JudgementArtifactType = (
        "approved_intent" if judgement.approved else "rejected_candidate"
    )
    rejection_reasons = judgement.failure_reasons
    if judgement.approved and not executable_intents and unsupported_baskets:
        artifact_type = "rejected_candidate"
        rejection_reasons = ("basket_runtime_not_supported",)
    intent_payload: Mapping[str, Any] = (
        {"intents": [_intent_payload(intent) for intent in executable_intents]}
        if artifact_type == "approved_intent"
        else {}
    )
    return StrategyJudgementArtifact(
        artifact_id=f"artifact-{judgement.judgement_id}",
        strategy_id=judgement.strategy_id,
        strategy_version_id=judgement.strategy_version_id,
        artifact_type=artifact_type,
        observation_refs=judgement.evidence_refs,
        candidate_id=judgement.candidate_id,
        judgement_id=judgement.judgement_id,
        judgement_summary=judgement.rationale,
        evidence_refs=judgement.evidence_refs or ("strategy_judgement",),
        assumptions=(),
        rejection_reasons=rejection_reasons,
        intent_payload=intent_payload,
        created_at=judgement.created_at,
    )


def _execution_artifact(
    intent: TradeIntent,
    plan: ExecutionPlan,
) -> StrategyExecutionArtifact:
    rejected = plan.rejection_reason is not None
    return StrategyExecutionArtifact(
        artifact_id=f"artifact-{plan.plan_id}",
        strategy_id=plan.strategy_id,
        strategy_version_id=plan.strategy_version_id,
        artifact_type="rejected_execution_plan" if rejected else "accepted_execution_plan",
        intent_id=intent.intent_id,
        plan_id=plan.plan_id,
        execution_policy=plan.execution_policy,
        execution_plan_payload=_plan_payload(plan),
        risk_decision_payload={},
        venue_response_ids=(),
        reconciliation_status=None,
        post_trade_status=None,
        evidence_refs=plan.evidence_refs or intent.evidence_refs or ("execution_planner",),
        rejection_reasons=(plan.rejection_reason,) if plan.rejection_reason else (),
        created_at=plan.created_at,
    )


def _bridge_rejection_artifact(
    intent: TradeIntent,
    plan: ExecutionPlan,
    *,
    rejection_reason: str,
    as_of: datetime,
) -> StrategyExecutionArtifact:
    payload = {
        **_plan_payload(plan),
        "bridge_rejection_reason": rejection_reason,
    }
    return StrategyExecutionArtifact(
        artifact_id=f"artifact-{plan.plan_id}-bridge-rejected-{rejection_reason}",
        strategy_id=plan.strategy_id,
        strategy_version_id=plan.strategy_version_id,
        artifact_type="rejected_execution_plan",
        intent_id=intent.intent_id,
        plan_id=plan.plan_id,
        execution_policy=plan.execution_policy,
        execution_plan_payload=payload,
        risk_decision_payload={},
        venue_response_ids=(),
        reconciliation_status=None,
        post_trade_status=None,
        evidence_refs=plan.evidence_refs or intent.evidence_refs or ("runtime_bridge",),
        rejection_reasons=(rejection_reason,),
        created_at=as_of,
    )


def _unknown_strategy_artifact(
    *,
    strategy_id: str,
    strategy_version_id: str,
    as_of: datetime,
) -> StrategyExecutionArtifact:
    return StrategyExecutionArtifact(
        artifact_id=f"artifact-unknown-{strategy_id}-{strategy_version_id}",
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        artifact_type="rejected_execution_plan",
        intent_id=f"strategy-{strategy_id}",
        plan_id=f"plan-unknown-{strategy_id}-{strategy_version_id}",
        execution_policy=None,
        execution_plan_payload={"rejection_reason": "unknown_strategy_version"},
        risk_decision_payload={},
        venue_response_ids=(),
        reconciliation_status=None,
        post_trade_status=None,
        evidence_refs=("strategy_registry",),
        rejection_reasons=("unknown_strategy_version",),
        created_at=as_of,
    )


def _unsupported_basket_artifact(
    intent: BasketIntent,
    *,
    as_of: datetime,
) -> StrategyExecutionArtifact:
    return StrategyExecutionArtifact(
        artifact_id=f"artifact-basket-runtime-not-supported-{intent.basket_id}",
        strategy_id=intent.strategy_id,
        strategy_version_id=intent.strategy_version_id,
        artifact_type="rejected_execution_plan",
        intent_id=intent.basket_id,
        plan_id=f"plan-basket-runtime-not-supported-{intent.basket_id}",
        execution_policy=intent.execution_policy,
        execution_plan_payload={"rejection_reason": "basket_runtime_not_supported"},
        risk_decision_payload={},
        venue_response_ids=(),
        reconciliation_status=None,
        post_trade_status=None,
        evidence_refs=intent.evidence_refs,
        rejection_reasons=("basket_runtime_not_supported",),
        created_at=as_of,
    )


def _decision_from_order(order: PlannedOrder, intent: TradeIntent) -> TradeDecision:
    return TradeDecision(
        decision_id=f"decision-{order.planned_order_id}",
        market_id=order.market_id,
        token_id=order.token_id,
        venue=order.venue,
        side=order.side,
        notional_usdc=order.notional_usdc,
        order_type="limit",
        max_slippage_bps=order.max_slippage_bps,
        stop_conditions=["agent_strategy_runtime"],
        prob_estimate=intent.expected_price,
        expected_edge=order.expected_edge,
        time_in_force=order.time_in_force,
        opportunity_id=f"agent-{intent.candidate_id}",
        strategy_id=order.strategy_id,
        strategy_version_id=order.strategy_version_id,
        limit_price=order.limit_price,
        action=order.side,
        outcome=order.outcome,
        model_id=f"agent-strategy:{intent.strategy_id}",
        intent_key=order.intent_key,
    )


def _intent_payload(intent: TradeIntent | BasketIntent) -> Mapping[str, Any]:
    if isinstance(intent, BasketIntent):
        return {
            "basket_id": intent.basket_id,
            "execution_policy": intent.execution_policy,
            "leg_count": len(intent.legs),
            "evidence_refs": list(intent.evidence_refs),
        }
    return {
        "intent_id": intent.intent_id,
        "candidate_id": intent.candidate_id,
        "market_id": intent.market_id,
        "token_id": intent.token_id,
        "venue": intent.venue,
        "side": intent.side,
        "outcome": intent.outcome,
        "limit_price": intent.limit_price,
        "notional_usdc": intent.notional_usdc,
        "expected_price": intent.expected_price,
        "expected_edge": intent.expected_edge,
        "max_slippage_bps": intent.max_slippage_bps,
        "time_in_force": intent.time_in_force.value,
        "evidence_refs": list(intent.evidence_refs),
    }


def _plan_payload(plan: ExecutionPlan) -> Mapping[str, Any]:
    return {
        "plan_id": plan.plan_id,
        "intent_id": plan.intent_id,
        "quote_hash": plan.quote_hash,
        "rejection_reason": plan.rejection_reason,
        "audit_metadata": dict(plan.audit_metadata),
        "planned_orders": [_planned_order_payload(order) for order in plan.planned_orders],
        "leg_rejection_reasons": dict(plan.leg_rejection_reasons),
    }


def _planned_order_payload(order: PlannedOrder) -> Mapping[str, Any]:
    return {
        "planned_order_id": order.planned_order_id,
        "intent_id": order.intent_id,
        "intent_key": order.intent_key,
        "market_id": order.market_id,
        "token_id": order.token_id,
        "venue": order.venue,
        "side": order.side,
        "outcome": order.outcome,
        "notional_usdc": order.notional_usdc,
        "limit_price": order.limit_price,
        "expected_edge": order.expected_edge,
        "max_slippage_bps": order.max_slippage_bps,
        "time_in_force": order.time_in_force.value,
        "strategy_id": order.strategy_id,
        "strategy_version_id": order.strategy_version_id,
        "quote_hash": order.quote_hash,
    }
