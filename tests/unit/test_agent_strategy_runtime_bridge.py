from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from pms.artifacts.models import StrategyExecutionArtifact, StrategyJudgementArtifact
from pms.config import PMSSettings
from pms.core.enums import RunMode, TimeInForce
from pms.core.models import TradeDecision
from pms.execution.planner import ExecutionPlan, PlannedOrder
from pms.runner import Runner
from pms.strategies.intents import (
    BasketIntent,
    StrategyContext,
    StrategyJudgement,
    TradeIntent,
)
from pms.strategies.registry import StrategyModuleRegistry
from pms.strategies.runtime_bridge import AgentStrategyRuntimeBridge, StrategyRunResult


NOW = datetime(2026, 4, 28, 13, 0, tzinfo=UTC)


class _FakeModule:
    strategy_id = "ripple"
    strategy_version_id = "ripple-v1"

    def __init__(self, results: Sequence[StrategyRunResult]) -> None:
        self.results = tuple(results)
        self.contexts: list[StrategyContext] = []

    async def run(
        self,
        context: StrategyContext,
    ) -> Sequence[TradeIntent | BasketIntent]:
        self.contexts.append(context)
        return tuple(intent for result in self.results for intent in result.intents)

    async def run_with_artifacts(
        self,
        context: StrategyContext,
    ) -> Sequence[StrategyRunResult]:
        self.contexts.append(context)
        return self.results


class _FakePlanner:
    def __init__(self, plans: dict[str, ExecutionPlan]) -> None:
        self.plans = plans
        self.calls: list[TradeIntent] = []

    async def plan(self, intent: TradeIntent, *, as_of: datetime) -> ExecutionPlan:
        assert as_of == NOW
        self.calls.append(intent)
        return self.plans[intent.intent_id]


class _RecordingArtifactStore:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.judgement_artifacts: list[StrategyJudgementArtifact] = []
        self.execution_artifacts: list[StrategyExecutionArtifact] = []

    async def insert_judgement_artifact(
        self,
        artifact: StrategyJudgementArtifact,
    ) -> None:
        self.events.append(f"judgement:{artifact.artifact_type}")
        self.judgement_artifacts.append(artifact)

    async def insert_execution_artifact(
        self,
        artifact: StrategyExecutionArtifact,
    ) -> None:
        self.events.append(f"execution:{artifact.artifact_type}")
        self.execution_artifacts.append(artifact)


def _settings(*, enabled: bool) -> PMSSettings:
    return PMSSettings(
        mode=RunMode.BACKTEST,
        auto_migrate_default_v2=False,
        agent_strategy_runtime_enabled=enabled,
    )


def _intent(**overrides: object) -> TradeIntent:
    data: dict[str, object] = {
        "intent_id": "intent-ripple-1",
        "strategy_id": "ripple",
        "strategy_version_id": "ripple-v1",
        "candidate_id": "candidate-ripple-1",
        "market_id": "market-ripple-1",
        "token_id": "token-ripple-yes",
        "venue": "polymarket",
        "side": "BUY",
        "outcome": "YES",
        "limit_price": 0.57,
        "notional_usdc": 25.0,
        "expected_price": 0.66,
        "expected_edge": 0.09,
        "max_slippage_bps": 40,
        "time_in_force": TimeInForce.GTC,
        "evidence_refs": ("judgement-ripple-1",),
        "created_at": NOW,
    }
    data.update(overrides)
    return TradeIntent(**cast(Any, data))


def _judgement(**overrides: object) -> StrategyJudgement:
    data: dict[str, object] = {
        "judgement_id": "judgement-ripple-1",
        "candidate_id": "candidate-ripple-1",
        "strategy_id": "ripple",
        "strategy_version_id": "ripple-v1",
        "approved": True,
        "confidence": 0.74,
        "rationale": "approved deterministic fixture",
        "evidence_refs": ("doc://ripple/a", "doc://ripple/b"),
        "failure_reasons": (),
        "created_at": NOW,
    }
    data.update(overrides)
    return StrategyJudgement(**cast(Any, data))


def _planned_order(**overrides: object) -> PlannedOrder:
    data: dict[str, object] = {
        "planned_order_id": "planned-ripple-1",
        "intent_id": "intent-ripple-1",
        "intent_key": "intent-ripple-1",
        "market_id": "market-ripple-1",
        "token_id": "token-ripple-yes",
        "venue": "polymarket",
        "side": "BUY",
        "outcome": "YES",
        "notional_usdc": 25.0,
        "limit_price": 0.57,
        "expected_edge": 0.08,
        "max_slippage_bps": 40,
        "time_in_force": TimeInForce.GTC,
        "strategy_id": "ripple",
        "strategy_version_id": "ripple-v1",
        "quote_hash": "quote-ripple-1",
    }
    data.update(overrides)
    return PlannedOrder(**cast(Any, data))


def _accepted_plan(intent: TradeIntent) -> ExecutionPlan:
    return ExecutionPlan(
        plan_id="plan-ripple-accepted",
        intent_id=intent.intent_id,
        strategy_id=intent.strategy_id,
        strategy_version_id=intent.strategy_version_id,
        quote_hash="quote-ripple-1",
        planned_orders=(_planned_order(intent_id=intent.intent_id),),
        rejection_reason=None,
        audit_metadata={"edge_after_cost": 0.08},
        created_at=NOW,
        evidence_refs=intent.evidence_refs,
    )


def _rejected_plan(intent: TradeIntent) -> ExecutionPlan:
    return ExecutionPlan.rejected(
        intent=intent,
        reason="stale_book",
        quote_hash="quote-ripple-1",
        audit_metadata={"reason": "stale_book"},
        created_at=NOW,
    )


def _bridge(
    *,
    settings: PMSSettings,
    module: _FakeModule | None,
    planner: _FakePlanner,
    artifact_store: _RecordingArtifactStore,
    enqueue: Callable[[TradeDecision], Awaitable[None]],
) -> AgentStrategyRuntimeBridge:
    registry = StrategyModuleRegistry([module] if module is not None else [])
    return AgentStrategyRuntimeBridge(
        settings=settings,
        registry=registry,
        planner=planner,
        artifact_store=artifact_store,
        enqueue_decision=enqueue,
    )


def test_agent_strategy_runtime_setting_defaults_disabled() -> None:
    assert PMSSettings().agent_strategy_runtime_enabled is False


@pytest.mark.asyncio
async def test_bridge_rejects_when_disabled() -> None:
    intent = _intent()
    artifacts = _RecordingArtifactStore()
    runner = Runner(config=_settings(enabled=False))
    bridge = _bridge(
        settings=runner.config,
        module=_FakeModule([StrategyRunResult(judgement=_judgement(), intents=(intent,))]),
        planner=_FakePlanner({intent.intent_id: _accepted_plan(intent)}),
        artifact_store=artifacts,
        enqueue=runner.enqueue_accepted_decision,
    )

    with pytest.raises(RuntimeError, match="agent strategy runtime is disabled"):
        await bridge.run_once(
            strategy_id="ripple",
            strategy_version_id="ripple-v1",
            as_of=NOW,
        )

    assert runner._decision_queue.empty()  # noqa: SLF001
    assert artifacts.events == []


@pytest.mark.asyncio
async def test_bridge_persists_artifacts_before_enqueueing_accepted_decision() -> None:
    intent = _intent()
    judgement = _judgement()
    artifacts = _RecordingArtifactStore()
    runner = Runner(config=_settings(enabled=True))

    async def enqueue(decision: TradeDecision) -> None:
        artifacts.events.append("enqueue")
        await runner.enqueue_accepted_decision(decision)

    bridge = _bridge(
        settings=runner.config,
        module=_FakeModule([StrategyRunResult(judgement=judgement, intents=(intent,))]),
        planner=_FakePlanner({intent.intent_id: _accepted_plan(intent)}),
        artifact_store=artifacts,
        enqueue=enqueue,
    )

    report = await bridge.run_once(
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        as_of=NOW,
    )

    assert report.enqueued_decision_ids == ("decision-planned-ripple-1",)
    assert artifacts.events == [
        "judgement:approved_intent",
        "execution:accepted_execution_plan",
        "enqueue",
    ]
    work_item = runner._decision_queue.get_nowait()  # noqa: SLF001
    assert work_item.decision.decision_id == "decision-planned-ripple-1"
    assert work_item.decision.market_id == "market-ripple-1"
    assert work_item.decision.strategy_id == "ripple"
    assert work_item.decision.strategy_version_id == "ripple-v1"
    assert runner.state.orders == []


@pytest.mark.asyncio
async def test_bridge_persists_rejected_judgement_without_enqueue() -> None:
    judgement = _judgement(
        approved=False,
        confidence=0.25,
        failure_reasons=("insufficient_evidence",),
    )
    artifacts = _RecordingArtifactStore()
    runner = Runner(config=_settings(enabled=True))
    planner = _FakePlanner({})
    bridge = _bridge(
        settings=runner.config,
        module=_FakeModule([StrategyRunResult(judgement=judgement, intents=())]),
        planner=planner,
        artifact_store=artifacts,
        enqueue=runner.enqueue_accepted_decision,
    )

    report = await bridge.run_once(
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        as_of=NOW,
    )

    assert report.enqueued_decision_ids == ()
    assert planner.calls == []
    assert artifacts.events == ["judgement:rejected_candidate"]
    assert artifacts.judgement_artifacts[0].rejection_reasons == (
        "insufficient_evidence",
    )
    assert runner._decision_queue.empty()  # noqa: SLF001


@pytest.mark.asyncio
async def test_bridge_persists_planner_rejection_without_enqueue() -> None:
    intent = _intent()
    artifacts = _RecordingArtifactStore()
    runner = Runner(config=_settings(enabled=True))
    bridge = _bridge(
        settings=runner.config,
        module=_FakeModule([StrategyRunResult(judgement=_judgement(), intents=(intent,))]),
        planner=_FakePlanner({intent.intent_id: _rejected_plan(intent)}),
        artifact_store=artifacts,
        enqueue=runner.enqueue_accepted_decision,
    )

    report = await bridge.run_once(
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        as_of=NOW,
    )

    assert report.enqueued_decision_ids == ()
    assert artifacts.events == [
        "judgement:approved_intent",
        "execution:rejected_execution_plan",
    ]
    assert artifacts.execution_artifacts[0].rejection_reasons == ("stale_book",)
    assert runner._decision_queue.empty()  # noqa: SLF001


@pytest.mark.asyncio
async def test_bridge_persists_unknown_strategy_version_without_enqueue() -> None:
    artifacts = _RecordingArtifactStore()
    runner = Runner(config=_settings(enabled=True))
    bridge = _bridge(
        settings=runner.config,
        module=None,
        planner=_FakePlanner({}),
        artifact_store=artifacts,
        enqueue=runner.enqueue_accepted_decision,
    )

    report = await bridge.run_once(
        strategy_id="missing",
        strategy_version_id="missing-v1",
        as_of=NOW,
    )

    assert report.enqueued_decision_ids == ()
    assert artifacts.events == ["execution:rejected_execution_plan"]
    assert artifacts.execution_artifacts[0].rejection_reasons == (
        "unknown_strategy_version",
    )
    assert runner._decision_queue.empty()  # noqa: SLF001
