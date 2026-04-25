from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import MarketSignal, Opportunity, OrderState, Portfolio, TradeDecision
from pms.runner import ActuatorWorkItem, Runner, StrategyControllerRuntime


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.PAPER,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _runner() -> Runner:
    return Runner(
        config=_settings(),
        historical_data_path=FIXTURE_PATH,
    )


def _signal(*, market_id: str = "market-cp10") -> MarketSignal:
    return MarketSignal(
        market_id=market_id,
        token_id=f"{market_id}-yes",
        venue="polymarket",
        title=f"Will {market_id} settle YES?",
        yes_price=0.41,
        volume_24h=1500.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.40, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal={"resolved_outcome": 1.0},
        fetched_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _opportunity() -> Opportunity:
    return Opportunity(
        opportunity_id="opportunity-cp10",
        market_id="market-cp10",
        token_id="market-cp10-yes",
        side="yes",
        selected_factor_values={"edge": 0.18},
        expected_edge=0.18,
        rationale="cp10 decision",
        target_size_usdc=25.0,
        expiry=datetime(2026, 4, 23, 10, 15, tzinfo=UTC),
        staleness_policy="cp10",
        strategy_id="default",
        strategy_version_id="default-v1",
        created_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        factor_snapshot_hash="snapshot-cp10",
    )


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="decision-cp10",
        market_id="market-cp10",
        token_id="market-cp10-yes",
        venue="polymarket",
        side=Side.BUY.value,
        notional_usdc=25.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["cp10"],
        prob_estimate=0.67,
        expected_edge=0.18,
        time_in_force=TimeInForce.GTC,
        opportunity_id="opportunity-cp10",
        strategy_id="default",
        strategy_version_id="default-v1",
        limit_price=0.41,
        action=Side.BUY.value,
        model_id="model-cp10",
    )


def _matched_order(decision: TradeDecision) -> OrderState:
    now = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    return OrderState(
        order_id="order-cp10",
        decision_id=decision.decision_id,
        status=OrderStatus.MATCHED.value,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_notional_usdc=decision.notional_usdc,
        filled_notional_usdc=decision.notional_usdc,
        remaining_notional_usdc=0.0,
        fill_price=decision.limit_price,
        submitted_at=now,
        last_updated_at=now,
        raw_status="matched",
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        filled_quantity=decision.notional_usdc / decision.limit_price,
    )


class _OpportunityAwareControllerDouble:
    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[Opportunity, TradeDecision] | None:
        del signal, portfolio
        return _opportunity(), _decision()


class _OpportunityStoreDouble:
    async def insert(self, opportunity: Opportunity) -> None:
        del opportunity


class _DecisionStoreDouble:
    async def insert(
        self,
        decision: TradeDecision,
        *,
        factor_snapshot_hash: str | None,
        created_at: datetime,
        expires_at: datetime,
        status: str = "pending",
    ) -> None:
        del decision, factor_snapshot_hash, created_at, expires_at, status


class _ExecutorDouble:
    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio,
        *,
        dedup_acquired: bool = False,
    ) -> OrderState:
        del portfolio, dedup_acquired
        return _matched_order(decision)


class _FailingExecutorDouble:
    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio,
        *,
        dedup_acquired: bool = False,
    ) -> OrderState:
        del decision, portfolio, dedup_acquired
        raise RuntimeError("cp10 boom")


class _EvaluatorSpoolDouble:
    def enqueue(self, fill: Any, decision: TradeDecision) -> None:
        del fill, decision


@pytest.mark.asyncio
async def test_controller_loop_emits_sensor_signal_event() -> None:
    runner = _runner()
    forwarded: asyncio.Queue[MarketSignal] = asyncio.Queue()
    runner._controller_runtimes["default"] = StrategyControllerRuntime(  # noqa: SLF001
        strategy_id="default",
        strategy_version_id="default-v1",
        controller=cast(Any, object()),
        asset_ids=None,
    )
    runner._controller_signal_queues["default"] = forwarded  # noqa: SLF001
    runner._stop_event.set()  # noqa: SLF001
    signal = _signal()
    await runner.sensor_stream.queue.put(signal)

    await asyncio.wait_for(runner._controller_loop(), timeout=1.0)  # noqa: SLF001

    replay, subscriber = await runner.event_bus.subscribe(last_event_id=0)
    assert [item.event_type for item in replay] == ["sensor.signal"]
    assert replay[0].market_id == "market-cp10"
    assert forwarded.get_nowait().market_id == "market-cp10"
    await runner.event_bus.unsubscribe(subscriber)


@pytest.mark.asyncio
async def test_controller_pipeline_emits_controller_decision_event() -> None:
    runner = _runner()
    queue: asyncio.Queue[MarketSignal] = asyncio.Queue()
    runner._controller_runtimes["default"] = StrategyControllerRuntime(  # noqa: SLF001
        strategy_id="default",
        strategy_version_id="default-v1",
        controller=cast(Any, _OpportunityAwareControllerDouble()),
        asset_ids=None,
    )
    runner._controller_signal_queues["default"] = queue  # noqa: SLF001
    runner.opportunity_store = cast(Any, _OpportunityStoreDouble())
    runner.decision_store = cast(Any, _DecisionStoreDouble())
    runner._controller_task = asyncio.create_task(asyncio.sleep(0))  # noqa: SLF001
    await runner._controller_task
    await queue.put(_signal())

    await asyncio.wait_for(runner._controller_pipeline_loop("default"), timeout=1.0)  # noqa: SLF001

    replay, subscriber = await runner.event_bus.subscribe(last_event_id=0)
    assert [item.event_type for item in replay] == ["controller.decision"]
    assert replay[0].decision_id == "decision-cp10"
    await runner.event_bus.unsubscribe(subscriber)


@pytest.mark.asyncio
async def test_actuator_loop_emits_fill_event() -> None:
    runner = _runner()
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner._controller_task = asyncio.create_task(asyncio.sleep(0))  # noqa: SLF001
    await runner._controller_task
    runner._stop_event.set()  # noqa: SLF001
    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(_decision(), _signal())
    )

    await asyncio.wait_for(runner._actuator_loop(), timeout=1.0)  # noqa: SLF001

    replay, subscriber = await runner.event_bus.subscribe(last_event_id=0)
    assert [item.event_type for item in replay] == ["actuator.fill"]
    assert replay[0].fill_id is not None
    await runner.event_bus.unsubscribe(subscriber)


@pytest.mark.asyncio
async def test_actuator_loop_emits_error_event_when_execution_fails() -> None:
    runner = _runner()
    runner.actuator_executor = cast(Any, _FailingExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner._controller_task = asyncio.create_task(asyncio.sleep(0))  # noqa: SLF001
    await runner._controller_task
    runner._stop_event.set()  # noqa: SLF001
    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(_decision(), _signal())
    )

    await asyncio.wait_for(runner._actuator_loop(), timeout=1.0)  # noqa: SLF001

    replay, subscriber = await runner.event_bus.subscribe(last_event_id=0)
    assert [item.event_type for item in replay] == ["error"]
    assert replay[0].summary.endswith("cp10 boom")
    await runner.event_bus.unsubscribe(subscriber)


@pytest.mark.asyncio
async def test_actuator_loop_handles_malformed_work_item_without_unbound_decision() -> None:
    runner = _runner()
    runner._controller_task = asyncio.create_task(asyncio.sleep(0))  # noqa: SLF001
    await runner._controller_task
    runner._stop_event.set()  # noqa: SLF001
    malformed_queue = cast(asyncio.Queue[Any], runner._decision_queue)  # noqa: SLF001
    await malformed_queue.put(object())

    await asyncio.wait_for(runner._actuator_loop(), timeout=1.0)  # noqa: SLF001

    replay, subscriber = await runner.event_bus.subscribe(last_event_id=0)
    assert [item.event_type for item in replay] == ["error"]
    assert "malformed actuator work item" in replay[0].summary
    assert replay[0].market_id is None
    assert replay[0].decision_id is None
    await runner.event_bus.unsubscribe(subscriber)
