from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import MarketSignal, OrderState, Portfolio, TradeDecision
from pms.runner import Runner


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


def _signal(*, market_id: str) -> MarketSignal:
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


def _decision(*, market_id: str, notional_usdc: float = 20.5) -> TradeDecision:
    return TradeDecision(
        decision_id=f"decision-{market_id}",
        market_id=market_id,
        token_id=f"{market_id}-yes",
        venue="polymarket",
        side=Side.BUY.value,
        notional_usdc=notional_usdc,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["cp06"],
        prob_estimate=0.7,
        expected_edge=0.2,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"opportunity-{market_id}",
        strategy_id="default",
        strategy_version_id="default-v1",
        limit_price=0.41,
        action=Side.BUY.value,
        model_id="model-a",
    )


def _matched_order(decision: TradeDecision) -> OrderState:
    now = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    return OrderState(
        order_id=f"order-{decision.market_id}",
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


class _ExecutorDouble:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio,
    ) -> OrderState:
        del portfolio
        self.calls.append(decision.market_id)
        return _matched_order(decision)


class _EvaluatorSpoolDouble:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def enqueue(self, fill: Any, decision: TradeDecision) -> None:
        self.calls.append((fill.market_id, decision.decision_id))


class _RecordingFillStore:
    def __init__(self, runner: Runner) -> None:
        self.runner = runner
        self.calls: list[str] = []

    async def insert(self, fill: Any) -> None:
        assert self.runner.state.fills[-1] == fill
        self.calls.append(fill.market_id)


class _FlakyFillStore(_RecordingFillStore):
    def __init__(self, runner: Runner) -> None:
        super().__init__(runner)
        self.fail_first = True

    async def insert(self, fill: Any) -> None:
        assert self.runner.state.fills[-1] == fill
        self.calls.append(fill.market_id)
        if self.fail_first:
            self.fail_first = False
            raise RuntimeError("fill store down")


def _mark_controller_done(runner: Runner) -> None:
    future = asyncio.get_running_loop().create_future()
    future.set_result(None)
    runner._controller_task = future  # noqa: SLF001


async def _run_actuator_loop(runner: Runner) -> None:
    await asyncio.wait_for(runner._actuator_loop(), timeout=1.0)  # noqa: SLF001


@pytest.mark.asyncio
async def test_actuator_loop_persists_fill_after_appending_runner_state() -> None:
    runner = _runner()
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    _mark_controller_done(runner)

    decision = _decision(market_id="market-cp06-a")
    await runner._decision_queue.put((decision, _signal(market_id="market-cp06-a")))  # noqa: SLF001

    await _run_actuator_loop(runner)

    assert [fill.market_id for fill in runner.state.fills] == ["market-cp06-a"]
    assert cast(_RecordingFillStore, runner.fill_store).calls == ["market-cp06-a"]
    assert runner.portfolio.locked_usdc == pytest.approx(20.5)
    assert cast(_EvaluatorSpoolDouble, runner._evaluator_spool).calls == [
        ("market-cp06-a", "decision-market-cp06-a")
    ]


@pytest.mark.asyncio
async def test_actuator_loop_logs_fill_store_failures_and_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _runner()
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.fill_store = cast(Any, _FlakyFillStore(runner))
    _mark_controller_done(runner)

    await runner._decision_queue.put(  # noqa: SLF001
        (_decision(market_id="market-cp06-a"), _signal(market_id="market-cp06-a"))
    )
    await runner._decision_queue.put(  # noqa: SLF001
        (_decision(market_id="market-cp06-b"), _signal(market_id="market-cp06-b"))
    )

    caplog.set_level(logging.WARNING, logger="pms.runner")

    await _run_actuator_loop(runner)

    assert [fill.market_id for fill in runner.state.fills] == [
        "market-cp06-a",
        "market-cp06-b",
    ]
    assert cast(_FlakyFillStore, runner.fill_store).calls == [
        "market-cp06-a",
        "market-cp06-b",
    ]
    assert runner.portfolio.locked_usdc == pytest.approx(41.0)
    assert cast(_EvaluatorSpoolDouble, runner._evaluator_spool).calls == [
        ("market-cp06-a", "decision-market-cp06-a"),
        ("market-cp06-b", "decision-market-cp06-b"),
    ]
    assert "fill persistence failed" in caplog.text
