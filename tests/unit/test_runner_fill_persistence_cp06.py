from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from pms.actuator.adapters.polymarket import PolymarketSubmissionUnknownError
from pms.config import ControllerSettings, PMSSettings, PolymarketSettings, RiskSettings
from pms.core.enums import MarketStatus, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import MarketSignal, OrderState, Portfolio, TradeDecision
from pms.runner import ActuatorWorkItem, Runner


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


def _live_settings(tmp_path: Path) -> PMSSettings:
    return PMSSettings(
        mode=RunMode.LIVE,
        live_trading_enabled=True,
        auto_migrate_default_v2=False,
        live_emergency_audit_path=str(tmp_path / "live-emergency-audit.jsonl"),
        controller=ControllerSettings(time_in_force="IOC"),
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
        polymarket=PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0xabc",
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
        *,
        dedup_acquired: bool = False,
    ) -> OrderState:
        del portfolio, dedup_acquired
        self.calls.append(decision.market_id)
        return _matched_order(decision)


class _LegacyExecutorDouble:
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


class _RecordingOrderStore:
    def __init__(self, runner: Runner) -> None:
        self.runner = runner
        self.calls: list[str] = []

    async def insert(self, order: OrderState) -> None:
        assert self.runner.state.orders[-1] == order
        self.calls.append(order.market_id)


class _FlakyOrderStore(_RecordingOrderStore):
    def __init__(self, runner: Runner) -> None:
        super().__init__(runner)
        self.fail_first = True

    async def insert(self, order: OrderState) -> None:
        assert self.runner.state.orders[-1] == order
        self.calls.append(order.market_id)
        if self.fail_first:
            self.fail_first = False
            raise RuntimeError("order store down")


class _AlwaysFailOrderStore(_RecordingOrderStore):
    async def insert(self, order: OrderState) -> None:
        assert self.runner.state.orders[-1] == order
        self.calls.append(order.market_id)
        raise RuntimeError("order store down")


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


class _AlwaysFailFillStore(_RecordingFillStore):
    async def insert(self, fill: Any) -> None:
        assert self.runner.state.fills[-1] == fill
        self.calls.append(fill.market_id)
        raise RuntimeError("fill store down")


class _SubmissionUnknownExecutorDouble:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio,
        *,
        dedup_acquired: bool = False,
    ) -> OrderState:
        del portfolio, dedup_acquired
        self.calls.append(decision.market_id)
        raise PolymarketSubmissionUnknownError(
            "venue timeout",
            order_state=_matched_order(decision),
        )


class _DecisionStatusStore:
    def __init__(self) -> None:
        self.transitions: list[tuple[str, str, str]] = []

    async def update_status(
        self,
        decision_id: str,
        *,
        current_status: str,
        next_status: str,
        updated_at: datetime,
    ) -> bool:
        del updated_at
        self.transitions.append((decision_id, current_status, next_status))
        return True


def _mark_controller_done(runner: Runner) -> None:
    runner._controller_task = asyncio.create_task(asyncio.sleep(0))  # noqa: SLF001


async def _run_actuator_loop(runner: Runner) -> None:
    await asyncio.wait_for(runner._actuator_loop(), timeout=1.0)  # noqa: SLF001


@pytest.mark.asyncio
async def test_actuator_loop_persists_fill_after_appending_runner_state() -> None:
    runner = _runner()
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.order_store = cast(Any, _RecordingOrderStore(runner))
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    _mark_controller_done(runner)

    decision = _decision(market_id="market-cp06-a")
    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(decision, _signal(market_id="market-cp06-a"))
    )

    await _run_actuator_loop(runner)

    assert [order.market_id for order in runner.state.orders] == ["market-cp06-a"]
    assert cast(_RecordingOrderStore, runner.order_store).calls == ["market-cp06-a"]
    assert [fill.market_id for fill in runner.state.fills] == ["market-cp06-a"]
    assert cast(_RecordingFillStore, runner.fill_store).calls == ["market-cp06-a"]
    assert runner.portfolio.locked_usdc == pytest.approx(20.5)
    assert cast(_EvaluatorSpoolDouble, runner._evaluator_spool).calls == [
        ("market-cp06-a", "decision-market-cp06-a")
    ]


@pytest.mark.asyncio
async def test_actuator_loop_logs_order_store_failures_and_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _runner()
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.order_store = cast(Any, _FlakyOrderStore(runner))
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    _mark_controller_done(runner)

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-cp06-a"),
            _signal(market_id="market-cp06-a"),
        )
    )
    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-cp06-b"),
            _signal(market_id="market-cp06-b"),
        )
    )

    caplog.set_level(logging.WARNING, logger="pms.runner")

    await _run_actuator_loop(runner)

    assert [order.market_id for order in runner.state.orders] == [
        "market-cp06-a",
        "market-cp06-b",
    ]
    assert cast(_FlakyOrderStore, runner.order_store).calls == [
        "market-cp06-a",
        "market-cp06-b",
    ]
    assert [fill.market_id for fill in runner.state.fills] == [
        "market-cp06-a",
        "market-cp06-b",
    ]
    assert cast(_RecordingFillStore, runner.fill_store).calls == [
        "market-cp06-a",
        "market-cp06-b",
    ]
    assert runner.portfolio.locked_usdc == pytest.approx(41.0)
    assert cast(_EvaluatorSpoolDouble, runner._evaluator_spool).calls == [
        ("market-cp06-a", "decision-market-cp06-a"),
        ("market-cp06-b", "decision-market-cp06-b"),
    ]
    assert "order persistence failed" in caplog.text


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
        ActuatorWorkItem(
            _decision(market_id="market-cp06-a"),
            _signal(market_id="market-cp06-a"),
        )
    )
    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-cp06-b"),
            _signal(market_id="market-cp06-b"),
        )
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


@pytest.mark.asyncio
async def test_actuator_loop_supports_legacy_executor_without_dedup_kwarg() -> None:
    runner = _runner()
    runner.actuator_executor = cast(Any, _LegacyExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    _mark_controller_done(runner)

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-cp06-legacy"),
            _signal(market_id="market-cp06-legacy"),
            dedup_acquired=True,
        )
    )

    await _run_actuator_loop(runner)

    assert [fill.market_id for fill in runner.state.fills] == ["market-cp06-legacy"]
    assert cast(_LegacyExecutorDouble, runner.actuator_executor).calls == [
        "market-cp06-legacy"
    ]


@pytest.mark.asyncio
async def test_live_order_persistence_failure_suspends_trading(
    tmp_path: Path,
) -> None:
    runner = Runner(config=_live_settings(tmp_path))
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.order_store = cast(Any, _AlwaysFailOrderStore(runner))
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    _mark_controller_done(runner)

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-live-order-fail"),
            _signal(market_id="market-live-order-fail"),
        )
    )

    with pytest.raises(RuntimeError, match="LIVE persistence failure"):
        await _run_actuator_loop(runner)

    assert runner.live_trading_suspended is True
    assert runner._stop_event.is_set()  # noqa: SLF001
    assert runner.portfolio.locked_usdc == pytest.approx(0.0)
    assert cast(_RecordingFillStore, runner.fill_store).calls == []
    audit_rows = [
        json.loads(line)
        for line in (tmp_path / "live-emergency-audit.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    assert audit_rows[0]["phase"] == "order_state"
    assert audit_rows[0]["decision_id"] == "decision-market-live-order-fail"
    assert audit_rows[0]["order_id"] == "order-market-live-order-fail"


@pytest.mark.asyncio
async def test_live_fill_persistence_failure_suspends_trading(
    tmp_path: Path,
) -> None:
    runner = Runner(config=_live_settings(tmp_path))
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.order_store = cast(Any, _RecordingOrderStore(runner))
    runner.fill_store = cast(Any, _AlwaysFailFillStore(runner))
    _mark_controller_done(runner)

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-live-fill-fail"),
            _signal(market_id="market-live-fill-fail"),
        )
    )

    with pytest.raises(RuntimeError, match="LIVE persistence failure"):
        await _run_actuator_loop(runner)

    assert runner.live_trading_suspended is True
    assert runner._stop_event.is_set()  # noqa: SLF001
    assert runner.portfolio.locked_usdc == pytest.approx(0.0)
    assert cast(_EvaluatorSpoolDouble, runner._evaluator_spool).calls == []
    audit_rows = [
        json.loads(line)
        for line in (tmp_path / "live-emergency-audit.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    assert audit_rows[0]["phase"] == "fill"
    assert audit_rows[0]["decision_id"] == "decision-market-live-fill-fail"
    assert audit_rows[0]["order_id"] == "order-market-live-fill-fail"


@pytest.mark.asyncio
async def test_submission_unknown_persistence_failure_hard_halts(
    tmp_path: Path,
) -> None:
    runner = Runner(config=_live_settings(tmp_path))
    runner.actuator_executor = cast(Any, _SubmissionUnknownExecutorDouble())
    runner.order_store = cast(Any, _AlwaysFailOrderStore(runner))
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    _mark_controller_done(runner)

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-submission-unknown-store-fail"),
            _signal(market_id="market-submission-unknown-store-fail"),
        )
    )

    with pytest.raises(RuntimeError, match="LIVE persistence failure"):
        await _run_actuator_loop(runner)

    assert runner.live_trading_suspended is True
    assert runner._stop_event.is_set()  # noqa: SLF001
    audit_rows = [
        json.loads(line)
        for line in (tmp_path / "live-emergency-audit.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    assert audit_rows[0]["phase"] == "submission_unknown_order_state"
    assert audit_rows[0]["decision_id"] == (
        "decision-market-submission-unknown-store-fail"
    )


@pytest.mark.asyncio
async def test_actuator_loop_advances_decision_status_through_filled() -> None:
    runner = _runner()
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.order_store = cast(Any, _RecordingOrderStore(runner))
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    runner.decision_store = cast(Any, _DecisionStatusStore())
    _mark_controller_done(runner)

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-decision-status"),
            _signal(market_id="market-decision-status"),
        )
    )

    await _run_actuator_loop(runner)

    assert cast(_DecisionStatusStore, runner.decision_store).transitions == [
        ("decision-market-decision-status", "queued", "submitted"),
        ("decision-market-decision-status", "submitted", "filled"),
    ]
