from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from pms.actuator.adapters.polymarket import PolymarketSubmissionUnknownError
from pms.config import (
    ControllerSettings,
    PMSSettings,
    PolymarketSettings,
    PositionExitSettings,
    RiskSettings,
)
from pms.core.enums import MarketStatus, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import FillRecord, MarketSignal, OrderState, Portfolio, Position
from pms.core.models import TradeDecision
from pms.runner import ActuatorWorkItem, Runner, _portfolio_with_fill


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


def _settings_with_exit_reentry_cooldown(*, cooldown_s: float) -> PMSSettings:
    settings = _settings()
    settings.position_exit = PositionExitSettings(
        enabled=True,
        stop_loss_pct=30.0,
        profit_take_pct=50.0,
        max_holding_days=7,
        reentry_cooldown_s=cooldown_s,
    )
    return settings


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
            funder_address="0x1111111111111111111111111111111111111111",
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


def test_portfolio_with_fill_keeps_strategy_versions_separate() -> None:
    portfolio = Portfolio(
        total_usdc=1_000.0,
        free_usdc=950.0,
        locked_usdc=50.0,
        open_positions=[
            Position(
                market_id="market-strategy-tags",
                token_id="token-strategy-tags",
                venue="polymarket",
                side="BUY",
                shares_held=100.0,
                avg_entry_price=0.5,
                unrealized_pnl=0.0,
                locked_usdc=50.0,
                strategy_id="strategy-a",
                strategy_version_id="strategy-a-v1",
            )
        ],
    )
    fill = FillRecord(
        trade_id="trade-strategy-tags",
        fill_id="fill-strategy-tags",
        order_id="order-strategy-tags",
        decision_id="decision-strategy-tags",
        market_id="market-strategy-tags",
        token_id="token-strategy-tags",
        venue="polymarket",
        side="BUY",
        fill_price=0.25,
        fill_notional_usdc=25.0,
        fill_quantity=100.0,
        executed_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        filled_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        status="filled",
        anomaly_flags=[],
        strategy_id="strategy-b",
        strategy_version_id="strategy-b-v1",
    )

    updated = _portfolio_with_fill(portfolio, fill)

    assert len(updated.open_positions) == 2
    assert {position.strategy_id for position in updated.open_positions} == {
        "strategy-a",
        "strategy-b",
    }


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
        self.decision_evidence: dict[str, object] | None = None

    def enqueue(
        self,
        fill: Any,
        decision: TradeDecision,
        *,
        decision_evidence: dict[str, object] | None = None,
    ) -> None:
        self.calls.append((fill.market_id, decision.decision_id))
        self.decision_evidence = decision_evidence


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


class _SecretFailOrderStore(_RecordingOrderStore):
    def __init__(self, runner: Runner, message: str) -> None:
        super().__init__(runner)
        self.message = message

    async def insert(self, order: OrderState) -> None:
        assert self.runner.state.orders[-1] == order
        self.calls.append(order.market_id)
        raise RuntimeError(self.message)


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


class _RejectingExecutorDouble:
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
        now = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
        return OrderState(
            order_id=f"order-{decision.market_id}",
            decision_id=decision.decision_id,
            status="rejected",
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            requested_notional_usdc=decision.notional_usdc,
            filled_notional_usdc=0.0,
            remaining_notional_usdc=decision.notional_usdc,
            fill_price=None,
            submitted_at=now,
            last_updated_at=now,
            raw_status="venue_rejection",
            strategy_id=decision.strategy_id,
            strategy_version_id=decision.strategy_version_id,
            filled_quantity=0.0,
        )


class _FailingExecutorDouble:
    def __init__(self, message: str) -> None:
        self.message = message
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
        raise ConnectionError(self.message)


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
    evidence = cast(_EvaluatorSpoolDouble, runner._evaluator_spool).decision_evidence
    assert evidence is not None
    assert evidence["mid_quote_baseline_prob_estimate"] == pytest.approx(0.405)


@pytest.mark.asyncio
async def test_paper_actuator_uses_queued_target_token_orderbook_snapshot() -> None:
    runner = _runner()
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.order_store = cast(Any, _RecordingOrderStore(runner))
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    _mark_controller_done(runner)
    market_id = "market-paper-snapshot"
    no_token_id = f"{market_id}-no"
    yes_signal = _signal(market_id=market_id)
    no_signal = replace(
        _signal(market_id=market_id),
        token_id=no_token_id,
        orderbook={
            "bids": [{"price": 0.40, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
    )
    decision = replace(
        _decision(market_id=market_id),
        token_id=no_token_id,
        outcome="NO",
        limit_price=0.41,
    )
    runner._remember_signal_for_decision_evidence(no_signal)  # noqa: SLF001
    runner._remember_paper_orderbook(no_signal)  # noqa: SLF001

    enqueued = await runner._enqueue_decision(  # noqa: SLF001
        decision,
        signal=yes_signal,
        queued_at=yes_signal.fetched_at,
    )
    runner._paper_orderbooks[no_token_id] = {  # noqa: SLF001
        "bids": [{"price": 0.40, "size": 100.0}],
        "asks": [{"price": 0.45, "size": 100.0}],
    }

    await _run_actuator_loop(runner)

    assert enqueued is True
    assert [fill.fill_price for fill in runner.state.fills] == [pytest.approx(0.41)]
    assert runner.state.orders[0].status == OrderStatus.MATCHED.value


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
async def test_actuator_loop_releases_position_exit_key_after_rejected_order() -> None:
    runner = _runner()
    runner.actuator_executor = cast(Any, _RejectingExecutorDouble())
    _mark_controller_done(runner)
    decision = _decision(market_id="market-exit-retry")
    key = (
        decision.strategy_id,
        decision.strategy_version_id,
        decision.market_id,
        decision.token_id,
        "stop_loss",
    )
    runner._emitted_position_exit_keys.add(key)  # noqa: SLF001
    runner._position_exit_keys_by_decision[decision.decision_id] = key  # noqa: SLF001

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(decision, _signal(market_id=decision.market_id))
    )

    await _run_actuator_loop(runner)

    assert key not in runner._emitted_position_exit_keys  # noqa: SLF001
    assert decision.decision_id not in runner._position_exit_keys_by_decision  # noqa: SLF001


@pytest.mark.asyncio
async def test_actuator_loop_quarantines_reentry_after_position_exit_fill() -> None:
    runner = Runner(
        config=_settings_with_exit_reentry_cooldown(cooldown_s=1_800.0),
        historical_data_path=FIXTURE_PATH,
    )
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.order_store = cast(Any, _RecordingOrderStore(runner))
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    _mark_controller_done(runner)

    market_id = "market-exit-reentry"
    exit_decision = replace(
        _decision(market_id=market_id),
        decision_id=f"exit-stop_loss-{market_id}",
        side=Side.SELL.value,
        action=Side.SELL.value,
        stop_conditions=["position_exit:stop_loss"],
        risk_group_id="event:exit-reentry",
    )
    signal = _signal(market_id=market_id)
    runner.portfolio = Portfolio(
        total_usdc=1_000.0,
        free_usdc=979.5,
        locked_usdc=20.5,
        open_positions=[
            Position(
                market_id=market_id,
                token_id=exit_decision.token_id,
                venue="polymarket",
                side=Side.BUY.value,
                shares_held=50.0,
                avg_entry_price=0.41,
                unrealized_pnl=0.0,
                locked_usdc=20.5,
                strategy_id=exit_decision.strategy_id,
                strategy_version_id=exit_decision.strategy_version_id,
                risk_group_id=exit_decision.risk_group_id,
            )
        ],
    )

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(exit_decision, signal)
    )

    await _run_actuator_loop(runner)

    next_entry = replace(
        _decision(market_id=market_id),
        risk_group_id="event:exit-reentry",
    )
    diagnostic = runner._pre_actuator_diagnostic(next_entry, signal)  # noqa: SLF001

    assert diagnostic is not None
    assert diagnostic.code == "position_exit_reentry_cooldown"
    assert diagnostic.metadata["cooldown_s"] == pytest.approx(1_800.0)
    assert diagnostic.metadata["seconds_remaining"] == pytest.approx(1_800.0)

    runner.portfolio = Portfolio(
        total_usdc=1_000.0,
        free_usdc=999.0,
        locked_usdc=1.0,
        open_positions=[
            Position(
                market_id=market_id,
                token_id=next_entry.token_id,
                venue="polymarket",
                side=Side.BUY.value,
                shares_held=2.0,
                avg_entry_price=0.50,
                unrealized_pnl=0.0,
                locked_usdc=1.0,
                strategy_id=next_entry.strategy_id,
                strategy_version_id=next_entry.strategy_version_id,
                risk_group_id=next_entry.risk_group_id,
            )
        ],
    )
    reducing_decision = replace(
        next_entry,
        decision_id="manual-risk-reducing-exit",
        side=Side.SELL.value,
        action=Side.SELL.value,
    )
    assert runner._pre_actuator_diagnostic(  # noqa: SLF001
        reducing_decision,
        signal,
    ) is None
    runner.portfolio = Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )

    key = (
        next_entry.strategy_id,
        next_entry.strategy_version_id,
        next_entry.market_id,
        next_entry.token_id,
        next_entry.risk_group_id,
    )
    runner._position_exit_reentry_quarantine_until[key] = (  # noqa: SLF001
        signal.fetched_at - timedelta(seconds=1)
    )

    assert runner._pre_actuator_diagnostic(next_entry, signal) is None  # noqa: SLF001


def test_pre_actuator_diagnostic_rejects_unfillable_paper_orderbook() -> None:
    runner = _runner()
    market_id = "thin-paper-book"
    signal = _signal(market_id=market_id)
    decision = _decision(market_id=market_id, notional_usdc=20.5)
    runner._paper_orderbooks[decision.token_id or decision.market_id] = {  # noqa: SLF001
        "bids": [{"price": 0.40, "size": 100.0}],
        "asks": [{"price": 0.41, "size": 1.0}],
    }

    diagnostic = runner._pre_actuator_diagnostic(decision, signal)  # noqa: SLF001

    assert diagnostic is not None
    assert diagnostic.code == "paper_orderbook_insufficient_liquidity"
    assert diagnostic.metadata["decision_notional_usdc"] == pytest.approx(20.5)
    assert "executable depth is insufficient" in str(diagnostic.metadata["failure"])


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
async def test_live_persistence_failure_redacts_credentials_in_exception_event_and_audit(
    tmp_path: Path,
) -> None:
    credential_values = (
        "private-key-secret",
        "api-key-secret",
        "api-secret-secret",
        "passphrase-secret",
        "0x2222222222222222222222222222222222222222",
    )
    secret_dsn = "postgresql://admin:supersecret@db.internal.example.com:5432/pms_live"
    settings = _live_settings(tmp_path).model_copy(
        update={
            "polymarket": PolymarketSettings(
                private_key=credential_values[0],
                api_key=credential_values[1],
                api_secret=credential_values[2],
                api_passphrase=credential_values[3],
                signature_type=1,
                funder_address=credential_values[4],
            )
        }
    )
    runner = Runner(config=settings)
    runner.actuator_executor = cast(Any, _ExecutorDouble())
    runner._evaluator_spool = cast(Any, _EvaluatorSpoolDouble())  # noqa: SLF001
    runner.order_store = cast(
        Any,
        _SecretFailOrderStore(
            runner,
            "order store failed "
            f"{credential_values[0]} {credential_values[1]} "
            f"{credential_values[2]} {credential_values[3]} {credential_values[4]} "
            f"{secret_dsn} password=keyword-secret",
        ),
    )
    runner.fill_store = cast(Any, _RecordingFillStore(runner))
    _mark_controller_done(runner)
    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-live-secret-persistence-fail"),
            _signal(market_id="market-live-secret-persistence-fail"),
        )
    )

    with pytest.raises(RuntimeError) as exc_info:
        await _run_actuator_loop(runner)

    replay, subscriber = await runner.event_bus.subscribe(last_event_id=0)
    await runner.event_bus.unsubscribe(subscriber)
    audit_rows = [
        json.loads(line)
        for line in (tmp_path / "live-emergency-audit.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]

    rendered = (
        str(exc_info.value)
        + "\n"
        + "\n".join(event.summary for event in replay)
        + "\n"
        + json.dumps(audit_rows, sort_keys=True)
    )
    assert "LIVE persistence failure during order_state" in rendered
    assert "<redacted-polymarket-credential>" in rendered
    assert "<redacted-database-url>" in rendered
    assert "password=<redacted>" in rendered
    for credential in credential_values:
        assert credential not in rendered
    assert "supersecret" not in rendered
    assert "keyword-secret" not in rendered
    assert "admin" not in rendered


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
async def test_live_actuator_execution_failure_redacts_credentials_in_logs_and_events(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    credential_values = (
        "private-key-secret",
        "api-key-secret",
        "api-secret-secret",
        "passphrase-secret",
        "0x2222222222222222222222222222222222222222",
    )
    secret_dsn = "postgresql://admin:supersecret@db.internal.example.com:5432/pms_live"
    settings = _live_settings(tmp_path).model_copy(
        update={
            "polymarket": PolymarketSettings(
                private_key=credential_values[0],
                api_key=credential_values[1],
                api_secret=credential_values[2],
                api_passphrase=credential_values[3],
                signature_type=1,
                funder_address=credential_values[4],
            )
        }
    )
    runner = Runner(config=settings)
    runner.actuator_executor = cast(
        Any,
        _FailingExecutorDouble(
            "venue submit failed "
            f"{credential_values[0]} {credential_values[1]} "
            f"{credential_values[2]} {credential_values[3]} {credential_values[4]} "
            f"{secret_dsn} password=keyword-secret"
        ),
    )
    _mark_controller_done(runner)
    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(
            _decision(market_id="market-live-actuator-fail"),
            _signal(market_id="market-live-actuator-fail"),
        )
    )

    caplog.set_level(logging.WARNING, logger="pms.runner")

    await _run_actuator_loop(runner)
    replay, subscriber = await runner.event_bus.subscribe(last_event_id=0)
    await runner.event_bus.unsubscribe(subscriber)

    rendered = caplog.text + "\n".join(event.summary for event in replay)
    assert "actuator execution failed" in rendered
    assert "<redacted-polymarket-credential>" in rendered
    assert "<redacted-database-url>" in rendered
    assert "password=<redacted>" in rendered
    for credential in credential_values:
        assert credential not in rendered
    assert "supersecret" not in rendered
    assert "keyword-secret" not in rendered
    assert "admin" not in rendered


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
