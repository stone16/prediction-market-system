from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Literal, cast

import pytest

from pms.actuator.adapters.polymarket import (
    LiveOrderPreview,
    PolymarketActuator,
    PolymarketOrderResult,
    PolymarketSubmissionUnknownError,
)
from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import (
    ControllerSettings,
    LLMSettings,
    PMSSettings,
    PolymarketSettings,
    RiskSettings,
    validate_live_mode_ready,
)
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.factor_snapshot import FactorSnapshot
from pms.controller.forecasters.llm import LLMForecaster
from pms.controller.pipeline import ControllerPipeline
from pms.core.enums import MarketStatus, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import (
    LiveTradingDisabledError,
    MarketSignal,
    OrderState,
    Portfolio,
    TradeDecision,
)
from pms.factors.composition import evaluate_branch_probabilities
from pms.runner import ActuatorWorkItem, Runner
from pms.storage.dedup_store import InMemoryDedupStore
from pms.storage.feedback_store import FeedbackStore
from pms.strategies.defaults import DEFAULT_STRATEGY_COMPOSITION
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from tests.support.fake_stores import InMemoryFeedbackStore


def _signal(
    *,
    yes_price: float = 0.10,
    external_signal: dict[str, Any] | None = None,
) -> MarketSignal:
    return MarketSignal(
        market_id="m-live-blocker",
        token_id="t-yes",
        venue="polymarket",
        title="Will live blocker tests pass?",
        yes_price=yes_price,
        volume_24h=10_000.0,
        resolves_at=datetime(2026, 5, 1, tzinfo=UTC),
        orderbook={
            "bids": [{"price": yes_price - 0.01, "size": 100.0}],
            "asks": [{"price": yes_price + 0.01, "size": 100.0}],
        },
        external_signal=external_signal or {},
        fetched_at=datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _active_strategy(
    *,
    composition: Sequence[FactorCompositionStep] = DEFAULT_STRATEGY_COMPOSITION,
) -> ActiveStrategy:
    return ActiveStrategy(
        strategy_id="default",
        strategy_version_id="default-live-blockers",
        config=StrategyConfig(
            strategy_id="default",
            factor_composition=tuple(composition),
            metadata=(("owner", "system"),),
        ),
        risk=RiskParams(
            max_position_notional_usdc=1_000.0,
            max_daily_drawdown_pct=0.0,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl")),
        forecaster=ForecasterSpec(
            forecasters=(("rules", ()), ("stats", ()), ("llm", ())),
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=30,
            volume_min_usdc=0.0,
        ),
    )


class ConstantForecaster:
    def __init__(self, probability: float, rationale: str = "constant") -> None:
        self.probability = probability
        self.rationale = rationale

    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return self.probability, 0.0, self.rationale

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return self.probability


class FixedSizer:
    def size(self, *, prob: float, market_price: float, portfolio: Portfolio) -> float:
        del prob, market_price, portfolio
        return 10.0


@dataclass(frozen=True)
class SnapshotReader:
    snapshot_value: FactorSnapshot

    async def snapshot(
        self,
        *,
        market_id: str,
        as_of: datetime,
        required: Sequence[FactorCompositionStep],
        strategy_id: str,
        strategy_version_id: str,
    ) -> FactorSnapshot:
        del market_id, as_of, required, strategy_id, strategy_version_id
        return self.snapshot_value


@pytest.mark.asyncio
async def test_default_strategy_does_not_trade_when_required_raw_factors_missing() -> None:
    pipeline = ControllerPipeline(
        strategy=_active_strategy(),
        factor_reader=SnapshotReader(
            FactorSnapshot(
                values={},
                missing_factors=(
                    ("fair_value_spread", ""),
                    ("metaculus_prior", ""),
                    ("yes_count", ""),
                    ("no_count", ""),
                ),
                snapshot_hash="missing-raw",
            )
        ),
        forecasters=(ConstantForecaster(0.10), ConstantForecaster(0.10)),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        settings=PMSSettings(
            mode=RunMode.LIVE,
            controller=ControllerSettings(min_volume=0.0),
            risk=RiskSettings(min_order_usdc=1.0),
        ),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    assert pipeline.last_diagnostic is not None
    assert pipeline.last_diagnostic.code == "missing_required_factors"


@pytest.mark.asyncio
async def test_strategy_does_not_trade_when_required_raw_factor_is_stale() -> None:
    pipeline = ControllerPipeline(
        strategy=_active_strategy(),
        factor_reader=SnapshotReader(
            FactorSnapshot(
                values={("metaculus_prior", ""): 0.8},
                missing_factors=(),
                stale_factors=(("metaculus_prior", ""),),
                snapshot_hash="stale-raw",
            )
        ),
        forecasters=(ConstantForecaster(0.8),),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        settings=PMSSettings(
            mode=RunMode.LIVE,
            controller=ControllerSettings(min_volume=0.0),
            risk=RiskSettings(min_order_usdc=1.0),
        ),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    assert pipeline.last_diagnostic is not None
    assert pipeline.last_diagnostic.code == "stale_required_factors"


def test_disabled_llm_forecaster_does_not_emit_neutral_runtime_factor() -> None:
    result = LLMForecaster(config=LLMSettings(enabled=False)).predict(_signal())

    assert result is None


def test_posterior_branch_missing_all_inputs_does_not_emit_statistical_probability() -> None:
    branch_probabilities = evaluate_branch_probabilities(
        (
            FactorCompositionStep(
                factor_id="metaculus_prior",
                role="posterior_prior",
                param="",
                weight=2.0,
                threshold=None,
            ),
            FactorCompositionStep(
                factor_id="yes_count",
                role="posterior_success",
                param="",
                weight=1.0,
                threshold=None,
            ),
            FactorCompositionStep(
                factor_id="no_count",
                role="posterior_failure",
                param="",
                weight=1.0,
                threshold=None,
            ),
        ),
        {("yes_price", ""): 0.10},
    )

    assert "statistical" not in branch_probabilities


def _live_settings(*, tif: str = "IOC") -> PMSSettings:
    return PMSSettings(
        mode=RunMode.LIVE,
        live_trading_enabled=True,
        auto_migrate_default_v2=False,
        controller=ControllerSettings(time_in_force=tif, min_volume=0.0),
        risk=RiskSettings(
            max_position_per_market=1_000.0,
            max_total_exposure=10_000.0,
            min_order_usdc=1.0,
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


def _decision(
    *,
    decision_id: str = "d-live-blocker",
    time_in_force: TimeInForce = TimeInForce.IOC,
    side: Literal["BUY", "SELL"] = Side.BUY.value,
    action: Literal["BUY", "SELL"] | None = Side.BUY.value,
    intent_key: str | None = None,
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id="m-live-blocker",
        token_id="t-yes",
        venue="polymarket",
        side=side,
        notional_usdc=10.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["unit-test"],
        prob_estimate=0.7,
        expected_edge=0.2,
        time_in_force=time_in_force,
        opportunity_id=f"op-{decision_id}",
        strategy_id="default",
        strategy_version_id="default-v1",
        action=action,
        limit_price=0.4,
        outcome="YES",
        intent_key=intent_key,
    )


def test_live_mode_rejects_gtc_until_open_order_ledger_exists() -> None:
    with pytest.raises(LiveTradingDisabledError, match="LIVE GTC disabled"):
        validate_live_mode_ready(_live_settings(tif="GTC"))


def test_trade_decision_rejects_action_side_mismatch() -> None:
    with pytest.raises(ValueError, match="side/action mismatch"):
        _decision(side=Side.SELL.value, action=Side.BUY.value)


@dataclass
class AllowFirstOrderGate:
    async def approve_first_order(self, preview: LiveOrderPreview) -> bool:
        del preview
        return True

    async def consume(self, preview: LiveOrderPreview) -> None:
        del preview


@dataclass
class RecordingClient:
    submitted: list[object]

    async def submit_order(
        self,
        order: object,
        credentials: object,
    ) -> PolymarketOrderResult:
        del credentials
        self.submitted.append(order)
        return PolymarketOrderResult(
            order_id="pm-live-blocker",
            status=OrderStatus.MATCHED.value,
            raw_status="matched",
            filled_notional_usdc=10.0,
            remaining_notional_usdc=0.0,
            fill_price=0.4,
            filled_quantity=25.0,
        )


@pytest.mark.asyncio
async def test_polymarket_actuator_requires_pre_submit_quote_guard() -> None:
    client = RecordingClient(submitted=[])
    actuator = PolymarketActuator(
        _live_settings(),
        client=client,
        operator_gate=AllowFirstOrderGate(),
    )

    with pytest.raises(LiveTradingDisabledError, match="pre-submit quote guard"):
        await actuator.execute(_decision(), _portfolio())

    assert client.submitted == []


@dataclass
class UnknownSubmissionAdapter:
    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        del decision, portfolio
        raise PolymarketSubmissionUnknownError("timeout")


@pytest.mark.asyncio
async def test_executor_attaches_submission_unknown_order_state_before_reraising() -> None:
    decision = _decision()
    executor = ActuatorExecutor(
        adapter=UnknownSubmissionAdapter(),
        risk=RiskManager(
            RiskSettings(max_position_per_market=1_000.0, max_total_exposure=10_000.0)
        ),
        feedback=ActuatorFeedback(cast(FeedbackStore, InMemoryFeedbackStore())),
    )

    with pytest.raises(PolymarketSubmissionUnknownError) as exc_info:
        await executor.execute(decision, _portfolio())

    order_state = exc_info.value.order_state
    assert order_state is not None
    assert order_state.decision_id == decision.decision_id
    assert order_state.raw_status == "submission_unknown"


@dataclass
class RecordingOrderStore:
    inserted: list[OrderState]

    async def insert(self, order: OrderState) -> None:
        self.inserted.append(order)


@dataclass
class RaisingUnknownExecutor:
    order_state: OrderState

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio,
        *,
        dedup_acquired: bool = False,
    ) -> OrderState:
        del decision, portfolio, dedup_acquired
        error = PolymarketSubmissionUnknownError("timeout")
        error.order_state = self.order_state
        raise error


def _submission_unknown_order(decision: TradeDecision) -> OrderState:
    now = datetime(2026, 4, 26, tzinfo=UTC)
    return OrderState(
        order_id=f"unknown-{decision.decision_id}",
        decision_id=decision.decision_id,
        status=OrderStatus.INVALID.value,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_notional_usdc=decision.notional_usdc,
        filled_notional_usdc=0.0,
        remaining_notional_usdc=decision.notional_usdc,
        fill_price=None,
        submitted_at=now,
        last_updated_at=now,
        raw_status="submission_unknown",
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        filled_quantity=0.0,
    )


def _mark_controller_done(runner: Runner) -> None:
    import asyncio

    runner._controller_task = asyncio.create_task(asyncio.sleep(0))  # noqa: SLF001


@pytest.mark.asyncio
async def test_runner_persists_submission_unknown_and_suspends_live_orders() -> None:
    decision = _decision()
    order_state = _submission_unknown_order(decision)
    runner = Runner(config=_live_settings())
    runner.actuator_executor = cast(Any, RaisingUnknownExecutor(order_state))
    order_store = RecordingOrderStore(inserted=[])
    runner.order_store = cast(Any, order_store)
    _mark_controller_done(runner)

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(decision=decision, signal=None)
    )

    await runner._actuator_loop()  # noqa: SLF001

    assert runner.state.orders == [order_state]
    assert order_store.inserted == [order_state]
    assert runner.live_trading_suspended is True


@pytest.mark.asyncio
async def test_in_memory_dedup_blocks_same_economic_intent_key() -> None:
    store = InMemoryDedupStore()
    first = _decision(decision_id="d-intent-1", intent_key="intent:same")
    second = _decision(decision_id="d-intent-2", intent_key="intent:same")

    assert await store.acquire(first) is True
    assert await store.acquire(second) is False
