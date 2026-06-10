"""Evaluator -> Controller calibration feedback edge (runner side).

The runner injects ``_on_eval_record_for_calibration`` as the spool's
calibration sink (event-driven push per resolved record) and re-hydrates
each controller runtime's calibrator from the durable eval store when the
runtime is attached (restart / cold-start / mid-session swap).

Scope is strict ``(strategy_id, strategy_version_id)``: feeding another
version's resolved samples would unlock extreme-probability trading on a
model those outcomes never tested.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from pms.config import ControllerSettings, PMSSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.pipeline import ControllerPipeline
from pms.core.enums import MarketStatus, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import (
    EvalRecord,
    FillRecord,
    MarketSignal,
    Portfolio,
    TradeDecision,
)
from pms.evaluation.adapters.scoring import Scorer
from pms.evaluation.spool import EvalSpool
from pms.runner import Runner, StrategyControllerRuntime
from pms.storage.eval_store import EvalStore
from pms.strategies.projections import (
    ActiveStrategy,
    CalibrationSpec,
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


def _runner() -> Runner:
    return Runner(
        config=PMSSettings(
            mode=RunMode.BACKTEST,
            auto_migrate_default_v2=False,
            risk=RiskSettings(
                max_position_per_market=1000.0,
                max_total_exposure=10_000.0,
            ),
        ),
        historical_data_path=FIXTURE_PATH,
    )


def _eval_record(
    *,
    decision_id: str,
    strategy_id: str = "s-cal",
    strategy_version_id: str = "s-cal-v1",
    model_id: str | None = "model-a",
) -> EvalRecord:
    return EvalRecord(
        market_id="m-cal",
        decision_id=decision_id,
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        prob_estimate=0.7,
        resolved_outcome=1.0,
        brier_score=0.09,
        fill_status="matched",
        recorded_at=datetime(2026, 6, 10, tzinfo=UTC),
        citations=["unit-test"],
        model_id=model_id,
    )


class _CalibratedController:
    """Controller double exposing the duck-typed ``calibrator`` attribute the
    runner feeds (same access pattern as pipeline._resolved_sample_count)."""

    def __init__(self) -> None:
        self.calibrator = NetcalCalibrator()

    async def decide(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> TradeDecision | None:
        del signal, portfolio
        return None


class _CalibratorFreeController:
    async def decide(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> TradeDecision | None:
        del signal, portfolio
        return None


def _runtime(
    controller: object,
    *,
    strategy_id: str = "s-cal",
    strategy_version_id: str = "s-cal-v1",
) -> StrategyControllerRuntime:
    return StrategyControllerRuntime(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        controller=cast(Any, controller),
        asset_ids=None,
    )


class _StrategyScopedEvalStore:
    def __init__(
        self,
        records_by_key: dict[tuple[str, str], list[EvalRecord]] | None = None,
    ) -> None:
        self.records_by_key = records_by_key or {}
        self.queries: list[tuple[str, str]] = []

    async def all_for_strategy(
        self,
        strategy_id: str,
        strategy_version_id: str,
    ) -> list[EvalRecord]:
        self.queries.append((strategy_id, strategy_version_id))
        return list(self.records_by_key.get((strategy_id, strategy_version_id), []))


def test_runner_wires_calibration_sink_into_eval_spool() -> None:
    runner = _runner()

    assert (
        runner._evaluator_spool.calibration_sink  # noqa: SLF001
        == runner._on_eval_record_for_calibration  # noqa: SLF001
    )


def test_on_eval_record_routes_to_matching_runtime_calibrator() -> None:
    runner = _runner()
    controller = _CalibratedController()
    runner._controller_runtimes["s-cal"] = _runtime(controller)  # noqa: SLF001

    runner._on_eval_record_for_calibration(  # noqa: SLF001
        _eval_record(decision_id="d-1")
    )

    assert controller.calibrator.sample_count("model-a") == 1


def test_on_eval_record_skips_stale_version_records() -> None:
    runner = _runner()
    controller = _CalibratedController()
    runner._controller_runtimes["s-cal"] = _runtime(  # noqa: SLF001
        controller,
        strategy_version_id="s-cal-v2",
    )

    runner._on_eval_record_for_calibration(  # noqa: SLF001
        _eval_record(decision_id="d-stale", strategy_version_id="s-cal-v1")
    )

    assert controller.calibrator.sample_count("model-a") == 0


def test_on_eval_record_skips_unknown_strategy() -> None:
    runner = _runner()

    runner._on_eval_record_for_calibration(  # noqa: SLF001
        _eval_record(decision_id="d-unmatched", strategy_id="s-unknown")
    )


def test_on_eval_record_falls_back_to_unknown_model_id() -> None:
    runner = _runner()
    controller = _CalibratedController()
    runner._controller_runtimes["s-cal"] = _runtime(controller)  # noqa: SLF001

    runner._on_eval_record_for_calibration(  # noqa: SLF001
        _eval_record(decision_id="d-no-model", model_id=None)
    )

    assert controller.calibrator.sample_count("unknown") == 1


def test_on_eval_record_ignores_controller_without_calibrator() -> None:
    runner = _runner()
    runner._controller_runtimes["s-cal"] = _runtime(  # noqa: SLF001
        _CalibratorFreeController()
    )

    runner._on_eval_record_for_calibration(  # noqa: SLF001
        _eval_record(decision_id="d-duck")
    )


def _scored_decision(*, decision_id: str) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id="m-cal",
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        limit_price=0.4,
        notional_usdc=4.0,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["min_volume:100.00"],
        prob_estimate=0.7,
        expected_edge=0.3,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"op-{decision_id}",
        strategy_id="s-cal",
        strategy_version_id="s-cal-v1",
        model_id="model-a",
    )


def _resolved_fill(*, decision_id: str) -> FillRecord:
    now = datetime(2026, 6, 10, tzinfo=UTC)
    return FillRecord(
        trade_id=f"trade-{decision_id}",
        order_id=f"order-{decision_id}",
        decision_id=decision_id,
        market_id="m-cal",
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        fill_price=0.42,
        fill_notional_usdc=4.2,
        fill_quantity=10.0,
        executed_at=now,
        filled_at=now,
        status=OrderStatus.MATCHED.value,
        anomaly_flags=[],
        strategy_id="s-cal",
        strategy_version_id="s-cal-v1",
        resolved_outcome=1.0,
    )


class _AppendOnlyEvalStore:
    """Minimal eval-store fake; production dedups duplicate decision_ids with
    ON CONFLICT at append, but the sink still fires once per enqueue."""

    def __init__(self) -> None:
        self.appended: list[EvalRecord] = []

    async def append(self, record: EvalRecord) -> None:
        self.appended.append(record)


@pytest.mark.asyncio
async def test_duplicate_enqueue_same_decision_id_counts_one_calibration_sample(
) -> None:
    """Sweep re-enqueues (feat/resolution-ingestion) can deliver the same
    decision_id twice — e.g. a fill resolved at enqueue time AND swept, or a
    sweep retry. The store-level ON CONFLICT dedups the append, but the sink
    fires per enqueue, so NetcalCalibrator's per-(model_id, decision_id) dedup
    is what keeps a single resolution from counting twice toward clamp
    graduation."""
    runner = _runner()
    controller = _CalibratedController()
    runner._controller_runtimes["s-cal"] = _runtime(controller)  # noqa: SLF001
    spool = EvalSpool(
        store=cast(EvalStore, cast(object, _AppendOnlyEvalStore())),
        scorer=Scorer(),
        calibration_sink=runner._on_eval_record_for_calibration,  # noqa: SLF001
    )
    await spool.start()
    try:
        # Fresh, equal-by-value objects per enqueue: the production duplicate
        # is a DB-rehydrated record vs an in-memory pushed one, never the
        # same object identity.
        spool.enqueue(
            _resolved_fill(decision_id="d-dup"),
            _scored_decision(decision_id="d-dup"),
        )
        spool.enqueue(
            _resolved_fill(decision_id="d-dup"),
            _scored_decision(decision_id="d-dup"),
        )
        await asyncio.wait_for(spool.join(), timeout=1.0)
    finally:
        await spool.stop()

    assert controller.calibrator.sample_count("model-a") == 1


@pytest.mark.asyncio
async def test_hydrate_runtime_calibration_groups_records_by_model_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _runner()
    controller = _CalibratedController()
    runtime = _runtime(controller)
    runner.eval_store = cast(
        Any,
        _StrategyScopedEvalStore(
            {
                ("s-cal", "s-cal-v1"): [
                    _eval_record(decision_id="d-a1", model_id="model-a"),
                    _eval_record(decision_id="d-a2", model_id="model-a"),
                    _eval_record(decision_id="d-ensemble", model_id="ensemble"),
                    _eval_record(decision_id="d-none", model_id=None),
                ]
            }
        ),
    )

    with caplog.at_level(logging.INFO, logger="pms.runner"):
        await runner._hydrate_runtime_calibration(runtime)  # noqa: SLF001

    assert controller.calibrator.sample_count("model-a") == 2
    assert controller.calibrator.sample_count("ensemble") == 1
    assert controller.calibrator.sample_count("unknown") == 1
    hydration_logs = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "calibration_hydration"
    ]
    assert len(hydration_logs) == 1
    assert getattr(hydration_logs[0], "strategy_id", None) == "s-cal"
    assert getattr(hydration_logs[0], "strategy_version_id", None) == "s-cal-v1"
    assert getattr(hydration_logs[0], "resolved_records_by_model", None) == {
        "model-a": 2,
        "ensemble": 1,
        "unknown": 1,
    }


@pytest.mark.asyncio
async def test_hydrate_runtime_calibration_is_noop_on_empty_store() -> None:
    """The running paper soak has eval_records=0: hydration must be an exact
    no-op on calibrator state — fail-closed, no crash."""
    runner = _runner()
    controller = _CalibratedController()
    runner.eval_store = cast(Any, _StrategyScopedEvalStore())

    await runner._hydrate_runtime_calibration(_runtime(controller))  # noqa: SLF001

    assert controller.calibrator.sample_count("model-a") == 0
    assert controller.calibrator.sample_count("unknown") == 0


@pytest.mark.asyncio
async def test_hydrate_runtime_calibration_skips_store_query_without_calibrator(
) -> None:
    runner = _runner()
    store = _StrategyScopedEvalStore()
    runner.eval_store = cast(Any, store)

    await runner._hydrate_runtime_calibration(  # noqa: SLF001
        _runtime(_CalibratorFreeController())
    )

    assert store.queries == []


@pytest.mark.asyncio
async def test_configure_controllers_hydrates_runtime_before_attach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner()
    controller = _CalibratedController()
    runner.controller = cast(Any, controller)
    store = _StrategyScopedEvalStore(
        {
            ("default", "default-v1"): [
                _eval_record(
                    decision_id="d-default",
                    strategy_id="default",
                    strategy_version_id="default-v1",
                )
            ]
        }
    )
    runner.eval_store = cast(Any, store)
    events: list[str] = []

    original_all_for_strategy = store.all_for_strategy

    async def recording_all_for_strategy(
        strategy_id: str,
        strategy_version_id: str,
    ) -> list[EvalRecord]:
        events.append(f"hydrate:{strategy_id}")
        return await original_all_for_strategy(strategy_id, strategy_version_id)

    monkeypatch.setattr(store, "all_for_strategy", recording_all_for_strategy)
    monkeypatch.setattr(
        runner,
        "_attach_controller_runtime",
        lambda runtime: events.append(f"attach:{runtime.strategy_id}"),
    )

    await runner._configure_controllers()  # noqa: SLF001

    assert events == ["hydrate:default", "attach:default"]
    assert controller.calibrator.sample_count("model-a") == 1


@pytest.mark.asyncio
async def test_sync_controller_runtimes_hydrates_only_attached_runtimes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Discovery-driven syncs run repeatedly; hydration must hit the DB only
    for runtimes actually being (re)attached — new strategies and version
    replacements — never for unchanged ones."""
    runner = _runner()
    runner._strategy_registry = cast(Any, object())  # noqa: SLF001

    unchanged_controller = _CalibratedController()
    unchanged_runtime = _runtime(
        unchanged_controller,
        strategy_id="s-unchanged",
        strategy_version_id="s-unchanged-v1",
    )
    runner._controller_runtimes["s-unchanged"] = unchanged_runtime  # noqa: SLF001

    replaced_controller = _CalibratedController()
    runner._controller_runtimes["s-replaced"] = _runtime(  # noqa: SLF001
        _CalibratedController(),
        strategy_id="s-replaced",
        strategy_version_id="s-replaced-v1",
    )
    desired_replacement = _runtime(
        replaced_controller,
        strategy_id="s-replaced",
        strategy_version_id="s-replaced-v2",
    )

    new_controller = _CalibratedController()
    new_runtime = _runtime(
        new_controller,
        strategy_id="s-new",
        strategy_version_id="s-new-v1",
    )

    store = _StrategyScopedEvalStore(
        {
            ("s-replaced", "s-replaced-v2"): [
                _eval_record(
                    decision_id="d-replaced",
                    strategy_id="s-replaced",
                    strategy_version_id="s-replaced-v2",
                )
            ],
            ("s-new", "s-new-v1"): [
                _eval_record(
                    decision_id="d-new",
                    strategy_id="s-new",
                    strategy_version_id="s-new-v1",
                )
            ],
        }
    )
    runner.eval_store = cast(Any, store)

    async def fake_build() -> list[StrategyControllerRuntime]:
        return [unchanged_runtime, desired_replacement, new_runtime]

    monkeypatch.setattr(runner, "_build_controller_runtimes", fake_build)
    attached: list[str] = []
    monkeypatch.setattr(
        runner,
        "_attach_controller_runtime",
        lambda runtime: attached.append(runtime.strategy_id),
    )

    await runner._sync_controller_runtimes()  # noqa: SLF001

    assert sorted(attached) == ["s-new", "s-replaced"]
    assert sorted(store.queries) == [
        ("s-new", "s-new-v1"),
        ("s-replaced", "s-replaced-v2"),
    ]
    assert replaced_controller.calibrator.sample_count("model-a") == 1
    assert new_controller.calibrator.sample_count("model-a") == 1
    assert unchanged_controller.calibrator.sample_count("model-a") == 0


@pytest.mark.asyncio
async def test_sync_replace_attaches_before_hydration_so_concurrent_push_survives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A record pushed by the calibration sink while the replace-branch
    hydration query is in flight must reach the new runtime's calibrator.
    Hydrate-before-attach drops it (the old runtime is already released, the
    new one not yet attached) and the in-flight all_for_strategy query can
    also miss it — a lost sample until the next hydration event. Attach-first
    is safe because NetcalCalibrator dedups per (model_id, decision_id)."""
    runner = _runner()
    runner._strategy_registry = cast(Any, object())  # noqa: SLF001

    replaced_controller = _CalibratedController()
    runner._controller_runtimes["s-replaced"] = _runtime(  # noqa: SLF001
        _CalibratedController(),
        strategy_id="s-replaced",
        strategy_version_id="s-replaced-v1",
    )
    desired_replacement = _runtime(
        replaced_controller,
        strategy_id="s-replaced",
        strategy_version_id="s-replaced-v2",
    )

    store = _StrategyScopedEvalStore()
    original_all_for_strategy = store.all_for_strategy

    async def pushing_all_for_strategy(
        strategy_id: str,
        strategy_version_id: str,
    ) -> list[EvalRecord]:
        # Simulate the evaluator spool pushing a freshly resolved record
        # while the hydration DB read is awaited.
        runner._on_eval_record_for_calibration(  # noqa: SLF001
            _eval_record(
                decision_id="d-concurrent",
                strategy_id="s-replaced",
                strategy_version_id="s-replaced-v2",
            )
        )
        return await original_all_for_strategy(strategy_id, strategy_version_id)

    monkeypatch.setattr(store, "all_for_strategy", pushing_all_for_strategy)
    runner.eval_store = cast(Any, store)

    async def fake_build() -> list[StrategyControllerRuntime]:
        return [desired_replacement]

    monkeypatch.setattr(runner, "_build_controller_runtimes", fake_build)
    # Mirror only the sink-visible effect of attach (runtime registration);
    # the real attach also spawns the pipeline task, whose teardown this
    # unit test does not exercise.
    monkeypatch.setattr(
        runner,
        "_attach_controller_runtime",
        lambda runtime: runner._controller_runtimes.__setitem__(  # noqa: SLF001
            runtime.strategy_id, runtime
        ),
    )

    await runner._sync_controller_runtimes()  # noqa: SLF001

    assert replaced_controller.calibrator.sample_count("model-a") == 1


@pytest.mark.asyncio
async def test_sync_replace_keeps_runtime_attached_when_hydration_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An eval-store failure mid-replace must not leave the strategy detached
    until the next discovery sync: the runtime stays attached (fail-closed
    calibrator under-count) and hydration still fails loudly."""
    runner = _runner()
    runner._strategy_registry = cast(Any, object())  # noqa: SLF001

    desired_replacement = _runtime(
        _CalibratedController(),
        strategy_id="s-replaced",
        strategy_version_id="s-replaced-v2",
    )
    runner._controller_runtimes["s-replaced"] = _runtime(  # noqa: SLF001
        _CalibratedController(),
        strategy_id="s-replaced",
        strategy_version_id="s-replaced-v1",
    )

    class _FailingEvalStore:
        async def all_for_strategy(
            self,
            strategy_id: str,
            strategy_version_id: str,
        ) -> list[EvalRecord]:
            del strategy_id, strategy_version_id
            msg = "eval store unavailable"
            raise RuntimeError(msg)

    runner.eval_store = cast(Any, _FailingEvalStore())

    async def fake_build() -> list[StrategyControllerRuntime]:
        return [desired_replacement]

    monkeypatch.setattr(runner, "_build_controller_runtimes", fake_build)
    monkeypatch.setattr(
        runner,
        "_attach_controller_runtime",
        lambda runtime: runner._controller_runtimes.__setitem__(  # noqa: SLF001
            runtime.strategy_id, runtime
        ),
    )

    with pytest.raises(RuntimeError, match="eval store unavailable"):
        await runner._sync_controller_runtimes()  # noqa: SLF001

    attached = runner._controller_runtimes.get("s-replaced")  # noqa: SLF001
    assert attached is desired_replacement


# --- End-to-end graduation counter-trace -----------------------------------
# Mirror of tests/unit/test_live_trading_blockers.py clamp-probe setup: an
# extreme forecast (0.99) is rejected with zero resolved samples, then two
# resolved EvalRecords arrive through the sink path and the same pipeline
# emits.


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


def _clamp_probe_strategy(*, min_resolved_for_extreme: int) -> ActiveStrategy:
    return ActiveStrategy(
        strategy_id="clamp-probe",
        strategy_version_id="clamp-probe-v1",
        config=StrategyConfig(
            strategy_id="clamp-probe",
            factor_composition=(),
            metadata=(("owner", "system"), ("live_allowed", "false")),
        ),
        risk=RiskParams(
            max_position_notional_usdc=1_000.0,
            max_daily_drawdown_pct=0.0,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl")),
        forecaster=ForecasterSpec(forecasters=(("rules", ()),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=30,
            volume_min_usdc=0.0,
        ),
        calibration=CalibrationSpec(
            enabled=True,
            shrinkage_factor=1.0,  # no shrinkage, so 0.99 stays extreme
            shrinkage_bias=0.0,
            extreme_clamp_low=0.08,
            extreme_clamp_high=0.92,
            min_resolved_for_extreme=min_resolved_for_extreme,
        ),
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="m-graduation",
        token_id="t-yes",
        venue="polymarket",
        title="Will the clamp graduate?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 7, 1, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={},
        fetched_at=datetime(2026, 6, 10, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


@pytest.mark.asyncio
async def test_calibration_graduation_unlocks_extreme_clamp_via_sink_path() -> None:
    pipeline = ControllerPipeline(
        strategy=_clamp_probe_strategy(min_resolved_for_extreme=2),
        forecasters=(ConstantForecaster(0.99),),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        settings=PMSSettings(
            mode=RunMode.PAPER,
            controller=ControllerSettings(min_volume=0.0),
            risk=RiskSettings(min_order_usdc=1.0),
        ),
    )
    runner = _runner()
    runner._controller_runtimes["clamp-probe"] = _runtime(  # noqa: SLF001
        pipeline,
        strategy_id="clamp-probe",
        strategy_version_id="clamp-probe-v1",
    )

    rejected = await pipeline.on_signal(_signal(), portfolio=_portfolio())
    assert rejected is None
    diagnostic = pipeline.last_diagnostic
    assert diagnostic is not None
    assert diagnostic.code == "calibration_clamp_rejected"

    runner._on_eval_record_for_calibration(  # noqa: SLF001
        _eval_record(
            decision_id="d-resolved-0",
            strategy_id="clamp-probe",
            strategy_version_id="clamp-probe-v1",
            model_id="ConstantForecaster",
        )
    )

    # Pin the >= boundary: one resolved record is still below
    # min_resolved_for_extreme=2, so the clamp must keep rejecting — an
    # off-by-one unlock-early regression would emit here.
    still_rejected = await pipeline.on_signal(_signal(), portfolio=_portfolio())
    assert still_rejected is None, (
        "a single resolved eval record (< min_resolved_for_extreme) must not "
        "unlock the extreme-probability clamp"
    )
    boundary_diagnostic = pipeline.last_diagnostic
    assert boundary_diagnostic is not None
    assert boundary_diagnostic.code == "calibration_clamp_rejected"

    runner._on_eval_record_for_calibration(  # noqa: SLF001
        _eval_record(
            decision_id="d-resolved-1",
            strategy_id="clamp-probe",
            strategy_version_id="clamp-probe-v1",
            model_id="ConstantForecaster",
        )
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is not None, (
        "two resolved eval records (>= min_resolved_for_extreme) fed through "
        "the calibration sink must unlock the extreme-probability clamp"
    )
    _opportunity, decision = emission
    assert decision.prob_estimate == pytest.approx(0.99)
