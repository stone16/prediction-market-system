from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import cast

import pytest

from pms.core.enums import FeedbackSource, FeedbackTarget, OrderStatus, Side, TimeInForce
from pms.core.models import EvalRecord, Feedback, FillRecord, TradeDecision
from pms.evaluation.adapters.scoring import Scorer
from pms.evaluation.feedback import EvaluatorFeedback
from pms.evaluation.metrics import (
    MetricsCollector,
    MetricsSnapshot,
    StrategyMetricsSnapshot,
)
from pms.evaluation.spool import EvalSpool
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.strategies.projections import EvalSpec
from tests.support.fake_stores import InMemoryEvalStore, InMemoryFeedbackStore


def _decision(
    *,
    decision_id: str = "d-cp07",
    prob: float = 0.7,
    price: float = 0.4,
    strategy_id: str = "default",
    strategy_version_id: str = "default-v1",
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id="m-cp07",
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        limit_price=price,
        notional_usdc=price * 10.0,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["min_volume:100.00"],
        prob_estimate=prob,
        expected_edge=prob - price,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"op-{decision_id}",
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        model_id="model-a",
    )


def _fill(
    *,
    decision_id: str = "d-cp07",
    resolved_outcome: float | None = 1.0,
    fill_price: float = 0.42,
    status: str = OrderStatus.MATCHED.value,
    strategy_id: str = "default",
    strategy_version_id: str = "default-v1",
) -> FillRecord:
    now = datetime(2026, 4, 14, tzinfo=UTC)
    return FillRecord(
        trade_id=f"trade-{decision_id}",
        order_id=f"order-{decision_id}",
        decision_id=decision_id,
        market_id="m-cp07",
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        fill_price=fill_price,
        fill_notional_usdc=fill_price * 10.0,
        fill_quantity=10.0,
        executed_at=now,
        filled_at=now,
        status=status,
        anomaly_flags=[],
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        resolved_outcome=resolved_outcome,
    )


def _eval_record(
    *,
    decision_id: str = "d-cp07",
    strategy_id: str = "default",
    strategy_version_id: str = "default-v1",
    brier_score: float = 0.09,
    category: str = "model-a",
    model_id: str = "model-a",
    pnl: float = 1.0,
    slippage_bps: float = 10.0,
    filled: bool = True,
) -> EvalRecord:
    return EvalRecord(
        market_id="m-cp07",
        decision_id=decision_id,
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        prob_estimate=0.7,
        resolved_outcome=1.0,
        brier_score=brier_score,
        fill_status=OrderStatus.MATCHED.value if filled else OrderStatus.INVALID.value,
        recorded_at=datetime(2026, 4, 14, tzinfo=UTC),
        citations=["unit-test"],
        category=category,
        model_id=model_id,
        pnl=pnl,
        slippage_bps=slippage_bps,
        filled=filled,
    )


def _strategy_snapshot(
    *,
    strategy_id: str = "default",
    strategy_version_id: str = "default-v1",
    brier_score: float = 0.31,
    sample_count: int = 20,
    slippage_bps: float = 51.0,
    win_rate: float = 0.54,
) -> StrategyMetricsSnapshot:
    return StrategyMetricsSnapshot(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        brier_overall=brier_score,
        brier_by_category={"model-a": brier_score},
        brier_samples={"model-a": sample_count},
        record_count=sample_count,
        pnl=0.0,
        slippage_bps=slippage_bps,
        fill_rate=1.0,
        win_rate=win_rate,
        calibration_samples={"model-a": sample_count},
    )


def _feedback(feedback_id: str) -> Feedback:
    return Feedback(
        feedback_id=feedback_id,
        target=FeedbackTarget.CONTROLLER.value,
        source=FeedbackSource.EVALUATOR.value,
        message="threshold crossed",
        severity="warning",
        created_at=datetime(2026, 4, 14, tzinfo=UTC),
        category="brier",
    )


def test_scorer_brier_known_values() -> None:
    scorer = Scorer()

    first = scorer.score(_fill(resolved_outcome=1.0), _decision(prob=0.7))
    second = scorer.score(_fill(resolved_outcome=0.0), _decision(prob=0.5))

    assert first.brier_score == pytest.approx(0.09)
    assert second.brier_score == pytest.approx(0.25)


def test_scorer_pnl_for_buy_no_uses_complementary_contract_outcome() -> None:
    scorer = Scorer()
    decision = TradeDecision(
        decision_id="d-buy-no",
        market_id="m-cp07",
        token_id="t-no",
        venue="polymarket",
        side=Side.BUY.value,
        limit_price=0.38,
        notional_usdc=3.8,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["min_volume:100.00"],
        prob_estimate=0.62,
        expected_edge=0.24,
        time_in_force=TimeInForce.GTC,
        opportunity_id="op-d-buy-no",
        strategy_id="default",
        strategy_version_id="default-v1",
        model_id="model-a",
        outcome="NO",
    )

    record = scorer.score(
        _fill(decision_id="d-buy-no", resolved_outcome=0.0, fill_price=0.38),
        decision,
    )

    assert record.pnl == pytest.approx(6.2)


def test_scorer_brier_for_buy_no_uses_yes_probability_basis() -> None:
    scorer = Scorer()
    decision = TradeDecision(
        decision_id="d-buy-no-brier",
        market_id="m-cp07",
        token_id="t-no",
        venue="polymarket",
        side=Side.BUY.value,
        limit_price=0.38,
        notional_usdc=3.8,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["min_volume:100.00"],
        prob_estimate=0.7,
        expected_edge=0.32,
        time_in_force=TimeInForce.GTC,
        opportunity_id="op-d-buy-no-brier",
        strategy_id="default",
        strategy_version_id="default-v1",
        model_id="model-a",
        outcome="NO",
    )

    record = scorer.score(
        _fill(
            decision_id="d-buy-no-brier",
            resolved_outcome=0.0,
            fill_price=0.38,
        ),
        decision,
    )

    assert record.prob_estimate == pytest.approx(0.3)
    assert record.brier_score == pytest.approx(0.09)


def test_scorer_rejects_fill_and_decision_strategy_identity_mismatch() -> None:
    scorer = Scorer()

    with pytest.raises(ValueError, match="strategy identity must match"):
        scorer.score(
            _fill(strategy_id="alpha", strategy_version_id="alpha-v1"),
            _decision(strategy_id="beta", strategy_version_id="beta-v1"),
        )


@pytest.mark.asyncio
async def test_eval_spool_enqueue_is_non_blocking_and_scores_in_background(
) -> None:
    store = cast(EvalStore, InMemoryEvalStore())
    spool = EvalSpool(store=store, scorer=Scorer())
    await spool.start()
    try:
        started_at = time.perf_counter()
        for index in range(100):
            spool.enqueue(
                _fill(decision_id=f"d-{index}", resolved_outcome=1.0),
                _decision(prob=0.7),
            )
        elapsed_ms = (time.perf_counter() - started_at) * 1000

        await spool.join()
    finally:
        await spool.stop()

    assert elapsed_ms < 100
    assert len(await cast(InMemoryEvalStore, store).all()) == 100


@pytest.mark.asyncio
async def test_eval_spool_skips_unresolved_fills_and_keeps_running(
) -> None:
    store = cast(EvalStore, InMemoryEvalStore())
    spool = EvalSpool(store=store, scorer=Scorer())
    await spool.start()
    try:
        spool.enqueue(
            _fill(decision_id="d-unresolved", resolved_outcome=None),
            _decision(decision_id="d-unresolved"),
        )
        spool.enqueue(
            _fill(decision_id="d-resolved", resolved_outcome=1.0),
            _decision(decision_id="d-resolved"),
        )

        await asyncio.wait_for(spool.join(), timeout=1.0)
    finally:
        await spool.stop()

    assert [record.decision_id for record in await cast(InMemoryEvalStore, store).all()] == ["d-resolved"]


def test_metrics_snapshot_empty_and_aggregated_records() -> None:
    empty = MetricsCollector([]).global_ops_snapshot()

    assert empty.brier_overall is None

    snapshot = MetricsCollector(
        [
            _eval_record(decision_id="d1", brier_score=0.09, pnl=2.0, slippage_bps=10.0),
            _eval_record(
                decision_id="d2",
                brier_score=0.25,
                category="model-b",
                model_id="model-b",
                pnl=-1.0,
                slippage_bps=20.0,
                filled=False,
            ),
        ]
    ).global_ops_snapshot()

    assert snapshot.brier_overall == pytest.approx(0.17)
    assert snapshot.brier_by_category == {"model-a": 0.09, "model-b": 0.25}
    assert snapshot.pnl == 1.0
    assert snapshot.slippage_bps == pytest.approx(15.0)
    assert snapshot.fill_rate == pytest.approx(0.5)
    assert snapshot.calibration_samples == {"model-a": 1, "model-b": 1}


@pytest.mark.asyncio
async def test_evaluator_feedback_uses_per_strategy_eval_thresholds() -> None:
    generator = EvaluatorFeedback(cast(FeedbackStore, InMemoryFeedbackStore()))
    alpha_key = ("alpha", "alpha-v1")
    beta_key = ("beta", "beta-v1")

    feedback = await generator.generate(
        {
            alpha_key: (
                _strategy_snapshot(
                    strategy_id=alpha_key[0],
                    strategy_version_id=alpha_key[1],
                ),
                EvalSpec(
                    metrics=("brier", "pnl", "fill_rate"),
                    max_brier_score=0.30,
                    slippage_threshold_bps=50.0,
                    min_win_rate=0.55,
                ),
            ),
            beta_key: (
                _strategy_snapshot(
                    strategy_id=beta_key[0],
                    strategy_version_id=beta_key[1],
                ),
                EvalSpec(
                    metrics=("brier", "pnl", "fill_rate"),
                    max_brier_score=0.40,
                    slippage_threshold_bps=60.0,
                    min_win_rate=0.50,
                ),
            ),
        }
    )

    assert {item.category for item in feedback} == {
        "brier:model-a",
        "slippage",
        "win_rate",
    }
    assert all(item.metadata["strategy_id"] == "alpha" for item in feedback)
    assert all(item.metadata["strategy_version_id"] == "alpha-v1" for item in feedback)


@pytest.mark.asyncio
async def test_eval_spool_generates_deduped_feedback_from_runtime_metrics() -> None:
    store = cast(EvalStore, InMemoryEvalStore())
    feedback_store = cast(FeedbackStore, InMemoryFeedbackStore())

    async def metrics_provider() -> dict[tuple[str, str], tuple[StrategyMetricsSnapshot, EvalSpec]]:
        snapshots = MetricsCollector(
            await cast(InMemoryEvalStore, store).all()
        ).snapshot_by_strategy()
        return {
            key: (
                snapshot,
                EvalSpec(
                    metrics=("brier", "pnl", "fill_rate"),
                    max_brier_score=0.30,
                    slippage_threshold_bps=50.0,
                    min_win_rate=0.50,
                ),
            )
            for key, snapshot in snapshots.items()
        }

    spool = EvalSpool(
        store=store,
        scorer=Scorer(),
        feedback_generator=EvaluatorFeedback(feedback_store),
        metrics_provider=metrics_provider,
    )
    await spool.start()
    try:
        spool.enqueue(
            _fill(decision_id="d-feedback-1", resolved_outcome=1.0, fill_price=0.42),
            _decision(decision_id="d-feedback-1", price=0.4),
        )
        spool.enqueue(
            _fill(decision_id="d-feedback-2", resolved_outcome=1.0, fill_price=0.42),
            _decision(decision_id="d-feedback-2", price=0.4),
        )

        await asyncio.wait_for(spool.join(), timeout=1.0)
    finally:
        await spool.stop()

    feedback = await cast(InMemoryFeedbackStore, feedback_store).all()

    assert [item.category for item in feedback] == ["slippage"]
    assert feedback[0].metadata["strategy_id"] == "default"
    assert feedback[0].metadata["strategy_version_id"] == "default-v1"
    assert feedback[0].metadata["sample_size"] == 1
    assert feedback[0].metadata["market_cohort"] == "all_scored_fills"


@pytest.mark.asyncio
async def test_eval_spool_logs_feedback_errors_and_keeps_scoring(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = cast(EvalStore, InMemoryEvalStore())

    async def _failing_metrics_provider() -> dict[tuple[str, str], tuple[StrategyMetricsSnapshot, EvalSpec]]:
        raise RuntimeError("metrics boom")

    spool = EvalSpool(
        store=store,
        scorer=Scorer(),
        feedback_generator=EvaluatorFeedback(cast(FeedbackStore, InMemoryFeedbackStore())),
        metrics_provider=_failing_metrics_provider,
    )
    await spool.start()
    try:
        caplog.set_level("ERROR", logger="pms.evaluation.spool")
        spool.enqueue(
            _fill(decision_id="d-feedback-error", resolved_outcome=1.0),
            _decision(decision_id="d-feedback-error"),
        )
        spool.enqueue(
            _fill(decision_id="d-after-error", resolved_outcome=1.0),
            _decision(decision_id="d-after-error"),
        )
        await asyncio.wait_for(spool.join(), timeout=1.0)
    finally:
        await spool.stop()

    records = await cast(InMemoryEvalStore, store).all()
    assert [record.decision_id for record in records] == [
        "d-feedback-error",
        "d-after-error",
    ]
    assert "feedback generation failed in evaluator spool" in caplog.text


def test_metrics_snapshot_by_strategy_returns_only_present_keys() -> None:
    snapshots = MetricsCollector(
        [
            _eval_record(
                decision_id="alpha-1",
                strategy_id="alpha",
                strategy_version_id="alpha-v1",
                brier_score=0.09,
            ),
            _eval_record(
                decision_id="beta-1",
                strategy_id="beta",
                strategy_version_id="beta-v1",
                brier_score=0.25,
                category="model-b",
                model_id="model-b",
            ),
        ]
    ).snapshot_by_strategy()

    assert set(snapshots) == {("alpha", "alpha-v1"), ("beta", "beta-v1")}
    assert snapshots[("alpha", "alpha-v1")].brier_overall == pytest.approx(0.09)
    assert snapshots[("beta", "beta-v1")].brier_by_category == {"model-b": 0.25}
