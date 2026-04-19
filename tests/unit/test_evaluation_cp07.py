from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import cast

import pytest

from pms.config import RiskSettings
from pms.core.enums import FeedbackSource, FeedbackTarget, OrderStatus, Side
from pms.core.models import EvalRecord, Feedback, FillRecord, TradeDecision
from pms.evaluation.adapters.scoring import Scorer
from pms.evaluation.feedback import EvaluatorFeedback
from pms.evaluation.metrics import MetricsCollector, MetricsSnapshot
from pms.evaluation.spool import EvalSpool
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryEvalStore, InMemoryFeedbackStore


def _decision(
    *, decision_id: str = "d-cp07", prob: float = 0.7, price: float = 0.4
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id="m-cp07",
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        price=price,
        size=10.0,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["min_volume:100.00"],
        prob_estimate=prob,
        expected_edge=prob - price,
        time_in_force="GTC",
        opportunity_id=f"op-{decision_id}",
        strategy_id="default",
        strategy_version_id="default-v1",
        model_id="model-a",
    )


def _fill(
    *,
    decision_id: str = "d-cp07",
    resolved_outcome: float | None = 1.0,
    fill_price: float = 0.42,
    status: str = OrderStatus.MATCHED.value,
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
        fill_size=10.0,
        executed_at=now,
        filled_at=now,
        status=status,
        anomaly_flags=[],
        strategy_id="default",
        strategy_version_id="default-v1",
        resolved_outcome=resolved_outcome,
    )


def _eval_record(
    *,
    decision_id: str = "d-cp07",
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
        strategy_id="default",
        strategy_version_id="default-v1",
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


@pytest.mark.asyncio
async def test_eval_spool_enqueue_is_non_blocking_and_scores_in_background(
) -> None:
    store = cast(EvalStore, InMemoryEvalStore())
    spool = EvalSpool(store=store, scorer=Scorer())
    await spool.start()

    started_at = time.perf_counter()
    for index in range(100):
        spool.enqueue(
            _fill(decision_id=f"d-{index}", resolved_outcome=1.0),
            _decision(prob=0.7),
        )
    elapsed_ms = (time.perf_counter() - started_at) * 1000

    await spool.join()
    await spool.stop()

    assert elapsed_ms < 100
    assert len(await cast(InMemoryEvalStore, store).all()) == 100


@pytest.mark.asyncio
async def test_eval_spool_skips_unresolved_fills_and_keeps_running(
) -> None:
    store = cast(EvalStore, InMemoryEvalStore())
    spool = EvalSpool(store=store, scorer=Scorer())
    await spool.start()

    spool.enqueue(
        _fill(decision_id="d-unresolved", resolved_outcome=None),
        _decision(decision_id="d-unresolved"),
    )
    spool.enqueue(
        _fill(decision_id="d-resolved", resolved_outcome=1.0),
        _decision(decision_id="d-resolved"),
    )

    await asyncio.wait_for(spool.join(), timeout=1.0)
    await spool.stop()

    assert [record.decision_id for record in await cast(InMemoryEvalStore, store).all()] == ["d-resolved"]


def test_metrics_snapshot_empty_and_aggregated_records() -> None:
    empty = MetricsCollector([]).snapshot()

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
    ).snapshot()

    assert snapshot.brier_overall == pytest.approx(0.17)
    assert snapshot.brier_by_category == {"model-a": 0.09, "model-b": 0.25}
    assert snapshot.pnl == 1.0
    assert snapshot.slippage_bps == pytest.approx(15.0)
    assert snapshot.fill_rate == pytest.approx(0.5)
    assert snapshot.calibration_samples == {"model-a": 1, "model-b": 1}


@pytest.mark.asyncio
async def test_evaluator_feedback_threshold_boundaries() -> None:
    generator = EvaluatorFeedback(
        cast(FeedbackStore, InMemoryFeedbackStore()),
        risk=RiskSettings(
            max_brier_score=0.30,
            slippage_threshold_bps=50.0,
            min_win_rate=0.55,
        ),
    )

    below_sample_floor = MetricsSnapshot(
        brier_overall=0.31,
        brier_by_category={"model-a": 0.31},
        brier_samples={"model-a": 19},
        pnl=0.0,
        slippage_bps=50.0,
        fill_rate=1.0,
        win_rate=0.55,
        calibration_samples={"model-a": 19},
    )
    crossed = MetricsSnapshot(
        brier_overall=0.31,
        brier_by_category={"model-a": 0.31},
        brier_samples={"model-a": 20},
        pnl=0.0,
        slippage_bps=51.0,
        fill_rate=1.0,
        win_rate=0.54,
        calibration_samples={"model-a": 20},
    )

    assert await generator.generate(below_sample_floor) == []
    feedback = await generator.generate(crossed)

    assert {item.category for item in feedback} == {
        "brier:model-a",
        "slippage",
        "win_rate",
    }
