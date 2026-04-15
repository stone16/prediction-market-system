from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path

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
        stop_conditions=["model_id:model-a"],
        prob_estimate=prob,
        expected_edge=prob - price,
        time_in_force="GTC",
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
    tmp_path: Path,
) -> None:
    store = EvalStore(path=tmp_path / "eval_records.jsonl")
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
    assert len(store.all()) == 100
    assert (tmp_path / "eval_records.jsonl").exists()


@pytest.mark.asyncio
async def test_eval_spool_skips_unresolved_fills_and_keeps_running(
    tmp_path: Path,
) -> None:
    store = EvalStore(path=tmp_path / "eval_records.jsonl")
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

    assert [record.decision_id for record in store.all()] == ["d-resolved"]


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


def test_evaluator_feedback_threshold_boundaries() -> None:
    generator = EvaluatorFeedback(
        FeedbackStore(path=None),
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

    assert generator.generate(below_sample_floor) == []
    feedback = generator.generate(crossed)

    assert {item.category for item in feedback} == {
        "brier:model-a",
        "slippage",
        "win_rate",
    }


def test_feedback_store_list_and_resolve_cycle(tmp_path: Path) -> None:
    store = FeedbackStore(path=tmp_path / "feedback.jsonl")
    first = _feedback("fb-1")
    second = _feedback("fb-2")

    store.append(first)
    store.append(second)
    store.resolve("fb-1")

    unresolved = store.list(resolved=False)

    assert unresolved == [second]
    assert store.list(resolved=True)[0].feedback_id == "fb-1"
    assert (tmp_path / "feedback.jsonl").read_text(encoding="utf-8").count("\n") == 2


def test_eval_store_append_writes_jsonl(tmp_path: Path) -> None:
    store = EvalStore(path=tmp_path / "eval_records.jsonl")

    store.append(_eval_record())

    assert len(store.all()) == 1
    assert "d-cp07" in (tmp_path / "eval_records.jsonl").read_text(encoding="utf-8")


def test_feedback_store_reloads_from_disk(tmp_path: Path) -> None:
    path = tmp_path / "feedback.jsonl"
    first = FeedbackStore(path=path)
    first.append(_feedback("fb-reload"))
    first.resolve("fb-reload")

    reloaded = FeedbackStore(path=path)

    assert [item.feedback_id for item in reloaded.all()] == ["fb-reload"]
    assert reloaded.all()[0].resolved is True


def test_feedback_store_skips_malformed_reload_rows(tmp_path: Path) -> None:
    """Regression for codex-bot C2: non-dict / partial JSONL rows must not abort init."""
    path = tmp_path / "feedback.jsonl"
    store = FeedbackStore(path=path)
    store.append(_feedback("fb-good"))
    # Append poisonous rows: array, string, number, missing-field dict, invalid JSON.
    with path.open("a", encoding="utf-8") as stream:
        stream.write("[1, 2, 3]\n")
        stream.write("\"scalar\"\n")
        stream.write("42\n")
        stream.write("{\"partial\": true}\n")
        stream.write("not json at all\n")

    reloaded = FeedbackStore(path=path)

    assert [item.feedback_id for item in reloaded.all()] == ["fb-good"]


@pytest.mark.asyncio
async def test_eval_spool_unfilled_decision_regime(
    tmp_path: Path,
) -> None:
    """Regime 2: fill=None with a signal carrying resolved_outcome → _unfilled_record.

    Piecewise-domain coverage (CLAUDE.md rule): this test covers the regime where
    the decision was rejected / never filled but the market has since resolved.
    EvalSpool._run() must produce an EvalRecord with filled=False, pnl=0, slippage=0,
    and brier_score = (prob_estimate - resolved_outcome)**2.
    """
    from pms.core.models import MarketSignal

    prob_estimate = 0.7
    resolved_outcome = 1.0
    expected_brier = (prob_estimate - resolved_outcome) ** 2  # 0.09

    signal = MarketSignal(
        market_id="m-cp07",
        token_id="t-yes",
        venue="polymarket",
        title="Test market",
        yes_price=0.4,
        volume_24h=None,
        resolves_at=None,
        orderbook={},
        external_signal={"resolved_outcome": resolved_outcome},
        fetched_at=datetime(2026, 4, 14, tzinfo=UTC),
        market_status="resolved",
    )

    store = EvalStore(path=tmp_path / "eval_records.jsonl")
    spool = EvalSpool(store=store, scorer=Scorer())
    await spool.start()

    spool.enqueue(None, _decision(prob=prob_estimate), signal)

    await asyncio.wait_for(spool.join(), timeout=1.0)
    await spool.stop()

    assert len(store.all()) == 1
    record = store.all()[0]
    assert record.filled is False
    assert record.pnl == 0.0
    assert record.slippage_bps == 0.0
    assert record.brier_score == pytest.approx(expected_brier)
