from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.core.enums import OrderStatus
from pms.core.models import EvalRecord
from pms.evaluation.metrics import MetricsCollector


def _eval_record(
    *,
    decision_id: str,
    strategy_id: str,
    strategy_version_id: str,
    brier_score: float,
) -> EvalRecord:
    return EvalRecord(
        market_id="m-cp03",
        decision_id=decision_id,
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        prob_estimate=0.7,
        resolved_outcome=1.0,
        brier_score=brier_score,
        fill_status=OrderStatus.MATCHED.value,
        recorded_at=datetime(2026, 4, 19, tzinfo=UTC),
        citations=["cp03"],
        category="model-a",
        model_id="model-a",
        pnl=1.0,
        slippage_bps=10.0,
        filled=True,
    )


def test_snapshot_by_strategy_partitions_eval_records_by_strategy_version() -> None:
    records = [
        _eval_record(
            decision_id=f"alpha-{index}",
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
            brier_score=0.04,
        )
        for index in range(25)
    ] + [
        _eval_record(
            decision_id=f"beta-{index}",
            strategy_id="beta",
            strategy_version_id="beta-v1",
            brier_score=0.36,
        )
        for index in range(25)
    ]

    snapshots = MetricsCollector(records).snapshot_by_strategy()

    assert set(snapshots) == {("alpha", "alpha-v1"), ("beta", "beta-v1")}
    assert snapshots[("alpha", "alpha-v1")].brier_overall == pytest.approx(0.04)
    assert snapshots[("beta", "beta-v1")].brier_overall == pytest.approx(0.36)
    assert snapshots[("alpha", "alpha-v1")].brier_samples == {"model-a": 25}
    assert snapshots[("beta", "beta-v1")].brier_samples == {"model-a": 25}
