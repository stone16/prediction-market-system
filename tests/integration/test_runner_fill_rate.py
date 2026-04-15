"""Regression test for fill_rate wiring bug (observed 2026-04-15).

When risk rejects a decision, the runner must still produce an EvalRecord
with filled=False so that MetricsCollector reports a meaningful fill_rate.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import RunMode
from pms.evaluation.metrics import MetricsCollector
from pms.runner import Runner
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


@pytest.mark.asyncio
async def test_fill_rate_reflects_risk_rejections(tmp_path: Path) -> None:
    """fill_rate must be < 1.0 when risk gate rejects some decisions.

    The backtest fixture produces 100 signals / decisions. Setting a very tight
    risk cap forces most decisions to be rejected. Before the fix, only filled
    decisions were enqueued into the evaluator, so fill_rate was always 1.0.
    After the fix, every decision produces an EvalRecord (filled=False by
    default) and the rate is fills / decisions.
    """
    settings = PMSSettings(
        mode=RunMode.BACKTEST,
        risk=RiskSettings(
            max_position_usdc=0.01,   # tight cap — almost every decision rejected
            min_order_usdc=0.0,       # don't reject on size floor
        ),
    )

    runner = Runner(
        config=settings,
        historical_data_path=FIXTURE_PATH,
        eval_store=EvalStore(path=tmp_path / "eval_records.jsonl"),
        feedback_store=FeedbackStore(path=tmp_path / "feedback.jsonl"),
    )

    await runner.start()
    await asyncio.wait_for(runner.wait_until_idle(), timeout=10.0)
    await asyncio.wait_for(runner.stop(), timeout=5.0)

    decisions_total = len(runner.state.decisions)
    fills_total = len(runner.state.fills)
    eval_records = runner.eval_store.all()

    # Precondition: the tight risk cap must have caused rejections.
    assert decisions_total > 0, "fixture produced no decisions"
    assert fills_total < decisions_total, (
        f"expected some risk rejections but fills={fills_total} == decisions={decisions_total}"
    )

    # The evaluator must have received a record for every decision that has
    # a resolved_outcome in the signal (fixture marks all with resolved_outcome).
    assert len(eval_records) == decisions_total, (
        f"expected {decisions_total} eval records (one per decision), "
        f"got {len(eval_records)}"
    )

    # fill_rate must reflect the true ratio, not a tautological 1.0.
    snapshot = MetricsCollector(records=eval_records).snapshot()
    assert snapshot.fill_rate < 1.0, (
        f"fill_rate should be < 1.0 when risk rejects decisions, got {snapshot.fill_rate}"
    )
    assert snapshot.fill_rate == pytest.approx(fills_total / decisions_total)
