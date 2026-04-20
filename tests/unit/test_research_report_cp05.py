from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from pms.research.report import (
    EvaluationReportGenerator,
    _StrategyRunSnapshot,
)


def _snapshot(
    *,
    strategy_id: str,
    strategy_version_id: str,
    brier: float | None,
    pnl_cum: float | None,
    drawdown_max: float | None,
    fill_rate: float | None = 1.0,
    slippage_bps: float | None = 5.0,
) -> _StrategyRunSnapshot:
    return _StrategyRunSnapshot(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        brier=brier,
        pnl_cum=pnl_cum,
        drawdown_max=drawdown_max,
        fill_rate=fill_rate,
        slippage_bps=slippage_bps,
    )


def test_build_report_uses_prose_commentary_and_non_empty_next_action() -> None:
    report = EvaluationReportGenerator._build_report(
        run_id="run-1",
        ranking_metric="brier",
        strategy_runs=(
            _snapshot(
                strategy_id="alpha",
                strategy_version_id="alpha-v1",
                brier=0.12,
                pnl_cum=75.0,
                drawdown_max=20.0,
            ),
            _snapshot(
                strategy_id="beta",
                strategy_version_id="beta-v1",
                brier=0.08,
                pnl_cum=50.0,
                drawdown_max=10.0,
            ),
        ),
        warnings=(),
        generated_at=datetime(2026, 4, 20, tzinfo=UTC),
    )

    assert isinstance(report.attribution_commentary, str)
    assert not isinstance(report.attribution_commentary, Mapping)
    assert report.attribution_commentary
    assert report.next_action
    assert tuple(entry.rank for entry in report.ranked_strategies) == (1, 2)


def test_build_report_switches_order_between_brier_and_sharpe() -> None:
    snapshots = (
        _snapshot(
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
            brier=0.10,
            pnl_cum=80.0,
            drawdown_max=40.0,
        ),
        _snapshot(
            strategy_id="beta",
            strategy_version_id="beta-v1",
            brier=0.05,
            pnl_cum=30.0,
            drawdown_max=60.0,
        ),
        _snapshot(
            strategy_id="gamma",
            strategy_version_id="gamma-v1",
            brier=0.20,
            pnl_cum=50.0,
            drawdown_max=10.0,
        ),
    )

    brier_report = EvaluationReportGenerator._build_report(
        run_id="run-1",
        ranking_metric="brier",
        strategy_runs=snapshots,
        warnings=(),
        generated_at=datetime(2026, 4, 20, tzinfo=UTC),
    )
    sharpe_report = EvaluationReportGenerator._build_report(
        run_id="run-1",
        ranking_metric="sharpe",
        strategy_runs=snapshots,
        warnings=(),
        generated_at=datetime(2026, 4, 20, tzinfo=UTC),
    )

    assert [entry.strategy_id for entry in brier_report.ranked_strategies] == [
        "beta",
        "alpha",
        "gamma",
    ]
    assert [entry.strategy_id for entry in sharpe_report.ranked_strategies] == [
        "gamma",
        "alpha",
        "beta",
    ]
