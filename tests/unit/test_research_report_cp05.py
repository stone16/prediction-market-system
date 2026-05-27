from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pms.research.report as research_report

from pms.research.report import (
    EvaluationReportGenerator,
    _StrategyRunSnapshot,
    _warnings_from_spec_json,
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


def _slice_snapshot(
    *,
    strategy_id: str = "beta",
    strategy_version_id: str = "beta-v1",
    slice_label: str,
    slice_start: datetime,
    slice_end: datetime,
    brier: float | None = 0.08,
    pnl_cum: float | None = 12.5,
    drawdown_max: float | None = 3.0,
    fill_rate: float | None = 0.9,
    slippage_bps: float | None = 4.0,
    opportunity_count: int = 20,
    decision_count: int = 20,
    fill_count: int = 18,
) -> Any:
    slice_cls = getattr(research_report, "_StrategyRunSliceSnapshot", None)
    assert slice_cls is not None, "_StrategyRunSliceSnapshot missing"
    return slice_cls(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        slice_label=slice_label,
        slice_start=slice_start,
        slice_end=slice_end,
        slice_kind="out_of_sample",
        brier=brier,
        pnl_cum=pnl_cum,
        drawdown_max=drawdown_max,
        fill_rate=fill_rate,
        slippage_bps=slippage_bps,
        opportunity_count=opportunity_count,
        decision_count=decision_count,
        fill_count=fill_count,
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


def test_build_report_includes_walk_forward_slice_metric_rows() -> None:
    snapshots = (
        _snapshot(
            strategy_id="beta",
            strategy_version_id="beta-v1",
            brier=0.08,
            pnl_cum=50.0,
            drawdown_max=10.0,
        ),
    )
    slice_metrics = (
        _slice_snapshot(
            slice_label="2026-04-week-1",
            slice_start=datetime(2026, 4, 1, tzinfo=UTC),
            slice_end=datetime(2026, 4, 8, tzinfo=UTC),
        ),
        _slice_snapshot(
            slice_label="2026-04-week-2",
            slice_start=datetime(2026, 4, 8, tzinfo=UTC),
            slice_end=datetime(2026, 4, 15, tzinfo=UTC),
            brier=0.11,
            pnl_cum=9.0,
            drawdown_max=4.5,
            fill_rate=0.84,
            slippage_bps=6.0,
        ),
    )

    report = EvaluationReportGenerator._build_report(
        benchmark_rows=getattr(
            research_report,
            "_benchmark_rows_from_slice_metrics",
        )(slice_metrics),
        run_id="run-1",
        ranking_metric="brier",
        strategy_runs=snapshots,
        warnings=getattr(research_report, "_warnings_from_slice_metrics")(
            strategy_runs=snapshots,
            slice_metrics=slice_metrics,
        ),
        generated_at=datetime(2026, 4, 20, tzinfo=UTC),
    )

    assert report.warnings == ()
    assert len(report.benchmark_rows) == 2
    first_row = report.benchmark_rows[0]
    assert first_row["metric_type"] == "walk_forward_slice"
    assert first_row["strategy_id"] == "beta"
    assert first_row["strategy_version_id"] == "beta-v1"
    assert first_row["slice_label"] == "2026-04-week-1"
    assert first_row["slice_start"] == "2026-04-01T00:00:00+00:00"
    assert first_row["slice_end"] == "2026-04-08T00:00:00+00:00"
    assert first_row["slice_kind"] == "out_of_sample"
    assert first_row["brier"] == 0.08
    assert first_row["pnl_cum"] == 12.5
    assert first_row["drawdown_max"] == 3.0
    assert first_row["fill_rate"] == 0.9
    assert first_row["slippage_bps"] == 4.0
    assert first_row["opportunity_count"] == 20
    assert first_row["decision_count"] == 20
    assert first_row["fill_count"] == 18
    assert "2 out-of-sample slice metric row(s)" in report.attribution_commentary


def test_slice_metric_warnings_block_underpowered_slice_samples() -> None:
    warnings = getattr(research_report, "_warnings_from_slice_metrics")(
        strategy_runs=(
            _snapshot(
                strategy_id="beta",
                strategy_version_id="beta-v1",
                brier=0.08,
                pnl_cum=50.0,
                drawdown_max=10.0,
            ),
        ),
        slice_metrics=(
            _slice_snapshot(
                slice_label="category:politics",
                slice_start=datetime(2026, 4, 1, tzinfo=UTC),
                slice_end=datetime(2026, 4, 30, tzinfo=UTC),
                decision_count=19,
                fill_count=18,
            ),
            _slice_snapshot(
                slice_label="liquidity:volume_24h:1000-10000",
                slice_start=datetime(2026, 4, 1, tzinfo=UTC),
                slice_end=datetime(2026, 4, 30, tzinfo=UTC),
                decision_count=20,
                fill_count=18,
            ),
        ),
    )

    assert (
        "1 walk-forward/out-of-sample slice metric row(s) have fewer than "
        "20 decision samples; do not promote until slice coverage is sufficient"
    ) in warnings


def test_slice_metric_warnings_block_in_sample_only_report() -> None:
    warnings = getattr(research_report, "_warnings_from_slice_metrics")(
        strategy_runs=(
            _snapshot(
                strategy_id="alpha",
                strategy_version_id="alpha-v1",
                brier=0.12,
                pnl_cum=75.0,
                drawdown_max=20.0,
            ),
        ),
        slice_metrics=(),
    )

    assert warnings == (
        "no walk-forward/out-of-sample slice metrics were recorded; "
        "do not promote from an in-sample-only report",
    )


def test_warnings_from_spec_json_flags_static_live_execution_model() -> None:
    warnings = _warnings_from_spec_json(
        {
            "dataset": {"data_quality_gaps": []},
            "execution_model": {"calibration_source": "static_live_estimate"},
        }
    )

    assert (
        "execution model uses static Polymarket live estimates; "
        "calibrate from paper/live telemetry before promotion"
    ) in warnings


def test_warnings_from_spec_json_accepts_telemetry_calibrated_execution_model() -> None:
    warnings = _warnings_from_spec_json(
        {
            "dataset": {"data_quality_gaps": []},
            "execution_model": {
                "calibration_source": "telemetry_calibrated",
                "adverse_selection_bps": 5.0,
            },
        }
    )

    assert warnings == ()


def test_warnings_from_spec_json_flags_telemetry_profile_without_adverse_selection(
) -> None:
    warnings = _warnings_from_spec_json(
        {
            "dataset": {"data_quality_gaps": []},
            "execution_model": {
                "calibration_source": "telemetry_calibrated",
                "adverse_selection_bps": 0.0,
            },
        }
    )

    assert (
        "execution model telemetry profile has no adverse_selection_bps; "
        "include quote-drift samples before promotion"
    ) in warnings
