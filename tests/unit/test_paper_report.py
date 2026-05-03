from __future__ import annotations

from datetime import date

from pms.config import RiskSettings
from scripts.paper_report import PaperReportMetrics, render_report


def test_paper_report_renders_empty_day_without_crashing() -> None:
    report = render_report(
        PaperReportMetrics.empty(report_date=date(2026, 5, 3)),
        risk=RiskSettings(max_total_exposure=50.0),
    )

    assert "# Paper Daily Report - 2026-05-03" in report
    assert "| Decisions made | 0 |" in report
    assert "| Brier score (7d rolling) | N/A | < 0.20 |" in report
    assert "| (none today) | - | - |" in report
    assert "No trades today." in report


def test_paper_report_contains_all_gate_three_metrics() -> None:
    metrics = PaperReportMetrics(
        report_date=date(2026, 5, 7),
        strategy="ripple_v2",
        day_of_soak=4,
        decisions_made=12,
        decisions_accepted=3,
        decisions_rejected=9,
        fills=2,
        average_slippage_bps=12.5,
        todays_pnl=1.25,
        cumulative_pnl=3.5,
        max_drawdown_pct=4.0,
        open_positions=2,
        total_exposure=8.75,
        brier_score_7d=0.18,
        hit_rate=0.5,
        average_edge_bps=35.0,
        sharpe_ratio=0.7,
        risk_events=(("12:00", "rate_limit_exceeded", "halted"),),
    )

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert "| Fills | 2 |" in report
    assert "| Today's P&L | +$1.25 |" in report
    assert "| Max exposure | $50.00 |" in report
    assert "| Brier score (7d rolling) | 0.18 | < 0.20 |" in report
    assert "| Hit rate (all trades) | 50.0% | > 45% |" in report
    assert "| Average edge (bps) | 35.0 | > 5 |" in report
    assert "| Sharpe ratio (cumulative) | 0.70 | > 0 |" in report
