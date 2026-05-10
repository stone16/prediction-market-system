from __future__ import annotations

from datetime import UTC, date, datetime

from pms.config import RiskSettings
from pms.core.enums import TimeInForce
from pms.core.models import EvalRecord, TradeDecision
from scripts.paper_report import (
    PaperReportMetrics,
    build_paper_report_diagnostics,
    metrics_from_api_payloads,
    render_report,
)


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


def test_metrics_from_api_payloads_uses_live_runner_status() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T09:03:03.240858+00:00",
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "stale",
                    "last_signal_at": "2026-05-04T01:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 17,
                "diagnostics_total": 3,
            },
            "actuator": {
                "fills_total": 4,
            },
            "evaluator": {
                "brier_overall": 0.18,
            },
        },
        trades={
            "trades": [
                {"fill_notional_usdc": 2.5},
                {"fill_notional_usdc": 1.5},
            ],
        },
        positions={
            "positions": [
                {"locked_usdc": 3.0, "unrealized_pnl": 1.25},
                {"locked_usdc": 2.0, "unrealized_pnl": -0.25},
            ],
        },
        strategies={
            "strategies": [
                {
                    "strategy_id": "default",
                    "active_version_id": "default-v2",
                }
            ],
        },
    )

    assert metrics.strategy == "default@default-v2"
    assert metrics.day_of_soak == 2
    assert metrics.decisions_made == 17
    assert metrics.decisions_rejected == 3
    assert metrics.fills == 4
    assert metrics.open_positions == 2
    assert metrics.total_exposure == 5.0
    assert metrics.cumulative_pnl == 1.0
    assert metrics.brier_score_7d == 0.18
    assert (
        "sensor",
        "MarketDataSensor stale",
        "last_signal_at=2026-05-04T01:00:00+00:00",
    ) in metrics.risk_events


def test_metrics_from_api_payloads_records_missing_status_as_risk_event() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={},
        trades={},
        positions={},
    )

    assert metrics.day_of_soak == 0
    assert metrics.risk_events == (
        ("report generation", "runner_started_at missing", "check /status"),
    )


def test_paper_report_renders_calibration_and_cost_diagnostics() -> None:
    diagnostics = build_paper_report_diagnostics(
        eval_records=[
            *[_eval_record(f"low-{index}", prob=0.12, outcome=1.0) for index in range(3)],
            *[_eval_record(f"mid-{index}", prob=0.55, outcome=1.0) for index in range(3)],
            *[_eval_record(f"mid-loss-{index}", prob=0.58, outcome=0.0) for index in range(2)],
        ],
        decisions=[
            _decision(
                decision_id="d-cost-1",
                market_id="m-cost",
                prob_estimate=0.64,
                limit_price=0.52,
                spread_bps_at_decision=80,
            )
        ],
        log_events=[
            {"event": "clamp_rejection", "market_id": "m-extreme"},
            {"event": "clamp_rejection", "market_id": "m-extreme"},
            {"event": "funnel_selector", "discovered_count": 12, "selected_count": 5},
            {"event": "funnel_router", "routed_count": 4},
            {"event": "funnel_pipeline", "forecasted_count": 3, "traded_count": 1},
        ],
    )
    metrics = PaperReportMetrics(
        report_date=date(2026, 5, 8),
        fills=1,
        reliability_bins=diagnostics.reliability_bins,
        trade_costs=diagnostics.trade_costs,
        clamp_rejections=diagnostics.clamp_rejections,
        selection_funnel=diagnostics.selection_funnel,
    )

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert "## Calibration Reliability" in report
    assert "| [10%-20%) | 3 | insufficient data |" in report
    assert "| [50%-60%) | 5 | 60.0% |" in report
    assert "## Spread Cost Decomposition" in report
    assert "| d-cost-1 | m-cost | 12.0% | 0.8% | 11.2% |" in report
    assert "## Extreme Probability Rejections" in report
    assert "| m-extreme | 2 |" in report
    assert "## Selection Funnel" in report
    assert "| Discovered | 12 |" in report
    assert "| Selected | 5 |" in report
    assert "| Routed | 4 |" in report
    assert "| Forecasted | 3 |" in report
    assert "| Traded | 1 |" in report


def _eval_record(decision_id: str, *, prob: float, outcome: float) -> EvalRecord:
    return EvalRecord(
        market_id=f"market-{decision_id}",
        decision_id=decision_id,
        strategy_id="paper",
        strategy_version_id="paper-v1",
        prob_estimate=prob,
        resolved_outcome=outcome,
        brier_score=(prob - outcome) ** 2,
        fill_status="filled",
        recorded_at=datetime(2026, 5, 8, tzinfo=UTC),
        citations=[],
    )


def _decision(
    *,
    decision_id: str,
    market_id: str,
    prob_estimate: float,
    limit_price: float,
    spread_bps_at_decision: int,
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id=market_id,
        token_id="token",
        venue="polymarket",
        side="BUY",
        notional_usdc=1.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=[],
        prob_estimate=prob_estimate,
        expected_edge=prob_estimate - limit_price,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"op-{decision_id}",
        strategy_id="paper",
        strategy_version_id="paper-v1",
        limit_price=limit_price,
        action="BUY",
        spread_bps_at_decision=spread_bps_at_decision,
    )
