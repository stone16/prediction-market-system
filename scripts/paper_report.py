from __future__ import annotations

import argparse
import json
import math
import os
import re
import stat
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from uuid import uuid4
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pms.config import PMSSettings, RiskSettings
from pms.core.models import EvalRecord, TradeDecision
from pms.metrics import (
    LLM_DAILY_COST_USDC_METRIC,
    LLM_ESTIMATED_COST_USDC_TOTAL_METRIC,
)


DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"
REPORT_GENERATOR_ID = "scripts/paper_report.py"
_API_TIMEOUT_S = 5.0
_API_PAGE_SIZE = 200
_SECONDARY_BASELINE_ORDER = (
    "market_implied",
    "mid_quote",
    "last_trade",
    "category_prior",
)
_BASELINE_SOURCE_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")


@dataclass(frozen=True)
class PaperReportProvenance:
    artifact_mode: str
    output_path: str
    generated_by: str = REPORT_GENERATOR_ID
    generated_at: str = field(
        default_factory=lambda: datetime.now(tz=UTC).isoformat()
    )


@dataclass(frozen=True)
class ReliabilityBin:
    predicted_prob_range: str
    count: int
    actual_resolution_rate: float | None


@dataclass(frozen=True)
class TradeCostBreakdown:
    decision_id: str
    market_id: str
    gross_edge: float
    spread_cost: float
    net_edge: float


@dataclass(frozen=True)
class BaselineEvidenceCoverage:
    decisions: int
    market_implied_count: int
    mid_quote_count: int
    last_trade_count: int
    category_prior_count: int = 0


@dataclass(frozen=True)
class SelectionFunnel:
    discovered: int = 0
    selected: int = 0
    routed: int = 0
    forecasted: int = 0
    traded: int = 0


@dataclass(frozen=True)
class ExecutionConcentration:
    entry_fills: int
    distinct_markets: int
    distinct_risk_groups: int
    missing_risk_group_fills: int
    max_market_fill_share: float | None
    max_risk_group_fill_share: float | None


@dataclass(frozen=True)
class PaperReportDiagnostics:
    reliability_bins: tuple[ReliabilityBin, ...]
    trade_costs: tuple[TradeCostBreakdown, ...]
    clamp_rejections: tuple[tuple[str, int], ...]
    selection_funnel: SelectionFunnel


@dataclass(frozen=True)
class PaperReportMetrics:
    report_date: date
    strategy: str = "unknown"
    day_of_soak: int = 0
    decisions_made: int = 0
    decisions_accepted: int = 0
    decisions_rejected: int = 0
    fills: int = 0
    fill_rate: float | None = None
    average_slippage_bps: float | None = None
    todays_pnl: float = 0.0
    cumulative_pnl: float = 0.0
    pnl_source: str = "final_eval"
    current_unrealized_pnl: float = 0.0
    llm_cost_usdc: float = 0.0
    max_drawdown_pct: float | None = None
    open_positions: int = 0
    total_exposure: float = 0.0
    brier_score_7d: float | None = None
    baseline_brier_score_7d: float | None = None
    brier_improvement_7d: float | None = None
    baseline_brier_by_source: dict[str, float] = field(default_factory=dict)
    brier_improvement_by_source: dict[str, float] = field(default_factory=dict)
    hit_rate: float | None = None
    average_edge_bps: float | None = None
    average_fee_bps: float | None = None
    average_net_edge_bps: float | None = None
    sharpe_ratio: float | None = None
    unresolved_incidents: int = 0
    rejection_reasons: tuple[tuple[str, int], ...] = ()
    risk_events: tuple[tuple[str, str, str], ...] = ()
    reliability_bins: tuple[ReliabilityBin, ...] = ()
    trade_costs: tuple[TradeCostBreakdown, ...] = ()
    baseline_evidence: BaselineEvidenceCoverage | None = None
    clamp_rejections: tuple[tuple[str, int], ...] = ()
    selection_funnel: SelectionFunnel | None = None
    execution_concentration: ExecutionConcentration | None = None

    @classmethod
    def empty(cls, *, report_date: date) -> PaperReportMetrics:
        return cls(report_date=report_date)


@dataclass(frozen=True)
class PaperSoakGateConfig:
    min_soak_days: int = 30
    min_accepted_decisions: int = 30
    min_fills: int = 50
    max_slippage_bps: float = 50.0
    max_brier_score: float = 0.20
    min_brier_improvement: float = 0.0
    min_hit_rate: float = 0.45
    min_average_edge_bps: float = 5.0
    min_average_net_edge_bps: float = 0.0
    min_distinct_markets: int = 3
    min_distinct_risk_groups: int = 3
    max_market_fill_share: float = 0.60
    max_risk_group_fill_share: float = 0.60


@dataclass(frozen=True)
class PaperSoakGateCheck:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True)
class PaperSoakGateResult:
    checks: tuple[PaperSoakGateCheck, ...]

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks)

    def require_check(self, name: str) -> PaperSoakGateCheck:
        for check in self.checks:
            if check.name == name:
                return check
        msg = f"paper soak gate check not found: {name}"
        raise KeyError(msg)


def evaluate_paper_soak_gate(
    metrics: PaperReportMetrics,
    *,
    risk: RiskSettings,
    config: PaperSoakGateConfig = PaperSoakGateConfig(),
) -> PaperSoakGateResult:
    checks = [
        _check_min_int("soak_days", metrics.day_of_soak, config.min_soak_days),
        _check_min_int(
            "decisions_accepted",
            metrics.decisions_accepted,
            config.min_accepted_decisions,
        ),
        _check_min_int("fills", metrics.fills, config.min_fills),
        _check_distinct_markets(metrics, config=config),
        _check_distinct_risk_groups(metrics, config=config),
        _check_max_market_fill_share(metrics, config=config),
        _check_max_risk_group_fill_share(metrics, config=config),
        _check_optional_gt("fill_rate", metrics.fill_rate, 0.0),
        _check_optional_lte(
            "average_slippage_bps",
            metrics.average_slippage_bps,
            config.max_slippage_bps,
        ),
        _check_daily_pnl(metrics, risk=risk),
        _check_optional_gt("cumulative_pnl", metrics.cumulative_pnl, 0.0),
        _check_drawdown(metrics, risk=risk),
        _check_open_positions(metrics, risk=risk),
        _check_optional_lte("total_exposure", metrics.total_exposure, risk.max_total_exposure),
        _check_optional_lt("brier_score", metrics.brier_score_7d, config.max_brier_score),
        _check_optional_gt(
            "brier_improvement",
            metrics.brier_improvement_7d,
            config.min_brier_improvement,
        ),
        *_secondary_brier_improvement_checks(metrics.brier_improvement_by_source),
        _check_optional_gt("hit_rate", metrics.hit_rate, config.min_hit_rate),
        _check_optional_gt(
            "average_edge_bps",
            metrics.average_edge_bps,
            config.min_average_edge_bps,
        ),
        _check_optional_gt(
            "average_net_edge_bps",
            metrics.average_net_edge_bps,
            config.min_average_net_edge_bps,
        ),
        _check_optional_gt("sharpe_ratio", metrics.sharpe_ratio, 0.0),
        _check_strategy_evidence(metrics.strategy),
        _check_zero("unresolved_incidents", metrics.unresolved_incidents, "unresolved"),
        _check_zero("risk_events", len(metrics.risk_events), "risk event(s)"),
    ]
    return PaperSoakGateResult(tuple(checks))


def metrics_from_api_payloads(
    *,
    report_date: date,
    status: dict[str, Any],
    trades: dict[str, Any],
    positions: dict[str, Any],
    metrics: dict[str, Any] | None = None,
    decisions: object | None = None,
    strategies: dict[str, Any] | None = None,
    strategy_metrics: dict[str, Any] | None = None,
    risk_events: tuple[tuple[str, str, str], ...] = (),
) -> PaperReportMetrics:
    events = list(risk_events)
    events.extend(_strategy_evidence_risk_events(strategies))
    events.extend(_strategy_metrics_risk_events(strategies, strategy_metrics))
    events.extend(_run_mode_risk_events(status))
    events.extend(_sensor_risk_events(status))
    events.extend(_actuator_risk_events(status))
    events.extend(_unresolved_incident_evidence_risk_events(status))
    metric_rows = metrics or {}
    events.extend(_non_finite_metric_risk_events(metric_rows))
    events.extend(_non_finite_metric_mapping_risk_events(metric_rows))
    events.extend(_invalid_metric_mapping_source_risk_events(metric_rows))
    todays_pnl, daily_pnl_events = _daily_pnl_from_metrics(
        metric_rows,
        report_date=report_date,
    )
    events.extend(daily_pnl_events)
    baseline_brier_by_source = _float_mapping_from_dict(
        metric_rows,
        "baseline_brier_by_source",
    )
    brier_improvement_by_source = _float_mapping_from_dict(
        metric_rows,
        "brier_improvement_by_source",
    )
    events.extend(
        _secondary_baseline_score_risk_events(
            baseline_brier_by_source=baseline_brier_by_source,
            brier_improvement_by_source=brier_improvement_by_source,
        )
    )
    started_at = _parse_datetime(status.get("runner_started_at"))
    if started_at is None:
        events.append(("report generation", "runner_started_at missing", "check /status"))
        day_of_soak = 0
    else:
        day_of_soak = _elapsed_soak_days(
            started_at,
            observed_until=_soak_observed_until(status, report_date=report_date),
        )
    events.extend(_runtime_continuity_risk_events(status, day_of_soak=day_of_soak))

    controller = _dict_value(status, "controller")
    actuator = _dict_value(status, "actuator")
    evaluator = _dict_value(status, "evaluator")
    supervision = _dict_value(status, "supervision")
    rejection_reasons = _diagnostic_counts(controller)
    events.extend(_diagnostic_evidence_risk_events(controller, rejection_reasons))
    decision_rows = _rows_in_soak_window(
        _rows_from_payload(decisions, "decisions"),
        timestamp_key="created_at",
        started_at=started_at,
        report_date=report_date,
    )
    status_decisions_made = _int_from_dict(controller, "decisions_total")
    decisions_made = len(decision_rows)
    events.extend(
        _decision_payload_completeness_risk_events(
            decision_rows,
            status_decisions_made=status_decisions_made,
        )
    )
    events.extend(_non_finite_decision_risk_events(decision_rows))
    baseline_evidence = _baseline_evidence_coverage(decision_rows)
    events.extend(_baseline_evidence_risk_events(decision_rows))
    events.extend(
        _baseline_evidence_score_risk_events(
            baseline_evidence,
            baseline_brier_by_source=baseline_brier_by_source,
            brier_improvement_by_source=brier_improvement_by_source,
        )
    )
    trade_rows = _trade_rows_in_soak_window(
        _list_value(trades, "trades"),
        started_at=started_at,
        report_date=report_date,
    )
    entry_trade_rows = _entry_trade_rows(trade_rows)
    events.extend(_non_finite_trade_risk_events(trade_rows))
    execution_concentration = _execution_concentration(entry_trade_rows)
    events.extend(
        _execution_concentration_risk_events(
            execution_concentration,
            config=PaperSoakGateConfig(),
        )
    )
    position_rows = _list_value(positions, "positions")
    events.extend(_non_finite_position_risk_events(position_rows))

    total_exposure = sum(
        (Decimal(str(_float_from_dict(row, "locked_usdc"))) for row in position_rows),
        Decimal("0"),
    )
    current_unrealized_pnl = sum(
        (
            Decimal(str(_float_from_dict(row, "unrealized_pnl")))
            for row in position_rows
        ),
        Decimal("0"),
    )
    cumulative_pnl = Decimal(
        str(
            _cumulative_pnl_from_metrics(
                metric_rows,
                report_date=report_date,
            )
        )
    )
    llm_total_cost = Decimal(str(_llm_total_cost_usdc(metric_rows)))
    llm_daily_cost = Decimal(str(_llm_daily_cost_usdc(metric_rows)))
    cumulative_pnl -= llm_total_cost
    todays_pnl_decimal = Decimal(str(todays_pnl)) - llm_daily_cost
    max_drawdown_pct = _max_drawdown_pct_from_metrics(metric_rows)
    max_drawdown_pct_decimal = (
        None if max_drawdown_pct is None else Decimal(str(max_drawdown_pct))
    )
    raw_fill_rate = metric_rows.get("fill_rate")
    fill_rate_float = _optional_float_from_dict(metric_rows, "fill_rate")
    fill_rate = None if fill_rate_float is None else Decimal(str(fill_rate_float))
    invalid_fill_rate_supplied = raw_fill_rate is not None and fill_rate_float is None
    if (
        not invalid_fill_rate_supplied
        and (fill_rate is None or fill_rate <= Decimal("0.0"))
        and decision_rows
    ):
        entry_decisions = _entry_decision_rows(decision_rows)
        fill_rate = (
            Decimal(len(entry_trade_rows)) / Decimal(len(entry_decisions))
            if entry_decisions
            else Decimal("0.0")
        )

    return PaperReportMetrics(
        report_date=report_date,
        strategy=_strategy_label(status=status, strategies=strategies or {}),
        day_of_soak=day_of_soak,
        decisions_made=decisions_made,
        decisions_accepted=_accepted_decision_count(decision_rows, entry_trade_rows),
        decisions_rejected=_int_from_dict(controller, "diagnostics_total"),
        fills=len(entry_trade_rows),
        fill_rate=None if fill_rate is None else float(fill_rate),
        average_slippage_bps=_optional_float_from_dict(metric_rows, "slippage_bps"),
        todays_pnl=float(todays_pnl_decimal),
        cumulative_pnl=float(cumulative_pnl),
        pnl_source=_pnl_source_from_metrics(metric_rows),
        current_unrealized_pnl=float(current_unrealized_pnl),
        llm_cost_usdc=float(llm_total_cost),
        max_drawdown_pct=(
            None if max_drawdown_pct_decimal is None else float(max_drawdown_pct_decimal)
        ),
        open_positions=len(position_rows),
        total_exposure=float(total_exposure),
        brier_score_7d=_optional_float_from_dict_first(
            evaluator,
            ("brier_14d", "brier_overall"),
        ),
        baseline_brier_score_7d=_optional_float_from_dict_first(
            evaluator,
            ("baseline_brier_14d", "baseline_brier_overall"),
        ),
        brier_improvement_7d=_optional_float_from_dict_first(
            evaluator,
            ("brier_improvement_14d", "brier_improvement_overall"),
        ),
        baseline_brier_by_source=baseline_brier_by_source,
        brier_improvement_by_source=brier_improvement_by_source,
        hit_rate=_hit_rate_from_metrics(metric_rows, evaluator),
        average_edge_bps=_average_edge_bps(decision_rows),
        average_fee_bps=_average_fee_bps(trade_rows),
        average_net_edge_bps=_average_net_edge_bps(
            decision_rows,
            trade_rows=trade_rows,
            average_slippage_bps=_optional_float_from_dict(
                metric_rows,
                "slippage_bps",
            ),
        ),
        sharpe_ratio=_optional_float_from_dict(metric_rows, "sharpe_ratio"),
        unresolved_incidents=_int_from_dict(
            supervision,
            "unresolved_feedback_total",
        ),
        rejection_reasons=rejection_reasons,
        trade_costs=_trade_costs_from_decision_rows(decision_rows),
        baseline_evidence=baseline_evidence,
        execution_concentration=execution_concentration,
        risk_events=tuple(events),
    )


def build_paper_report_diagnostics(
    *,
    eval_records: Sequence[EvalRecord],
    decisions: Sequence[TradeDecision],
    log_events: Sequence[Mapping[str, object]],
) -> PaperReportDiagnostics:
    return PaperReportDiagnostics(
        reliability_bins=_reliability_bins(eval_records),
        trade_costs=_trade_costs(decisions),
        clamp_rejections=_clamp_rejections(log_events),
        selection_funnel=_selection_funnel(log_events),
    )


def load_live_metrics(
    *,
    report_date: date,
    api_base_url: str = DEFAULT_API_BASE_URL,
    api_token: str | None = None,
) -> PaperReportMetrics:
    status, status_error = _fetch_api_json(
        api_base_url=api_base_url,
        path="/status",
        api_token=api_token,
    )
    if status_error is not None:
        return PaperReportMetrics(
            report_date=report_date,
            risk_events=(
                ("report generation", "pms api unavailable", status_error),
            ),
        )

    events: list[tuple[str, str, str]] = []
    snapshot_cutoff = datetime.now(tz=UTC).isoformat()
    snapshot_query: Mapping[str, object] = {"until": snapshot_cutoff}
    trade_rows, trades_error = _fetch_api_list_pages(
        api_base_url=api_base_url,
        path="/trades",
        api_token=api_token,
        payload_key="trades",
        query=snapshot_query,
    )
    if trades_error is not None:
        events.append(("report generation", "/trades unavailable", trades_error))
        trades: dict[str, object] = {}
    else:
        trades = {"trades": trade_rows}

    positions, positions_error = _fetch_api_json(
        api_base_url=api_base_url,
        path="/positions",
        api_token=api_token,
    )
    if positions_error is not None:
        events.append(("report generation", "/positions unavailable", positions_error))
        positions = {}

    decisions, decisions_error = _fetch_api_list_pages(
        api_base_url=api_base_url,
        path="/decisions",
        api_token=api_token,
        query=snapshot_query,
    )
    if decisions_error is not None:
        events.append(("report generation", "/decisions unavailable", decisions_error))
        decisions = []

    metric_path, expected_metrics_window = _metrics_path_for_soak_window(
        status=status,
        report_date=report_date,
    )
    metric_payload, metric_error = _fetch_api_json(
        api_base_url=api_base_url,
        path=metric_path,
        api_token=api_token,
    )
    if metric_error is not None:
        events.append(("report generation", "/metrics unavailable", metric_error))
        metric_payload = {}
    elif expected_metrics_window is not None:
        events.extend(
            _metrics_window_risk_events(
                metric_payload,
                expected_since=expected_metrics_window[0],
                expected_until=expected_metrics_window[1],
            )
        )

    strategies, strategies_error = _fetch_api_json(
        api_base_url=api_base_url,
        path="/strategies",
        api_token=api_token,
    )
    strategies_payload: dict[str, Any] | None = strategies
    if strategies_error is not None:
        events.append(("report generation", "/strategies unavailable", strategies_error))
        strategies_payload = None
    strategy_metrics, strategy_metrics_error = _fetch_api_json(
        api_base_url=api_base_url,
        path="/strategies/metrics",
        api_token=api_token,
    )
    strategy_metrics_payload: dict[str, Any] | None = strategy_metrics
    if strategy_metrics_error is not None:
        events.append(
            ("report generation", "/strategies/metrics unavailable", strategy_metrics_error)
        )
        strategy_metrics_payload = None

    return metrics_from_api_payloads(
        report_date=report_date,
        status=status,
        decisions=decisions,
        trades=trades,
        positions=positions,
        metrics=metric_payload,
        strategies=strategies_payload,
        strategy_metrics=strategy_metrics_payload,
        risk_events=tuple(events),
    )


def render_report(
    metrics: PaperReportMetrics,
    *,
    risk: RiskSettings,
    provenance: PaperReportProvenance | None = None,
) -> str:
    lines = [
        f"# Paper Daily Report - {metrics.report_date.isoformat()}",
        "",
    ]
    if provenance is not None:
        lines.extend(_render_report_provenance_section(provenance))
    lines.extend(
        [
            "## Summary",
            "",
            "| Metric | Value | Gate |",
            "|---|---:|---|",
            f"| Strategy | {_escape_table_value(metrics.strategy)} | - |",
            f"| Day of soak | {metrics.day_of_soak} | 30 required |",
            f"| Decisions made | {metrics.decisions_made} | - |",
            f"| Decisions accepted | {metrics.decisions_accepted} | >= 30 by soak end |",
            f"| Controller diagnostic sample | {metrics.decisions_rejected} | - |",
            f"| Entry fills | {metrics.fills} | >= 50 by soak end |",
            f"| Distinct traded markets | {_concentration_count(metrics.execution_concentration, 'distinct_markets')} | >= 3 by soak end |",
            f"| Distinct traded risk groups | {_concentration_count(metrics.execution_concentration, 'distinct_risk_groups')} | >= 3 by soak end |",
            f"| Max market fill share | {_concentration_share(metrics.execution_concentration, 'max_market_fill_share')} | <= 60% by soak end |",
            f"| Max risk group fill share | {_concentration_share(metrics.execution_concentration, 'max_risk_group_fill_share')} | <= 60% by soak end |",
            f"| Fill rate | {_fmt_ratio_percent(metrics.fill_rate)} | > 0 |",
            f"| Average slippage (bps) | {_fmt_optional(metrics.average_slippage_bps, 1)} | <= 50 |",
            f"| Today's P&L | {_fmt_money_signed(metrics.todays_pnl)} | >= -daily limit |",
            f"| Cumulative P&L | {_fmt_money_signed(metrics.cumulative_pnl)} | > 0 by soak end |",
            f"| P&L source | {_escape_table_value(metrics.pnl_source)} | - |",
            f"| LLM cost (estimated) | {_fmt_money(metrics.llm_cost_usdc)} | deducted from P&L |",
            f"| Current open-position MTM | {_fmt_money_signed(metrics.current_unrealized_pnl)} | informational |",
            f"| Max drawdown | {_fmt_percent(metrics.max_drawdown_pct)} | <= {_fmt_percent(risk.max_drawdown_pct)} |",
            f"| Open positions | {metrics.open_positions} | <= {risk.max_open_positions or 'N/A'} |",
            f"| Total exposure | {_fmt_money(metrics.total_exposure)} | <= {_fmt_money(risk.max_total_exposure)} |",
            f"| Max exposure | {_fmt_money(risk.max_total_exposure)} | - |",
            f"| Brier score (14d rolling) | {_fmt_optional(metrics.brier_score_7d, 2)} | < 0.20 |",
            f"| Market baseline Brier (14d rolling) | {_fmt_optional(metrics.baseline_brier_score_7d, 2)} | - |",
            f"| Brier improvement vs baseline | {_fmt_optional(metrics.brier_improvement_7d, 2)} | > 0 |",
            f"| Hit rate (all trades) | {_fmt_ratio_percent(metrics.hit_rate)} | > 45% |",
            f"| Average edge (bps) | {_fmt_optional(metrics.average_edge_bps, 1)} | > 5 |",
            f"| Average fee (bps) | {_fmt_optional(metrics.average_fee_bps, 1)} | - |",
            f"| Average net edge after costs (bps) | {_fmt_optional(metrics.average_net_edge_bps, 1)} | > 0 |",
            f"| Sharpe ratio (cumulative) | {_fmt_optional(metrics.sharpe_ratio, 2)} | > 0 |",
            f"| Unresolved incidents | {metrics.unresolved_incidents} | 0 required |",
        ]
    )
    lines.extend(_render_go_no_go_section(metrics, risk=risk))
    lines.extend(
        [
            "",
            "## Risk Events",
            "",
            "| Time | Trigger | Status |",
            "|---|---|---|",
        ]
    )
    if metrics.risk_events:
        lines.extend(
            f"| {_escape_table_value(event_time)} | "
            f"{_escape_table_value(trigger)} | {_escape_table_value(status)} |"
            for event_time, trigger, status in metrics.risk_events
        )
    else:
        lines.append("| (none today) | - | - |")

    lines.extend(["", "## Trade Notes", ""])
    if metrics.fills == 0:
        lines.append("No trades today.")
    else:
        lines.append(
            f"{metrics.fills} entry fills executed with average slippage "
            f"{_fmt_optional(metrics.average_slippage_bps, 1)} bps."
        )
    lines.extend(_render_reliability_section(metrics.reliability_bins))
    lines.extend(_render_baseline_evidence_section(metrics.baseline_evidence))
    lines.extend(
        _render_secondary_baseline_score_section(
            baseline_brier_by_source=metrics.baseline_brier_by_source,
            brier_improvement_by_source=metrics.brier_improvement_by_source,
        )
    )
    lines.extend(_render_trade_cost_section(metrics.trade_costs))
    lines.extend(_render_rejection_reason_section(metrics.rejection_reasons))
    lines.extend(_render_clamp_rejection_section(metrics.clamp_rejections))
    lines.extend(_render_selection_funnel_section(metrics.selection_funnel))
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render the PMS paper daily report.")
    parser.add_argument(
        "--date",
        default=datetime.now(tz=UTC).date().isoformat(),
        help="Report date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--config",
        default="config.live-soak.yaml",
        help="PMS config path used for risk gates.",
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--output-dir",
        default="docs/paper-reports",
        help="Directory for the generated Markdown report.",
    )
    output_group.add_argument(
        "--output",
        default=None,
        help=(
            "Exact path for the generated Markdown report. Use this for the "
            "final LIVE GO artifact so provenance output_path matches "
            "live_paper_soak_report_path."
        ),
    )
    parser.add_argument(
        "--api-base-url",
        default=DEFAULT_API_BASE_URL,
        help="PMS API base URL used to populate live paper metrics.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Render an empty fallback report without calling the PMS API.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the report instead of writing it to disk.",
    )
    parser.add_argument(
        "--require-go",
        action="store_true",
        help="Return exit code 1 when the paper soak go/no-go gate fails.",
    )
    args = parser.parse_args(argv)

    try:
        report_date = date.fromisoformat(args.date)
    except ValueError:
        print(
            f"ERROR: --date must be YYYY-MM-DD, got {args.date!r}",
            file=sys.stderr,
        )
        return 2
    if args.require_go and report_date > datetime.now(tz=UTC).date():
        print(
            "paper report --require-go date must not be in the future",
            file=sys.stderr,
        )
        return 1
    try:
        settings = PMSSettings.load(args.config)
        metrics = (
            PaperReportMetrics.empty(report_date=report_date)
            if args.offline
            else load_live_metrics(
                report_date=report_date,
                api_base_url=args.api_base_url,
                api_token=settings.api_token,
            )
        )
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    gate = evaluate_paper_soak_gate(metrics, risk=settings.risk)
    if args.dry_run:
        report = render_report(
            metrics,
            risk=settings.risk,
            provenance=PaperReportProvenance(
                artifact_mode="dry_run",
                output_path="stdout",
            ),
        )
        print(report)
        return 0 if not args.require_go or gate.ok else 1

    try:
        if args.output is not None:
            output_path = Path(args.output).expanduser().resolve(strict=False)
        else:
            output_dir = Path(args.output_dir).expanduser().resolve(strict=False)
            output_path = output_dir / f"{report_date.isoformat()}.md"
        if args.require_go:
            _require_output_outside_working_tree(output_path)
            _require_go_output_distinct_from_live_inputs(
                output_path,
                settings=settings,
                config_path=Path(args.config),
            )
        _prepare_private_output_dir(output_path.parent)
        report = render_report(
            metrics,
            risk=settings.risk,
            provenance=PaperReportProvenance(
                artifact_mode="persisted",
                output_path=str(output_path),
            ),
        )
        _write_text_no_follow(output_path, report)
        print(output_path)
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0 if not args.require_go or gate.ok else 1


def _require_output_outside_working_tree(path: Path) -> None:
    configured_path = _absolute_path_without_symlink_resolution(path)
    resolved_path = path.expanduser().resolve(strict=False)
    working_tree = _working_tree_root(Path.cwd().resolve(strict=False))
    working_trees = [working_tree]
    for candidate in (configured_path, resolved_path):
        candidate_working_tree = _containing_working_tree_root(candidate)
        if candidate_working_tree is not None:
            working_trees.append(candidate_working_tree)

    for working_tree_candidate in dict.fromkeys(working_trees):
        if working_tree_candidate.parent == working_tree_candidate:
            continue
        for candidate in (configured_path, resolved_path):
            try:
                candidate.relative_to(working_tree_candidate)
            except ValueError:
                continue
            raise OSError(
                "paper report --require-go output must live outside the "
                f"working tree: {candidate}"
            )


def _absolute_path_without_symlink_resolution(path: Path) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return Path(os.path.abspath(expanded))


def _require_go_output_distinct_from_live_inputs(
    output_path: Path,
    *,
    settings: PMSSettings,
    config_path: Path,
) -> None:
    output_identities = _path_identities(output_path)
    approval_path = settings.polymarket.first_live_order_approval_path
    protected_paths: list[tuple[str, str | None]] = [
        ("LIVE config file", str(config_path)),
        (
            "LIVE credentialed preflight artifact",
            settings.live_preflight_artifact_path,
        ),
        ("LIVE first-order audit path", settings.live_first_order_audit_path),
        ("LIVE emergency audit path", settings.live_emergency_audit_path),
        ("LIVE operator approval path", approval_path),
        ("LIVE local secret file", settings.local_secret_file),
        (
            "LIVE operator rehearsal report",
            settings.live_operator_rehearsal_report_path,
        ),
        ("LIVE execution-model artifact", settings.live_execution_model_path),
        (
            "LIVE paper-vs-backtest execution diff artifact",
            settings.live_paper_backtest_diff_path,
        ),
        (
            "LIVE category-prior artifact",
            settings.controller.category_prior_observations_path,
        ),
        ("LIVE FLB calibration artifact", settings.strategies.flb_calibration_path),
        ("LIVE discord alert directory", settings.discord.alert_dir),
    ]
    if approval_path is not None and approval_path.strip() != "":
        protected_paths.append(
            ("LIVE operator approval sidecar path", f"{approval_path}.meta.json")
        )

    for label, raw_path in protected_paths:
        if raw_path is None or raw_path.strip() == "":
            continue
        if not _path_identities_overlap(
            output_identities,
            _path_identities(Path(raw_path)),
        ):
            continue
        msg = (
            "paper report output path must be distinct from "
            f"{label}: {output_path}"
        )
        raise ValueError(msg)


def _path_identities(path: Path) -> frozenset[Path]:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return frozenset(
        (
            Path(os.path.abspath(expanded)),
            expanded.resolve(strict=False),
        )
    )


def _path_identities_overlap(left: frozenset[Path], right: frozenset[Path]) -> bool:
    return any(
        _paths_overlap(left_path, right_path)
        for left_path in left
        for right_path in right
    )


def _paths_overlap(left: Path, right: Path) -> bool:
    if left == right:
        return True
    try:
        right.relative_to(left)
    except ValueError:
        pass
    else:
        return True
    try:
        left.relative_to(right)
    except ValueError:
        return False
    return True


def _working_tree_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return start


def _containing_working_tree_root(path: Path) -> Path | None:
    start = path if path.is_dir() else path.parent
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _prepare_private_output_dir(output_dir: Path) -> None:
    try:
        mode = output_dir.lstat().st_mode
    except FileNotFoundError:
        output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        os.chmod(output_dir, 0o700)
        return
    if not stat.S_ISDIR(mode):
        raise OSError(f"paper report output directory is not a directory: {output_dir}")
    permissions = stat.S_IMODE(mode)
    if permissions & 0o077:
        raise OSError(
            f"paper report output directory {output_dir} is too permissive; "
            f"run `chmod 700 {output_dir}`."
        )
    if not permissions & stat.S_IWUSR:
        raise OSError(
            f"paper report output directory {output_dir} is not owner-writable; "
            f"run `chmod 700 {output_dir}`."
        )


def _write_text_no_follow(path: Path, content: str) -> None:
    _require_regular_file_or_absent(path)
    fd, temp_path = _open_output_temp_file(path)
    published = False
    try:
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            fd = -1
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        _require_regular_file_or_absent(path)
        os.replace(temp_path, path)
        published = True
        _fsync_parent_directory(path)
    finally:
        if fd >= 0:
            os.close(fd)
        if not published:
            _unlink_regular_single_link_file_if_present(temp_path)


def _open_output_temp_file(path: Path) -> tuple[int, Path]:
    _require_regular_file_or_absent(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    for _ in range(16):
        temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            fd = os.open(temp_path, flags, 0o600)
        except FileExistsError:
            continue
        try:
            _require_open_regular_single_link_file(fd, temp_path)
            os.fchmod(fd, 0o600)
        except BaseException:
            os.close(fd)
            _unlink_regular_single_link_file_if_present(temp_path)
            raise
        return fd, temp_path
    raise FileExistsError(f"could not create temporary paper report for {path}")


def _unlink_regular_single_link_file_if_present(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_nlink != 1:
        return
    path.unlink()


def _fsync_parent_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path.parent, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        return
    finally:
        os.close(fd)


def _require_open_regular_single_link_file(fd: int, path: Path) -> None:
    path_stat = os.fstat(fd)
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError(f"paper report output path is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(f"paper report output path is not a single-link file: {path}")


def _require_regular_file_or_absent(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if not stat.S_ISREG(mode):
        raise OSError(f"paper report output path is not a regular file: {path}")
    if path.lstat().st_nlink != 1:
        raise OSError(f"paper report output path is not a single-link file: {path}")


def _fmt_money(value: float) -> str:
    return f"${value:.2f}"


def _fmt_money_signed(value: float) -> str:
    sign = "+" if value >= 0.0 else "-"
    return f"{sign}${abs(value):.2f}"


def _fmt_optional(value: float | None, precision: int) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{precision}f}"


def _fmt_percent(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.1f}%"


def _fmt_ratio_percent(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100.0:.1f}%"


def _concentration_count(
    concentration: ExecutionConcentration | None,
    field_name: str,
) -> str:
    if concentration is None:
        return "N/A"
    value = getattr(concentration, field_name)
    return str(value) if isinstance(value, int) else "N/A"


def _concentration_share(
    concentration: ExecutionConcentration | None,
    field_name: str,
) -> str:
    if concentration is None:
        return "N/A"
    value = getattr(concentration, field_name)
    return _fmt_ratio_percent(value if isinstance(value, float) else None)


def _fmt_probability_percent(value: float) -> str:
    return f"{value * 100.0:.1f}%"


def _render_go_no_go_section(
    metrics: PaperReportMetrics,
    *,
    risk: RiskSettings,
) -> list[str]:
    gate = evaluate_paper_soak_gate(metrics, risk=risk)
    lines = [
        "",
        "## Go/No-Go Gate",
        "",
        f"**Decision:** {'GO' if gate.ok else 'NO-GO'}",
        "",
        "| Check | Status | Detail |",
        "|---|---|---|",
    ]
    for check in gate.checks:
        status = "PASS" if check.ok else "FAIL"
        lines.append(f"| {check.name} | {status} | {_escape_table_value(check.detail)} |")
    return lines


def _render_report_provenance_section(
    provenance: PaperReportProvenance,
) -> list[str]:
    return [
        "## Report Provenance",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| generated_by | {_escape_table_value(provenance.generated_by)} |",
        f"| generated_at | {_escape_table_value(provenance.generated_at)} |",
        f"| artifact_mode | {_escape_table_value(provenance.artifact_mode)} |",
        f"| output_path | {_escape_table_value(provenance.output_path)} |",
        "",
    ]


def _escape_table_value(value: str) -> str:
    return " ".join(value.replace("|", "\\|").splitlines())


def _check_min_int(name: str, actual: int, minimum: int) -> PaperSoakGateCheck:
    if actual >= minimum:
        return PaperSoakGateCheck(name, True, f"{actual} >= {minimum}")
    return PaperSoakGateCheck(name, False, f"{actual} < {minimum}")


def _check_distinct_markets(
    metrics: PaperReportMetrics,
    *,
    config: PaperSoakGateConfig,
) -> PaperSoakGateCheck:
    concentration = metrics.execution_concentration
    if concentration is None:
        return _deferred_or_missing_concentration_check(
            "distinct_markets",
            metrics,
            config=config,
        )
    return _check_min_int(
        "distinct_markets",
        concentration.distinct_markets,
        config.min_distinct_markets,
    )


def _check_distinct_risk_groups(
    metrics: PaperReportMetrics,
    *,
    config: PaperSoakGateConfig,
) -> PaperSoakGateCheck:
    concentration = metrics.execution_concentration
    if concentration is None:
        return _deferred_or_missing_concentration_check(
            "distinct_risk_groups",
            metrics,
            config=config,
        )
    return _check_min_int(
        "distinct_risk_groups",
        concentration.distinct_risk_groups,
        config.min_distinct_risk_groups,
    )


def _check_max_market_fill_share(
    metrics: PaperReportMetrics,
    *,
    config: PaperSoakGateConfig,
) -> PaperSoakGateCheck:
    concentration = metrics.execution_concentration
    if concentration is None or concentration.max_market_fill_share is None:
        return _deferred_or_missing_concentration_check(
            "max_market_fill_share",
            metrics,
            config=config,
        )
    return _check_optional_lte(
        "max_market_fill_share",
        concentration.max_market_fill_share,
        config.max_market_fill_share,
    )


def _check_max_risk_group_fill_share(
    metrics: PaperReportMetrics,
    *,
    config: PaperSoakGateConfig,
) -> PaperSoakGateCheck:
    concentration = metrics.execution_concentration
    if concentration is None or concentration.max_risk_group_fill_share is None:
        return _deferred_or_missing_concentration_check(
            "max_risk_group_fill_share",
            metrics,
            config=config,
        )
    return _check_optional_lte(
        "max_risk_group_fill_share",
        concentration.max_risk_group_fill_share,
        config.max_risk_group_fill_share,
    )


def _deferred_or_missing_concentration_check(
    name: str,
    metrics: PaperReportMetrics,
    *,
    config: PaperSoakGateConfig,
) -> PaperSoakGateCheck:
    if metrics.fills < config.min_fills:
        return PaperSoakGateCheck(
            name,
            True,
            f"deferred until {config.min_fills} fills",
        )
    return PaperSoakGateCheck(name, False, "missing execution concentration evidence")


def _check_zero(name: str, actual: int, noun: str) -> PaperSoakGateCheck:
    if actual == 0:
        return PaperSoakGateCheck(name, True, f"0 {noun}")
    return PaperSoakGateCheck(name, False, f"{actual} {noun}")


def _check_strategy_evidence(strategy: str) -> PaperSoakGateCheck:
    labels = tuple(label.strip() for label in strategy.split(",") if label.strip())
    if not labels or any(
        label.lower() == "unknown"
        or "@" not in label
        or _looks_like_placeholder(label)
        for label in labels
    ):
        return PaperSoakGateCheck(
            "strategy_evidence",
            False,
            "missing concrete strategy_id@strategy_version_id",
        )
    return PaperSoakGateCheck(
        "strategy_evidence",
        True,
        ", ".join(labels),
    )


def _looks_like_placeholder(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "":
        return False
    placeholder_markers = (
        "fill_in",
        "__fill",
        "todo",
        "replace",
        "placeholder",
    )
    return any(marker in normalized for marker in placeholder_markers)


def _check_optional_lt(
    name: str,
    actual: float | None,
    threshold: float,
) -> PaperSoakGateCheck:
    if actual is None:
        return PaperSoakGateCheck(name, False, "missing")
    if actual < threshold:
        return PaperSoakGateCheck(name, True, f"{actual:.4f} < {threshold:.4f}")
    return PaperSoakGateCheck(name, False, f"{actual:.4f} >= {threshold:.4f}")


def _check_optional_lte(
    name: str,
    actual: float | None,
    threshold: float,
) -> PaperSoakGateCheck:
    if actual is None:
        return PaperSoakGateCheck(name, False, "missing")
    if actual <= threshold:
        return PaperSoakGateCheck(name, True, f"{actual:.4f} <= {threshold:.4f}")
    return PaperSoakGateCheck(name, False, f"{actual:.4f} > {threshold:.4f}")


def _check_optional_gt(
    name: str,
    actual: float | None,
    threshold: float,
) -> PaperSoakGateCheck:
    if actual is None:
        return PaperSoakGateCheck(name, False, "missing")
    if actual > threshold:
        return PaperSoakGateCheck(name, True, f"{actual:.4f} > {threshold:.4f}")
    return PaperSoakGateCheck(name, False, f"{actual:.4f} <= {threshold:.4f}")


def _secondary_brier_improvement_checks(
    improvements: Mapping[str, float],
) -> list[PaperSoakGateCheck]:
    return [
        _check_optional_gt(
            f"secondary_brier_improvement:{source}",
            improvement,
            0.0,
        )
        for source, improvement in _ordered_baseline_items(improvements)
    ]


def _check_daily_pnl(
    metrics: PaperReportMetrics,
    *,
    risk: RiskSettings,
) -> PaperSoakGateCheck:
    limit = risk.max_daily_loss_usdc
    if limit is None:
        return PaperSoakGateCheck(
            "todays_pnl",
            False,
            "risk.max_daily_loss_usdc missing",
        )
    threshold = -limit
    if metrics.todays_pnl >= threshold:
        return PaperSoakGateCheck(
            "todays_pnl",
            True,
            f"{metrics.todays_pnl:.4f} >= {threshold:.4f}",
        )
    return PaperSoakGateCheck(
        "todays_pnl",
        False,
        f"{metrics.todays_pnl:.4f} < {threshold:.4f}",
    )


def _check_drawdown(
    metrics: PaperReportMetrics,
    *,
    risk: RiskSettings,
) -> PaperSoakGateCheck:
    limit = risk.max_drawdown_pct
    if limit is None:
        return PaperSoakGateCheck(
            "max_drawdown_pct",
            False,
            "risk.max_drawdown_pct missing",
        )
    return _check_optional_lte("max_drawdown_pct", metrics.max_drawdown_pct, limit)


def _check_open_positions(
    metrics: PaperReportMetrics,
    *,
    risk: RiskSettings,
) -> PaperSoakGateCheck:
    limit = risk.max_open_positions
    if limit is None:
        return PaperSoakGateCheck(
            "open_positions",
            False,
            "risk.max_open_positions missing",
        )
    if metrics.open_positions <= limit:
        return PaperSoakGateCheck(
            "open_positions",
            True,
            f"{metrics.open_positions} <= {limit}",
        )
    return PaperSoakGateCheck(
        "open_positions",
        False,
        f"{metrics.open_positions} > {limit}",
    )


def _reliability_bins(records: Sequence[EvalRecord]) -> tuple[ReliabilityBin, ...]:
    outcomes_by_bin: list[list[float]] = [[] for _ in range(10)]
    for record in records:
        index = min(9, max(0, int(record.prob_estimate * 10)))
        outcomes_by_bin[index].append(record.resolved_outcome)

    bins: list[ReliabilityBin] = []
    for index, outcomes in enumerate(outcomes_by_bin):
        count = len(outcomes)
        actual_rate = None if count < 5 else sum(outcomes) / count
        bins.append(
            ReliabilityBin(
                predicted_prob_range=_probability_range_label(index),
                count=count,
                actual_resolution_rate=actual_rate,
            )
        )
    return tuple(bins)


def _probability_range_label(index: int) -> str:
    lower = index * 10
    upper = lower + 10
    if index == 9:
        return f"[{lower}%-{upper}%]"
    return f"[{lower}%-{upper}%)"


def _trade_costs(decisions: Sequence[TradeDecision]) -> tuple[TradeCostBreakdown, ...]:
    costs: list[TradeCostBreakdown] = []
    for decision in decisions:
        if decision.spread_bps_at_decision is None:
            continue
        gross_edge = decision.prob_estimate - decision.limit_price
        spread_cost = decision.spread_bps_at_decision / 10_000.0
        costs.append(
            TradeCostBreakdown(
                decision_id=decision.decision_id,
                market_id=decision.market_id,
                gross_edge=gross_edge,
                spread_cost=spread_cost,
                net_edge=gross_edge - spread_cost,
            )
        )
    return tuple(costs)


def _trade_costs_from_decision_rows(
    decisions: Sequence[Mapping[str, object]],
) -> tuple[TradeCostBreakdown, ...]:
    costs: list[TradeCostBreakdown] = []
    for row in _entry_decision_rows(decisions):
        spread_bps = _optional_float_from_mapping(row, "spread_bps_at_decision")
        gross_edge = _decision_edge(row)
        if spread_bps is None or gross_edge is None:
            continue
        spread_cost = spread_bps / 10_000.0
        costs.append(
            TradeCostBreakdown(
                decision_id=_string_from_mapping(row, "decision_id", "unknown"),
                market_id=_string_from_mapping(row, "market_id", "unknown"),
                gross_edge=gross_edge,
                spread_cost=spread_cost,
                net_edge=gross_edge - spread_cost,
            )
        )
    return tuple(costs)


def _hit_rate_from_metrics(
    metric_rows: dict[str, Any],
    evaluator: dict[str, Any],
) -> float | None:
    hit_rate = _optional_float_from_dict(metric_rows, "win_rate")
    if hit_rate is None:
        return None
    metric_record_count = _int_value(metric_rows.get("record_count"))
    evaluator_record_count = _int_value(evaluator.get("eval_records_total"))
    if metric_record_count == 0 or (
        metric_record_count is None and evaluator_record_count == 0
    ):
        return None
    return hit_rate


def _clamp_rejections(
    log_events: Sequence[Mapping[str, object]],
) -> tuple[tuple[str, int], ...]:
    counts: dict[str, int] = {}
    for event in log_events:
        if event.get("event") != "clamp_rejection":
            continue
        market_id = event.get("market_id")
        key = market_id if isinstance(market_id, str) and market_id else "unknown"
        counts[key] = counts.get(key, 0) + 1
    return tuple(sorted(counts.items()))


def _selection_funnel(log_events: Sequence[Mapping[str, object]]) -> SelectionFunnel:
    discovered = 0
    selected = 0
    routed = 0
    forecasted = 0
    traded = 0
    for event in log_events:
        event_name = event.get("event")
        if event_name == "funnel_selector":
            discovered += _event_int(event, "discovered_count")
            selected += _event_int(event, "selected_count")
        elif event_name == "funnel_router":
            routed += _event_int(event, "routed_count")
        elif event_name == "funnel_pipeline":
            forecasted += _event_int(event, "forecasted_count")
            traded += _event_int(event, "traded_count")
    return SelectionFunnel(
        discovered=discovered,
        selected=selected,
        routed=routed,
        forecasted=forecasted,
        traded=traded,
    )


def _event_int(event: Mapping[str, object], key: str) -> int:
    value = event.get(key)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _render_reliability_section(bins: Sequence[ReliabilityBin]) -> list[str]:
    lines = ["", "## Calibration Reliability", ""]
    if not bins:
        lines.append("No resolved evaluation records.")
        return lines
    lines.extend(
        [
            "| Predicted probability | Count | Actual resolution rate |",
            "|---|---:|---:|",
        ]
    )
    for bin_item in bins:
        actual_rate = (
            "insufficient data"
            if bin_item.actual_resolution_rate is None
            else _fmt_probability_percent(bin_item.actual_resolution_rate)
        )
        lines.append(
            f"| {bin_item.predicted_prob_range} | {bin_item.count} | {actual_rate} |"
        )
    return lines


def _render_baseline_evidence_section(
    coverage: BaselineEvidenceCoverage | None,
) -> list[str]:
    lines = ["", "## Baseline Evidence Coverage", ""]
    if coverage is None or coverage.decisions == 0:
        lines.append("No decision-time baseline evidence recorded.")
        return lines
    lines.extend(["| Baseline | Decisions | Coverage |", "|---|---:|---:|"])
    for label, count in (
        ("market_implied", coverage.market_implied_count),
        ("mid_quote", coverage.mid_quote_count),
        ("last_trade", coverage.last_trade_count),
        ("category_prior", coverage.category_prior_count),
    ):
        lines.append(
            f"| {label} | {count} / {coverage.decisions} | "
            f"{_fmt_ratio_percent(count / coverage.decisions)} |"
        )
    return lines


def _render_secondary_baseline_score_section(
    *,
    baseline_brier_by_source: Mapping[str, float],
    brier_improvement_by_source: Mapping[str, float],
) -> list[str]:
    lines = ["", "## Secondary Baseline Brier", ""]
    sources = _ordered_baseline_sources(
        set(baseline_brier_by_source) | set(brier_improvement_by_source)
    )
    if not sources:
        lines.append("No secondary baseline score metrics recorded.")
        return lines

    lines.extend(
        [
            "| Baseline | Baseline Brier | Brier improvement |",
            "|---|---:|---:|",
        ]
    )
    for source in sources:
        lines.append(
            f"| {_escape_table_value(source)} | "
            f"{_fmt_optional(baseline_brier_by_source.get(source), 4)} | "
            f"{_fmt_optional(brier_improvement_by_source.get(source), 4)} |"
        )
    return lines


def _render_trade_cost_section(costs: Sequence[TradeCostBreakdown]) -> list[str]:
    lines = ["", "## Spread Cost Decomposition", ""]
    if not costs:
        lines.append("No trade cost data.")
        return lines
    lines.extend(
        [
            "| Decision | Market | Gross edge | Spread cost | Net edge |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for cost in costs:
        lines.append(
            f"| {_escape_table_value(cost.decision_id)} | "
            f"{_escape_table_value(cost.market_id)} | "
            f"{_fmt_probability_percent(cost.gross_edge)} | "
            f"{_fmt_probability_percent(cost.spread_cost)} | "
            f"{_fmt_probability_percent(cost.net_edge)} |"
        )
    return lines


def _render_rejection_reason_section(rejections: Sequence[tuple[str, int]]) -> list[str]:
    lines = ["", "## Controller Rejection Reasons", ""]
    if not rejections:
        lines.append("No controller rejection reasons recorded.")
        return lines
    lines.extend(["| Reason | Count |", "|---|---:|"])
    for reason, count in rejections:
        lines.append(f"| {_escape_table_value(reason)} | {count} |")
    return lines


def _render_clamp_rejection_section(rejections: Sequence[tuple[str, int]]) -> list[str]:
    lines = ["", "## Extreme Probability Rejections", ""]
    if not rejections:
        lines.append("No clamp rejections recorded.")
        return lines
    lines.extend(["| Market | Rejections |", "|---|---:|"])
    for market_id, count in rejections:
        lines.append(f"| {_escape_table_value(market_id)} | {count} |")
    return lines


def _render_selection_funnel_section(funnel: SelectionFunnel | None) -> list[str]:
    lines = ["", "## Selection Funnel", ""]
    if funnel is None:
        lines.append("No funnel events recorded.")
        return lines
    lines.extend(["| Stage | Count |", "|---|---:|"])
    lines.append(f"| Discovered | {funnel.discovered} |")
    lines.append(f"| Selected | {funnel.selected} |")
    lines.append(f"| Routed | {funnel.routed} |")
    lines.append(f"| Forecasted | {funnel.forecasted} |")
    lines.append(f"| Traded | {funnel.traded} |")
    return lines


def _strategy_evidence_risk_events(
    strategies: dict[str, Any] | None,
) -> list[tuple[str, str, str]]:
    if strategies is None or _active_strategy_version_labels(strategies):
        return []
    return [
        (
            "report generation",
            "/strategies active version evidence missing",
            "paper-soak GO reports require active strategy_id@strategy_version_id rows",
        )
    ]


def _active_strategy_version_labels(strategies: dict[str, Any]) -> tuple[str, ...]:
    rows = _list_value(strategies, "strategies")
    labels: list[str] = []
    for row in rows:
        strategy_id = row.get("strategy_id")
        if not isinstance(strategy_id, str) or not strategy_id:
            continue
        active_version_id = row.get("active_version_id")
        if isinstance(active_version_id, str) and active_version_id:
            labels.append(f"{strategy_id}@{active_version_id}")
    return tuple(labels)


def _strategy_metrics_risk_events(
    strategies: dict[str, Any] | None,
    strategy_metrics: dict[str, Any] | None,
) -> list[tuple[str, str, str]]:
    if strategies is None or strategy_metrics is None:
        return []
    active_labels = set(_active_strategy_version_labels(strategies))
    if not active_labels:
        return []

    rows_by_label: dict[str, dict[str, Any]] = {}
    for row in _list_value(strategy_metrics, "strategies"):
        strategy_id = row.get("strategy_id")
        strategy_version_id = row.get("strategy_version_id")
        if (
            isinstance(strategy_id, str)
            and strategy_id
            and isinstance(strategy_version_id, str)
            and strategy_version_id
        ):
            rows_by_label[f"{strategy_id}@{strategy_version_id}"] = row

    events: list[tuple[str, str, str]] = []
    for label in sorted(active_labels):
        metric_row = rows_by_label.get(label)
        if metric_row is None:
            events.append(
                (
                    "strategy",
                    "active strategy metrics missing",
                    f"{label} missing from /strategies/metrics",
                )
            )
            continue
        record_count = _int_from_dict(metric_row, "record_count")
        if metric_row.get("insufficient_samples") is True or record_count <= 0:
            events.append(
                (
                    "strategy",
                    "active strategy samples insufficient",
                    f"{label} record_count={record_count}",
                )
            )
            continue
        pnl = _optional_float_from_dict(metric_row, "pnl")
        pnl_decimal = None if pnl is None else Decimal(str(pnl))
        if pnl_decimal is None or pnl_decimal <= Decimal("0.0"):
            events.append(
                (
                    "strategy",
                    "active strategy pnl not positive",
                    f"{label} pnl={0.0 if pnl is None else pnl:.4f}",
                )
            )
        fill_rate = _optional_float_from_dict(metric_row, "fill_rate")
        fill_rate_decimal = None if fill_rate is None else Decimal(str(fill_rate))
        if fill_rate_decimal is None or fill_rate_decimal <= Decimal("0.0"):
            events.append(
                (
                    "strategy",
                    "active strategy fill rate not positive",
                    f"{label} fill_rate={0.0 if fill_rate is None else fill_rate:.4f}",
                )
            )
        brier_improvement = _optional_float_from_dict(
            metric_row,
            "brier_improvement_overall",
        )
        brier_improvement_decimal = (
            None if brier_improvement is None else Decimal(str(brier_improvement))
        )
        if (
            brier_improvement_decimal is None
            or brier_improvement_decimal <= Decimal("0.0")
        ):
            events.append(
                (
                    "strategy",
                    "active strategy brier improvement not positive",
                    (
                        f"{label} brier_improvement_overall="
                        f"{0.0 if brier_improvement is None else brier_improvement:.4f}"
                    ),
                )
            )
    return events


def _strategy_label(
    *,
    status: dict[str, Any],
    strategies: dict[str, Any] | None,
) -> str:
    labels = list(_active_strategy_version_labels(strategies or {}))
    if labels:
        return ", ".join(labels)

    status_strategy = status.get("strategy")
    if isinstance(status_strategy, str) and status_strategy:
        return status_strategy
    return "unknown"


def _run_mode_risk_events(status: dict[str, Any]) -> list[tuple[str, str, str]]:
    if status.get("mode") == "paper":
        return []
    return [
        (
            "report generation",
            "paper mode evidence missing",
            "status.mode must be paper for paper-soak GO reports",
        )
    ]


def _sensor_risk_events(status: dict[str, Any]) -> list[tuple[str, str, str]]:
    events: list[tuple[str, str, str]] = []
    for row in _list_value(status, "sensors"):
        sensor_status = row.get("status")
        if sensor_status not in {"failed", "stale"}:
            continue
        name = str(row.get("name") or "unknown")
        last_signal_at = row.get("last_signal_at")
        detail = (
            f"last_signal_at={last_signal_at}"
            if isinstance(last_signal_at, str) and last_signal_at
            else "last_signal_at=none"
        )
        events.append(("sensor", f"{name} {sensor_status}", detail))
    return events


def _runtime_continuity_risk_events(
    status: dict[str, Any],
    *,
    day_of_soak: int,
) -> list[tuple[str, str, str]]:
    if day_of_soak < 30:
        return []
    continuity = status.get("runtime_continuity")
    if not isinstance(continuity, dict):
        return [
            (
                "report generation",
                "runtime continuity evidence missing",
                (
                    "status.runtime_continuity from postgres_runtime_heartbeats "
                    "is required once soak_days >= 30"
                ),
            )
        ]
    source = continuity.get("source")
    if source != "postgres_runtime_heartbeats":
        return [
            (
                "report generation",
                "runtime continuity evidence invalid",
                "status.runtime_continuity.source must be postgres_runtime_heartbeats",
            )
        ]
    healthy_days = _int_value(continuity.get("healthy_days"))
    if healthy_days is None or healthy_days < 30:
        return [
            (
                "report generation",
                "runtime continuity evidence insufficient",
                f"healthy_days={0 if healthy_days is None else healthy_days} < 30",
            )
        ]
    max_gap_seconds = _optional_float_from_mapping(continuity, "max_gap_seconds")
    if max_gap_seconds is None or max_gap_seconds > 300.0:
        return [
            (
                "report generation",
                "runtime continuity gap too large",
                f"max_gap_seconds={0.0 if max_gap_seconds is None else max_gap_seconds:.1f} > 300.0",
            )
        ]
    return []


def _actuator_risk_events(status: dict[str, Any]) -> list[tuple[str, str, str]]:
    actuator = _dict_value(status, "actuator")
    events: list[tuple[str, str, str]] = []
    if actuator.get("halted") is True:
        reason = str(actuator.get("halt_reason") or "unknown")
        triggered_at = str(actuator.get("halt_triggered_at") or "unknown")
        events.append(
            (
                "actuator",
                "active_halt",
                f"{reason} since {triggered_at}",
            )
        )
    halt_recovery_cycles_7d = _int_from_dict(
        actuator,
        "halt_recovery_cycles_7d",
    )
    if halt_recovery_cycles_7d <= 0:
        return events
    detail = (
        f"{halt_recovery_cycles_7d} recovered halt cycle(s) in trailing 7d"
    )
    events.append(("actuator", "halt_recovery_cycles_7d", detail))
    return events


def _unresolved_incident_evidence_risk_events(
    status: dict[str, Any],
) -> list[tuple[str, str, str]]:
    supervision = status.get("supervision")
    if not isinstance(supervision, dict):
        return [
            (
                "report generation",
                "unresolved incident evidence missing",
                "status.supervision.unresolved_feedback_total is required",
            )
        ]

    count = _int_value(supervision.get("unresolved_feedback_total"))
    if count is None or count < 0:
        return [
            (
                "report generation",
                "unresolved incident evidence invalid",
                "status.supervision.unresolved_feedback_total must be a non-negative integer",
            )
        ]
    return []


def _non_finite_metric_risk_events(
    metrics: dict[str, Any],
) -> list[tuple[str, str, str]]:
    events: list[tuple[str, str, str]] = []
    for key in (
        "fill_rate",
        "win_rate",
        "slippage_bps",
        "max_drawdown_pct",
        "sharpe_ratio",
    ):
        if _raw_numeric_is_non_finite(metrics.get(key)):
            events.append(
                (
                    "report generation",
                    "non-finite numeric evidence",
                    f"metrics.{key} must be finite",
                )
            )
    return events


def _non_finite_metric_mapping_risk_events(
    metrics: dict[str, Any],
) -> list[tuple[str, str, str]]:
    events: list[tuple[str, str, str]] = []
    for key in ("baseline_brier_by_source", "brier_improvement_by_source"):
        value = metrics.get(key)
        if not isinstance(value, Mapping):
            continue
        for source, raw_value in value.items():
            if not isinstance(source, str) or not source:
                continue
            if not _raw_numeric_is_non_finite(raw_value):
                continue
            events.append(
                (
                    "report generation",
                    "non-finite numeric evidence",
                    f"metrics.{key}.{source} must be finite",
                )
            )
    return events


def _invalid_metric_mapping_source_risk_events(
    metrics: dict[str, Any],
) -> list[tuple[str, str, str]]:
    events: list[tuple[str, str, str]] = []
    for key in ("baseline_brier_by_source", "brier_improvement_by_source"):
        value = metrics.get(key)
        if not isinstance(value, Mapping):
            continue
        for source in value:
            if _valid_baseline_source_label(source):
                continue
            events.append(
                (
                    "report generation",
                    "secondary baseline source invalid",
                    f"metrics.{key} source must be concrete lowercase snake_case: {source}",
                )
            )
    return events


def _secondary_baseline_score_risk_events(
    *,
    baseline_brier_by_source: Mapping[str, float],
    brier_improvement_by_source: Mapping[str, float],
) -> list[tuple[str, str, str]]:
    events: list[tuple[str, str, str]] = []
    missing_improvement = set(baseline_brier_by_source) - set(
        brier_improvement_by_source
    )
    for source in _ordered_baseline_sources(missing_improvement):
        events.append(
            (
                "report generation",
                "secondary baseline score incomplete",
                f"{source} baseline_brier_by_source lacks brier_improvement_by_source",
            )
        )

    missing_brier = set(brier_improvement_by_source) - set(baseline_brier_by_source)
    for source in _ordered_baseline_sources(missing_brier):
        events.append(
            (
                "report generation",
                "secondary baseline score incomplete",
                f"{source} brier_improvement_by_source lacks baseline_brier_by_source",
            )
        )
    return events


def _daily_pnl_from_metrics(
    metrics: dict[str, Any],
    *,
    report_date: date,
) -> tuple[float, list[tuple[str, str, str]]]:
    raw_series, series_path = _pnl_series_from_metrics(metrics)
    if not isinstance(raw_series, Sequence) or isinstance(raw_series, (str, bytes)):
        return 0.0, [
            (
                "report generation",
                "daily P&L evidence missing",
                f"{series_path} is required",
            )
        ]

    parsed_rows: list[tuple[datetime, Decimal]] = []
    for raw_row in raw_series:
        if not isinstance(raw_row, Mapping):
            return 0.0, [_invalid_daily_pnl_evidence_event(series_path)]
        recorded_at = _parse_datetime(raw_row.get("recorded_at"))
        pnl = _optional_float_from_mapping(raw_row, "pnl")
        if recorded_at is None or pnl is None:
            return 0.0, [_invalid_daily_pnl_evidence_event(series_path)]
        parsed_rows.append((recorded_at, Decimal(str(pnl))))

    if not parsed_rows:
        return 0.0, [
            (
                "report generation",
                "daily P&L evidence missing",
                f"{series_path} is empty",
            )
        ]

    parsed_rows.sort(key=lambda row: row[0])
    day_start = datetime(report_date.year, report_date.month, report_date.day, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    start_pnl = Decimal("0.0")
    end_pnl: Decimal | None = None
    for recorded_at, pnl_decimal in parsed_rows:
        if recorded_at < day_start:
            start_pnl = pnl_decimal
        if recorded_at < day_end:
            end_pnl = pnl_decimal

    if end_pnl is None:
        return 0.0, [
            (
                "report generation",
                "daily P&L evidence missing",
                f"{series_path} has no records before report day end",
            )
        ]
    return float(end_pnl - start_pnl), []


def _cumulative_pnl_from_metrics(
    metrics: dict[str, Any],
    *,
    report_date: date,
) -> float:
    raw_series, _series_path = _pnl_series_from_metrics(metrics)
    if not isinstance(raw_series, Sequence) or isinstance(raw_series, (str, bytes)):
        return 0.0

    day_start = datetime(report_date.year, report_date.month, report_date.day, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    latest_recorded_at: datetime | None = None
    latest_pnl: Decimal | None = None
    for raw_row in raw_series:
        if not isinstance(raw_row, Mapping):
            return 0.0
        recorded_at = _parse_datetime(raw_row.get("recorded_at"))
        pnl = _optional_float_from_mapping(raw_row, "pnl")
        if recorded_at is None or pnl is None:
            return 0.0
        if recorded_at >= day_end:
            continue
        if latest_recorded_at is None or recorded_at > latest_recorded_at:
            latest_recorded_at = recorded_at
            latest_pnl = Decimal(str(pnl))
    return 0.0 if latest_pnl is None else float(latest_pnl)


def _pnl_series_from_metrics(metrics: Mapping[str, Any]) -> tuple[object, str]:
    raw_series = metrics.get("pnl_series")
    if _is_non_empty_sequence(raw_series):
        return raw_series, "metrics.pnl_series"

    if raw_series is None or _is_empty_sequence(raw_series):
        quote_calibration = metrics.get("quote_calibration")
        if isinstance(quote_calibration, Mapping):
            raw_quote_series = quote_calibration.get("pnl_series")
            if _is_non_empty_sequence(raw_quote_series):
                return raw_quote_series, "metrics.quote_calibration.pnl_series"

    return raw_series, "metrics.pnl_series"


def _pnl_source_from_metrics(metrics: Mapping[str, Any]) -> str:
    _raw_series, series_path = _pnl_series_from_metrics(metrics)
    if series_path == "metrics.quote_calibration.pnl_series":
        return "quote_mtm"
    return "final_eval"


def _llm_total_cost_usdc(metrics: Mapping[str, Any]) -> float:
    return _optional_float_from_mapping(
        metrics,
        LLM_ESTIMATED_COST_USDC_TOTAL_METRIC,
    ) or 0.0


def _llm_daily_cost_usdc(metrics: Mapping[str, Any]) -> float:
    return _optional_float_from_mapping(metrics, LLM_DAILY_COST_USDC_METRIC) or 0.0


def _max_drawdown_pct_from_metrics(metrics: Mapping[str, Any]) -> float | None:
    if _pnl_source_from_metrics(metrics) == "quote_mtm":
        quote_calibration = metrics.get("quote_calibration")
        if isinstance(quote_calibration, Mapping):
            return _optional_float_from_mapping(
                quote_calibration,
                "max_drawdown_pct",
            )
        return None
    return _optional_float_from_mapping(metrics, "max_drawdown_pct")


def _accepted_decision_count(
    decision_rows: Sequence[Mapping[str, object]],
    entry_trade_rows: Sequence[Mapping[str, object]],
) -> int:
    accepted_decision_ids = {
        decision_id
        for row in decision_rows
        if (decision_id := _string_from_mapping(row, "decision_id", ""))
        and _decision_status_is_accepted(row.get("status"))
    }
    if accepted_decision_ids:
        return len(accepted_decision_ids)

    trade_decision_ids = {
        decision_id
        for row in entry_trade_rows
        if (decision_id := _string_from_mapping(row, "decision_id", ""))
        and not _is_exit_decision_id(decision_id)
    }
    if trade_decision_ids:
        return len(trade_decision_ids)
    return len(entry_trade_rows)


def _decision_status_is_accepted(raw_status: object) -> bool:
    if not isinstance(raw_status, str):
        return False
    return raw_status.lower() in {
        "accepted",
        "queued",
        "submitted",
        "partially_filled",
        "filled",
        "matched",
    }


def _is_empty_sequence(value: object) -> bool:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes))
        and len(value) == 0
    )


def _is_non_empty_sequence(value: object) -> bool:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes))
        and len(value) > 0
    )


def _invalid_daily_pnl_evidence_event(
    series_path: str = "metrics.pnl_series",
) -> tuple[str, str, str]:
    return (
        "report generation",
        "daily P&L evidence invalid",
        f"{series_path} entries require recorded_at and finite pnl",
    )


def _non_finite_position_risk_events(
    positions: Sequence[Mapping[str, object]],
) -> list[tuple[str, str, str]]:
    events: list[tuple[str, str, str]] = []
    seen_fields: set[str] = set()
    for row in positions:
        for key in ("locked_usdc", "unrealized_pnl"):
            if key in seen_fields:
                continue
            if not _raw_numeric_is_non_finite(row.get(key)):
                continue
            seen_fields.add(key)
            events.append(
                (
                    "report generation",
                    "non-finite numeric evidence",
                    f"positions.{key} must be finite",
                )
            )
    return events


def _non_finite_trade_risk_events(
    trades: Sequence[Mapping[str, object]],
) -> list[tuple[str, str, str]]:
    events: list[tuple[str, str, str]] = []
    seen_fields: set[str] = set()
    for row in trades:
        for key in ("fill_notional_usdc", "fees", "fee_bps"):
            if key in seen_fields:
                continue
            if not _raw_numeric_is_non_finite(row.get(key)):
                continue
            seen_fields.add(key)
            events.append(
                (
                    "report generation",
                    "non-finite numeric evidence",
                    f"trades.{key} must be finite",
                )
            )
    return events


def _non_finite_decision_risk_events(
    decisions: Sequence[Mapping[str, object]],
) -> list[tuple[str, str, str]]:
    events: list[tuple[str, str, str]] = []
    seen_fields: set[str] = set()
    for row in decisions:
        for key in (
            "expected_edge",
            "prob_estimate",
            "limit_price",
            "spread_bps_at_decision",
        ):
            if key in seen_fields:
                continue
            if not _raw_numeric_is_non_finite(row.get(key)):
                continue
            seen_fields.add(key)
            events.append(
                (
                    "report generation",
                    "non-finite numeric evidence",
                    f"decisions.{key} must be finite",
                )
            )
    return events


def _decision_payload_completeness_risk_events(
    decision_rows: Sequence[Mapping[str, object]],
    *,
    status_decisions_made: int,
) -> list[tuple[str, str, str]]:
    if status_decisions_made <= len(decision_rows):
        return []
    return [
        (
            "report generation",
            "decision payload incomplete",
            f"/decisions returned {len(decision_rows)} row(s), "
            f"but /status.controller.decisions_total reports {status_decisions_made}",
        )
    ]


def _baseline_evidence_coverage(
    decisions: Sequence[Mapping[str, object]],
) -> BaselineEvidenceCoverage | None:
    entry_decisions = _entry_decision_rows(decisions)
    if not entry_decisions:
        return None
    evidence_rows = _decision_evidence_rows(entry_decisions)
    return BaselineEvidenceCoverage(
        decisions=len(entry_decisions),
        market_implied_count=_baseline_evidence_count(
            evidence_rows,
            "market_implied_baseline_prob_estimate",
        ),
        mid_quote_count=_baseline_evidence_count(
            evidence_rows,
            "mid_quote_baseline_prob_estimate",
        ),
        last_trade_count=_baseline_evidence_count(
            evidence_rows,
            "last_trade_baseline_prob_estimate",
        ),
        category_prior_count=_baseline_evidence_count(
            evidence_rows,
            "category_prior_baseline_prob_estimate",
        ),
    )


def _baseline_evidence_count(
    evidence_rows: Sequence[Mapping[str, object]],
    key: str,
) -> int:
    return sum(
        1 for row in evidence_rows if _optional_float_from_mapping(row, key) is not None
    )


def _baseline_evidence_risk_events(
    decisions: Sequence[Mapping[str, object]],
) -> list[tuple[str, str, str]]:
    entry_decisions = _entry_decision_rows(decisions)
    if not entry_decisions:
        return []

    evidence_rows = _decision_evidence_rows(entry_decisions)

    events: list[tuple[str, str, str]] = []
    missing_rows = len(entry_decisions) - len(evidence_rows)
    if missing_rows > 0:
        events.append(
            (
                "report generation",
                "secondary baseline evidence incomplete",
                f"{missing_rows} reported decision(s) lack decision_evidence",
            )
        )

    for key in (
        "market_implied_baseline_prob_estimate",
        "mid_quote_baseline_prob_estimate",
    ):
        missing = len(evidence_rows) - _baseline_evidence_count(evidence_rows, key)
        if missing <= 0:
            continue
        events.append(
            (
                "report generation",
                "secondary baseline evidence incomplete",
                f"{missing} decision(s) with decision_evidence lack {key}",
            )
        )
    return events


def _baseline_evidence_score_risk_events(
    coverage: BaselineEvidenceCoverage | None,
    *,
    baseline_brier_by_source: Mapping[str, float],
    brier_improvement_by_source: Mapping[str, float],
) -> list[tuple[str, str, str]]:
    if coverage is None:
        return []

    events: list[tuple[str, str, str]] = []
    for source, count in _baseline_evidence_source_counts(coverage):
        if count <= 0:
            continue
        if source not in baseline_brier_by_source:
            events.append(
                (
                    "report generation",
                    "secondary baseline score incomplete",
                    f"{source} decision-time evidence lacks baseline_brier_by_source",
                )
            )
        if source not in brier_improvement_by_source:
            events.append(
                (
                    "report generation",
                    "secondary baseline score incomplete",
                    f"{source} decision-time evidence lacks brier_improvement_by_source",
                )
            )
    return events


def _baseline_evidence_source_counts(
    coverage: BaselineEvidenceCoverage,
) -> tuple[tuple[str, int], ...]:
    return (
        ("market_implied", coverage.market_implied_count),
        ("mid_quote", coverage.mid_quote_count),
        ("last_trade", coverage.last_trade_count),
        ("category_prior", coverage.category_prior_count),
    )


def _decision_evidence_rows(
    decisions: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    for decision in decisions:
        evidence = decision.get("decision_evidence")
        if isinstance(evidence, Mapping):
            rows.append(evidence)
    return tuple(rows)


def _fetch_api_payload(
    *,
    api_base_url: str,
    path: str,
    api_token: str | None,
) -> tuple[object, str | None]:
    url = f"{api_base_url.rstrip('/')}{path}"
    headers = {"Accept": "application/json"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"

    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=_API_TIMEOUT_S) as response:
            return _loads_json_rejecting_duplicate_keys(
                response.read().decode("utf-8")
            ), None
    except HTTPError as exc:
        return {}, f"HTTP {exc.code}"
    except json.JSONDecodeError as exc:
        return {}, exc.__class__.__name__
    except (TimeoutError, URLError, OSError, ValueError) as exc:
        if isinstance(exc, ValueError):
            return {}, str(exc)
        return {}, exc.__class__.__name__


def _fetch_api_list_pages(
    *,
    api_base_url: str,
    path: str,
    api_token: str | None,
    payload_key: str | None = None,
    query: Mapping[str, object] | None = None,
) -> tuple[list[object], str | None]:
    rows: list[object] = []
    offset = 0
    previous_page: list[object] | None = None
    while True:
        page_query: dict[str, object] = {"limit": _API_PAGE_SIZE, "offset": offset}
        if query is not None:
            page_query.update(query)
        page_path = f"{path}?{urlencode(page_query)}"
        payload, error = _fetch_api_payload(
            api_base_url=api_base_url,
            path=page_path,
            api_token=api_token,
        )
        if error is not None:
            return [], error
        page_payload: object
        if payload_key is None:
            page_payload = payload
        elif isinstance(payload, dict):
            page_payload = payload.get(payload_key)
        else:
            return [], "invalid JSON payload"
        if not isinstance(page_payload, list):
            return [], "invalid JSON payload"
        if previous_page is not None and page_payload == previous_page:
            return [], "pagination did not advance"
        rows.extend(page_payload)
        if len(page_payload) < _API_PAGE_SIZE:
            return rows, None
        previous_page = list(page_payload)
        offset += _API_PAGE_SIZE


def _loads_json_rejecting_duplicate_keys(text: str) -> object:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        seen: set[str] = set()
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in seen:
                msg = f"duplicate JSON key: {key}"
                raise ValueError(msg)
            seen.add(key)
            result[key] = value
        return result

    return json.loads(text, object_pairs_hook=reject_duplicate_keys)


def _fetch_api_json(
    *,
    api_base_url: str,
    path: str,
    api_token: str | None,
) -> tuple[dict[str, Any], str | None]:
    payload, error = _fetch_api_payload(
        api_base_url=api_base_url,
        path=path,
        api_token=api_token,
    )
    if error is not None:
        return {}, error

    if not isinstance(payload, dict):
        return {}, "invalid JSON payload"
    return payload, None


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _metrics_path_for_soak_window(
    *,
    status: dict[str, Any],
    report_date: date,
) -> tuple[str, tuple[str, str] | None]:
    started_at = _parse_datetime(status.get("runner_started_at"))
    if started_at is None:
        return "/metrics", None
    window_end = _paper_report_window_end(report_date)
    query = urlencode(
        {
            "since": started_at.isoformat(),
            "until": window_end.isoformat(),
        }
    )
    return f"/metrics?{query}", (started_at.isoformat(), window_end.isoformat())


def _elapsed_soak_days(started_at: datetime, *, observed_until: datetime) -> int:
    elapsed_seconds = (
        _aware_datetime(observed_until) - _aware_datetime(started_at)
    ).total_seconds()
    return max(0, int(elapsed_seconds // 86_400))


def _aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _soak_observed_until(status: dict[str, Any], *, report_date: date) -> datetime:
    window_end = _paper_report_window_end(report_date)
    continuity = status.get("runtime_continuity")
    if isinstance(continuity, Mapping):
        last_observed_at = _parse_datetime(continuity.get("last_observed_at"))
        if last_observed_at is not None:
            return min(last_observed_at, window_end)
    return min(datetime.now(tz=UTC), window_end)


def _metrics_window_risk_events(
    payload: dict[str, Any],
    *,
    expected_since: str,
    expected_until: str,
) -> list[tuple[str, str, str]]:
    if (
        payload.get("window_started_at") == expected_since
        and payload.get("window_ended_at") == expected_until
    ):
        return []
    return [
        (
            "report generation",
            "metrics window evidence mismatch",
            "/metrics window must match paper-soak interval",
        )
    ]


def _trade_rows_in_soak_window(
    rows: Sequence[dict[str, Any]],
    *,
    started_at: datetime | None,
    report_date: date,
) -> list[dict[str, Any]]:
    return _rows_in_soak_window(
        rows,
        timestamp_key="filled_at",
        started_at=started_at,
        report_date=report_date,
    )


def _rows_in_soak_window(
    rows: Sequence[dict[str, Any]],
    *,
    timestamp_key: str,
    started_at: datetime | None,
    report_date: date,
) -> list[dict[str, Any]]:
    if started_at is None:
        return []

    window_end = _paper_report_window_end(report_date)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _parse_datetime(row.get(timestamp_key))
        if timestamp is None:
            continue
        if started_at <= timestamp < window_end:
            filtered.append(row)
    return filtered


def _paper_report_window_end(report_date: date) -> datetime:
    return (
        datetime(report_date.year, report_date.month, report_date.day, tzinfo=UTC)
        + timedelta(days=1)
    )


def _dict_value(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if isinstance(value, dict):
        return value
    return {}


def _list_value(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _rows_from_payload(payload: object | None, key: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return _list_value(payload, key)
    return []


def _diagnostic_counts(controller: dict[str, Any]) -> tuple[tuple[str, int], ...]:
    raw_counts = controller.get("diagnostic_counts")
    if not isinstance(raw_counts, dict):
        return ()
    counts: list[tuple[str, int]] = []
    for reason, value in raw_counts.items():
        if not isinstance(reason, str) or not reason.strip():
            continue
        count = _int_value(value)
        if count is None or count <= 0:
            continue
        counts.append((reason, count))
    return tuple(sorted(counts))


def _diagnostic_evidence_risk_events(
    controller: dict[str, Any],
    diagnostic_counts: Sequence[tuple[str, int]],
) -> list[tuple[str, str, str]]:
    diagnostics_total = _int_from_dict(controller, "diagnostics_total")
    if diagnostics_total <= 0:
        return []

    raw_counts = controller.get("diagnostic_counts")
    if not isinstance(raw_counts, dict):
        return [
            (
                "report generation",
                "controller rejection evidence missing",
                "status.controller.diagnostic_counts is required when diagnostics_total > 0",
            )
        ]

    if any(not isinstance(reason, str) or not reason.strip() for reason in raw_counts):
        return [
            (
                "report generation",
                "controller rejection evidence malformed",
                "status.controller.diagnostic_counts keys must be non-empty strings",
            )
        ]

    counted = sum(count for _reason, count in diagnostic_counts)
    if counted < diagnostics_total:
        return [
            (
                "report generation",
                "controller rejection evidence incomplete",
                "status.controller.diagnostic_counts sum "
                f"{counted} is less than diagnostics_total {diagnostics_total}",
            )
        ]
    if counted > diagnostics_total:
        return [
            (
                "report generation",
                "controller rejection evidence inconsistent",
                "status.controller.diagnostic_counts sum "
                f"{counted} does not match diagnostics_total {diagnostics_total}",
            )
        ]
    return []


def _average_edge_bps(decisions: Sequence[Mapping[str, object]]) -> float | None:
    edges = [
        edge
        for row in _entry_decision_rows(decisions)
        if (edge := _decision_edge(row)) is not None
    ]
    if not edges:
        return None
    return sum(edges) / len(edges) * 10_000.0


def _average_fee_bps(trades: Sequence[Mapping[str, object]]) -> float | None:
    fee_values = [_optional_float_from_mapping(row, "fees") for row in trades]
    total_notional = sum(
        _optional_float_from_mapping(row, "fill_notional_usdc") or 0.0
        for row in trades
    )
    if total_notional > 0.0 and fee_values and all(
        value is not None for value in fee_values
    ):
        total_fees = sum(value for value in fee_values if value is not None)
        return total_fees / total_notional * 10_000.0

    fee_bps_values = [
        fee_bps
        for row in trades
        if (fee_bps := _optional_float_from_mapping(row, "fee_bps")) is not None
    ]
    if not fee_bps_values:
        return None
    return sum(fee_bps_values) / len(fee_bps_values)


def _average_spread_cost_bps(
    decisions: Sequence[Mapping[str, object]],
) -> float | None:
    spreads = [
        spread_bps
        for row in _entry_decision_rows(decisions)
        if (spread_bps := _optional_float_from_mapping(row, "spread_bps_at_decision"))
        is not None
    ]
    if not spreads:
        return None
    return sum(spreads) / len(spreads)


def _average_net_edge_bps(
    decisions: Sequence[Mapping[str, object]],
    *,
    trade_rows: Sequence[Mapping[str, object]],
    average_slippage_bps: float | None,
) -> float | None:
    average_edge = _average_edge_bps(decisions)
    average_spread_cost = _average_spread_cost_bps(decisions)
    average_fee = _average_fee_bps(_entry_trade_rows(trade_rows))
    if (
        average_edge is None
        or average_spread_cost is None
        or average_slippage_bps is None
        or average_fee is None
    ):
        return None
    return average_edge - average_spread_cost - average_slippage_bps - average_fee


def _entry_decision_rows(
    decisions: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    return tuple(
        row for row in decisions if not _is_exit_decision_id(row.get("decision_id"))
    )


def _entry_trade_rows(
    trade_rows: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    return tuple(
        row for row in trade_rows if not _is_exit_decision_id(row.get("decision_id"))
    )


def _execution_concentration(
    entry_trade_rows: Sequence[Mapping[str, object]],
) -> ExecutionConcentration | None:
    if not entry_trade_rows:
        return None

    market_ids = [
        market_id
        for row in entry_trade_rows
        if (market_id := _string_from_mapping(row, "market_id", "")) != ""
    ]
    risk_group_ids = [
        risk_group_id
        for row in entry_trade_rows
        if (risk_group_id := _string_from_mapping(row, "risk_group_id", "")) != ""
    ]
    market_counts = Counter(market_ids)
    risk_group_counts = Counter(risk_group_ids)
    entry_fills = len(entry_trade_rows)
    entry_fills_decimal = Decimal(entry_fills)
    return ExecutionConcentration(
        entry_fills=entry_fills,
        distinct_markets=len(market_counts),
        distinct_risk_groups=len(risk_group_counts),
        missing_risk_group_fills=entry_fills - len(risk_group_ids),
        max_market_fill_share=(
            float(Decimal(max(market_counts.values())) / entry_fills_decimal)
            if market_counts
            else None
        ),
        max_risk_group_fill_share=(
            float(Decimal(max(risk_group_counts.values())) / entry_fills_decimal)
            if risk_group_counts
            else None
        ),
    )


def _execution_concentration_risk_events(
    concentration: ExecutionConcentration | None,
    *,
    config: PaperSoakGateConfig,
) -> list[tuple[str, str, str]]:
    if concentration is None or concentration.entry_fills < config.min_fills:
        return []

    events: list[tuple[str, str, str]] = []
    if (
        concentration.distinct_markets > 0
        and concentration.distinct_markets < config.min_distinct_markets
    ):
        events.append(
            (
                "report generation",
                "execution market concentration too high",
                (
                    f"distinct_markets={concentration.distinct_markets} "
                    f"< {config.min_distinct_markets}"
                ),
            )
        )
    if concentration.distinct_markets > 0 and concentration.missing_risk_group_fills:
        events.append(
            (
                "report generation",
                "execution risk group evidence missing",
                (
                    f"{concentration.missing_risk_group_fills} entry fill(s) "
                    "lack risk_group_id"
                ),
            )
        )
    if (
        concentration.distinct_risk_groups > 0
        and concentration.distinct_risk_groups < config.min_distinct_risk_groups
    ):
        events.append(
            (
                "report generation",
                "execution risk group concentration too high",
                (
                    f"distinct_risk_groups={concentration.distinct_risk_groups} "
                    f"< {config.min_distinct_risk_groups}"
                ),
            )
        )
    if (
        concentration.max_market_fill_share is not None
        and Decimal(str(concentration.max_market_fill_share))
        > Decimal(str(config.max_market_fill_share))
    ):
        events.append(
            (
                "report generation",
                "execution market fill share too high",
                (
                    f"max_market_fill_share="
                    f"{concentration.max_market_fill_share:.4f} "
                    f"> {config.max_market_fill_share:.4f}"
                ),
            )
        )
    if (
        concentration.max_risk_group_fill_share is not None
        and Decimal(str(concentration.max_risk_group_fill_share))
        > Decimal(str(config.max_risk_group_fill_share))
    ):
        events.append(
            (
                "report generation",
                "execution risk group fill share too high",
                (
                    f"max_risk_group_fill_share="
                    f"{concentration.max_risk_group_fill_share:.4f} "
                    f"> {config.max_risk_group_fill_share:.4f}"
                ),
            )
        )
    return events


def _is_exit_decision_id(value: object) -> bool:
    return isinstance(value, str) and value.startswith("exit-")


def _decision_edge(row: Mapping[str, object]) -> float | None:
    expected_edge = _optional_float_from_mapping(row, "expected_edge")
    if expected_edge is not None:
        return expected_edge
    prob_estimate = _optional_float_from_mapping(row, "prob_estimate")
    limit_price = _optional_float_from_mapping(row, "limit_price")
    if prob_estimate is None or limit_price is None:
        return None
    return prob_estimate - limit_price


def _int_from_dict(payload: dict[str, Any], key: str, *, fallback: int = 0) -> int:
    value = payload.get(key)
    parsed = _int_value(value)
    return fallback if parsed is None else parsed


def _int_value(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _float_from_dict(payload: dict[str, Any], key: str) -> float:
    value = _optional_float_from_dict(payload, key)
    return 0.0 if value is None else value


def _optional_float_from_dict(payload: dict[str, Any], key: str) -> float | None:
    return _optional_float_from_mapping(payload, key)


def _optional_float_from_dict_first(
    payload: dict[str, Any],
    keys: Sequence[str],
) -> float | None:
    for key in keys:
        value = _optional_float_from_dict(payload, key)
        if value is not None:
            return value
    return None


def _float_mapping_from_dict(payload: dict[str, Any], key: str) -> dict[str, float]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        return {}

    parsed: dict[str, float] = {}
    for source, raw_value in value.items():
        if not _valid_baseline_source_label(source):
            continue
        score = _optional_float_value(raw_value)
        if score is None:
            continue
        parsed[source] = score
    return parsed


def _valid_baseline_source_label(source: object) -> bool:
    if not isinstance(source, str):
        return False
    normalized = source.strip()
    return (
        normalized == source
        and _BASELINE_SOURCE_PATTERN.fullmatch(normalized) is not None
        and not _looks_like_placeholder(normalized)
    )


def _optional_float_from_mapping(
    payload: Mapping[str, object],
    key: str,
) -> float | None:
    return _optional_float_value(payload.get(key))


def _string_from_mapping(
    payload: Mapping[str, object],
    key: str,
    fallback: str,
) -> str:
    value = payload.get(key)
    return value if isinstance(value, str) and value else fallback


def _optional_float_value(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return _finite_float_or_none(float(value))
    if isinstance(value, str):
        try:
            return _finite_float_or_none(float(value))
        except ValueError:
            return None
    return None


def _ordered_baseline_items(
    values: Mapping[str, float],
) -> tuple[tuple[str, float], ...]:
    return tuple((source, values[source]) for source in _ordered_baseline_sources(values))


def _ordered_baseline_sources(sources: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(sources, key=_baseline_source_sort_key))


def _baseline_source_sort_key(source: str) -> tuple[int, str]:
    try:
        index = _SECONDARY_BASELINE_ORDER.index(source)
    except ValueError:
        index = len(_SECONDARY_BASELINE_ORDER)
    return index, source


def _raw_numeric_is_non_finite(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int | float):
        return not math.isfinite(float(value))
    if isinstance(value, str):
        try:
            parsed = float(value)
        except ValueError:
            return False
        return not math.isfinite(parsed)
    return False


def _finite_float_or_none(value: float) -> float | None:
    return value if math.isfinite(value) else None


if __name__ == "__main__":
    raise SystemExit(main())
