from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from pms.config import PMSSettings, RiskSettings


@dataclass(frozen=True)
class PaperReportMetrics:
    report_date: date
    strategy: str = "ripple_v2"
    day_of_soak: int = 0
    decisions_made: int = 0
    decisions_accepted: int = 0
    decisions_rejected: int = 0
    fills: int = 0
    average_slippage_bps: float | None = None
    todays_pnl: float = 0.0
    cumulative_pnl: float = 0.0
    max_drawdown_pct: float | None = None
    open_positions: int = 0
    total_exposure: float = 0.0
    brier_score_7d: float | None = None
    hit_rate: float | None = None
    average_edge_bps: float | None = None
    sharpe_ratio: float | None = None
    risk_events: tuple[tuple[str, str, str], ...] = ()

    @classmethod
    def empty(cls, *, report_date: date) -> PaperReportMetrics:
        return cls(report_date=report_date)


def render_report(metrics: PaperReportMetrics, *, risk: RiskSettings) -> str:
    lines = [
        f"# Paper Daily Report - {metrics.report_date.isoformat()}",
        "",
        "## Summary",
        "",
        "| Metric | Value | Gate |",
        "|---|---:|---|",
        f"| Strategy | {metrics.strategy} | - |",
        f"| Day of soak | {metrics.day_of_soak} | 30 required |",
        f"| Decisions made | {metrics.decisions_made} | - |",
        f"| Decisions accepted | {metrics.decisions_accepted} | - |",
        f"| Decisions rejected | {metrics.decisions_rejected} | - |",
        f"| Fills | {metrics.fills} | - |",
        f"| Average slippage (bps) | {_fmt_optional(metrics.average_slippage_bps, 1)} | <= 50 |",
        f"| Today's P&L | {_fmt_money_signed(metrics.todays_pnl)} | >= -daily limit |",
        f"| Cumulative P&L | {_fmt_money_signed(metrics.cumulative_pnl)} | > 0 by soak end |",
        f"| Max drawdown | {_fmt_percent(metrics.max_drawdown_pct)} | <= {_fmt_percent(risk.max_drawdown_pct)} |",
        f"| Open positions | {metrics.open_positions} | <= {risk.max_open_positions or 'N/A'} |",
        f"| Total exposure | {_fmt_money(metrics.total_exposure)} | <= {_fmt_money(risk.max_total_exposure)} |",
        f"| Max exposure | {_fmt_money(risk.max_total_exposure)} | - |",
        f"| Brier score (7d rolling) | {_fmt_optional(metrics.brier_score_7d, 2)} | < 0.20 |",
        f"| Hit rate (all trades) | {_fmt_ratio_percent(metrics.hit_rate)} | > 45% |",
        f"| Average edge (bps) | {_fmt_optional(metrics.average_edge_bps, 1)} | > 5 |",
        f"| Sharpe ratio (cumulative) | {_fmt_optional(metrics.sharpe_ratio, 2)} | > 0 |",
        "",
        "## Risk Events",
        "",
        "| Time | Trigger | Status |",
        "|---|---|---|",
    ]
    if metrics.risk_events:
        lines.extend(
            f"| {event_time} | {trigger} | {status} |"
            for event_time, trigger, status in metrics.risk_events
        )
    else:
        lines.append("| (none today) | - | - |")

    lines.extend(["", "## Trade Notes", ""])
    if metrics.fills == 0:
        lines.append("No trades today.")
    else:
        lines.append(
            f"{metrics.fills} fills executed with average slippage "
            f"{_fmt_optional(metrics.average_slippage_bps, 1)} bps."
        )
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
    parser.add_argument(
        "--output-dir",
        default="docs/paper-reports",
        help="Directory for the generated Markdown report.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the report instead of writing it to disk.",
    )
    args = parser.parse_args(argv)

    report_date = date.fromisoformat(args.date)
    settings = PMSSettings.load(args.config)
    report = render_report(
        PaperReportMetrics.empty(report_date=report_date),
        risk=settings.risk,
    )
    if args.dry_run:
        print(report)
        return 0

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{report_date.isoformat()}.md"
    output_path.write_text(report, encoding="utf-8")
    print(output_path)
    return 0


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


if __name__ == "__main__":
    raise SystemExit(main())
