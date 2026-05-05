from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pms.config import PMSSettings, RiskSettings


DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"
_API_TIMEOUT_S = 5.0


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


def metrics_from_api_payloads(
    *,
    report_date: date,
    status: dict[str, Any],
    trades: dict[str, Any],
    positions: dict[str, Any],
    risk_events: tuple[tuple[str, str, str], ...] = (),
) -> PaperReportMetrics:
    events = list(risk_events)
    started_at = _parse_datetime(status.get("runner_started_at"))
    if started_at is None:
        events.append(("report generation", "runner_started_at missing", "check /status"))
        day_of_soak = 0
    else:
        day_of_soak = max(0, (report_date - started_at.date()).days)

    controller = _dict_value(status, "controller")
    actuator = _dict_value(status, "actuator")
    evaluator = _dict_value(status, "evaluator")
    trade_rows = _list_value(trades, "trades")
    position_rows = _list_value(positions, "positions")

    total_exposure = sum(_float_from_dict(row, "locked_usdc") for row in position_rows)
    cumulative_pnl = sum(_float_from_dict(row, "unrealized_pnl") for row in position_rows)

    return PaperReportMetrics(
        report_date=report_date,
        strategy=str(status.get("strategy") or "ripple_v2"),
        day_of_soak=day_of_soak,
        decisions_made=_int_from_dict(controller, "decisions_total"),
        decisions_rejected=_int_from_dict(controller, "diagnostics_total"),
        fills=_int_from_dict(actuator, "fills_total", fallback=len(trade_rows)),
        cumulative_pnl=cumulative_pnl,
        open_positions=len(position_rows),
        total_exposure=total_exposure,
        brier_score_7d=_optional_float_from_dict(evaluator, "brier_overall"),
        risk_events=tuple(events),
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
    trades, trades_error = _fetch_api_json(
        api_base_url=api_base_url,
        path="/trades?limit=200",
        api_token=api_token,
    )
    if trades_error is not None:
        events.append(("report generation", "/trades unavailable", trades_error))
        trades = {}

    positions, positions_error = _fetch_api_json(
        api_base_url=api_base_url,
        path="/positions",
        api_token=api_token,
    )
    if positions_error is not None:
        events.append(("report generation", "/positions unavailable", positions_error))
        positions = {}

    return metrics_from_api_payloads(
        report_date=report_date,
        status=status,
        trades=trades,
        positions=positions,
        risk_events=tuple(events),
    )


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
    args = parser.parse_args(argv)

    report_date = date.fromisoformat(args.date)
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
    report = render_report(
        metrics,
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


def _fetch_api_json(
    *,
    api_base_url: str,
    path: str,
    api_token: str | None,
) -> tuple[dict[str, Any], str | None]:
    url = f"{api_base_url.rstrip('/')}{path}"
    headers = {"Accept": "application/json"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"

    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=_API_TIMEOUT_S) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return {}, f"HTTP {exc.code}"
    except (TimeoutError, URLError, OSError, json.JSONDecodeError) as exc:
        return {}, exc.__class__.__name__

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


def _int_from_dict(payload: dict[str, Any], key: str, *, fallback: int = 0) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        return fallback
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return fallback
    return fallback


def _float_from_dict(payload: dict[str, Any], key: str) -> float:
    value = _optional_float_from_dict(payload, key)
    return 0.0 if value is None else value


def _optional_float_from_dict(payload: dict[str, Any], key: str) -> float | None:
    value = payload.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


if __name__ == "__main__":
    raise SystemExit(main())
