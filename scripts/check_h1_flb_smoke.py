"""Validate saved H1 FLB PAPER smoke snapshots.

This checker is for plumbing evidence only. It validates that live market data
can activate the configured ``h1_flb`` strategy, produce version-bound PAPER
decisions, flow through the paper actuator, and publish quote/open-position
metrics. It does not validate alpha quality or replace the real FLB artifact
gate or 30-day paper-soak GO report.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pms.metrics import (
    SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC,
    SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC,
    SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC,
    SELECTION_FUNNEL_ROUTED_TOTAL_METRIC,
    SELECTION_FUNNEL_SELECTED_TOTAL_METRIC,
    SELECTION_FUNNEL_TRADED_TOTAL_METRIC,
)


H1_FLB_STRATEGY_ID = "h1_flb"
FIRST_TRADE_TIME_SECONDS_METRIC = "pms.ui.first_trade_time_seconds"
_ACCEPTED_DECISION_STATUSES = frozenset(
    {"accepted", "queued", "submitted", "partially_filled", "filled"}
)
_BAD_SENSOR_STATUSES = frozenset({"failed", "stale", "stopped"})
_SELECTION_FUNNEL_METRICS = (
    SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC,
    SELECTION_FUNNEL_SELECTED_TOTAL_METRIC,
    SELECTION_FUNNEL_ROUTED_TOTAL_METRIC,
    SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC,
    SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC,
    SELECTION_FUNNEL_TRADED_TOTAL_METRIC,
)
_REQUIRED_DECISION_EVIDENCE_NUMERIC_KEYS = (
    "category_prior_baseline_prob_estimate",
    "market_implied_baseline_prob_estimate",
    "mid_quote_baseline_prob_estimate",
    "net_edge_after_costs",
    "fee_edge_at_decision",
    "spread_edge_at_decision",
)


@dataclass(frozen=True, slots=True)
class H1FlbSmokeCheck:
    name: str
    passed: bool
    detail: str


def check_h1_flb_smoke(
    *,
    status: object,
    strategies: object,
    markets: object,
    decisions: object,
    trades: object,
    positions: object,
    metrics: object,
    min_heartbeats: int = 1,
    min_markets: int = 1,
    min_decisions: int = 1,
    min_trades: int = 1,
    min_positions: int = 1,
) -> tuple[H1FlbSmokeCheck, ...]:
    """Return machine-checkable H1 FLB PAPER smoke checks."""
    status_obj = _as_mapping(status)
    metrics_obj = _as_mapping(metrics)
    active_version_id = _active_h1_version_id(strategies)
    accepted_decisions = _accepted_decision_rows(
        decisions,
        active_version_id=active_version_id,
    )
    return (
        _check_paper_mode(status_obj),
        _check_runtime_continuity(status_obj, min_heartbeats=min_heartbeats),
        _check_sensor_activity(status_obj),
        _check_active_strategy(active_version_id),
        _check_market_discovery(markets, min_markets=min_markets),
        _check_controller_decisions(
            status_obj,
            decisions,
            active_version_id=active_version_id,
            accepted_rows=accepted_decisions,
            min_decisions=min_decisions,
        ),
        _check_h1_decision_evidence(accepted_decisions, min_decisions=min_decisions),
        _check_paper_trades(
            status_obj,
            trades,
            active_version_id=active_version_id,
            min_trades=min_trades,
        ),
        _check_open_positions(
            positions,
            active_version_id=active_version_id,
            min_positions=min_positions,
        ),
        _check_selection_funnel(metrics_obj),
        _check_first_trade_time(metrics_obj),
        _check_quote_calibration(
            metrics_obj,
            min_records=min_trades,
            min_positions=min_positions,
        ),
    )


def _check_paper_mode(status: Mapping[str, object]) -> H1FlbSmokeCheck:
    actuator = _as_mapping(status.get("actuator"))
    mode = _string(status.get("mode"))
    actuator_mode = _string(actuator.get("mode"))
    running = status.get("running")
    if mode != "paper":
        return H1FlbSmokeCheck("paper_mode", False, f"mode must be paper: {mode!r}")
    if actuator_mode not in {"paper", ""}:
        return H1FlbSmokeCheck(
            "paper_mode",
            False,
            f"actuator.mode must be paper: {actuator_mode!r}",
        )
    if running is not True:
        return H1FlbSmokeCheck(
            "paper_mode",
            False,
            f"runner must be running when the status snapshot is captured: {running!r}",
        )
    return H1FlbSmokeCheck(
        "paper_mode",
        True,
        "mode=paper; actuator.mode=paper; running=true",
    )


def _check_runtime_continuity(
    status: Mapping[str, object],
    *,
    min_heartbeats: int,
) -> H1FlbSmokeCheck:
    continuity = _as_mapping(status.get("runtime_continuity"))
    heartbeat_count = _int_value(continuity.get("heartbeat_count"))
    unhealthy_count = _int_value(continuity.get("unhealthy_heartbeat_count"))
    if heartbeat_count < min_heartbeats:
        return H1FlbSmokeCheck(
            "runtime_continuity",
            False,
            f"heartbeat_count={heartbeat_count} below required {min_heartbeats}",
        )
    if unhealthy_count > 0:
        return H1FlbSmokeCheck(
            "runtime_continuity",
            False,
            f"unhealthy_heartbeat_count={unhealthy_count}",
        )
    return H1FlbSmokeCheck(
        "runtime_continuity",
        True,
        f"heartbeat_count={heartbeat_count}; unhealthy_heartbeat_count=0",
    )


def _check_sensor_activity(status: Mapping[str, object]) -> H1FlbSmokeCheck:
    rows = _rows_from_payload(status.get("sensors"), "sensors")
    if not rows:
        return H1FlbSmokeCheck("sensor_activity", False, "no sensor rows")
    bad_rows = [
        f"{_string(row.get('name'))}:{_string(row.get('status'))}"
        for row in rows
        if _string(row.get("status")) in _BAD_SENSOR_STATUSES
    ]
    if bad_rows:
        return H1FlbSmokeCheck(
            "sensor_activity",
            False,
            f"sensor not usable: {', '.join(bad_rows)}",
        )
    names = {_string(row.get("name")) for row in rows}
    missing_layers: list[str] = []
    if not any("Discovery" in name for name in names):
        missing_layers.append("MarketDiscoverySensor")
    if not any("Data" in name for name in names):
        missing_layers.append("MarketDataSensor")
    if missing_layers:
        return H1FlbSmokeCheck(
            "sensor_activity",
            False,
            f"missing sensor layer(s): {', '.join(missing_layers)}",
        )
    if not any(_string(row.get("last_signal_at")) for row in rows):
        return H1FlbSmokeCheck(
            "sensor_activity",
            False,
            "no last_signal_at observed",
        )
    statuses = ", ".join(
        f"{_string(row.get('name'))}:{_string(row.get('status'))}" for row in rows
    )
    return H1FlbSmokeCheck("sensor_activity", True, statuses)


def _active_h1_version_id(strategies: object) -> str | None:
    rows = _rows_from_payload(strategies, "strategies")
    for row in rows:
        if _string(row.get("strategy_id")) != H1_FLB_STRATEGY_ID:
            continue
        active_version_id = _string(row.get("active_version_id"))
        return active_version_id or None
    return None


def _check_active_strategy(active_version_id: str | None) -> H1FlbSmokeCheck:
    if active_version_id is not None:
        return H1FlbSmokeCheck(
            "active_strategy",
            True,
            f"h1_flb@{active_version_id}",
        )
    return H1FlbSmokeCheck(
        "active_strategy",
        False,
        "h1_flb is not present in /strategies or has no active_version_id",
    )


def _check_market_discovery(markets: object, *, min_markets: int) -> H1FlbSmokeCheck:
    market_obj = _as_mapping(markets)
    rows = _rows_from_payload(markets, "markets")
    total = _int_value(market_obj.get("total"))
    observed = max(total, len(rows))
    if observed < min_markets:
        return H1FlbSmokeCheck(
            "market_discovery",
            False,
            f"observed_markets={observed} below required {min_markets}",
        )
    return H1FlbSmokeCheck("market_discovery", True, f"observed_markets={observed}")


def _accepted_decision_rows(
    decisions: object,
    *,
    active_version_id: str | None,
) -> list[Mapping[str, object]]:
    rows = [
        row
        for row in _rows_from_payload(decisions, "decisions")
        if _string(row.get("strategy_id")) == H1_FLB_STRATEGY_ID
    ]
    rows = [
        row
        for row in rows
        if active_version_id is None
        or _string(row.get("strategy_version_id")) == active_version_id
    ]
    return [
        row
        for row in rows
        if _string(row.get("status")) in _ACCEPTED_DECISION_STATUSES
    ]


def _check_controller_decisions(
    status: Mapping[str, object],
    decisions: object,
    *,
    active_version_id: str | None,
    accepted_rows: Sequence[Mapping[str, object]],
    min_decisions: int,
) -> H1FlbSmokeCheck:
    controller = _as_mapping(status.get("controller"))
    status_decisions = _int_value(controller.get("decisions_total"))
    rows = [
        row
        for row in _rows_from_payload(decisions, "decisions")
        if _string(row.get("strategy_id")) == H1_FLB_STRATEGY_ID
    ]
    mismatched_versions = sorted(
        {
            _string(row.get("strategy_version_id")) or "<missing>"
            for row in rows
            if active_version_id is not None
            and _string(row.get("strategy_version_id")) != active_version_id
        }
    )
    if mismatched_versions:
        return H1FlbSmokeCheck(
            "controller_decisions",
            False,
            (
                "h1_flb decision rows must match active "
                f"h1_flb@{active_version_id}; saw {', '.join(mismatched_versions)}"
            ),
        )
    if status_decisions < min_decisions:
        return H1FlbSmokeCheck(
            "controller_decisions",
            False,
            f"status.controller.decisions_total={status_decisions} below required {min_decisions}",
        )
    if len(accepted_rows) < min_decisions:
        return H1FlbSmokeCheck(
            "controller_decisions",
            False,
            f"accepted h1_flb decision rows={len(accepted_rows)} below required {min_decisions}",
        )
    decision_ids = ", ".join(_string(row.get("decision_id")) for row in accepted_rows)
    return H1FlbSmokeCheck(
        "controller_decisions",
        True,
        f"decisions_total={status_decisions}; accepted_decisions={decision_ids}",
    )


def _check_h1_decision_evidence(
    accepted_rows: Sequence[Mapping[str, object]],
    *,
    min_decisions: int,
) -> H1FlbSmokeCheck:
    rows_with_evidence = 0
    missing: dict[str, set[str]] = {}
    for row in accepted_rows:
        decision_id = _string(row.get("decision_id")) or "<missing>"
        evidence = _as_mapping(row.get("decision_evidence"))
        missing_keys = {
            key
            for key in _REQUIRED_DECISION_EVIDENCE_NUMERIC_KEYS
            if _float_value_or_none(evidence.get(key)) is None
        }
        if missing_keys:
            missing[decision_id] = missing_keys
            continue
        rows_with_evidence += 1
    if missing:
        first_decision = sorted(missing)[0]
        return H1FlbSmokeCheck(
            "h1_decision_evidence",
            False,
            (
                f"{first_decision} missing numeric FLB evidence key(s): "
                f"{', '.join(sorted(missing[first_decision]))}"
            ),
        )
    if rows_with_evidence < min_decisions:
        return H1FlbSmokeCheck(
            "h1_decision_evidence",
            False,
            f"h1_flb decision evidence rows={rows_with_evidence} below required {min_decisions}",
        )
    return H1FlbSmokeCheck(
        "h1_decision_evidence",
        True,
        f"decision_evidence_rows={rows_with_evidence}",
    )


def _check_paper_trades(
    status: Mapping[str, object],
    trades: object,
    *,
    active_version_id: str | None,
    min_trades: int,
) -> H1FlbSmokeCheck:
    actuator = _as_mapping(status.get("actuator"))
    fills_total = _int_value(actuator.get("fills_total"))
    if fills_total < min_trades:
        return H1FlbSmokeCheck(
            "paper_trades",
            False,
            f"status.actuator.fills_total={fills_total} below required {min_trades}",
        )
    rows = [
        row
        for row in _rows_from_payload(trades, "trades")
        if _string(row.get("strategy_id")) == H1_FLB_STRATEGY_ID
    ]
    mismatched_versions = sorted(
        {
            _string(row.get("strategy_version_id")) or "<missing>"
            for row in rows
            if active_version_id is not None
            and _string(row.get("strategy_version_id")) != active_version_id
        }
    )
    if mismatched_versions:
        return H1FlbSmokeCheck(
            "paper_trades",
            False,
            (
                "h1_flb trade rows must match active "
                f"h1_flb@{active_version_id}; saw {', '.join(mismatched_versions)}"
            ),
        )
    rows = [
        row
        for row in rows
        if (
            active_version_id is None
            or _string(row.get("strategy_version_id")) == active_version_id
        )
        and _float_value(row.get("fill_notional_usdc")) > 0.0
        and _float_value(row.get("fill_quantity")) > 0.0
    ]
    if len(rows) < min_trades:
        return H1FlbSmokeCheck(
            "paper_trades",
            False,
            "no h1_flb trade rows with positive fill quantity/notional",
        )
    trade_ids = ", ".join(_string(row.get("trade_id")) for row in rows)
    return H1FlbSmokeCheck(
        "paper_trades",
        True,
        f"fills_total={fills_total}; trades={trade_ids}",
    )


def _check_open_positions(
    positions: object,
    *,
    active_version_id: str | None,
    min_positions: int,
) -> H1FlbSmokeCheck:
    rows = [
        row
        for row in _rows_from_payload(positions, "positions")
        if _string(row.get("strategy_id")) == H1_FLB_STRATEGY_ID
    ]
    mismatched_versions = sorted(
        {
            _string(row.get("strategy_version_id")) or "<missing>"
            for row in rows
            if active_version_id is not None
            and _string(row.get("strategy_version_id")) != active_version_id
        }
    )
    if mismatched_versions:
        return H1FlbSmokeCheck(
            "open_positions",
            False,
            (
                "h1_flb position rows must match active "
                f"h1_flb@{active_version_id}; saw {', '.join(mismatched_versions)}"
            ),
        )
    rows = [
        row
        for row in rows
        if (
            active_version_id is None
            or _string(row.get("strategy_version_id")) == active_version_id
        )
        and _float_value(row.get("shares_held")) > 0.0
    ]
    if len(rows) < min_positions:
        return H1FlbSmokeCheck(
            "open_positions",
            False,
            f"open h1_flb positions={len(rows)} below required {min_positions}",
        )
    return H1FlbSmokeCheck("open_positions", True, f"open_positions={len(rows)}")


def _check_selection_funnel(metrics: Mapping[str, object]) -> H1FlbSmokeCheck:
    values: list[tuple[str, float]] = []
    for name in _SELECTION_FUNNEL_METRICS:
        value = _metric_value(metrics, name)
        if value is None:
            return H1FlbSmokeCheck(
                "selection_funnel",
                False,
                f"{name} missing or non-numeric",
            )
        values.append((name, value))
    for name, value in values:
        if value < 1.0:
            return H1FlbSmokeCheck(
                "selection_funnel",
                False,
                f"{name}={_format_number(value)} below required 1",
            )
    detail = "; ".join(f"{name}={_format_number(value)}" for name, value in values)
    return H1FlbSmokeCheck("selection_funnel", True, detail)


def _check_first_trade_time(metrics: Mapping[str, object]) -> H1FlbSmokeCheck:
    value = _metric_value(metrics, FIRST_TRADE_TIME_SECONDS_METRIC)
    if value is None or value < 0.0:
        return H1FlbSmokeCheck(
            "first_trade_time",
            False,
            f"{FIRST_TRADE_TIME_SECONDS_METRIC} missing, non-numeric, or negative",
        )
    return H1FlbSmokeCheck(
        "first_trade_time",
        True,
        f"{FIRST_TRADE_TIME_SECONDS_METRIC}={_format_number(value)}",
    )


def _check_quote_calibration(
    metrics: Mapping[str, object],
    *,
    min_records: int,
    min_positions: int,
) -> H1FlbSmokeCheck:
    quote_calibration = _as_mapping(metrics.get("quote_calibration"))
    record_count = _int_value(quote_calibration.get("record_count"))
    if record_count < min_records:
        return H1FlbSmokeCheck(
            "quote_calibration",
            False,
            f"quote_calibration.record_count={record_count} below required {min_records}",
        )
    mark_to_market = _as_mapping(metrics.get("mark_to_market"))
    open_positions = _int_value(mark_to_market.get("open_positions"))
    if open_positions < min_positions:
        return H1FlbSmokeCheck(
            "quote_calibration",
            False,
            f"mark_to_market.open_positions={open_positions} below required {min_positions}",
        )
    return H1FlbSmokeCheck(
        "quote_calibration",
        True,
        f"quote_records={record_count}; open_positions={open_positions}",
    )


def _rows_from_payload(payload: object, key: str) -> list[Mapping[str, object]]:
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        return [_as_mapping(item) for item in payload if isinstance(item, Mapping)]
    payload_obj = _as_mapping(payload)
    rows = payload_obj.get(key)
    if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)):
        return [_as_mapping(item) for item in rows if isinstance(item, Mapping)]
    return []


def _as_mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    return {}


def _string(value: object) -> str:
    if isinstance(value, str):
        return value
    return ""


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return int(value)
    return 0


def _float_value(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return 0.0


def _float_value_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return None


def _metric_value(metrics: Mapping[str, object], name: str) -> float | None:
    raw_value = metrics.get(name)
    if isinstance(raw_value, bool):
        return None
    if isinstance(raw_value, (int, float)) and math.isfinite(float(raw_value)):
        return float(raw_value)
    return None


def _format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:g}"


def _read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _format_plain(checks: Sequence[H1FlbSmokeCheck]) -> str:
    lines: list[str] = []
    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        lines.append(f"[{status}] {check.name}: {check.detail}")
    return "\n".join(lines)


def _format_json(checks: Sequence[H1FlbSmokeCheck]) -> str:
    payload = {
        "ok": all(check.passed for check in checks),
        "checks": [asdict(check) for check in checks],
    }
    return json.dumps(payload, allow_nan=False, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate saved H1 FLB PAPER smoke API snapshots."
    )
    parser.add_argument("--status-json", required=True, help="Saved /status JSON.")
    parser.add_argument(
        "--strategies-json",
        required=True,
        help="Saved /strategies JSON.",
    )
    parser.add_argument("--markets-json", required=True, help="Saved /markets JSON.")
    parser.add_argument(
        "--decisions-json",
        required=True,
        help="Saved /decisions JSON.",
    )
    parser.add_argument("--trades-json", required=True, help="Saved /trades JSON.")
    parser.add_argument(
        "--positions-json",
        required=True,
        help="Saved /positions JSON.",
    )
    parser.add_argument("--metrics-json", required=True, help="Saved /metrics JSON.")
    parser.add_argument("--min-heartbeats", type=int, default=1)
    parser.add_argument("--min-markets", type=int, default=1)
    parser.add_argument("--min-decisions", type=int, default=1)
    parser.add_argument("--min-trades", type=int, default=1)
    parser.add_argument("--min-positions", type=int, default=1)
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    args = parser.parse_args(argv)

    try:
        checks = check_h1_flb_smoke(
            status=_read_json(Path(str(args.status_json))),
            strategies=_read_json(Path(str(args.strategies_json))),
            markets=_read_json(Path(str(args.markets_json))),
            decisions=_read_json(Path(str(args.decisions_json))),
            trades=_read_json(Path(str(args.trades_json))),
            positions=_read_json(Path(str(args.positions_json))),
            metrics=_read_json(Path(str(args.metrics_json))),
            min_heartbeats=int(args.min_heartbeats),
            min_markets=int(args.min_markets),
            min_decisions=int(args.min_decisions),
            min_trades=int(args.min_trades),
            min_positions=int(args.min_positions),
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        checks = (H1FlbSmokeCheck("snapshot_load", False, str(exc)),)
    output = _format_json(checks) if bool(args.json) else _format_plain(checks)
    print(output)
    return 0 if all(check.passed for check in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
