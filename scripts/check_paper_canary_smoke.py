"""Validate saved PAPER canary smoke snapshots.

The paper canary is a no-credential plumbing smoke: live market data should
reach the controller, produce at least one ``paper_canary_v1`` decision, and
flow through the paper actuator into a matched fill. This checker validates
captured API payloads so the smoke evidence is repeatable without keeping the
API process running.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pms.metrics import (
    SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC,
    SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC,
    SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC,
    SELECTION_FUNNEL_ROUTED_TOTAL_METRIC,
    SELECTION_FUNNEL_SELECTED_TOTAL_METRIC,
    SELECTION_FUNNEL_TRADED_TOTAL_METRIC,
)


PAPER_CANARY_STRATEGY_ID = "paper_canary_v1"
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


@dataclass(frozen=True, slots=True)
class PaperCanarySmokeCheck:
    name: str
    passed: bool
    detail: str


def check_paper_canary_smoke(
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
) -> tuple[PaperCanarySmokeCheck, ...]:
    """Return machine-checkable paper canary smoke checks."""
    status_obj = _as_mapping(status)
    metrics_obj = _as_mapping(metrics)
    return (
        _check_paper_mode(status_obj),
        _check_runtime_continuity(status_obj, min_heartbeats=min_heartbeats),
        _check_sensor_activity(status_obj),
        _check_active_strategy(strategies),
        _check_market_discovery(markets, min_markets=min_markets),
        _check_controller_decisions(
            status_obj,
            decisions,
            min_decisions=min_decisions,
        ),
        _check_paper_trades(status_obj, trades, min_trades=min_trades),
        _check_open_positions(positions),
        _check_selection_funnel(metrics_obj),
        _check_first_trade_time(metrics_obj),
    )


def _check_paper_mode(status: Mapping[str, object]) -> PaperCanarySmokeCheck:
    actuator = _as_mapping(status.get("actuator"))
    mode = _string(status.get("mode"))
    actuator_mode = _string(actuator.get("mode"))
    running = status.get("running")
    if mode != "paper":
        return PaperCanarySmokeCheck("paper_mode", False, f"mode must be paper: {mode!r}")
    if actuator_mode not in {"paper", ""}:
        return PaperCanarySmokeCheck(
            "paper_mode",
            False,
            f"actuator.mode must be paper: {actuator_mode!r}",
        )
    if running is not True:
        return PaperCanarySmokeCheck(
            "paper_mode",
            False,
            f"runner must be running when the status snapshot is captured: {running!r}",
        )
    return PaperCanarySmokeCheck(
        "paper_mode",
        True,
        "mode=paper; actuator.mode=paper; running=true",
    )


def _check_runtime_continuity(
    status: Mapping[str, object],
    *,
    min_heartbeats: int,
) -> PaperCanarySmokeCheck:
    continuity = _as_mapping(status.get("runtime_continuity"))
    heartbeat_count = _int_value(continuity.get("heartbeat_count"))
    unhealthy_count = _int_value(continuity.get("unhealthy_heartbeat_count"))
    if heartbeat_count < min_heartbeats:
        return PaperCanarySmokeCheck(
            "runtime_continuity",
            False,
            f"heartbeat_count={heartbeat_count} below required {min_heartbeats}",
        )
    if unhealthy_count > 0:
        return PaperCanarySmokeCheck(
            "runtime_continuity",
            False,
            f"unhealthy_heartbeat_count={unhealthy_count}",
        )
    return PaperCanarySmokeCheck(
        "runtime_continuity",
        True,
        f"heartbeat_count={heartbeat_count}; unhealthy_heartbeat_count=0",
    )


def _check_sensor_activity(status: Mapping[str, object]) -> PaperCanarySmokeCheck:
    rows = _rows_from_payload(status.get("sensors"), "sensors")
    if not rows:
        return PaperCanarySmokeCheck("sensor_activity", False, "no sensor rows")
    bad_rows = [
        f"{_string(row.get('name'))}:{_string(row.get('status'))}"
        for row in rows
        if _string(row.get("status")) in _BAD_SENSOR_STATUSES
    ]
    if bad_rows:
        return PaperCanarySmokeCheck(
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
        return PaperCanarySmokeCheck(
            "sensor_activity",
            False,
            f"missing sensor layer(s): {', '.join(missing_layers)}",
        )
    if not any(_string(row.get("last_signal_at")) for row in rows):
        return PaperCanarySmokeCheck(
            "sensor_activity",
            False,
            "no last_signal_at observed",
        )
    statuses = ", ".join(
        f"{_string(row.get('name'))}:{_string(row.get('status'))}" for row in rows
    )
    return PaperCanarySmokeCheck("sensor_activity", True, statuses)


def _check_active_strategy(strategies: object) -> PaperCanarySmokeCheck:
    rows = _rows_from_payload(strategies, "strategies")
    for row in rows:
        if _string(row.get("strategy_id")) != PAPER_CANARY_STRATEGY_ID:
            continue
        active_version_id = _string(row.get("active_version_id"))
        if active_version_id == "":
            return PaperCanarySmokeCheck(
                "active_strategy",
                False,
                "paper_canary_v1 has no active_version_id",
            )
        return PaperCanarySmokeCheck(
            "active_strategy",
            True,
            f"paper_canary_v1@{active_version_id}",
        )
    return PaperCanarySmokeCheck(
        "active_strategy",
        False,
        "paper_canary_v1 is not present in /strategies",
    )


def _check_market_discovery(markets: object, *, min_markets: int) -> PaperCanarySmokeCheck:
    market_obj = _as_mapping(markets)
    rows = _rows_from_payload(markets, "markets")
    total = _int_value(market_obj.get("total"))
    observed = max(total, len(rows))
    if observed < min_markets:
        return PaperCanarySmokeCheck(
            "market_discovery",
            False,
            f"observed_markets={observed} below required {min_markets}",
        )
    return PaperCanarySmokeCheck(
        "market_discovery",
        True,
        f"observed_markets={observed}",
    )


def _check_controller_decisions(
    status: Mapping[str, object],
    decisions: object,
    *,
    min_decisions: int,
) -> PaperCanarySmokeCheck:
    controller = _as_mapping(status.get("controller"))
    status_decisions = _int_value(controller.get("decisions_total"))
    rows = [
        row
        for row in _rows_from_payload(decisions, "decisions")
        if _string(row.get("strategy_id")) == PAPER_CANARY_STRATEGY_ID
    ]
    accepted_rows = [
        row
        for row in rows
        if _string(row.get("status")) in _ACCEPTED_DECISION_STATUSES
    ]
    if status_decisions < min_decisions:
        return PaperCanarySmokeCheck(
            "controller_decisions",
            False,
            f"status.controller.decisions_total={status_decisions} below required {min_decisions}",
        )
    if len(accepted_rows) < min_decisions:
        return PaperCanarySmokeCheck(
            "controller_decisions",
            False,
            (
                f"accepted paper_canary_v1 decision rows={len(accepted_rows)} "
                f"below required {min_decisions}"
            ),
        )
    decision_ids = ", ".join(_string(row.get("decision_id")) for row in accepted_rows)
    return PaperCanarySmokeCheck(
        "controller_decisions",
        True,
        f"decisions_total={status_decisions}; accepted_decisions={decision_ids}",
    )


def _check_paper_trades(
    status: Mapping[str, object],
    trades: object,
    *,
    min_trades: int,
) -> PaperCanarySmokeCheck:
    actuator = _as_mapping(status.get("actuator"))
    fills_total = _int_value(actuator.get("fills_total"))
    if fills_total < min_trades:
        return PaperCanarySmokeCheck(
            "paper_trades",
            False,
            f"status.actuator.fills_total={fills_total} below required {min_trades}",
        )
    rows = [
        row
        for row in _rows_from_payload(trades, "trades")
        if _string(row.get("strategy_id")) == PAPER_CANARY_STRATEGY_ID
    ]
    rows = [
        row
        for row in rows
        if _float_value(row.get("fill_notional_usdc")) > 0.0
        and _float_value(row.get("fill_quantity")) > 0.0
    ]
    if len(rows) < min_trades:
        return PaperCanarySmokeCheck(
            "paper_trades",
            False,
            f"no paper_canary_v1 trade rows with positive fill quantity/notional",
        )
    trade_ids = ", ".join(_string(row.get("trade_id")) for row in rows)
    return PaperCanarySmokeCheck(
        "paper_trades",
        True,
        f"fills_total={fills_total}; trades={trade_ids}",
    )


def _check_open_positions(positions: object) -> PaperCanarySmokeCheck:
    rows = [
        row
        for row in _rows_from_payload(positions, "positions")
        if _string(row.get("strategy_id")) == PAPER_CANARY_STRATEGY_ID
        and _float_value(row.get("shares_held")) > 0.0
    ]
    if not rows:
        return PaperCanarySmokeCheck(
            "open_positions",
            False,
            "no open paper_canary_v1 positions",
        )
    return PaperCanarySmokeCheck(
        "open_positions",
        True,
        f"open_positions={len(rows)}",
    )


def _check_selection_funnel(metrics: Mapping[str, object]) -> PaperCanarySmokeCheck:
    values: list[tuple[str, float]] = []
    for name in _SELECTION_FUNNEL_METRICS:
        value = _metric_value(metrics, name)
        if value is None:
            return PaperCanarySmokeCheck(
                "selection_funnel",
                False,
                f"{name} missing or non-numeric",
            )
        values.append((name, value))
    for name, value in values:
        if value < 1.0:
            return PaperCanarySmokeCheck(
                "selection_funnel",
                False,
                f"{name}={_format_number(value)} below required 1",
            )
    detail = "; ".join(f"{name}={_format_number(value)}" for name, value in values)
    return PaperCanarySmokeCheck("selection_funnel", True, detail)


def _check_first_trade_time(metrics: Mapping[str, object]) -> PaperCanarySmokeCheck:
    value = _metric_value(metrics, FIRST_TRADE_TIME_SECONDS_METRIC)
    if value is None or value < 0.0:
        return PaperCanarySmokeCheck(
            "first_trade_time",
            False,
            f"{FIRST_TRADE_TIME_SECONDS_METRIC} missing, non-numeric, or negative",
        )
    return PaperCanarySmokeCheck(
        "first_trade_time",
        True,
        f"{FIRST_TRADE_TIME_SECONDS_METRIC}={_format_number(value)}",
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


def _format_plain(checks: Sequence[PaperCanarySmokeCheck]) -> str:
    lines: list[str] = []
    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        lines.append(f"[{status}] {check.name}: {check.detail}")
    return "\n".join(lines)


def _format_json(checks: Sequence[PaperCanarySmokeCheck]) -> str:
    payload = {
        "ok": all(check.passed for check in checks),
        "checks": [asdict(check) for check in checks],
    }
    return json.dumps(payload, allow_nan=False, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate saved PAPER canary smoke API snapshots."
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
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    args = parser.parse_args(argv)

    try:
        checks = check_paper_canary_smoke(
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
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        checks = (
            PaperCanarySmokeCheck(
                "snapshot_load",
                False,
                str(exc),
            ),
        )
    output = _format_json(checks) if bool(args.json) else _format_plain(checks)
    print(output)
    return 0 if all(check.passed for check in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
