from __future__ import annotations

import importlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

from pms.metrics import (
    SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC,
    SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC,
    SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC,
    SELECTION_FUNNEL_ROUTED_TOTAL_METRIC,
    SELECTION_FUNNEL_SELECTED_TOTAL_METRIC,
    SELECTION_FUNNEL_TRADED_TOTAL_METRIC,
)


def _module() -> ModuleType:
    spec = importlib.util.find_spec("scripts.check_paper_canary_smoke")
    assert spec is not None, "scripts/check_paper_canary_smoke.py must exist"
    return importlib.import_module("scripts.check_paper_canary_smoke")


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _passing_snapshot_paths(tmp_path: Path) -> dict[str, Path]:
    strategy_version_id = "canary-version"
    return {
        "status": _write_json(
            tmp_path / "status.json",
            {
                "mode": "paper",
                "running": True,
                "runtime_continuity": {
                    "source": "postgres_runtime_heartbeats",
                    "heartbeat_count": 3,
                    "unhealthy_heartbeat_count": 0,
                },
                "sensors": [
                    {
                        "name": "MarketDiscoverySensor",
                        "status": "running",
                        "last_signal_at": "2026-06-03T06:40:00+00:00",
                    },
                    {
                        "name": "MarketDataSensor",
                        "status": "running",
                        "last_signal_at": "2026-06-03T06:40:00+00:00",
                    },
                ],
                "controller": {
                    "decisions_total": 1,
                    "diagnostics_total": 5,
                    "diagnostic_counts": {"spread_too_wide": 4},
                },
                "actuator": {
                    "mode": "paper",
                    "fills_total": 1,
                    "halted": False,
                },
            },
        ),
        "strategies": _write_json(
            tmp_path / "strategies.json",
            {
                "strategies": [
                    {
                        "strategy_id": "paper_canary_v1",
                        "active_version_id": strategy_version_id,
                        "created_at": "2026-06-03T06:35:00+00:00",
                    }
                ]
            },
        ),
        "markets": _write_json(
            tmp_path / "markets.json",
            {
                "markets": [
                    {
                        "market_id": "m-1",
                        "question": "Will the paper canary trade?",
                        "venue": "polymarket",
                        "yes_token_id": "yes-1",
                        "no_token_id": "no-1",
                        "updated_at": "2026-06-03T06:39:00+00:00",
                        "subscribed": True,
                    }
                ],
                "limit": 5,
                "offset": 0,
                "total": 2120,
            },
        ),
        "decisions": _write_json(
            tmp_path / "decisions.json",
            [
                {
                    "decision_id": "d-1",
                    "market_id": "m-1",
                    "strategy_id": "paper_canary_v1",
                    "strategy_version_id": strategy_version_id,
                    "status": "filled",
                    "limit_price": 0.768,
                    "spread_bps_at_decision": 78,
                    "decision_evidence": {
                        "forecaster": "paper_canary",
                        "strategy_evidence": (
                            f"paper_canary_v1@{strategy_version_id}"
                        ),
                    },
                    "created_at": "2026-06-03T06:41:00+00:00",
                }
            ],
        ),
        "trades": _write_json(
            tmp_path / "trades.json",
            {
                "trades": [
                    {
                        "trade_id": "t-1",
                        "fill_id": "f-1",
                        "order_id": "o-1",
                        "decision_id": "d-1",
                        "market_id": "m-1",
                        "strategy_id": "paper_canary_v1",
                        "strategy_version_id": strategy_version_id,
                        "status": "matched",
                        "fill_notional_usdc": 1.0,
                        "fill_quantity": 1.3,
                        "fill_price": 0.768,
                    }
                ],
                "limit": 50,
                "offset": 0,
            },
        ),
        "positions": _write_json(
            tmp_path / "positions.json",
            {
                "positions": [
                    {
                        "market_id": "m-1",
                        "token_id": "yes-1",
                        "venue": "polymarket",
                        "side": "buy",
                        "shares_held": 1.3,
                        "avg_entry_price": 0.768,
                        "unrealized_pnl": 0.0,
                        "locked_usdc": 1.0,
                        "strategy_id": "paper_canary_v1",
                        "strategy_version_id": strategy_version_id,
                    }
                ]
            },
        ),
        "metrics": _write_json(
            tmp_path / "metrics.json",
            {
                SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC: 10.0,
                SELECTION_FUNNEL_SELECTED_TOTAL_METRIC: 4.0,
                SELECTION_FUNNEL_ROUTED_TOTAL_METRIC: 3.0,
                SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC: 2.0,
                SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC: 1.0,
                SELECTION_FUNNEL_TRADED_TOTAL_METRIC: 1.0,
                "pms.ui.first_trade_time_seconds": 0.05,
                "mark_to_market": {"open_positions": 1},
            },
        ),
    }


def _argv(paths: dict[str, Path], *extra: str) -> list[str]:
    return [
        "--status-json",
        str(paths["status"]),
        "--strategies-json",
        str(paths["strategies"]),
        "--markets-json",
        str(paths["markets"]),
        "--decisions-json",
        str(paths["decisions"]),
        "--trades-json",
        str(paths["trades"]),
        "--positions-json",
        str(paths["positions"]),
        "--metrics-json",
        str(paths["metrics"]),
        *extra,
    ]


def test_check_paper_canary_smoke_passes_with_live_data_controller_and_paper_fill(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    paths = _passing_snapshot_paths(tmp_path)

    exit_code = module.main(_argv(paths))

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "[PASS] paper_mode:" in captured.out
    assert "[PASS] active_strategy:" in captured.out
    assert "[PASS] paper_trades:" in captured.out
    assert "[PASS] selection_funnel:" in captured.out


def test_check_paper_canary_smoke_fails_without_a_canary_trade(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    paths = _passing_snapshot_paths(tmp_path)
    _write_json(paths["trades"], {"trades": [], "limit": 50, "offset": 0})

    exit_code = module.main(_argv(paths))

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "[FAIL] paper_trades:" in captured.out
    assert "no paper_canary_v1 trade rows" in captured.out


def test_check_paper_canary_smoke_fails_when_selection_funnel_never_traded(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    paths = _passing_snapshot_paths(tmp_path)
    metrics = json.loads(paths["metrics"].read_text(encoding="utf-8"))
    metrics[SELECTION_FUNNEL_TRADED_TOTAL_METRIC] = 0.0
    _write_json(paths["metrics"], metrics)

    exit_code = module.main(_argv(paths))

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "[FAIL] selection_funnel:" in captured.out
    assert "pms_selection_funnel_traded_total=0" in captured.out


def test_check_paper_canary_smoke_json_output_is_machine_readable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    paths = _passing_snapshot_paths(tmp_path)

    exit_code = module.main(_argv(paths, "--json"))

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert {check["name"] for check in payload["checks"]} >= {
        "paper_mode",
        "active_strategy",
        "paper_trades",
        "selection_funnel",
    }
