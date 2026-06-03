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
    spec = importlib.util.find_spec("scripts.check_h1_flb_smoke")
    assert spec is not None, "scripts/check_h1_flb_smoke.py must exist"
    return importlib.import_module("scripts.check_h1_flb_smoke")


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _passing_snapshot_paths(tmp_path: Path) -> dict[str, Path]:
    strategy_version_id = "h1-version"
    return {
        "status": _write_json(
            tmp_path / "status.json",
            {
                "mode": "paper",
                "running": True,
                "runtime_continuity": {
                    "source": "postgres_runtime_heartbeats",
                    "heartbeat_count": 2,
                    "unhealthy_heartbeat_count": 0,
                },
                "sensors": [
                    {
                        "name": "MarketDiscoverySensor",
                        "status": "running",
                        "last_signal_at": "2026-06-03T08:30:00+00:00",
                    },
                    {
                        "name": "MarketDataSensor",
                        "status": "running",
                        "last_signal_at": "2026-06-03T08:30:00+00:00",
                    },
                ],
                "controller": {
                    "decisions_total": 3,
                    "diagnostics_total": 19,
                },
                "actuator": {
                    "mode": "paper",
                    "fills_total": 3,
                    "halted": False,
                },
            },
        ),
        "strategies": _write_json(
            tmp_path / "strategies.json",
            {
                "strategies": [
                    {
                        "strategy_id": "default",
                        "active_version_id": None,
                    },
                    {
                        "strategy_id": "h1_flb",
                        "active_version_id": strategy_version_id,
                    },
                ]
            },
        ),
        "markets": _write_json(
            tmp_path / "markets.json",
            {
                "markets": [
                    {
                        "market_id": "m-1",
                        "question": "Will H1 FLB paper trade?",
                        "venue": "polymarket",
                    }
                ],
                "limit": 5,
                "offset": 0,
                "total": 978,
            },
        ),
        "decisions": _write_json(
            tmp_path / "decisions.json",
            [
                {
                    "decision_id": "d-1",
                    "market_id": "m-1",
                    "strategy_id": "h1_flb",
                    "strategy_version_id": strategy_version_id,
                    "status": "filled",
                    "forecaster": "FlbForecaster",
                    "decision_evidence": {
                        "category_prior_baseline_prob_estimate": 0.36,
                        "market_implied_baseline_prob_estimate": 0.95,
                        "mid_quote_baseline_prob_estimate": 0.94,
                        "net_edge_after_costs": 0.02,
                        "fee_edge_at_decision": 0.004,
                        "spread_edge_at_decision": 0.003,
                    },
                }
            ],
        ),
        "trades": _write_json(
            tmp_path / "trades.json",
            {
                "trades": [
                    {
                        "trade_id": "t-1",
                        "decision_id": "d-1",
                        "market_id": "m-1",
                        "strategy_id": "h1_flb",
                        "strategy_version_id": strategy_version_id,
                        "status": "matched",
                        "fill_notional_usdc": 1.0,
                        "fill_quantity": 1.05,
                        "fill_price": 0.953,
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
                        "strategy_id": "h1_flb",
                        "strategy_version_id": strategy_version_id,
                        "shares_held": 1.05,
                        "locked_usdc": 1.0,
                    }
                ]
            },
        ),
        "metrics": _write_json(
            tmp_path / "metrics.json",
            {
                SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC: 978.0,
                SELECTION_FUNNEL_SELECTED_TOTAL_METRIC: 642.0,
                SELECTION_FUNNEL_ROUTED_TOTAL_METRIC: 22.0,
                SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC: 3.0,
                SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC: 3.0,
                SELECTION_FUNNEL_TRADED_TOTAL_METRIC: 3.0,
                "pms.ui.first_trade_time_seconds": 0.035141,
                "quote_calibration": {"record_count": 3},
                "mark_to_market": {"open_positions": 3, "locked_usdc": 3.0},
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


def test_check_h1_flb_smoke_passes_with_filled_h1_paper_trade(
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
    assert "[PASS] h1_decision_evidence:" in captured.out
    assert "[PASS] paper_trades:" in captured.out
    assert "[PASS] quote_calibration:" in captured.out


def test_check_h1_flb_smoke_rejects_stale_strategy_version_rows(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    paths = _passing_snapshot_paths(tmp_path)
    stale_version = "stale-h1-version"
    decisions = json.loads(paths["decisions"].read_text(encoding="utf-8"))
    trades = json.loads(paths["trades"].read_text(encoding="utf-8"))
    positions = json.loads(paths["positions"].read_text(encoding="utf-8"))
    decisions[0]["strategy_version_id"] = stale_version
    trades["trades"][0]["strategy_version_id"] = stale_version
    positions["positions"][0]["strategy_version_id"] = stale_version
    _write_json(paths["decisions"], decisions)
    _write_json(paths["trades"], trades)
    _write_json(paths["positions"], positions)

    exit_code = module.main(_argv(paths))

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "[FAIL] controller_decisions:" in captured.out
    assert "h1_flb@h1-version" in captured.out
    assert stale_version in captured.out


def test_check_h1_flb_smoke_rejects_missing_decision_cost_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    paths = _passing_snapshot_paths(tmp_path)
    decisions = json.loads(paths["decisions"].read_text(encoding="utf-8"))
    decisions[0]["decision_evidence"].pop("net_edge_after_costs")
    _write_json(paths["decisions"], decisions)

    exit_code = module.main(_argv(paths))

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "[FAIL] h1_decision_evidence:" in captured.out
    assert "net_edge_after_costs" in captured.out


def test_check_h1_flb_smoke_honors_min_positions(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    paths = _passing_snapshot_paths(tmp_path)

    exit_code = module.main(_argv(paths, "--min-positions", "2"))

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "[FAIL] open_positions:" in captured.out
    assert "below required 2" in captured.out


def test_check_h1_flb_smoke_json_output_is_machine_readable(
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
        "active_strategy",
        "h1_decision_evidence",
        "paper_trades",
        "quote_calibration",
    }
