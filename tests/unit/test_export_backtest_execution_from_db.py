from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

from scripts import export_backtest_execution_from_db


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_export_backtest_execution_writes_strict_diff_csv(tmp_path: Path) -> None:
    output_dir = tmp_path / "secure"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "backtest-execution-export.csv"

    export_backtest_execution_from_db.write_backtest_execution_csv(
        [
            {
                "decision_id": "decision-filled",
                "strategy_id": "h1_flb",
                "strategy_version_id": "h1-flb-v1",
                "market_id": "market-1",
                "status": "filled",
                "slippage_bps": 2.5,
                "pnl": 1.25,
                "rejection_reason": None,
                "created_at": datetime(2026, 6, 1, tzinfo=UTC),
            },
            {
                "decision_id": "decision-rejected",
                "strategy_id": "h1_flb",
                "strategy_version_id": "h1-flb-v1",
                "market_id": "market-2",
                "status": "rejected",
                "slippage_bps": None,
                "pnl": 0.0,
                "rejection_reason": "ioc_unfilled",
                "created_at": datetime(2026, 6, 1, 0, 1, tzinfo=UTC),
            },
        ],
        output_path,
    )

    assert _read_csv(output_path) == [
        {
            "decision_id": "decision-filled",
            "strategy_id": "h1_flb",
            "strategy_version_id": "h1-flb-v1",
            "market_id": "market-1",
            "status": "filled",
            "slippage_bps": "2.500000",
            "pnl": "1.250000",
            "rejection_reason": "",
        },
        {
            "decision_id": "decision-rejected",
            "strategy_id": "h1_flb",
            "strategy_version_id": "h1-flb-v1",
            "market_id": "market-2",
            "status": "rejected",
            "slippage_bps": "",
            "pnl": "0.000000",
            "rejection_reason": "ioc_unfilled",
        },
    ]
