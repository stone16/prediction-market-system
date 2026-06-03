from __future__ import annotations

import csv
from pathlib import Path

import pytest

from scripts import export_paper_execution_from_api


def _decision(
    decision_id: str,
    *,
    status: str,
    side: str = "BUY",
    limit_price: float = 0.40,
    evidence: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "decision_id": decision_id,
        "market_id": "market-1",
        "strategy_id": "h1_flb",
        "strategy_version_id": "h1-flb-v1",
        "side": side,
        "action": side,
        "limit_price": limit_price,
        "status": status,
        "created_at": "2026-06-01T00:00:00+00:00",
        "decision_evidence": evidence or {},
    }


def _trade(
    decision_id: str,
    *,
    fill_price: float,
    filled_at: str = "2026-06-01T00:00:02+00:00",
) -> dict[str, object]:
    return {
        "trade_id": f"trade-{decision_id}",
        "decision_id": decision_id,
        "market_id": "market-1",
        "fill_price": fill_price,
        "filled_at": filled_at,
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_export_paper_execution_writes_strict_execution_and_telemetry_csvs(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "secure"
    output_dir.mkdir(mode=0o700)
    execution_path = output_dir / "paper-execution-export.csv"
    telemetry_path = output_dir / "paper-execution-telemetry.csv"

    export_paper_execution_from_api.export_paper_execution_artifacts(
        decisions=[
            _decision(
                "decision-filled",
                status="filled",
                evidence={
                    "execution_pnl": 1.25,
                    "adverse_selection_bps": 4.0,
                },
            ),
            _decision(
                "decision-rejected",
                status="rejected",
                evidence={
                    "execution_pnl": 0.0,
                    "rejection_reason": "risk_cap",
                },
            ),
        ],
        trades=[_trade("decision-filled", fill_price=0.41)],
        execution_output=execution_path,
        telemetry_output=telemetry_path,
        require_adverse_selection=True,
    )

    assert _read_csv(execution_path) == [
        {
            "decision_id": "decision-filled",
            "strategy_id": "h1_flb",
            "strategy_version_id": "h1-flb-v1",
            "market_id": "market-1",
            "status": "filled",
            "slippage_bps": "250.000000",
            "pnl": "1.250000",
            "rejection_reason": "",
        },
        {
            "decision_id": "decision-rejected",
            "strategy_id": "h1_flb",
            "strategy_version_id": "h1-flb-v1",
            "market_id": "market-1",
            "status": "rejected",
            "slippage_bps": "",
            "pnl": "0.000000",
            "rejection_reason": "risk_cap",
        },
    ]
    assert _read_csv(telemetry_path) == [
        {
            "slippage_bps": "250.000000",
            "latency_ms": "2000.000000",
            "adverse_selection_bps": "4.000000",
        }
    ]


def test_export_paper_execution_requires_adverse_selection_when_enabled(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "secure"
    output_dir.mkdir(mode=0o700)

    with pytest.raises(ValueError, match="missing adverse_selection_bps"):
        export_paper_execution_from_api.export_paper_execution_artifacts(
            decisions=[
                _decision(
                    "decision-filled",
                    status="filled",
                    evidence={"execution_pnl": 1.25},
                )
            ],
            trades=[_trade("decision-filled", fill_price=0.41)],
            execution_output=output_dir / "paper-execution-export.csv",
            telemetry_output=output_dir / "paper-execution-telemetry.csv",
            require_adverse_selection=True,
        )

    assert not (output_dir / "paper-execution-export.csv").exists()
    assert not (output_dir / "paper-execution-telemetry.csv").exists()


def test_export_paper_execution_rejects_open_decisions_by_default(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "secure"
    output_dir.mkdir(mode=0o700)

    with pytest.raises(ValueError, match="non-terminal PAPER decision"):
        export_paper_execution_from_api.export_paper_execution_artifacts(
            decisions=[
                _decision(
                    "decision-open",
                    status="submitted",
                    evidence={"execution_pnl": 0.0},
                )
            ],
            trades=[],
            execution_output=output_dir / "paper-execution-export.csv",
            telemetry_output=output_dir / "paper-execution-telemetry.csv",
        )
