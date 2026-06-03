from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts import export_backtest_execution_from_db


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class _FakeConnection:
    def __init__(self, *, status: str | None) -> None:
        self.status = status
        self.fetch_called = False
        self.closed = False

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        del query, args
        if self.status is None:
            return None
        return {"status": self.status}

    async def fetch(self, query: str, *args: object) -> tuple[dict[str, object], ...]:
        del query, args
        self.fetch_called = True
        return ()

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_fetch_backtest_execution_rows_refuses_non_completed_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection(status="running")

    async def _connect(database_url: str) -> _FakeConnection:
        assert database_url == "postgresql://example"
        return connection

    monkeypatch.setattr(
        "scripts.export_backtest_execution_from_db.asyncpg.connect",
        _connect,
    )

    with pytest.raises(ValueError, match="status=running"):
        await export_backtest_execution_from_db._fetch_backtest_execution_rows(
            database_url="postgresql://example",
            run_id="11111111-1111-1111-1111-111111111111",
        )

    assert connection.fetch_called is False
    assert connection.closed is True


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
