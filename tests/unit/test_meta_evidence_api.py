from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast

import asyncpg
import pytest

from pms.api.routes.decay import get_strategy_decay_status
from pms.core.enums import OrderStatus


class _Connection:
    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        if "SELECT active_version_id" in query:
            return {"active_version_id": "meta-v1"}
        if "FROM strategy_performance_peaks" in query:
            return {
                "strategy_id": "meta-strategy",
                "strategy_version_id": "meta-v1",
                "peak_sharpe_7d": 1.0,
                "peak_sharpe_30d": 1.0,
                "peak_hit_rate": 1.0,
                "recorded_at": datetime(2026, 5, 5, tzinfo=UTC),
            }
        if "FROM alpha_competition_snapshots" in query:
            return {
                "snapshot_id": "snapshot-meta",
                "strategy_id": "meta-strategy",
                "strategy_version_id": "meta-v1",
                "snapshot_date": date(2026, 5, 6),
                "mean_edge_30d": 0.05,
                "mean_spread_bps_30d": 120.0,
                "edge_trend_slope_90d": None,
                "spread_trend_slope_90d": None,
                "sample_count_30d": 12,
                "trend_status": "warming_up",
                "days_collected": 12,
                "short_term_slope_30d": None,
                "short_term_slope_60d": None,
                "interpretation": "warming_up",
                "created_at": datetime(2026, 5, 6, tzinfo=UTC),
            }
        del query, args
        return None

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        assert "FROM eval_records" in query
        del args
        return [
            {
                "market_id": "market-meta",
                "decision_id": f"decision-{index}",
                "strategy_id": "meta-strategy",
                "strategy_version_id": "meta-v1",
                "prob_estimate": 0.7,
                "resolved_outcome": 1.0,
                "brier_score": 0.09,
                "fill_status": OrderStatus.MATCHED.value,
                "recorded_at": datetime(2026, 5, 1 + index, tzinfo=UTC),
                "citations": ["unit"],
                "category": None,
                "model_id": None,
                "pnl": 1.0,
                "slippage_bps": 10.0,
                "filled": True,
                "edge_at_decision": 0.05,
                "spread_bps_at_decision": 120,
            }
            for index in range(10)
        ]

    async def execute(self, query: str, *args: object) -> str:
        del query, args
        return "OK"


class _Acquire:
    async def __aenter__(self) -> _Connection:
        return _Connection()

    async def __aexit__(self, *_: object) -> None:
        return None


class _Pool:
    def acquire(self) -> _Acquire:
        return _Acquire()


@pytest.mark.asyncio
async def test_decay_status_route_payload_includes_decay_and_competition_metrics() -> None:
    payload = await get_strategy_decay_status(
        cast(asyncpg.Pool, _Pool()),
        strategy_id="meta-strategy",
    )

    assert payload["strategy_id"] == "meta-strategy"
    assert payload["strategy_version_id"] == "meta-v1"
    assert payload["decay_status"] in {"healthy", "insufficient_peak_data"}
    assert payload["resolved_sample_count"] == 10
    assert payload["alpha_competition"] == {
        "mean_edge_30d": 0.05,
        "mean_spread_bps_30d": 120.0,
        "edge_trend_slope_90d": None,
        "spread_trend_slope_90d": None,
        "trend_status": "warming_up",
        "days_collected": 12,
        "interpretation": "warming_up",
        "sample_count_30d": 12,
        "last_snapshot_date": "2026-05-06",
    }
