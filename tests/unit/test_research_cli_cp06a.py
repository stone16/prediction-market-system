from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from pms.research.cache import FactorPanel, FactorPanelCache
from pms.research.sweep import CachedFactorPanelLoader


def test_pms_research_help_lists_sweep_and_worker_subcommands() -> None:
    result = subprocess.run(
        ["uv", "run", "pms-research", "--help"],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "sweep" in result.stdout
    assert "worker" in result.stdout


@pytest.mark.asyncio
async def test_cached_factor_panel_loader_hits_real_loader_once_per_cache_key() -> None:
    calls: list[tuple[str, str, tuple[str, ...], datetime, datetime]] = []

    async def fake_loader(
        _pool: Any,
        *,
        factor_id: str,
        param: str,
        market_ids: list[str],
        ts_start: datetime,
        ts_end: datetime,
    ) -> FactorPanel:
        calls.append((factor_id, param, tuple(market_ids), ts_start, ts_end))
        return cast(FactorPanel, {market_ids[0]: ()})

    loader = CachedFactorPanelLoader(
        pool=cast(Any, object()),
        cache=FactorPanelCache(),
        load_panel=fake_loader,
    )
    ts_start = datetime(2026, 4, 1, tzinfo=UTC)
    ts_end = datetime(2026, 4, 30, tzinfo=UTC)

    first_panel = await loader.get_panel(
        factor_id="orderbook_imbalance",
        param="",
        market_ids=("sweep-market",),
        ts_start=ts_start,
        ts_end=ts_end,
    )
    second_panel = await loader.get_panel(
        factor_id="orderbook_imbalance",
        param="",
        market_ids=("sweep-market",),
        ts_start=ts_start,
        ts_end=ts_end,
    )

    assert first_panel == {"sweep-market": ()}
    assert second_panel == first_panel
    assert calls == [
        ("orderbook_imbalance", "", ("sweep-market",), ts_start, ts_end)
    ]
    assert loader.cache.hit_rate() == pytest.approx(0.5)
