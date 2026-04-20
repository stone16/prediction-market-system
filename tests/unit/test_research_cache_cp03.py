from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from pms.factors.base import FactorValueRow
from pms.factors.service import FactorService
from pms.research.cache import FactorPanelCache, FactorPanelKey
from pms.storage.market_data_store import PostgresMarketDataStore


def _time_bounds() -> tuple[datetime, datetime]:
    return (
        datetime(2026, 4, 1, tzinfo=UTC),
        datetime(2026, 4, 30, 23, 59, tzinfo=UTC),
    )


def _key(*, threshold: float, market_ids: list[str]) -> FactorPanelKey:
    ts_start, ts_end = _time_bounds()
    return FactorPanelKey.from_inputs(
        factor_id="orderbook_imbalance",
        param={"threshold": threshold, "window": 30},
        market_ids=market_ids,
        ts_start=ts_start,
        ts_end=ts_end,
    )


class _EmptySignalStream:
    def __aiter__(self) -> "_EmptySignalStream":
        return self

    async def __anext__(self) -> Any:
        raise StopAsyncIteration


class _PanelConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self.last_fetch_args: tuple[object, ...] | None = None

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        del query
        self.last_fetch_args = args
        return self._rows


class _PanelAcquireContext:
    def __init__(self, connection: _PanelConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _PanelConnection:
        return self._connection

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb


class _PanelPool:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.connection = _PanelConnection(rows)

    def acquire(self) -> _PanelAcquireContext:
        return _PanelAcquireContext(self.connection)


def test_factor_panel_key_normalizes_param_and_market_id_order() -> None:
    ts_start, ts_end = _time_bounds()

    key = FactorPanelKey.from_inputs(
        factor_id="fair_value_gap",
        param={"window": 30, "threshold": 0.02},
        market_ids=["market-b", "market-a"],
        ts_start=ts_start,
        ts_end=ts_end,
    )

    assert key.param == (("threshold", 0.02), ("window", 30))
    assert key.market_ids == frozenset({"market-a", "market-b"})


def test_factor_panel_cache_tracks_hits_misses_and_zero_safe_hit_rate() -> None:
    cache = FactorPanelCache()
    key = _key(threshold=0.01, market_ids=["market-a"])
    panel = {
        "market-a": (
            FactorValueRow(
                factor_id="orderbook_imbalance",
                param="",
                market_id="market-a",
                ts=datetime(2026, 4, 18, tzinfo=UTC),
                value=0.12,
            ),
        )
    }

    assert cache.hit_rate() == 0.0
    assert cache.get(key) is None
    cache.put(key, panel)

    assert cache.get(key) == panel
    assert cache.hits == 1
    assert cache.misses == 1
    assert cache.hit_rate() == 0.5


def test_sweep_cache_key_derivation_distinguishes_param_variants_only() -> None:
    keys = [_key(threshold=0.01 + (step / 100.0), market_ids=["market-a"]) for step in range(10)]

    assert len(set(keys)) == 10

    factor_compositions = ("weighted", "rules", "bayes")
    raw_factor_keys = [
        _key(threshold=0.01, market_ids=["market-a", "market-b"])
        for _composition in factor_compositions
    ]

    assert len(set(raw_factor_keys)) == 1


def test_factor_panel_cache_round_trip_preserves_market_id_mapping() -> None:
    cache = FactorPanelCache()
    first_key = _key(threshold=0.01, market_ids=["market-b", "market-a"])
    equivalent_key = _key(threshold=0.01, market_ids=["market-a", "market-b"])
    panel = {
        "market-b": (
            FactorValueRow(
                factor_id="orderbook_imbalance",
                param="",
                market_id="market-b",
                ts=datetime(2026, 4, 18, tzinfo=UTC),
                value=0.21,
            ),
        ),
        "market-a": (
            FactorValueRow(
                factor_id="orderbook_imbalance",
                param="",
                market_id="market-a",
                ts=datetime(2026, 4, 18, tzinfo=UTC),
                value=0.34,
            ),
        ),
    }

    cache.put(first_key, panel)
    cached_panel = cache.get(equivalent_key)

    assert list(cached_panel or {}) == ["market-b", "market-a"]
    assert cached_panel is not None
    assert cached_panel["market-a"][0].value == 0.34
    assert cached_panel["market-b"][0].value == 0.21


def test_factor_panel_cache_disabled_path_forces_miss_without_counting() -> None:
    cache = FactorPanelCache(enabled=False)
    key = _key(threshold=0.01, market_ids=["market-a"])

    cache.put(key, {})

    assert cache.get(key) is None
    assert cache.hits == 0
    assert cache.misses == 0
    assert cache.hit_rate() == 0.0


@pytest.mark.asyncio
async def test_factor_service_get_panel_preserves_requested_market_order() -> None:
    ts_start, ts_end = _time_bounds()
    pool = _PanelPool(
        [
            {
                "factor_id": "orderbook_imbalance",
                "param": "",
                "market_id": "market-a",
                "ts": datetime(2026, 4, 18, tzinfo=UTC),
                "value": 0.11,
            },
            {
                "factor_id": "orderbook_imbalance",
                "param": "",
                "market_id": "market-b",
                "ts": datetime(2026, 4, 18, tzinfo=UTC),
                "value": 0.22,
            },
        ]
    )
    service = FactorService(
        pool=cast(Any, pool),
        store=cast(PostgresMarketDataStore, object()),
        cadence_s=1.0,
        factors=(),
        signal_stream=_EmptySignalStream(),
    )

    panel = await service.get_panel(
        "orderbook_imbalance",
        {"window": 30, "threshold": 0.01},
        ["market-b", "market-a"],
        ts_start,
        ts_end,
    )

    assert list(panel) == ["market-b", "market-a"]
    assert panel["market-a"][0].value == 0.11
    assert panel["market-b"][0].value == 0.22
    assert pool.connection.last_fetch_args is not None
    assert pool.connection.last_fetch_args[1] == "threshold=0.01&window=30"
