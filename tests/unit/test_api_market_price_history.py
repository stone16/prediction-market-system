from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Sequence

import pytest

from pms.storage.market_data_store import MarketPriceSnapshotRow


@dataclass
class _PriceHistoryStoreDouble:
    results: list[Sequence[MarketPriceSnapshotRow] | None]
    calls: list[tuple[str, datetime, int]] = field(default_factory=list)

    async def read_price_history(
        self,
        *,
        condition_id: str,
        since: datetime,
        limit: int,
    ) -> Sequence[MarketPriceSnapshotRow] | None:
        self.calls.append((condition_id, since, limit))
        return self.results.pop(0)


@pytest.mark.asyncio
async def test_price_history_endpoint_returns_empty_array_for_no_snapshots() -> None:
    from pms.api.routes.markets import get_price_history

    now = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    store = _PriceHistoryStoreDouble(results=[[]])

    payload = await get_price_history(
        store,
        condition_id="market-empty",
        since=now - timedelta(hours=1),
        limit=1440,
        now=now,
    )

    assert payload.model_dump(mode="json") == {
        "condition_id": "market-empty",
        "snapshots": [],
    }
    assert store.calls == [("market-empty", now - timedelta(hours=1), 1440)]


@pytest.mark.asyncio
async def test_price_history_endpoint_404_for_unknown_market() -> None:
    from pms.api.routes.markets import MarketPriceHistoryNotFoundError, get_price_history

    now = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    store = _PriceHistoryStoreDouble(results=[None])

    with pytest.raises(MarketPriceHistoryNotFoundError):
        await get_price_history(
            store,
            condition_id="market-missing",
            since=now - timedelta(hours=1),
            limit=1440,
            now=now,
        )


@pytest.mark.asyncio
async def test_price_history_endpoint_default_since_is_24h_ago() -> None:
    from pms.api.routes.markets import get_price_history

    now = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    store = _PriceHistoryStoreDouble(results=[[]])

    await get_price_history(
        store,
        condition_id="market-default-since",
        since=None,
        limit=1440,
        now=now,
    )

    assert store.calls == [
        ("market-default-since", now - timedelta(hours=24), 1440)
    ]


@pytest.mark.asyncio
async def test_price_history_endpoint_caps_limit_at_10000() -> None:
    from pms.api.routes.markets import get_price_history

    now = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    store = _PriceHistoryStoreDouble(results=[[]])

    await get_price_history(
        store,
        condition_id="market-capped",
        since=now - timedelta(hours=1),
        limit=50_000,
        now=now,
    )

    assert store.calls == [("market-capped", now - timedelta(hours=1), 10_000)]
