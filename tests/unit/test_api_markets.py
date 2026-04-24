from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx
import pytest

from pms.api.app import create_app
from pms.runner import Runner
from pms.storage.market_data_store import MarketFilters


def _record(
    *,
    market_id: str,
    question: str,
    venue: str = "polymarket",
    volume_24h: float | None = 1000.0,
    updated_at: datetime | None = None,
    resolves_at: datetime | None = None,
    yes_token_id: str | None = None,
    no_token_id: str | None = None,
    yes_price: float | None = None,
    no_price: float | None = None,
    best_bid: float | None = None,
    best_ask: float | None = None,
    last_trade_price: float | None = None,
    liquidity: float | None = None,
    spread_bps: int | None = None,
    price_updated_at: datetime | None = None,
    subscription_source: str | None = None,
) -> Any:
    from pms.api.routes.markets import StoredMarketRow

    timestamp = updated_at or datetime(2026, 4, 23, 9, 0, tzinfo=UTC)
    return StoredMarketRow(
        market_id=market_id,
        question=question,
        venue=cast(Any, venue),
        volume_24h=volume_24h,
        updated_at=timestamp,
        resolves_at=resolves_at,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        yes_price=yes_price,
        no_price=no_price,
        best_bid=best_bid,
        best_ask=best_ask,
        last_trade_price=last_trade_price,
        liquidity=liquidity,
        spread_bps=spread_bps,
        price_updated_at=price_updated_at,
        subscription_source=subscription_source,
    )


class _StoreDouble:
    def __init__(self, rows: list[Any], total: int) -> None:
        self._rows = rows
        self._total = total
        self.calls: list[tuple[int, int, MarketFilters, frozenset[str]]] = []
        self.by_id_calls: list[tuple[str, frozenset[str]]] = []

    async def read_markets(
        self,
        *,
        limit: int,
        offset: int,
        filters: MarketFilters | None = None,
        current_asset_ids: frozenset[str] = frozenset(),
        now: datetime | None = None,
    ) -> tuple[list[Any], int]:
        del now
        self.calls.append((limit, offset, filters or MarketFilters(), current_asset_ids))
        return self._rows, self._total

    async def read_market_by_id(
        self,
        *,
        market_id: str,
        current_asset_ids: frozenset[str] = frozenset(),
        now: datetime | None = None,
    ) -> Any | None:
        del now
        self.by_id_calls.append((market_id, current_asset_ids))
        return next((row for row in self._rows if row.market_id == market_id), None)


@pytest.mark.asyncio
async def test_list_markets_paginates_and_marks_subscribed_rows() -> None:
    from pms.api.routes.markets import list_markets

    rows = [
        _record(
            market_id="market-2",
            question="Will market 2 resolve?",
            updated_at=datetime(2026, 4, 23, 11, 0, tzinfo=UTC),
            yes_token_id="token-2-yes",
            no_token_id="token-2-no",
        ),
        _record(
            market_id="market-3",
            question="Will market 3 resolve?",
            updated_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
            yes_token_id="token-3-yes",
            no_token_id="token-3-no",
        ),
    ]
    store = _StoreDouble(rows=rows, total=5)

    payload = await list_markets(
        store,
        current_asset_ids=frozenset({"token-3-no"}),
        limit=2,
        offset=2,
    )

    assert store.calls == [(2, 2, MarketFilters(), frozenset({"token-3-no"}))]
    assert payload.limit == 2
    assert payload.offset == 2
    assert payload.total == 5
    assert [row.model_dump(mode="json") for row in payload.markets] == [
        {
            "market_id": "market-2",
            "question": "Will market 2 resolve?",
            "venue": "polymarket",
            "volume_24h": 1000.0,
            "updated_at": "2026-04-23T11:00:00+00:00",
            "resolves_at": None,
            "yes_token_id": "token-2-yes",
            "no_token_id": "token-2-no",
            "yes_price": None,
            "no_price": None,
            "best_bid": None,
            "best_ask": None,
            "last_trade_price": None,
            "liquidity": None,
            "spread_bps": None,
            "price_updated_at": None,
            "subscription_source": None,
            "subscribed": False,
        },
        {
            "market_id": "market-3",
            "question": "Will market 3 resolve?",
            "venue": "polymarket",
            "volume_24h": 1000.0,
            "updated_at": "2026-04-23T10:00:00+00:00",
            "resolves_at": None,
            "yes_token_id": "token-3-yes",
            "no_token_id": "token-3-no",
            "yes_price": None,
            "no_price": None,
            "best_bid": None,
            "best_ask": None,
            "last_trade_price": None,
            "liquidity": None,
            "spread_bps": None,
            "price_updated_at": None,
            "subscription_source": None,
            "subscribed": True,
        },
    ]


@pytest.mark.asyncio
async def test_list_markets_response_includes_price_fields() -> None:
    from pms.api.routes.markets import list_markets

    price_updated_at = datetime(2026, 4, 23, 11, 30, tzinfo=UTC)
    store = _StoreDouble(
        rows=[
            _record(
                market_id="market-priced",
                question="Will price fields serialize?",
                updated_at=datetime(2026, 4, 23, 11, 31, tzinfo=UTC),
                yes_token_id="market-priced-yes",
                no_token_id="market-priced-no",
                yes_price=0.62,
                no_price=0.38,
                best_bid=0.61,
                best_ask=0.63,
                last_trade_price=0.62,
                liquidity=2500.25,
                spread_bps=200,
                price_updated_at=price_updated_at,
                subscription_source="user",
            )
        ],
        total=1,
    )

    payload = await list_markets(
        store,
        current_asset_ids=frozenset(),
        limit=20,
        offset=0,
    )

    assert payload.markets[0].model_dump(mode="json") == {
        "market_id": "market-priced",
        "question": "Will price fields serialize?",
        "venue": "polymarket",
        "volume_24h": 1000.0,
        "updated_at": "2026-04-23T11:31:00+00:00",
        "resolves_at": None,
        "yes_token_id": "market-priced-yes",
        "no_token_id": "market-priced-no",
        "yes_price": 0.62,
        "no_price": 0.38,
        "best_bid": 0.61,
        "best_ask": 0.63,
        "last_trade_price": 0.62,
        "liquidity": 2500.25,
        "spread_bps": 200,
        "price_updated_at": "2026-04-23T11:30:00+00:00",
        "subscription_source": "user",
        "subscribed": False,
    }


@pytest.mark.asyncio
async def test_get_market_returns_detail_row_with_resolves_at() -> None:
    from pms.api.routes.markets import get_market

    resolves_at = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    store = _StoreDouble(
        rows=[
            _record(
                market_id="market-detail",
                question="Will detail route serialize resolves_at?",
                updated_at=datetime(2026, 4, 23, 11, 31, tzinfo=UTC),
                resolves_at=resolves_at,
                yes_token_id="market-detail-yes",
                no_token_id="market-detail-no",
            )
        ],
        total=1,
    )

    payload = await get_market(
        store,
        current_asset_ids=frozenset({"market-detail-yes"}),
        market_id="market-detail",
    )

    assert store.by_id_calls == [("market-detail", frozenset({"market-detail-yes"}))]
    assert payload.model_dump(mode="json")["resolves_at"] == resolves_at.isoformat()
    assert payload.subscribed is True


@pytest.mark.asyncio
async def test_get_markets_returns_503_when_runner_pg_pool_is_uninitialized() -> None:
    app = create_app(Runner(), auto_start=False)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/markets?limit=20&offset=0")

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Runner PostgreSQL pool is not initialized"
    }


@pytest.mark.asyncio
async def test_markets_route_forwards_filters_to_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _StoreDouble(rows=[], total=0)
    monkeypatch.setattr("pms.api.app.PostgresMarketDataStore", lambda _: store)
    runner = Runner()
    setattr(runner, "_pg_pool", object())
    app = create_app(runner, auto_start=False)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/markets",
            params={
                "limit": "50",
                "offset": "10",
                "q": "election",
                "volume_min": "1000.5",
                "liquidity_min": "2500.25",
                "spread_max_bps": "300",
                "yes_min": "0.2",
                "yes_max": "0.8",
                "resolves_within_days": "14",
                "subscribed": "idle",
            },
        )

    assert response.status_code == 200
    assert store.calls == [
        (
            50,
            10,
            MarketFilters(
                q="election",
                volume_min=1000.5,
                liquidity_min=2500.25,
                spread_max_bps=300,
                yes_min=0.2,
                yes_max=0.8,
                resolves_within_days=14,
                subscribed="idle",
            ),
            frozenset(),
        )
    ]
