from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx
import pytest

from pms.api.app import create_app
from pms.runner import Runner


def _record(
    *,
    market_id: str,
    question: str,
    venue: str = "polymarket",
    volume_24h: float | None = 1000.0,
    updated_at: datetime | None = None,
    yes_token_id: str | None = None,
    no_token_id: str | None = None,
) -> Any:
    from pms.api.routes.markets import StoredMarketRow

    timestamp = updated_at or datetime(2026, 4, 23, 9, 0, tzinfo=UTC)
    return StoredMarketRow(
        market_id=market_id,
        question=question,
        venue=cast(Any, venue),
        volume_24h=volume_24h,
        updated_at=timestamp,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
    )


class _StoreDouble:
    def __init__(self, rows: list[Any], total: int) -> None:
        self._rows = rows
        self._total = total
        self.calls: list[tuple[int, int]] = []

    async def read_markets(
        self,
        *,
        limit: int,
        offset: int,
        now: datetime | None = None,
    ) -> tuple[list[Any], int]:
        del now
        self.calls.append((limit, offset))
        return self._rows, self._total


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

    assert store.calls == [(2, 2)]
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
            "yes_token_id": "token-2-yes",
            "no_token_id": "token-2-no",
            "subscribed": False,
        },
        {
            "market_id": "market-3",
            "question": "Will market 3 resolve?",
            "venue": "polymarket",
            "volume_24h": 1000.0,
            "updated_at": "2026-04-23T10:00:00+00:00",
            "yes_token_id": "token-3-yes",
            "no_token_id": "token-3-no",
            "subscribed": True,
        },
    ]


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
