from __future__ import annotations

from typing import cast

import httpx
import pytest

from pms.api.app import create_app
from pms.core.models import Position
from pms.runner import Runner


class _StoreDouble:
    def __init__(self, positions: list[Position]) -> None:
        self._positions = positions
        self.calls = 0

    async def read_positions(self) -> list[Position]:
        self.calls += 1
        return list(self._positions)


@pytest.mark.asyncio
async def test_list_positions_returns_serialized_positions() -> None:
    from pms.api.routes.positions import list_positions

    store = _StoreDouble(
        [
            Position(
                market_id="market-cp06",
                token_id="token-cp06",
                venue="polymarket",
                side="BUY",
                shares_held=50.0,
                avg_entry_price=0.41,
                unrealized_pnl=0.0,
                locked_usdc=20.5,
            )
        ]
    )

    payload = await list_positions(store)

    assert store.calls == 1
    assert payload.model_dump(mode="json") == {
        "positions": [
            {
                "market_id": "market-cp06",
                "token_id": "token-cp06",
                "venue": "polymarket",
                "side": "BUY",
                "shares_held": 50.0,
                "avg_entry_price": 0.41,
                "unrealized_pnl": 0.0,
                "locked_usdc": 20.5,
            }
        ]
    }


@pytest.mark.asyncio
async def test_get_positions_returns_503_when_runner_pg_pool_is_uninitialized() -> None:
    app = create_app(Runner(), auto_start=False)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/positions",
            headers={"Authorization": "Bearer expected-token"},
        )

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Runner PostgreSQL pool is not initialized"
    }
