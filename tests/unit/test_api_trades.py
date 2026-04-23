from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from pms.api.app import create_app
from pms.runner import Runner
from pms.storage.fill_store import StoredTradeRow


class _StoreDouble:
    def __init__(self, rows: list[StoredTradeRow]) -> None:
        self._rows = rows
        self.calls: list[int] = []

    async def read_trades(self, *, limit: int) -> list[StoredTradeRow]:
        self.calls.append(limit)
        return list(self._rows)


@pytest.mark.asyncio
async def test_list_trades_returns_market_question_and_fill_fields() -> None:
    from pms.api.routes.trades import list_trades

    store = _StoreDouble(
        [
            StoredTradeRow(
                trade_id="trade-cp06",
                fill_id="fill-cp06",
                order_id="order-cp06",
                decision_id="decision-cp06",
                market_id="market-cp06",
                question="Will CP06 persist fills?",
                token_id="token-cp06",
                venue="polymarket",
                side="BUY",
                fill_price=0.41,
                fill_notional_usdc=20.5,
                fill_quantity=50.0,
                executed_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
                filled_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
                status="matched",
                strategy_id="default",
                strategy_version_id="default-v1",
            )
        ]
    )

    payload = await list_trades(store, limit=25)

    assert store.calls == [25]
    assert payload.model_dump(mode="json") == {
        "trades": [
            {
                "trade_id": "trade-cp06",
                "fill_id": "fill-cp06",
                "order_id": "order-cp06",
                "decision_id": "decision-cp06",
                "market_id": "market-cp06",
                "question": "Will CP06 persist fills?",
                "token_id": "token-cp06",
                "venue": "polymarket",
                "side": "BUY",
                "fill_price": 0.41,
                "fill_notional_usdc": 20.5,
                "fill_quantity": 50.0,
                "executed_at": "2026-04-23T10:00:00Z",
                "filled_at": "2026-04-23T10:00:00Z",
                "status": "matched",
                "strategy_id": "default",
                "strategy_version_id": "default-v1",
            }
        ],
        "limit": 25,
    }


@pytest.mark.asyncio
async def test_get_trades_returns_503_when_runner_pg_pool_is_uninitialized() -> None:
    app = create_app(Runner(), auto_start=False)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/trades",
            headers={"Authorization": "Bearer expected-token"},
        )

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Runner PostgreSQL pool is not initialized"
    }
