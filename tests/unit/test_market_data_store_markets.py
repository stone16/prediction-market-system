from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from pms.core.models import Market
from pms.storage.market_data_store import PostgresMarketDataStore


@dataclass
class FakeConnection:
    fetch_results: list[list[dict[str, object]]] = field(default_factory=list)
    fetch_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    execute_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append((query, args))
        if not self.fetch_results:
            return []
        return self.fetch_results.pop(0)

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "INSERT 0 1"


class FakeAcquire:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        return None


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self._connection)


def _row(
    *,
    market_id: str,
    question: str,
    venue: str = "polymarket",
    volume_24h: float | None = 1500.0,
    updated_at: datetime,
    yes_token_id: str | None,
    no_token_id: str | None,
    total_count: int,
) -> dict[str, object]:
    return {
        "market_id": market_id,
        "question": question,
        "venue": venue,
        "volume_24h": volume_24h,
        "updated_at": updated_at,
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "total_count": total_count,
    }


@pytest.mark.asyncio
async def test_read_markets_returns_rows_total_and_filters_to_active_markets_in_sql() -> None:
    updated_at = datetime(2026, 4, 23, 9, 30, tzinfo=UTC)
    connection = FakeConnection(
        fetch_results=[
            [
                _row(
                    market_id="market-1",
                    question="Will market 1 resolve?",
                    updated_at=updated_at,
                    yes_token_id="market-1-yes",
                    no_token_id="market-1-no",
                    total_count=2,
                ),
                _row(
                    market_id="market-2",
                    question="Will market 2 resolve?",
                    updated_at=updated_at,
                    yes_token_id="market-2-yes",
                    no_token_id="market-2-no",
                    total_count=2,
                ),
            ]
        ]
    )
    store = PostgresMarketDataStore(FakePool(connection))

    rows, total = await store.read_markets(limit=20, offset=5)

    assert total == 2
    assert [row.market_id for row in rows] == ["market-1", "market-2"]
    assert rows[0].yes_token_id == "market-1-yes"
    assert rows[0].no_token_id == "market-1-no"
    assert len(connection.fetch_calls) == 1

    query, args = connection.fetch_calls[0]
    assert "resolves_at IS NULL OR markets.resolves_at > $1" in query
    assert "COUNT(*) OVER()" in query
    assert args[1:] == (20, 5)
    assert isinstance(args[0], datetime)


@pytest.mark.asyncio
async def test_read_markets_returns_zero_total_when_query_returns_no_rows() -> None:
    connection = FakeConnection(fetch_results=[[]])
    store = PostgresMarketDataStore(FakePool(connection))

    rows, total = await store.read_markets(limit=20, offset=0)

    assert rows == []
    assert total == 0


@pytest.mark.asyncio
async def test_write_market_upserts_current_price_fields() -> None:
    connection = FakeConnection()
    store = PostgresMarketDataStore(FakePool(connection))
    created_at = datetime(2026, 4, 24, 8, 0, tzinfo=UTC)
    price_updated_at = datetime(2026, 4, 24, 8, 1, tzinfo=UTC)

    await store.write_market(
        Market(
            condition_id="market-priced",
            slug="market-priced",
            question="Will write_market persist prices?",
            venue="polymarket",
            resolves_at=None,
            created_at=created_at,
            last_seen_at=price_updated_at,
            volume_24h=500.0,
            yes_price=0.52,
            no_price=0.48,
            best_bid=0.51,
            best_ask=0.53,
            last_trade_price=0.52,
            liquidity=1500.0,
            spread_bps=200,
            price_updated_at=price_updated_at,
        )
    )

    query, args = connection.execute_calls[0]
    assert "yes_price" in query
    assert "no_price" in query
    assert "price_updated_at = EXCLUDED.price_updated_at" in query
    assert args[8:] == (
        0.52,
        0.48,
        0.51,
        0.53,
        0.52,
        1500.0,
        200,
        price_updated_at,
    )


@pytest.mark.asyncio
async def test_write_price_snapshot_inserts_row() -> None:
    connection = FakeConnection()
    store = PostgresMarketDataStore(FakePool(connection))
    snapshot_at = datetime(2026, 4, 24, 9, 0, tzinfo=UTC)

    await store.write_price_snapshot(
        condition_id="market-snapshot",
        snapshot_at=snapshot_at,
        yes_price=0.61,
        no_price=0.39,
        best_bid=0.60,
        best_ask=0.62,
        last_trade_price=0.61,
        liquidity=2500.0,
        volume_24h=1250.0,
    )

    query, args = connection.execute_calls[0]
    assert "INSERT INTO market_price_snapshots" in query
    assert "condition_id" in query
    assert "volume_24h" in query
    assert args == (
        "market-snapshot",
        snapshot_at,
        0.61,
        0.39,
        0.60,
        0.62,
        0.61,
        2500.0,
        1250.0,
    )
