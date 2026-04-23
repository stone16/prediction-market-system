from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from pms.storage.market_data_store import PostgresMarketDataStore


@dataclass
class FakeConnection:
    fetch_results: list[list[dict[str, object]]] = field(default_factory=list)
    fetch_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append((query, args))
        if not self.fetch_results:
            return []
        return self.fetch_results.pop(0)


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
