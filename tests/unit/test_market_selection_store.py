from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

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
    condition_id: str,
    slug: str,
    question: str,
    venue: str = "polymarket",
    resolves_at: datetime | None,
    created_at: datetime,
    last_seen_at: datetime,
    volume_24h: float | None = 1000.0,
    token_id: str | None,
    outcome: str | None,
) -> dict[str, object]:
    return {
        "condition_id": condition_id,
        "slug": slug,
        "question": question,
        "venue": venue,
        "resolves_at": resolves_at,
        "created_at": created_at,
        "last_seen_at": last_seen_at,
        "volume_24h": volume_24h,
        "token_id": token_id,
        "outcome": outcome,
    }


@pytest.mark.asyncio
async def test_read_eligible_markets_groups_joined_tokens_and_keeps_zero_token_markets() -> None:
    created_at = datetime(2026, 4, 20, tzinfo=UTC)
    resolves_at = created_at + timedelta(days=10)
    connection = FakeConnection(
        fetch_results=[
            [
                _row(
                    condition_id="market-1",
                    slug="market-1",
                    question="Will market 1 resolve?",
                    resolves_at=resolves_at,
                    created_at=created_at,
                    last_seen_at=created_at,
                    token_id="token-yes",
                    outcome="YES",
                ),
                _row(
                    condition_id="market-1",
                    slug="market-1",
                    question="Will market 1 resolve?",
                    resolves_at=resolves_at,
                    created_at=created_at,
                    last_seen_at=created_at,
                    token_id="token-no",
                    outcome="NO",
                ),
                _row(
                    condition_id="market-2",
                    slug="market-2",
                    question="Will market 2 resolve?",
                    resolves_at=None,
                    created_at=created_at,
                    last_seen_at=created_at,
                    token_id=None,
                    outcome=None,
                ),
            ]
        ]
    )
    store = PostgresMarketDataStore(FakePool(connection))

    markets = await store.read_eligible_markets("polymarket", 30, 500.0)

    assert [market.condition_id for market, _ in markets] == ["market-1", "market-2"]
    assert [token.token_id for token in markets[0][1]] == ["token-yes", "token-no"]
    assert markets[1][1] == []
    assert markets[0][0].volume_24h == 1000.0
    assert len(connection.fetch_calls) == 1
    _, args = connection.fetch_calls[0]
    assert args[0] == "polymarket"
    assert isinstance(args[1], datetime)
    assert isinstance(args[2], datetime)
    assert args[3] == 500.0


@pytest.mark.asyncio
async def test_read_eligible_markets_returns_empty_list_and_uses_null_upper_bound_for_open_horizon() -> None:
    connection = FakeConnection(fetch_results=[[]])
    store = PostgresMarketDataStore(FakePool(connection))

    markets = await store.read_eligible_markets("kalshi", None, 0.0)

    assert markets == []
    assert len(connection.fetch_calls) == 1
    _, args = connection.fetch_calls[0]
    assert args[0] == "kalshi"
    assert isinstance(args[1], datetime)
    assert args[2] is None
    assert args[3] == 0.0
