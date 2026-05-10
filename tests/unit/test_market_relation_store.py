from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest

from pms.core.models import MarketRelation, MarketRelationType
from pms.storage.market_relation_store import MarketRelationStore


class _FakeAcquire:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _FakeConnection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        return None


class _FakePool:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.connection)


class _FakeConnection:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_rows: list[dict[str, object]] = []
        self.execute_result = "DELETE 2"

    async def executemany(
        self,
        query: str,
        args: list[tuple[object, ...]],
    ) -> None:
        self.executemany_calls.append((query, args))

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.execute_calls.append((query, args))
        return self.fetch_rows

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return self.execute_result


def _relation(
    *,
    relation_type: MarketRelationType = MarketRelationType.SUBSET,
    detected_at: datetime = datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
) -> MarketRelation:
    return MarketRelation(
        id=None,
        market_id_a="market-a",
        market_id_b="market-b",
        relation_type=relation_type,
        confidence=0.91,
        detected_at=detected_at,
        metadata={"source": "test"},
    )


def test_market_relation_model_is_frozen_and_declares_enum_values() -> None:
    relation = _relation()

    assert {item.value for item in MarketRelationType} == {
        "subset",
        "contradiction",
        "independent",
        "similar",
    }
    with pytest.raises(FrozenInstanceError):
        relation.confidence = 0.1  # type: ignore[misc]


@pytest.mark.asyncio
async def test_market_relation_store_inserts_relations_as_jsonb_metadata() -> None:
    connection = _FakeConnection()
    store = MarketRelationStore(_FakePool(connection))
    detected_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)

    await store.insert_relations(
        [
            _relation(
                relation_type=MarketRelationType.CONTRADICTION,
                detected_at=detected_at,
            )
        ]
    )

    assert len(connection.executemany_calls) == 1
    query, args = connection.executemany_calls[0]
    assert "INSERT INTO market_relations" in query
    assert args == [
        (
            "market-a",
            "market-b",
            "contradiction",
            0.91,
            detected_at,
            '{"source":"test"}',
        )
    ]


@pytest.mark.asyncio
async def test_market_relation_store_reads_relations_for_either_market_side() -> None:
    connection = _FakeConnection()
    detected_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    connection.fetch_rows = [
        {
            "id": 7,
            "market_id_a": "market-a",
            "market_id_b": "market-b",
            "relation_type": "subset",
            "confidence": 0.91,
            "detected_at": detected_at,
            "metadata": {"source": "test"},
        }
    ]
    store = MarketRelationStore(_FakePool(connection))

    relations = await store.get_relations_for_market("market-b")

    assert relations == [
        MarketRelation(
            id=7,
            market_id_a="market-a",
            market_id_b="market-b",
            relation_type=MarketRelationType.SUBSET,
            confidence=0.91,
            detected_at=detected_at,
            metadata={"source": "test"},
        )
    ]
    query, args = connection.execute_calls[0]
    assert "WHERE market_id_a = $1 OR market_id_b = $1" in query
    assert args == ("market-b",)


@pytest.mark.asyncio
async def test_market_relation_store_deletes_rows_older_than_ttl() -> None:
    connection = _FakeConnection()
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    store = MarketRelationStore(_FakePool(connection))

    deleted = await store.delete_stale_relations(ttl=timedelta(hours=2), now=now)

    assert deleted == 2
    query, args = connection.execute_calls[0]
    assert "DELETE FROM market_relations" in query
    assert args == (now - timedelta(hours=2),)
