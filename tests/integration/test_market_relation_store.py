from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
from typing import cast

import asyncpg
import pytest

from pms.core.models import MarketRelation, MarketRelationType
from pms.storage.market_relation_store import MarketRelationStore


PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
    pytest.mark.skipif(
        PMS_TEST_DATABASE_URL is None,
        reason="set PMS_TEST_DATABASE_URL to the compose-backed PostgreSQL URI",
    ),
]


class _AcquireConnection:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> asyncpg.Connection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        return None


class _SingleConnectionPool:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    def acquire(self) -> _AcquireConnection:
        return _AcquireConnection(self._connection)


@pytest.mark.asyncio(loop_scope="session")
async def test_market_relation_store_crud_round_trip(db_conn: asyncpg.Connection) -> None:
    store = MarketRelationStore(cast(asyncpg.Pool, _SingleConnectionPool(db_conn)))
    detected_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)

    await store.insert_relations(
        [
            MarketRelation(
                id=None,
                market_id_a="cp05-market-a",
                market_id_b="cp05-market-b",
                relation_type=MarketRelationType.SUBSET,
                confidence=0.9,
                detected_at=detected_at,
                metadata={"source": "integration"},
            ),
            MarketRelation(
                id=None,
                market_id_a="cp05-market-a",
                market_id_b="cp05-market-c",
                relation_type=MarketRelationType.INDEPENDENT,
                confidence=0.4,
                detected_at=detected_at - timedelta(days=10),
                metadata={},
            ),
        ]
    )

    relations = await store.get_relations_for_market("cp05-market-b")

    assert len(relations) == 1
    assert relations[0].market_id_a == "cp05-market-a"
    assert relations[0].market_id_b == "cp05-market-b"
    assert relations[0].relation_type is MarketRelationType.SUBSET
    assert relations[0].metadata == {"source": "integration"}

    deleted = await store.delete_stale_relations(
        ttl=timedelta(days=5),
        now=detected_at,
    )

    assert deleted >= 1
