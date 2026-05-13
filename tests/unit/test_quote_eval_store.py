from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import asyncpg
import pytest

from pms.core.models import QuoteEvalRecord
from pms.storage.quote_eval_store import QuoteEvalStore


class _RecordingConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_rows: list[object] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def fetch(self, query: str, *args: object) -> list[object]:
        self.fetch_calls.append((query, args))
        return list(self.fetch_rows)


class _AcquireContext:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _RecordingConnection:
        return self._connection

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb


class _RecordingPool:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(self._connection)


def _record() -> QuoteEvalRecord:
    return QuoteEvalRecord(
        fill_id="fill-quote-unit",
        decision_id="decision-quote-unit",
        market_id="market-quote-unit",
        token_id="token-quote-unit",
        strategy_id="default",
        strategy_version_id="default-v1",
        prob_estimate=0.70,
        quote_price=0.64,
        quote_source="postgres_snapshot",
        quote_lag_seconds=3600,
        quote_score=0.0036,
        mtm_pnl=1.2,
        book_ts=datetime(2026, 4, 14, 11, 0, tzinfo=UTC),
        recorded_at=datetime(2026, 4, 14, 11, 0, tzinfo=UTC),
        citations=["trade-quote-unit"],
        category="model-a",
        model_id="model-a",
    )


@pytest.mark.asyncio
async def test_quote_eval_store_requires_bound_pool_for_append() -> None:
    with pytest.raises(RuntimeError, match="QuoteEvalStore pool is not bound"):
        await QuoteEvalStore().append(_record())


@pytest.mark.asyncio
async def test_quote_eval_store_appends_idempotent_rows() -> None:
    connection = _RecordingConnection()
    store = QuoteEvalStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    await store.append(_record())

    assert "CREATE TABLE IF NOT EXISTS quote_eval_records" in connection.execute_calls[0][0]
    assert "idx_quote_eval_records_strategy_identity_recorded_at" in connection.execute_calls[1][0]
    insert_query, insert_args = connection.execute_calls[2]
    assert "ON CONFLICT (fill_id, quote_lag_seconds) DO UPDATE" in insert_query
    assert len(insert_args) == 17
    assert insert_args[:4] == (
        "fill-quote-unit",
        "decision-quote-unit",
        "market-quote-unit",
        "token-quote-unit",
    )


@pytest.mark.asyncio
async def test_quote_eval_store_maps_rows() -> None:
    connection = _RecordingConnection()
    connection.fetch_rows = [
        {
            "fill_id": "fill-quote-unit",
            "decision_id": "decision-quote-unit",
            "market_id": "market-quote-unit",
            "token_id": "token-quote-unit",
            "strategy_id": "default",
            "strategy_version_id": "default-v1",
            "prob_estimate": 0.70,
            "quote_price": 0.64,
            "quote_source": "postgres_snapshot",
            "quote_lag_seconds": 3600,
            "quote_score": 0.0036,
            "mtm_pnl": 1.2,
            "book_ts": datetime(2026, 4, 14, 11, 0, tzinfo=UTC),
            "recorded_at": datetime(2026, 4, 14, 11, 0, tzinfo=UTC),
            "citations": ["trade-quote-unit"],
            "category": "model-a",
            "model_id": "model-a",
        }
    ]
    store = QuoteEvalStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    assert await store.all() == [_record()]
    assert "ORDER BY recorded_at ASC, fill_id ASC" in connection.fetch_calls[0][0]
