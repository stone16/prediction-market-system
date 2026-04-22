from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path

import asyncpg
import pytest

from pms.core.models import MarketSignal
from pms.research.replay import MarketUniverseReplayEngine
from pms.research.specs import (
    BacktestDataset,
    BacktestExecutionConfig,
    BacktestSpec,
    ExecutionModel,
    RiskPolicy,
)
from pms.storage.market_data_store import PostgresMarketDataStore
from tests.integration.test_market_data_store import (
    _level,
    _market as _md_market,
    _price_change as _md_price_change,
    _snapshot,
    _token as _md_token,
    _trade as _md_trade,
)


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


def _spec(*, market_id: str) -> BacktestSpec:
    return BacktestSpec(
        strategy_versions=(("alpha", "v1"),),
        dataset=BacktestDataset(
            source="jsonl",
            version="cp21",
            coverage_start=datetime(2026, 4, 1, tzinfo=UTC),
            coverage_end=datetime(2026, 4, 4, tzinfo=UTC),
            market_universe_filter={"venue": "polymarket", "market_ids": (market_id,)},
            data_quality_gaps=(),
        ),
        execution_model=ExecutionModel.polymarket_paper(),
        risk_policy=RiskPolicy(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        date_range_start=datetime(2026, 4, 1, tzinfo=UTC),
        date_range_end=datetime(2026, 4, 4, tzinfo=UTC),
    )


async def _collect(
    engine: MarketUniverseReplayEngine,
    spec: BacktestSpec,
) -> list[MarketSignal]:
    return [signal async for signal in engine.stream(spec, BacktestExecutionConfig(chunk_days=3))]


@pytest.mark.asyncio(loop_scope="session")
async def test_replay_engine_from_jsonl_matches_pg_stream_for_equivalent_events(
    pg_pool: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    market_id = "cp21-jsonl-market"
    token_id = "cp21-jsonl-token"
    store = PostgresMarketDataStore(pg_pool)
    await store.write_market(
        _md_market(
            condition_id=market_id,
            slug=market_id,
            question="Will the CP21 parity check stay aligned?",
            created_at=datetime(2026, 3, 31, tzinfo=UTC),
            last_seen_at=datetime(2026, 4, 4, tzinfo=UTC),
        )
    )
    await store.write_token(_md_token(token_id=token_id, condition_id=market_id))
    await store.write_book_snapshot(
        _snapshot(
            market_id=market_id,
            token_id=token_id,
            ts=datetime(2026, 4, 1, tzinfo=UTC),
            hash_value="cp21-snapshot-1",
            source="checkpoint",
        ),
        [
            _level(snapshot_id=0, market_id=market_id, side="BUY", price=0.42, size=120.0),
            _level(snapshot_id=0, market_id=market_id, side="SELL", price=0.58, size=120.0),
        ],
    )
    await store.write_price_change(
        _md_price_change(
            market_id=market_id,
            token_id=token_id,
            ts=datetime(2026, 4, 2, tzinfo=UTC),
            side="BUY",
            price=0.44,
            size=140.0,
            best_bid=0.44,
            best_ask=0.58,
            hash_value="cp21-delta-1",
        )
    )
    await store.write_trade(
        _md_trade(
            market_id=market_id,
            token_id=token_id,
            ts=datetime(2026, 4, 3, tzinfo=UTC),
            price=0.47,
        )
    )

    fixture = tmp_path / "cp21-parity.jsonl"
    fixture.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts": "2026-04-01T00:00:00+00:00",
                        "event_type": "book_snapshot",
                        "market_id": market_id,
                        "token_id": token_id,
                        "title": "Will the CP21 parity check stay aligned?",
                        "venue": "polymarket",
                        "sequence": 1,
                        "orderbook": {
                            "bids": [{"price": 0.42, "size": 120.0}],
                            "asks": [{"price": 0.58, "size": 120.0}],
                        },
                    }
                ),
                json.dumps(
                    {
                        "ts": "2026-04-02T00:00:00+00:00",
                        "event_type": "level_update",
                        "market_id": market_id,
                        "token_id": token_id,
                        "side": "bid",
                        "price": 0.44,
                        "size": 140.0,
                        "best_bid": 0.44,
                        "best_ask": 0.58,
                        "sequence": 2,
                    }
                ),
                json.dumps(
                    {
                        "ts": "2026-04-03T00:00:00+00:00",
                        "event_type": "trade",
                        "market_id": market_id,
                        "token_id": token_id,
                        "price": 0.47,
                        "sequence": 3,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    spec = _spec(market_id=market_id)
    pg_signals = await _collect(MarketUniverseReplayEngine(pool=pg_pool), spec)
    jsonl_signals = await _collect(MarketUniverseReplayEngine.from_jsonl(fixture), spec)

    assert len(pg_signals) == len(jsonl_signals) == 3
    for pg_signal, jsonl_signal in zip(pg_signals, jsonl_signals, strict=True):
        assert pg_signal.fetched_at == jsonl_signal.fetched_at
        assert pg_signal.orderbook == jsonl_signal.orderbook
        assert pg_signal.yes_price == pytest.approx(jsonl_signal.yes_price)
        assert pg_signal.external_signal["raw_event_type"] == jsonl_signal.external_signal["raw_event_type"]
