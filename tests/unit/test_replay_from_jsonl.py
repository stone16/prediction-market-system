from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pms.research.replay import MarketUniverseReplayEngine
from pms.research.specs import (
    BacktestDataset,
    BacktestExecutionConfig,
    BacktestSpec,
    ExecutionModel,
    RiskPolicy,
)


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "backtest" / "timestamped_events_30day.jsonl"


def _spec() -> BacktestSpec:
    return BacktestSpec(
        strategy_versions=(("alpha", "v1"),),
        dataset=BacktestDataset(
            source="jsonl",
            version="cp21",
            coverage_start=datetime(2026, 4, 1, tzinfo=UTC),
            coverage_end=datetime(2026, 5, 1, tzinfo=UTC),
            market_universe_filter={"venue": "polymarket", "market_ids": ("cp21-fixture-market",)},
            data_quality_gaps=(),
        ),
        execution_model=ExecutionModel.polymarket_paper(),
        risk_policy=RiskPolicy(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        date_range_start=datetime(2026, 4, 1, tzinfo=UTC),
        date_range_end=datetime(2026, 4, 30, 23, 0, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_replay_engine_from_jsonl_streams_large_fixture_in_order() -> None:
    engine = MarketUniverseReplayEngine.from_jsonl(FIXTURE)
    signals = [
        signal
        async for signal in engine.stream(_spec(), BacktestExecutionConfig(chunk_days=7))
    ]

    assert len(signals) >= 500
    assert len(signals) == 720
    assert [signal.fetched_at for signal in signals] == sorted(signal.fetched_at for signal in signals)
    assert signals[0].market_id == "cp21-fixture-market"
    assert signals[-1].token_id == "cp21-fixture-yes"

    raw_sequences = [
        json.loads(line)["sequence"]
        for line in FIXTURE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert raw_sequences == sorted(raw_sequences)

    state = await engine.book_state_at(
        datetime(2026, 4, 5, 12, 0, tzinfo=UTC),
        market_id="cp21-fixture-market",
        token_id="cp21-fixture-yes",
    )
    assert state == signals[(4 * 24) + 12].orderbook


@pytest.mark.asyncio
async def test_replay_engine_from_jsonl_rejects_non_monotonic_timestamps(tmp_path: Path) -> None:
    fixture = tmp_path / "bad-ts.jsonl"
    fixture.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts": "2026-04-01T00:00:00+00:00",
                        "event_type": "book_snapshot",
                        "market_id": "market-a",
                        "token_id": "token-a",
                        "sequence": 1,
                        "orderbook": {"bids": [], "asks": []},
                    }
                ),
                json.dumps(
                    {
                        "ts": "2026-03-31T23:00:00+00:00",
                        "event_type": "book_snapshot",
                        "market_id": "market-a",
                        "token_id": "token-a",
                        "sequence": 2,
                        "orderbook": {"bids": [], "asks": []},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    engine = MarketUniverseReplayEngine.from_jsonl(fixture)

    with pytest.raises(ValueError, match="monotonic ts"):
        async for _ in engine.stream(_spec(), BacktestExecutionConfig(chunk_days=7)):
            pass


@pytest.mark.asyncio
async def test_replay_engine_from_jsonl_rejects_non_monotonic_sequence(tmp_path: Path) -> None:
    fixture = tmp_path / "bad-sequence.jsonl"
    fixture.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts": "2026-04-01T00:00:00+00:00",
                        "event_type": "book_snapshot",
                        "market_id": "market-a",
                        "token_id": "token-a",
                        "sequence": 2,
                        "orderbook": {"bids": [], "asks": []},
                    }
                ),
                json.dumps(
                    {
                        "ts": "2026-04-01T01:00:00+00:00",
                        "event_type": "book_snapshot",
                        "market_id": "market-a",
                        "token_id": "token-a",
                        "sequence": 2,
                        "orderbook": {"bids": [], "asks": []},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    engine = MarketUniverseReplayEngine.from_jsonl(fixture)

    with pytest.raises(ValueError, match="monotonic sequence"):
        async for _ in engine.stream(_spec(), BacktestExecutionConfig(chunk_days=7)):
            pass
