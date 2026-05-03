from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import RunMode
from pms.runner import Runner
from pms.strategies.flb import FlbAgent, FlbController, FlbStrategyModule, LiveFlbSource
from pms.strategies.flb.source import FlbMarketSnapshot
from pms.strategies.intents import TradeIntent


NOW = datetime(2026, 5, 3, 15, 0, tzinfo=UTC)


class _StaticFlbMarketReader:
    def __init__(self, market: FlbMarketSnapshot | None) -> None:
        self.market = market
        self.calls: list[tuple[str, datetime]] = []

    async def latest(
        self,
        market_id: str,
        *,
        as_of: datetime,
    ) -> FlbMarketSnapshot | None:
        self.calls.append((market_id, as_of))
        return self.market


class _Row(dict[str, object]):
    def __getattr__(self, item: str) -> object:
        return self[item]


class _RecordingFlbConnection:
    def __init__(
        self,
        *,
        resolved_market_id: str | None = "market-flb-1",
        market_row: _Row | None = None,
        price_row: _Row | None = None,
    ) -> None:
        self.resolved_market_id = resolved_market_id
        self.market_row = market_row
        self.price_row = price_row
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, query: str, *args: object) -> object | None:
        self.fetchval_calls.append((query, args))
        return self.resolved_market_id

    async def fetchrow(self, query: str, *args: object) -> _Row | None:
        self.fetchrow_calls.append((query, args))
        if "FROM market_price_snapshots" in query:
            return self.price_row
        return self.market_row


class _AcquireFlbConnection:
    def __init__(self, connection: _RecordingFlbConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _RecordingFlbConnection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        return None


class _FlbPool:
    def __init__(self, connection: _RecordingFlbConnection) -> None:
        self.connection = connection

    def acquire(self) -> _AcquireFlbConnection:
        return _AcquireFlbConnection(self.connection)


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.PAPER,
        risk=RiskSettings(max_position_per_market=5.0),
    )


def _market(**overrides: object) -> FlbMarketSnapshot:
    data = {
        "market_id": "market-flb-1",
        "title": "Will H1 FLB production wiring emit the contrarian side?",
        "yes_token_id": "token-yes",
        "no_token_id": "token-no",
        "yes_price": 0.05,
        "observed_at": NOW,
        "yes_best_ask": 0.05,
        "no_best_ask": 0.96,
        "resolves_at": NOW + timedelta(days=5),
    }
    data.update(overrides)
    return FlbMarketSnapshot(**data)  # type: ignore[arg-type]


def _market_row(**overrides: object) -> _Row:
    data: dict[str, object] = {
        "market_id": "market-flb-1",
        "question": "Will H1 FLB production wiring emit the contrarian side?",
        "venue": "polymarket",
        "resolves_at": NOW + timedelta(days=5),
        "last_seen_at": NOW - timedelta(minutes=2),
        "yes_price": 0.03,
        "no_price": 0.97,
        "best_bid": 0.02,
        "best_ask": 0.04,
        "price_updated_at": NOW - timedelta(minutes=2),
        "closed": False,
        "accepting_orders": True,
        "status_updated_at": NOW - timedelta(days=1),
        "yes_token_id": "token-yes",
        "no_token_id": "token-no",
    }
    data.update(overrides)
    return _Row(data)


def _price_row(**overrides: object) -> _Row:
    data: dict[str, object] = {
        "snapshot_at": NOW - timedelta(minutes=10),
        "yes_price": 0.05,
        "no_price": 0.95,
        "best_bid": 0.04,
        "best_ask": 0.06,
    }
    data.update(overrides)
    return _Row(data)


def test_runner_builds_flb_strategy_module_with_live_components() -> None:
    runner = Runner(config=_settings())
    reader = _StaticFlbMarketReader(_market())

    module = runner.build_flb_strategy_module(
        strategy_id="h1_flb",
        strategy_version_id="h1-flb-v1",
        market_ids=("market-flb-1",),
        market_reader=reader,
    )

    assert isinstance(module, FlbStrategyModule)
    assert isinstance(module.source, LiveFlbSource)
    assert isinstance(module.controller, FlbController)
    assert isinstance(module.agent, FlbAgent)
    assert module.strategy_id == "h1_flb"
    assert module.strategy_version_id == "h1-flb-v1"


@pytest.mark.asyncio
async def test_runner_calls_flb_module_without_live_capital() -> None:
    runner = Runner(config=_settings())
    reader = _StaticFlbMarketReader(_market())

    results = await runner.run_flb_strategy_once(
        strategy_id="h1_flb",
        strategy_version_id="h1-flb-v1",
        market_ids=("market-flb-1",),
        as_of=NOW,
        market_reader=reader,
    )

    assert reader.calls == [("market-flb-1", NOW)]
    assert len(results) == 1
    judgement = results[0].judgement
    assert judgement is not None
    assert judgement.approved is True

    assert len(results[0].intents) == 1
    intent = results[0].intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.outcome == "NO"
    assert intent.token_id == "token-no"
    assert intent.limit_price == pytest.approx(0.96)
    assert intent.expected_edge == pytest.approx(0.02)
    assert intent.notional_usdc <= 5.0

    assert runner.state.decisions == []
    assert runner._decision_queue.empty()  # noqa: SLF001


def test_runner_flb_module_requires_explicit_markets_without_pg_pool() -> None:
    runner = Runner(config=_settings())

    with pytest.raises(ValueError, match="market_ids"):
        runner.build_flb_strategy_module(
            strategy_id="h1_flb",
            strategy_version_id="h1-flb-v1",
            market_ids=(),
            market_reader=_StaticFlbMarketReader(None),
        )


@pytest.mark.asyncio
async def test_postgres_flb_reader_uses_as_of_price_snapshot() -> None:
    runner = Runner(config=_settings())
    connection = _RecordingFlbConnection(
        market_row=_market_row(yes_price=0.03),
        price_row=_price_row(yes_price=0.08, best_bid=0.07, best_ask=0.09),
    )
    runner.bind_pg_pool(_FlbPool(connection))

    results = await runner.run_flb_strategy_once(
        strategy_id="h1_flb",
        strategy_version_id="h1-flb-v1",
        market_ids=("token-yes",),
        as_of=NOW,
    )

    assert len(results) == 1
    intent = results[0].intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.outcome == "NO"
    assert intent.expected_price == pytest.approx(0.97)
    assert intent.limit_price == pytest.approx(0.95)
    assert connection.fetchval_calls[0][1] == ("token-yes",)
    assert connection.fetchrow_calls[1][1] == ("market-flb-1", NOW)
    assert "ORDER BY match_rank ASC" in connection.fetchval_calls[0][0]
    assert "snapshot_at <= $2" in connection.fetchrow_calls[1][0]


@pytest.mark.asyncio
async def test_postgres_flb_reader_rejects_future_status_change() -> None:
    runner = Runner(config=_settings())
    connection = _RecordingFlbConnection(
        market_row=_market_row(
            closed=True,
            status_updated_at=NOW + timedelta(minutes=1),
        ),
        price_row=_price_row(),
    )
    runner.bind_pg_pool(_FlbPool(connection))

    results = await runner.run_flb_strategy_once(
        strategy_id="h1_flb",
        strategy_version_id="h1-flb-v1",
        market_ids=("market-flb-1",),
        as_of=NOW,
    )

    assert results == ()
