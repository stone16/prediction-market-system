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

