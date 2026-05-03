from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
import tomllib

import pytest

from pms.core.models import Portfolio
from pms.strategies.base import (
    StrategyAgent,
    StrategyController,
    StrategyModule,
    StrategyObservationSource,
)
from pms.strategies.flb.agent import FlbAgent
from pms.strategies.flb.controller import FlbController
from pms.strategies.flb.source import (
    FLB_RESEARCH_REF,
    FlbPositionSizer,
    LiveFlbSource,
    FlbMarketSnapshot,
)
from pms.strategies.flb.strategy import FlbStrategyModule
from pms.strategies.intents import StrategyContext, TradeIntent


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]


def _context() -> StrategyContext:
    return StrategyContext(
        strategy_id="h1_flb",
        strategy_version_id="h1-flb-v1",
        as_of=NOW,
    )


def _portfolio(free_usdc: float = 100.0) -> Portfolio:
    return Portfolio(
        total_usdc=free_usdc,
        free_usdc=free_usdc,
        locked_usdc=0.0,
        open_positions=[],
    )


class _FixedSizer:
    def __init__(self, notional_usdc: float = 5.0) -> None:
        self.notional_usdc = notional_usdc
        self.calls: list[tuple[float, float]] = []

    def size(
        self,
        *,
        prob: float,
        market_price: float,
        portfolio: Portfolio,
    ) -> float:
        del portfolio
        self.calls.append((prob, market_price))
        return self.notional_usdc


class _ZeroSizer:
    def size(
        self,
        *,
        prob: float,
        market_price: float,
        portfolio: Portfolio,
    ) -> float:
        del prob, market_price, portfolio
        return 0.0


class _StaticMarketReader:
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


def _market(**overrides: object) -> FlbMarketSnapshot:
    data: dict[str, object] = {
        "market_id": "market-flb-1",
        "title": "Will the H1 FLB strategy choose the contrarian side?",
        "yes_token_id": "token-yes",
        "no_token_id": "token-no",
        "yes_price": 0.05,
        "observed_at": NOW,
        "yes_best_ask": 0.05,
        "no_best_ask": 0.96,
        "resolves_at": NOW + timedelta(days=7),
    }
    data.update(overrides)
    return FlbMarketSnapshot(**cast(Any, data))


def _module(
    market: FlbMarketSnapshot | None,
    *,
    sizer: FlbPositionSizer | None = None,
) -> FlbStrategyModule:
    return FlbStrategyModule(
        source=LiveFlbSource(
            market_ids=("market-flb-1",),
            market_reader=_StaticMarketReader(market),
            position_sizer=sizer or _FixedSizer(),
            portfolio=_portfolio(),
        ),
        controller=FlbController(),
        agent=FlbAgent(),
        strategy_id="h1_flb",
        strategy_version_id="h1-flb-v1",
    )


def test_flb_components_satisfy_strategy_protocols() -> None:
    module = _module(_market())

    source: StrategyObservationSource = module.source
    controller: StrategyController = module.controller
    agent: StrategyAgent = module.agent
    strategy_module: StrategyModule = module

    assert source is module.source
    assert controller is module.controller
    assert agent is module.agent
    assert strategy_module.strategy_id == "h1_flb"


@pytest.mark.asyncio
async def test_flb_longshot_signal_buys_no_contract() -> None:
    sizer = _FixedSizer(5.0)
    module = _module(_market(yes_price=0.05, no_best_ask=0.96), sizer=sizer)

    intents = await module.run(_context())

    assert len(intents) == 1
    intent = intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.outcome == "NO"
    assert intent.side == "BUY"
    assert intent.token_id == "token-no"
    assert intent.limit_price == pytest.approx(0.96)
    assert intent.expected_price == pytest.approx(0.98)
    assert intent.expected_edge == pytest.approx(0.02)
    assert intent.notional_usdc == pytest.approx(5.0)
    assert len(sizer.calls) == 1
    assert sizer.calls[0][0] == pytest.approx(0.98)
    assert sizer.calls[0][1] == pytest.approx(0.96)
    assert FLB_RESEARCH_REF in intent.evidence_refs


@pytest.mark.asyncio
async def test_flb_favorite_signal_buys_yes_contract() -> None:
    module = _module(_market(yes_price=0.95, yes_best_ask=0.94))

    intents = await module.run(_context())

    assert len(intents) == 1
    intent = intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.outcome == "YES"
    assert intent.side == "BUY"
    assert intent.token_id == "token-yes"
    assert intent.limit_price == pytest.approx(0.94)
    assert intent.expected_price == pytest.approx(0.96)


@pytest.mark.asyncio
async def test_flb_source_ignores_middle_decile_markets() -> None:
    module = _module(_market(yes_price=0.50, yes_best_ask=0.50, no_best_ask=0.50))

    assert await module.run(_context()) == ()


@pytest.mark.asyncio
async def test_flb_source_suppresses_zero_sized_trades() -> None:
    module = _module(_market(), sizer=_ZeroSizer())

    assert await module.run(_context()) == ()


@pytest.mark.asyncio
async def test_flb_source_skips_resolved_markets() -> None:
    module = _module(_market(resolves_at=NOW - timedelta(minutes=1)))

    assert await module.run(_context()) == ()


@pytest.mark.asyncio
async def test_flb_module_rejects_context_strategy_mismatch() -> None:
    module = _module(_market())
    context = StrategyContext(
        strategy_id="other",
        strategy_version_id="h1-flb-v1",
        as_of=NOW,
    )

    with pytest.raises(ValueError, match="context strategy identity must match"):
        await module.run(context)


def test_strategy_import_linter_contract_includes_flb_plugin() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    contracts: Sequence[dict[str, object]] = pyproject["tool"]["importlinter"][
        "contracts"
    ]
    contract = next(
        candidate
        for candidate in contracts
        if candidate["name"] == "Strategy plugins: no actuator, controller, or venue adapter imports"
    )

    assert contract["source_modules"] == [
        "pms.strategies.ripple",
        "pms.strategies.flb",
    ]
