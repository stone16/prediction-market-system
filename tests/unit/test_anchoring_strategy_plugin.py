from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
import tomllib

import pytest

from pms.core.models import Portfolio
from pms.strategies.anchoring import (
    ANCHORING_RESEARCH_REF,
    AnchoringAgent,
    AnchoringController,
    AnchoringLagStrategyModule,
    AnchoringMarketSnapshot,
    LiveAnchoringSource,
)
from pms.strategies.base import StrategyAgent, StrategyController, StrategyModule, StrategyObservationSource
from pms.strategies.intents import StrategyContext, TradeIntent


NOW = datetime(2026, 5, 4, 1, 0, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]


def _context() -> StrategyContext:
    return StrategyContext(
        strategy_id="h2_anchoring_lag",
        strategy_version_id="h2-anchoring-lag-v1",
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


class _StaticAnchoringReader:
    def __init__(self, market: AnchoringMarketSnapshot | None) -> None:
        self.market = market
        self.calls: list[tuple[str, datetime]] = []

    async def latest(
        self,
        market_id: str,
        *,
        as_of: datetime,
    ) -> AnchoringMarketSnapshot | None:
        self.calls.append((market_id, as_of))
        return self.market


def _market(**overrides: object) -> AnchoringMarketSnapshot:
    data: dict[str, object] = {
        "market_id": "market-h2-1",
        "title": "Will H2 anchoring lag detect delayed repricing?",
        "yes_token_id": "token-yes",
        "no_token_id": "token-no",
        "yes_price": 0.50,
        "llm_posterior": 0.80,
        "llm_confidence": 0.70,
        "news_timestamp": NOW,
        "observed_at": NOW,
        "yes_best_ask": 0.51,
        "no_best_ask": 0.50,
        "resolves_at": NOW + timedelta(days=7),
        "news_ref": "news:headline-1",
    }
    data.update(overrides)
    return AnchoringMarketSnapshot(**cast(Any, data))


def _module(
    market: AnchoringMarketSnapshot | None,
    *,
    sizer: Any | None = None,
) -> AnchoringLagStrategyModule:
    return AnchoringLagStrategyModule(
        source=LiveAnchoringSource(
            market_ids=("market-h2-1",),
            market_reader=_StaticAnchoringReader(market),
            position_sizer=sizer or _FixedSizer(),
            portfolio=_portfolio(),
        ),
        controller=AnchoringController(),
        agent=AnchoringAgent(),
        strategy_id="h2_anchoring_lag",
        strategy_version_id="h2-anchoring-lag-v1",
    )


def test_anchoring_components_satisfy_strategy_protocols() -> None:
    module = _module(_market())

    source: StrategyObservationSource = module.source
    controller: StrategyController = module.controller
    agent: StrategyAgent = module.agent
    strategy_module: StrategyModule = module

    assert source is module.source
    assert controller is module.controller
    assert agent is module.agent
    assert strategy_module.strategy_id == "h2_anchoring_lag"


@pytest.mark.asyncio
async def test_anchoring_positive_divergence_buys_yes_contract() -> None:
    sizer = _FixedSizer(5.0)
    module = _module(_market(yes_price=0.50, llm_posterior=0.80, yes_best_ask=0.51), sizer=sizer)

    intents = await module.run(_context())

    assert len(intents) == 1
    intent = intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.outcome == "YES"
    assert intent.side == "BUY"
    assert intent.token_id == "token-yes"
    assert intent.limit_price == pytest.approx(0.51)
    assert intent.expected_price == pytest.approx(0.80)
    assert intent.expected_edge == pytest.approx(0.29)
    assert intent.notional_usdc == pytest.approx(5.0)
    assert len(sizer.calls) == 1
    assert sizer.calls[0][0] == pytest.approx(0.80)
    assert sizer.calls[0][1] == pytest.approx(0.51)
    assert ANCHORING_RESEARCH_REF in intent.evidence_refs


@pytest.mark.asyncio
async def test_anchoring_negative_divergence_buys_no_contract() -> None:
    module = _module(
        _market(
            yes_price=0.75,
            llm_posterior=0.45,
            no_best_ask=0.26,
        )
    )

    intents = await module.run(_context())

    assert len(intents) == 1
    intent = intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.outcome == "NO"
    assert intent.side == "BUY"
    assert intent.token_id == "token-no"
    assert intent.limit_price == pytest.approx(0.26)
    assert intent.expected_price == pytest.approx(0.55)
    assert intent.expected_edge == pytest.approx(0.29)


@pytest.mark.asyncio
async def test_anchoring_source_rejects_stale_or_low_confidence_signals() -> None:
    assert await _module(_market(news_timestamp=NOW - timedelta(hours=24))).run(_context()) == ()
    assert await _module(_market(llm_confidence=0.50)).run(_context()) == ()


@pytest.mark.asyncio
async def test_anchoring_source_suppresses_zero_sized_trades() -> None:
    module = _module(_market(), sizer=_ZeroSizer())

    assert await module.run(_context()) == ()


@pytest.mark.asyncio
async def test_anchoring_module_rejects_context_strategy_mismatch() -> None:
    module = _module(_market())
    context = StrategyContext(
        strategy_id="other",
        strategy_version_id="h2-anchoring-lag-v1",
        as_of=NOW,
    )

    with pytest.raises(ValueError, match="context strategy identity must match"):
        await module.run(context)


def test_strategy_import_linter_contract_includes_anchoring_plugin() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    contracts: Sequence[dict[str, object]] = pyproject["tool"]["importlinter"]["contracts"]
    contract = next(
        candidate
        for candidate in contracts
        if candidate["name"] == "Strategy plugins: no actuator, controller, or venue adapter imports"
    )

    assert contract["source_modules"] == [
        "pms.strategies.ripple",
        "pms.strategies.flb",
        "pms.strategies.anchoring",
    ]
