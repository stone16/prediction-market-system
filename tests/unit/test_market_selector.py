from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
import importlib
import logging
from typing import Any

import pytest

from pms.core.models import Market, Token, Venue
from pms.strategies.projections import MarketSelectionSpec


def _load_symbol(module_name: str, symbol_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in red phase
        pytest.fail(f"{module_name} is missing: {exc}")
    return getattr(module, symbol_name)


def _eligible_market(
    market_id: str,
    *,
    venue: Venue = "polymarket",
    token_ids: tuple[str, ...] = ("yes", "no"),
) -> tuple[Market, list[Token]]:
    now = datetime(2026, 4, 18, 9, 0, tzinfo=UTC)
    return (
        Market(
            condition_id=market_id,
            slug=f"slug-{market_id}",
            question=f"Question {market_id}",
            venue=venue,
            resolves_at=now + timedelta(days=3),
            created_at=now - timedelta(days=2),
            last_seen_at=now,
        ),
        [
            Token(
                token_id=token_id,
                condition_id=market_id,
                outcome=("YES" if index == 0 else "NO"),
            )
            for index, token_id in enumerate(token_ids)
        ],
    )


def test_union_merge_policy_returns_sorted_union_without_conflicts() -> None:
    strategy_market_set_cls = _load_symbol(
        "pms.market_selection.merge",
        "StrategyMarketSet",
    )
    union_merge_policy_cls = _load_symbol(
        "pms.market_selection.merge",
        "UnionMergePolicy",
    )

    result = union_merge_policy_cls().merge(
        [
            strategy_market_set_cls(
                strategy_id="alpha",
                strategy_version_id="alpha-v1",
                asset_ids=frozenset({"asset-b", "asset-a"}),
            ),
            strategy_market_set_cls(
                strategy_id="beta",
                strategy_version_id="beta-v2",
                asset_ids=frozenset({"asset-c", "asset-a"}),
            ),
        ]
    )

    assert result.asset_ids == ["asset-a", "asset-b", "asset-c"]
    assert result.conflicts == []


def test_merge_dataclasses_are_frozen() -> None:
    merge_conflict_cls = _load_symbol("pms.market_selection.merge", "MergeConflict")
    merge_result_cls = _load_symbol("pms.market_selection.merge", "MergeResult")
    strategy_market_set_cls = _load_symbol(
        "pms.market_selection.merge",
        "StrategyMarketSet",
    )

    strategy_market_set = strategy_market_set_cls(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        asset_ids=frozenset({"asset-a"}),
    )
    merge_conflict = merge_conflict_cls(
        market_id="market-1",
        strategy_ids=("alpha", "beta"),
        description="conflict",
    )
    merge_result = merge_result_cls(
        asset_ids=["asset-a"],
        conflicts=[merge_conflict],
    )

    with pytest.raises(FrozenInstanceError):
        strategy_market_set.strategy_id = "mutated"
    with pytest.raises(FrozenInstanceError):
        merge_conflict.market_id = "mutated"
    with pytest.raises(FrozenInstanceError):
        merge_result.asset_ids = []


@pytest.mark.asyncio
async def test_market_selector_builds_strategy_sets_before_merging(
) -> None:
    selector_module = importlib.import_module("pms.market_selection.selector")
    market_selector_cls = getattr(selector_module, "MarketSelector")
    merge_result_cls = _load_symbol("pms.market_selection.merge", "MergeResult")
    strategy_market_set_cls = _load_symbol(
        "pms.market_selection.merge",
        "StrategyMarketSet",
    )

    class FakeRegistry:
        def __init__(self) -> None:
            self.calls = 0

        async def list_market_selections(
            self,
        ) -> list[tuple[str, str, MarketSelectionSpec]]:
            self.calls += 1
            return [
                (
                    "alpha",
                    "alpha-v1",
                    MarketSelectionSpec(
                        venue="polymarket",
                        resolution_time_max_horizon_days=7,
                        volume_min_usdc=500.0,
                    ),
                ),
                (
                    "beta",
                    "beta-v2",
                    MarketSelectionSpec(
                        venue="kalshi",
                        resolution_time_max_horizon_days=30,
                        volume_min_usdc=1000.0,
                    ),
                ),
            ]

    class FakeStore:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int | None, float]] = []

        async def read_eligible_markets(
            self,
            venue: str,
            max_horizon_days: int | None,
            min_volume_usdc: float,
        ) -> list[tuple[Market, list[Token]]]:
            self.calls.append((venue, max_horizon_days, min_volume_usdc))
            if venue == "polymarket":
                return [
                    _eligible_market(
                        "market-a",
                        venue="polymarket",
                        token_ids=("pm-yes", "pm-no"),
                    )
                ]
            return [
                _eligible_market(
                    "market-b",
                    venue="kalshi",
                    token_ids=("ka-yes", "ka-no"),
                )
            ]

    class RecordingMergePolicy:
        def __init__(self) -> None:
            self.selections: list[object] | None = None

        def merge(self, selections: list[object]) -> object:
            self.selections = selections
            return merge_result_cls(
                asset_ids=["ka-no", "ka-yes", "pm-no", "pm-yes"],
                conflicts=[],
            )

    fake_store = FakeStore()
    fake_registry = FakeRegistry()
    merge_policy = RecordingMergePolicy()
    selector = market_selector_cls(
        store=fake_store,
        registry=fake_registry,
        merge_policy=merge_policy,
    )

    result = await selector.select()

    assert fake_registry.calls == 1
    assert fake_store.calls == [
        ("polymarket", 7, 500.0),
        ("kalshi", 30, 1000.0),
    ]
    assert merge_policy.selections == [
        strategy_market_set_cls(
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
            asset_ids=frozenset({"pm-yes", "pm-no"}),
        ),
        strategy_market_set_cls(
            strategy_id="beta",
            strategy_version_id="beta-v2",
            asset_ids=frozenset({"ka-yes", "ka-no"}),
        ),
    ]
    assert result.asset_ids == ["ka-no", "ka-yes", "pm-no", "pm-yes"]
    assert result.conflicts == []


@pytest.mark.asyncio
async def test_market_selector_select_per_strategy_returns_pre_merge_strategy_sets() -> None:
    selector_module = importlib.import_module("pms.market_selection.selector")
    market_selector_cls = getattr(selector_module, "MarketSelector")
    strategy_market_set_cls = _load_symbol(
        "pms.market_selection.merge",
        "StrategyMarketSet",
    )

    class FakeRegistry:
        async def list_market_selections(
            self,
        ) -> list[tuple[str, str, MarketSelectionSpec]]:
            return [
                (
                    "alpha",
                    "alpha-v1",
                    MarketSelectionSpec(
                        venue="polymarket",
                        resolution_time_max_horizon_days=7,
                        volume_min_usdc=500.0,
                    ),
                ),
                (
                    "beta",
                    "beta-v2",
                    MarketSelectionSpec(
                        venue="kalshi",
                        resolution_time_max_horizon_days=30,
                        volume_min_usdc=1000.0,
                    ),
                ),
            ]

    class FakeStore:
        async def read_eligible_markets(
            self,
            venue: str,
            max_horizon_days: int | None,
            min_volume_usdc: float,
        ) -> list[tuple[Market, list[Token]]]:
            del max_horizon_days, min_volume_usdc
            if venue == "polymarket":
                return [
                    _eligible_market(
                        "market-a",
                        venue="polymarket",
                        token_ids=("pm-yes", "pm-no"),
                    )
                ]
            return [
                _eligible_market(
                    "market-b",
                    venue="kalshi",
                    token_ids=("ka-yes", "ka-no"),
                )
            ]

    class FailingMergePolicy:
        def merge(self, selections: list[object]) -> object:
            msg = f"select_per_strategy should not merge selections: {selections!r}"
            raise AssertionError(msg)

    selector = market_selector_cls(
        store=FakeStore(),
        registry=FakeRegistry(),
        merge_policy=FailingMergePolicy(),
    )

    selections = await selector.select_per_strategy()

    assert selections == [
        strategy_market_set_cls(
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
            asset_ids=frozenset({"pm-yes", "pm-no"}),
        ),
        strategy_market_set_cls(
            strategy_id="beta",
            strategy_version_id="beta-v2",
            asset_ids=frozenset({"ka-yes", "ka-no"}),
        ),
    ]


@pytest.mark.asyncio
async def test_market_selector_logs_count_and_warns_when_no_active_strategies(
    caplog: pytest.LogCaptureFixture,
) -> None:
    selector_module = importlib.import_module("pms.market_selection.selector")
    market_selector_cls = getattr(selector_module, "MarketSelector")
    merge_result_cls = _load_symbol("pms.market_selection.merge", "MergeResult")

    class EmptyRegistry:
        async def list_market_selections(
            self,
        ) -> list[tuple[str, str, MarketSelectionSpec]]:
            return []

    class UnusedStore:
        async def read_eligible_markets(
            self,
            venue: str,
            max_horizon_days: int | None,
            min_volume_usdc: float,
        ) -> list[tuple[Market, list[Token]]]:
            raise AssertionError("read_eligible_markets should not be called")

    class RecordingMergePolicy:
        def __init__(self) -> None:
            self.calls: list[list[object]] = []

        def merge(self, selections: list[object]) -> object:
            self.calls.append(selections)
            return merge_result_cls(asset_ids=[], conflicts=[])

    merge_policy = RecordingMergePolicy()
    selector = market_selector_cls(
        store=UnusedStore(),
        registry=EmptyRegistry(),
        merge_policy=merge_policy,
    )

    with caplog.at_level(logging.INFO):
        result = await selector.select()

    assert merge_policy.calls == [[]]
    assert result.asset_ids == []
    assert "processed 0 active strategies" in caplog.text
    assert "data sensor will idle until a strategy is activated" in caplog.text
