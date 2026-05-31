from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
import importlib
import logging
from typing import Any

import pytest

from pms.core.models import BookSummary, Market, Token, Venue
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
    liquidity: float | None = None,
    spread_bps: int | None = None,
    accepting_orders: bool | None = None,
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
            liquidity=liquidity,
            spread_bps=spread_bps,
            accepting_orders=accepting_orders,
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


def _book_summary(
    *,
    spread_bps: float = 50.0,
    depth_usdc: float = 500.0,
) -> BookSummary:
    return BookSummary(
        best_bid=0.49,
        best_ask=0.51,
        spread_bps=spread_bps,
        depth_usdc=depth_usdc,
        timestamp=datetime(2026, 4, 18, 9, 0, tzinfo=UTC),
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
                        venue="polymarket",
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
            if venue == "polymarket" and max_horizon_days == 7:
                return [
                    _eligible_market(
                        "market-a",
                        venue="polymarket",
                        token_ids=("pm-yes", "pm-no"),
                    )
                ]
            return [
                _eligible_market(
                    "market-b-polymarket",
                    venue="polymarket",
                    token_ids=("pm-b-yes", "pm-b-no"),
                )
            ]

    class RecordingMergePolicy:
        def __init__(self) -> None:
            self.selections: list[object] | None = None

        def merge(self, selections: list[object]) -> object:
            self.selections = selections
            return merge_result_cls(
                asset_ids=["pm-b-no", "pm-b-yes", "pm-no", "pm-yes"],
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
        ("polymarket", 30, 1000.0),
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
            asset_ids=frozenset({"pm-b-yes", "pm-b-no"}),
        ),
    ]
    assert result.asset_ids == ["pm-b-no", "pm-b-yes", "pm-no", "pm-yes"]
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
                        venue="polymarket",
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
            raise AssertionError(f"unexpected venue {venue}")

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
            asset_ids=frozenset({"pm-yes", "pm-no"}),
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


@pytest.mark.asyncio
async def test_market_selector_applies_spread_depth_liquidity_and_status_filters() -> None:
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
                        spread_max_bps=100.0,
                        depth_min_usdc=250.0,
                        liquidity_min_usdc=400.0,
                        accepting_orders=True,
                    ),
                )
            ]

    class FakeStore:
        async def read_eligible_markets(
            self,
            venue: str,
            max_horizon_days: int | None,
            min_volume_usdc: float,
        ) -> list[tuple[Market, list[Token]]]:
            del venue, max_horizon_days, min_volume_usdc
            return [
                _eligible_market(
                    "all-pass",
                    token_ids=("all-pass-yes", "all-pass-no"),
                    liquidity=500.0,
                    accepting_orders=True,
                ),
                _eligible_market(
                    "wide-spread",
                    token_ids=("wide-spread-yes", "wide-spread-no"),
                    liquidity=500.0,
                    accepting_orders=True,
                ),
                _eligible_market(
                    "shallow-depth",
                    token_ids=("shallow-depth-yes", "shallow-depth-no"),
                    liquidity=500.0,
                    accepting_orders=True,
                ),
                _eligible_market(
                    "low-liquidity",
                    token_ids=("low-liquidity-yes", "low-liquidity-no"),
                    liquidity=300.0,
                    accepting_orders=True,
                ),
                _eligible_market(
                    "not-accepting",
                    token_ids=("not-accepting-yes", "not-accepting-no"),
                    liquidity=500.0,
                    accepting_orders=False,
                ),
                _eligible_market(
                    "missing-book",
                    token_ids=("missing-book-yes", "missing-book-no"),
                    liquidity=500.0,
                    accepting_orders=True,
                ),
            ]

        async def get_latest_book_summary(self, market_id: str) -> BookSummary | None:
            return {
                "all-pass": _book_summary(),
                "wide-spread": _book_summary(spread_bps=150.0),
                "shallow-depth": _book_summary(depth_usdc=200.0),
                "low-liquidity": _book_summary(),
                "not-accepting": _book_summary(),
                "missing-book": None,
            }[market_id]

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
            asset_ids=frozenset({"all-pass-yes", "all-pass-no"}),
        )
    ]


@pytest.mark.asyncio
async def test_market_selector_bootstraps_book_filters_from_discovery_snapshot() -> None:
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
                        spread_max_bps=100.0,
                        depth_min_usdc=250.0,
                    ),
                )
            ]

    class FakeStore:
        async def read_eligible_markets(
            self,
            venue: str,
            max_horizon_days: int | None,
            min_volume_usdc: float,
        ) -> list[tuple[Market, list[Token]]]:
            del venue, max_horizon_days, min_volume_usdc
            return [
                _eligible_market(
                    "bootstrap-pass",
                    token_ids=("bootstrap-pass-yes", "bootstrap-pass-no"),
                    liquidity=500.0,
                    spread_bps=50,
                ),
                _eligible_market(
                    "bootstrap-wide",
                    token_ids=("bootstrap-wide-yes", "bootstrap-wide-no"),
                    liquidity=500.0,
                    spread_bps=150,
                ),
                _eligible_market(
                    "bootstrap-shallow",
                    token_ids=("bootstrap-shallow-yes", "bootstrap-shallow-no"),
                    liquidity=200.0,
                    spread_bps=50,
                ),
                _eligible_market(
                    "bootstrap-missing-spread",
                    token_ids=(
                        "bootstrap-missing-spread-yes",
                        "bootstrap-missing-spread-no",
                    ),
                    liquidity=500.0,
                ),
            ]

        async def get_latest_book_summary(self, market_id: str) -> BookSummary | None:
            del market_id
            return None

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
            asset_ids=frozenset({"bootstrap-pass-yes", "bootstrap-pass-no"}),
        )
    ]


@pytest.mark.asyncio
async def test_market_selector_bypasses_book_query_when_book_filters_are_disabled() -> None:
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
                        spread_max_bps=None,
                        depth_min_usdc=None,
                        liquidity_min_usdc=None,
                        accepting_orders=False,
                    ),
                )
            ]

    class FakeStore:
        async def read_eligible_markets(
            self,
            venue: str,
            max_horizon_days: int | None,
            min_volume_usdc: float,
        ) -> list[tuple[Market, list[Token]]]:
            del venue, max_horizon_days, min_volume_usdc
            return [
                _eligible_market(
                    "missing-book",
                    token_ids=("missing-book-yes", "missing-book-no"),
                    accepting_orders=False,
                )
            ]

        async def get_latest_book_summary(self, market_id: str) -> BookSummary | None:
            raise AssertionError(f"book summary should not be queried for {market_id}")

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
            asset_ids=frozenset({"missing-book-yes", "missing-book-no"}),
        )
    ]
