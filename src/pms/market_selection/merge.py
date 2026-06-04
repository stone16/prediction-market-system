from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True, slots=True)
class StrategyMarketSet:
    strategy_id: str
    strategy_version_id: str
    asset_ids: frozenset[str]
    ranked_asset_ids: tuple[str, ...] = field(
        default_factory=tuple,
        compare=False,
        repr=False,
    )


@dataclass(frozen=True, slots=True)
class MergeConflict:
    market_id: str
    strategy_ids: tuple[str, ...]
    description: str


@dataclass(frozen=True, slots=True)
class MergeResult:
    asset_ids: list[str]
    conflicts: list[MergeConflict]


class MergePolicy(Protocol):
    def merge(self, selections: Sequence[StrategyMarketSet]) -> MergeResult: ...


class UnionMergePolicy:
    def merge(self, selections: Sequence[StrategyMarketSet]) -> MergeResult:
        seen: set[str] = set()
        asset_ids: list[str] = []
        for selection in selections:
            for asset_id in _selection_asset_ids_in_priority_order(selection):
                if asset_id in seen:
                    continue
                seen.add(asset_id)
                asset_ids.append(asset_id)
        return MergeResult(
            asset_ids=asset_ids,
            conflicts=[],
        )


def _selection_asset_ids_in_priority_order(
    selection: StrategyMarketSet,
) -> tuple[str, ...]:
    if selection.ranked_asset_ids:
        return selection.ranked_asset_ids
    return tuple(sorted(selection.asset_ids))
