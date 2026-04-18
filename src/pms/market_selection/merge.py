from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class StrategyMarketSet:
    strategy_id: str
    strategy_version_id: str
    asset_ids: frozenset[str]


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
        return MergeResult(
            asset_ids=sorted(
                {
                    asset_id
                    for selection in selections
                    for asset_id in selection.asset_ids
                }
            ),
            conflicts=[],
        )
