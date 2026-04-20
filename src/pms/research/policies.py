"""Research comparison policies and similarity metrics."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Literal, cast


SelectionDenominator = Literal["backtest_set", "live_set", "union"]


def _freeze_alias_map(raw_mapping: Mapping[str, str]) -> Mapping[str, str]:
    return cast(Mapping[str, str], MappingProxyType(dict(raw_mapping)))


def _apply_offset(timestamp: datetime, offset_s: float) -> datetime:
    return timestamp + timedelta(seconds=offset_s)


@dataclass(frozen=True, slots=True)
class TimeAlignmentPolicy:
    """Timestamp offsets for backtest/live comparison alignment.

    Example:
        Use `generated_offset_s=-3600.0` when generated timestamps should be
        compared one hour earlier than their recorded value.
    """

    generated_offset_s: float = 0.0
    exchange_offset_s: float = 0.0
    ingest_offset_s: float = 0.0
    evaluation_offset_s: float = 0.0

    def apply_generated(self, timestamp: datetime) -> datetime:
        return _apply_offset(timestamp, self.generated_offset_s)

    def apply_exchange(self, timestamp: datetime) -> datetime:
        return _apply_offset(timestamp, self.exchange_offset_s)

    def apply_ingest(self, timestamp: datetime) -> datetime:
        return _apply_offset(timestamp, self.ingest_offset_s)

    def apply_evaluation(self, timestamp: datetime) -> datetime:
        return _apply_offset(timestamp, self.evaluation_offset_s)


@dataclass(frozen=True, slots=True)
class SymbolNormalizationPolicy:
    """Alias maps for normalizing historical or venue-specific identifiers.

    Example:
        Use `token_id_aliases={"BTC-USDT": "BTCUSDT"}` when historical data and
        live data disagree on token-id formatting.
    """

    token_id_aliases: Mapping[str, str] = field(default_factory=dict)
    market_id_aliases: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "token_id_aliases", _freeze_alias_map(self.token_id_aliases))
        object.__setattr__(self, "market_id_aliases", _freeze_alias_map(self.market_id_aliases))

    def normalize_token_id(self, token_id: str) -> str:
        return self.token_id_aliases.get(token_id, token_id)

    def normalize_market_id(self, market_id: str) -> str:
        return self.market_id_aliases.get(market_id, market_id)


@dataclass(frozen=True, slots=True)
class SelectionSimilarityMetric:
    """Overlap metric with an explicit denominator choice.

    Example:
        Use `SelectionSimilarityMetric(denominator="union")` when the overlap
        should be interpreted as Jaccard similarity across both selections.
    """

    denominator: SelectionDenominator

    def compute(
        self,
        backtest_set: frozenset[str],
        live_set: frozenset[str],
    ) -> float:
        intersection_size = len(backtest_set & live_set)
        denominator_size = self._denominator_size(backtest_set, live_set)
        if denominator_size == 0:
            return 0.0
        return intersection_size / denominator_size

    def _denominator_size(
        self,
        backtest_set: frozenset[str],
        live_set: frozenset[str],
    ) -> int:
        if self.denominator == "backtest_set":
            return len(backtest_set)
        if self.denominator == "live_set":
            return len(live_set)
        return len(backtest_set | live_set)


__all__ = [
    "SelectionDenominator",
    "SelectionSimilarityMetric",
    "SymbolNormalizationPolicy",
    "TimeAlignmentPolicy",
]
