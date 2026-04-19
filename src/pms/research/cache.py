from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeAlias

from pms.factors.base import FactorValueRow


FactorPanel: TypeAlias = Mapping[str, tuple[FactorValueRow, ...]]


@dataclass(frozen=True, slots=True)
class FactorPanelKey:
    factor_id: str
    param: tuple[tuple[str, object], ...]
    market_ids: frozenset[str]
    ts_start: datetime
    ts_end: datetime

    @classmethod
    def from_inputs(
        cls,
        *,
        factor_id: str,
        param: str | Mapping[str, Any] | None,
        market_ids: Sequence[str],
        ts_start: datetime,
        ts_end: datetime,
    ) -> "FactorPanelKey":
        return cls(
            factor_id=factor_id,
            param=_normalize_param(param),
            market_ids=frozenset(str(market_id) for market_id in market_ids),
            ts_start=ts_start,
            ts_end=ts_end,
        )


@dataclass(slots=True)
class FactorPanelCache:
    enabled: bool = True
    hits: int = 0
    misses: int = 0
    _panels: dict[FactorPanelKey, FactorPanel] = field(default_factory=dict)

    def get(self, key: FactorPanelKey) -> FactorPanel | None:
        if not self.enabled:
            return None
        panel = self._panels.get(key)
        if panel is None:
            self.misses += 1
            return None
        self.hits += 1
        return panel

    def put(self, key: FactorPanelKey, panel: FactorPanel) -> None:
        if not self.enabled:
            return
        self._panels[key] = panel

    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / max(1, total)


def _normalize_param(param: str | Mapping[str, Any] | None) -> tuple[tuple[str, object], ...]:
    if param is None or param == "":
        return ()
    if isinstance(param, str):
        return (("__raw__", param),)
    return tuple(sorted((str(key), _freeze_param_value(value)) for key, value in param.items()))


def _freeze_param_value(value: object) -> object:
    if isinstance(value, Mapping):
        return tuple(
            sorted((str(key), _freeze_param_value(nested)) for key, nested in value.items())
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_param_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        frozen_items = (_freeze_param_value(item) for item in value)
        return tuple(sorted(frozen_items, key=repr))
    return value
