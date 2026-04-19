"""Research backtest specification value objects."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
import json
from typing import Any, Literal, TypeAlias

from pms.evaluation.metrics import StrategyVersionKey
from pms.strategies.projections import RiskParams


RiskPolicy: TypeAlias = RiskParams


def _json_sort_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _is_scalar_sequence(value: Sequence[Any]) -> bool:
    return all(
        isinstance(item, (str, int, float, bool, type(None), datetime))
        for item in value
    )


def _normalize_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            msg = "BacktestSpec hashing requires timezone-aware datetimes"
            raise ValueError(msg)
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return _normalize_value(asdict(value))
    if isinstance(value, Mapping):
        return {
            key: _normalize_value(value[key])
            for key in sorted(value)
        }
    if isinstance(value, frozenset | set):
        normalized_items = [_normalize_value(item) for item in value]
        return sorted(normalized_items, key=_json_sort_key)
    if isinstance(value, tuple):
        normalized_items = [_normalize_value(item) for item in value]
        if _is_scalar_sequence(value):
            return normalized_items
        return sorted(normalized_items, key=_json_sort_key)
    if isinstance(value, list):
        normalized_items = [_normalize_value(item) for item in value]
        return sorted(normalized_items, key=_json_sort_key)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    msg = f"BacktestSpec hashing does not support value of type {type(value).__name__}"
    raise TypeError(msg)


def _compute_config_hash(*, spec: BacktestSpec) -> str:
    canonical_payload = _normalize_value(
        {
            "strategy_versions": spec.strategy_versions,
            "dataset": spec.dataset,
            "execution_model": spec.execution_model,
            "risk_policy": spec.risk_policy,
            "date_range_start": spec.date_range_start,
            "date_range_end": spec.date_range_end,
        }
    )
    canonical_json = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return sha256(canonical_json.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class BacktestExecutionConfig:
    chunk_days: int = 7
    time_budget: int = 1800


@dataclass(frozen=True, slots=True)
class ExecutionModel:
    fee_rate: float
    slippage_bps: float
    latency_ms: float
    staleness_ms: float
    fill_policy: Literal["immediate_or_cancel", "limit_if_touched"]

    def fee_curve(self, *, price: float, shares: float) -> float:
        return shares * self.fee_rate * price * (1.0 - price)

    @classmethod
    def polymarket_paper(cls) -> ExecutionModel:
        return cls(
            fee_rate=0.0,
            slippage_bps=0.0,
            latency_ms=0.0,
            staleness_ms=float("inf"),
            fill_policy="immediate_or_cancel",
        )

    @classmethod
    def polymarket_live_estimate(cls) -> ExecutionModel:
        return cls(
            # source: https://docs.polymarket.com/trading/fees Finance/Politics/Mentions/Tech taker fee; retrieved 2026-04-19T00:00:00+08:00
            fee_rate=0.04,
            # source: S5 /metrics SQL median slippage_bps query on eval_records over the trailing 7 days; retrieved 2026-04-19T00:00:00+08:00
            slippage_bps=10.0,
            # source: SensorWatchdog telemetry logs (expected path logs/sensor-watchdog.jsonl) p95 round-trip latency; retrieved 2026-04-19T00:00:00+08:00
            latency_ms=250.0,
            # source: src/pms/sensor/watchdog.py timeout_s=120.0 as the local staleness ceiling; retrieved 2026-04-19T00:00:00+08:00
            staleness_ms=120_000.0,
            # source: CP01b acceptance criteria default both profiles to immediate_or_cancel; retrieved 2026-04-19T00:00:00+08:00
            fill_policy="immediate_or_cancel",
        )


@dataclass(frozen=True, slots=True)
class BacktestDataset:
    source: str
    version: str
    coverage_start: datetime
    coverage_end: datetime
    market_universe_filter: Mapping[str, Any]
    data_quality_gaps: tuple[tuple[datetime, datetime, str], ...]


@dataclass(frozen=True, slots=True)
class BacktestSpec:
    strategy_versions: tuple[StrategyVersionKey, ...]
    dataset: BacktestDataset
    execution_model: ExecutionModel
    risk_policy: RiskPolicy
    date_range_start: datetime
    date_range_end: datetime
    config_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "config_hash", _compute_config_hash(spec=self))


__all__ = [
    "BacktestDataset",
    "BacktestExecutionConfig",
    "BacktestSpec",
    "ExecutionModel",
    "RiskPolicy",
]
