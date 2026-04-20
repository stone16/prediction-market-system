"""Research backtest specification value objects."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
import json
import math
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
    if isinstance(value, float):
        if math.isnan(value):
            return ".nan"
        if math.isinf(value):
            return ".inf" if value > 0 else "-.inf"
        return value
    if isinstance(value, (str, int, bool)) or value is None:
        return value

    msg = f"BacktestSpec hashing does not support value of type {type(value).__name__}"
    raise TypeError(msg)


_HASH_IGNORED_EXECUTION_MODEL_FIELDS: frozenset[str] = frozenset(
    {
        # staleness_ms and latency_ms are declared on ExecutionModel but the
        # research replay engine does not yet apply them. Including them in
        # the hash would treat identical-behavior profiles as distinct runs
        # and poison cache dedup. Remove from this set once the runner
        # consumes the field.
        "latency_ms",
        "staleness_ms",
    }
)


def _hashable_execution_model(execution_model: object) -> Any:
    """Returns the subset of execution_model fields that actually influence
    backtest output today. See `_HASH_IGNORED_EXECUTION_MODEL_FIELDS`."""
    if not is_dataclass(execution_model) or isinstance(execution_model, type):
        return execution_model
    fields = asdict(execution_model)
    return {
        key: value
        for key, value in fields.items()
        if key not in _HASH_IGNORED_EXECUTION_MODEL_FIELDS
    }


def _compute_config_hash(*, spec: BacktestSpec) -> str:
    canonical_payload = _normalize_value(
        {
            "strategy_versions": spec.strategy_versions,
            "dataset": spec.dataset,
            "execution_model": _hashable_execution_model(spec.execution_model),
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

    def __post_init__(self) -> None:
        if self.chunk_days <= 0:
            msg = "BacktestExecutionConfig.chunk_days must be positive"
            raise ValueError(msg)
        if self.time_budget <= 0:
            msg = "BacktestExecutionConfig.time_budget must be positive"
            raise ValueError(msg)


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
        _validate_backtest_spec(self)
        object.__setattr__(self, "config_hash", _compute_config_hash(spec=self))


def _validate_backtest_spec(spec: BacktestSpec) -> None:
    if not spec.strategy_versions:
        msg = "BacktestSpec.strategy_versions must be non-empty"
        raise ValueError(msg)
    if len(set(spec.strategy_versions)) != len(spec.strategy_versions):
        msg = "BacktestSpec.strategy_versions contains duplicate entries"
        raise ValueError(msg)
    for label, value in (
        ("date_range_start", spec.date_range_start),
        ("date_range_end", spec.date_range_end),
        ("dataset.coverage_start", spec.dataset.coverage_start),
        ("dataset.coverage_end", spec.dataset.coverage_end),
    ):
        if value.tzinfo is None or value.utcoffset() is None:
            msg = f"BacktestSpec.{label} must be timezone-aware"
            raise ValueError(msg)
    if spec.date_range_start >= spec.date_range_end:
        msg = "BacktestSpec.date_range_start must precede date_range_end"
        raise ValueError(msg)
    if spec.date_range_start < spec.dataset.coverage_start:
        msg = (
            "BacktestSpec.date_range_start must fall within "
            "BacktestDataset.coverage range"
        )
        raise ValueError(msg)
    if spec.date_range_end > spec.dataset.coverage_end:
        msg = (
            "BacktestSpec.date_range_end must fall within "
            "BacktestDataset.coverage range"
        )
        raise ValueError(msg)


__all__ = [
    "BacktestDataset",
    "BacktestExecutionConfig",
    "BacktestSpec",
    "ExecutionModel",
    "RiskPolicy",
]
