"""Research backtest specification value objects."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
import json
import math
from typing import Any, Literal, TypeAlias, cast

from pms.evaluation.metrics import StrategyVersionKey
from pms.strategies.projections import RiskParams


RiskPolicy: TypeAlias = RiskParams
FillPolicy: TypeAlias = Literal[
    "immediate_or_cancel",
    "limit_if_touched",
    "good_til_cancelled",
    "fill_or_kill",
]
ExecutionModelCalibrationSource: TypeAlias = Literal[
    "manual",
    "idealized_paper",
    "static_live_estimate",
    "telemetry_calibrated",
]
SUPPORTED_FILL_POLICIES = frozenset(
    {
        "immediate_or_cancel",
        "limit_if_touched",
        "good_til_cancelled",
        "fill_or_kill",
    }
)
SUPPORTED_EXECUTION_MODEL_CALIBRATION_SOURCES = frozenset(
    {
        "manual",
        "idealized_paper",
        "static_live_estimate",
        "telemetry_calibrated",
    }
)


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


def _hashable_execution_model(execution_model: object) -> Any:
    """Returns the execution_model fields that influence backtest output."""
    if not is_dataclass(execution_model) or isinstance(execution_model, type):
        return execution_model
    return {
        item.name: value
        for item in fields(execution_model)
        if item.name != "latency_model"
        if not callable(value := getattr(cast(Any, execution_model), item.name))
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
    fill_policy: FillPolicy
    displayed_depth_fill_ratio: float = 1.0
    adverse_selection_bps: float = 0.0
    order_ttl_ms: int = 60_000
    price_invalidation_streak: int = 10
    replay_window_ms: int = 86_400_000
    calibration_source: ExecutionModelCalibrationSource = "manual"
    latency_model: Callable[[float], float] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        _nonnegative_finite(self.fee_rate, "fee_rate")
        _nonnegative_finite(self.slippage_bps, "slippage_bps")
        _nonnegative_finite(self.latency_ms, "latency_ms")
        _positive_or_infinite(self.staleness_ms, "staleness_ms")
        _unit_interval_open_closed(
            self.displayed_depth_fill_ratio,
            "displayed_depth_fill_ratio",
        )
        _nonnegative_finite(self.adverse_selection_bps, "adverse_selection_bps")
        _positive_int(self.order_ttl_ms, "order_ttl_ms")
        _positive_int(
            self.price_invalidation_streak,
            "price_invalidation_streak",
        )
        _positive_int(self.replay_window_ms, "replay_window_ms")
        if self.fill_policy not in SUPPORTED_FILL_POLICIES:
            msg = "ExecutionModel.fill_policy must be a supported fill policy"
            raise ValueError(msg)
        if (
            self.calibration_source
            not in SUPPORTED_EXECUTION_MODEL_CALIBRATION_SOURCES
        ):
            msg = (
                "ExecutionModel.calibration_source must be a supported "
                "calibration source"
            )
            raise ValueError(msg)

    def compute_fee(self, *, notional_usdc: float, fill_price: float) -> float:
        return notional_usdc * self.fee_rate * (1.0 - fill_price)

    @classmethod
    def polymarket_paper(cls) -> ExecutionModel:
        return cls(
            fee_rate=0.0,
            slippage_bps=0.0,
            latency_ms=0.0,
            staleness_ms=float("inf"),
            fill_policy="immediate_or_cancel",
            calibration_source="idealized_paper",
        )

    @classmethod
    def polymarket_live_estimate(cls) -> ExecutionModel:
        return cls(
            # source: https://docs.polymarket.com/trading/fees Finance/Politics/Mentions/Tech taker fee; retrieved 2026-04-19T00:00:00+08:00
            fee_rate=0.04,
            # source: S5 /metrics SQL median slippage_bps query on eval_records over the trailing 7 days; retrieved 2026-04-19T00:00:00+08:00
            slippage_bps=10.0,
            # source: conservative queue/adverse-selection placeholder until paper telemetry drift samples are exported; retrieved 2026-05-27T00:00:00+08:00
            adverse_selection_bps=5.0,
            # source: SensorWatchdog telemetry logs (expected path logs/sensor-watchdog.jsonl) p95 round-trip latency; retrieved 2026-04-19T00:00:00+08:00
            latency_ms=250.0,
            # source: src/pms/sensor/watchdog.py timeout_s=120.0 as the local staleness ceiling; retrieved 2026-04-19T00:00:00+08:00
            staleness_ms=120_000.0,
            # source: CP01b acceptance criteria default both profiles to immediate_or_cancel; retrieved 2026-04-19T00:00:00+08:00
            fill_policy="immediate_or_cancel",
            calibration_source="static_live_estimate",
        )

    @classmethod
    def from_observed_telemetry(
        cls,
        *,
        fee_rate: float,
        slippage_bps_samples: Sequence[float],
        latency_ms_samples: Sequence[float],
        staleness_ms: float,
        displayed_depth_fill_ratio: float = 1.0,
        adverse_selection_bps_samples: Sequence[float] = (),
        fill_policy: FillPolicy = "immediate_or_cancel",
        order_ttl_ms: int = 60_000,
        price_invalidation_streak: int = 10,
        replay_window_ms: int = 86_400_000,
    ) -> ExecutionModel:
        fee_rate_value = _nonnegative_finite(fee_rate, "fee_rate")
        staleness_value = _positive_finite(staleness_ms, "staleness_ms")
        displayed_depth_ratio = _unit_interval_open_closed(
            displayed_depth_fill_ratio,
            "displayed_depth_fill_ratio",
        )
        slippage_values = _telemetry_samples(
            slippage_bps_samples,
            "slippage_bps_samples",
        )
        latency_values = _telemetry_samples(
            latency_ms_samples,
            "latency_ms_samples",
        )
        adverse_selection_bps = 0.0
        if adverse_selection_bps_samples:
            adverse_selection_bps = _nearest_rank_percentile(
                _telemetry_samples(
                    adverse_selection_bps_samples,
                    "adverse_selection_bps_samples",
                ),
                0.95,
            )
        return cls(
            fee_rate=fee_rate_value,
            slippage_bps=_median(slippage_values),
            latency_ms=_nearest_rank_percentile(latency_values, 0.95),
            staleness_ms=staleness_value,
            fill_policy=fill_policy,
            displayed_depth_fill_ratio=displayed_depth_ratio,
            adverse_selection_bps=adverse_selection_bps,
            order_ttl_ms=order_ttl_ms,
            price_invalidation_streak=price_invalidation_streak,
            replay_window_ms=replay_window_ms,
            calibration_source="telemetry_calibrated",
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


def _telemetry_samples(values: Sequence[float], field_name: str) -> tuple[float, ...]:
    if not values:
        msg = f"{field_name} must contain at least one sample"
        raise ValueError(msg)
    return tuple(_nonnegative_finite(value, field_name) for value in values)


def _nonnegative_finite(value: float, field_name: str) -> float:
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0.0:
        msg = f"{field_name} must be finite and >= 0"
        raise ValueError(msg)
    return numeric


def _positive_or_infinite(value: float, field_name: str) -> float:
    numeric = float(value)
    if math.isinf(numeric) and numeric > 0.0:
        return numeric
    return _positive_finite(numeric, field_name)


def _unit_interval_open_closed(value: float, field_name: str) -> float:
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0.0 or numeric > 1.0:
        msg = f"{field_name} must satisfy 0.0 < value <= 1.0"
        raise ValueError(msg)
    return numeric


def _positive_finite(value: float, field_name: str) -> float:
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0.0:
        msg = f"{field_name} must be finite and > 0"
        raise ValueError(msg)
    return numeric


def _positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        msg = f"{field_name} must be an integer > 0"
        raise ValueError(msg)
    return value


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def _nearest_rank_percentile(values: Sequence[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


__all__ = [
    "BacktestDataset",
    "BacktestExecutionConfig",
    "BacktestSpec",
    "ExecutionModel",
    "ExecutionModelCalibrationSource",
    "FillPolicy",
    "RiskPolicy",
]
