"""Public codecs for research backtest specs and execution config."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import json
import math
from typing import Any, cast

from pms.research.specs import (
    BacktestDataset,
    BacktestExecutionConfig,
    BacktestSpec,
    ExecutionModel,
    FillPolicy,
    RiskPolicy,
)


def deserialize_backtest_spec(raw_value: object) -> BacktestSpec:
    payload = _json_object(raw_value)
    strategy_versions_raw = payload.get("strategy_versions", ())
    if not isinstance(strategy_versions_raw, list):
        msg = "BacktestSpec.strategy_versions must decode to a JSON array"
        raise TypeError(msg)
    strategy_versions: list[tuple[str, str]] = []
    for item in strategy_versions_raw:
        if not isinstance(item, list | tuple) or len(item) != 2:
            msg = "BacktestSpec.strategy_versions entries must be pairs"
            raise TypeError(msg)
        strategy_versions.append((str(item[0]), str(item[1])))
    return BacktestSpec(
        strategy_versions=tuple(strategy_versions),
        dataset=_deserialize_dataset(payload["dataset"]),
        execution_model=_deserialize_execution_model(payload["execution_model"]),
        risk_policy=_deserialize_risk_policy(payload["risk_policy"]),
        date_range_start=_deserialize_datetime(payload["date_range_start"]),
        date_range_end=_deserialize_datetime(payload["date_range_end"]),
    )


def deserialize_execution_config(raw_value: object) -> BacktestExecutionConfig:
    payload = _json_object(raw_value)
    return BacktestExecutionConfig(
        chunk_days=_coerce_int(
            payload.get("chunk_days", 7),
            field_name="BacktestExecutionConfig.chunk_days",
        ),
        time_budget=_coerce_int(
            payload.get("time_budget", 1800),
            field_name="BacktestExecutionConfig.time_budget",
        ),
    )


def serialize_backtest_spec(spec: BacktestSpec) -> dict[str, object]:
    return {
        "strategy_versions": [
            [strategy_id, strategy_version_id]
            for strategy_id, strategy_version_id in spec.strategy_versions
        ],
        "dataset": {
            "source": spec.dataset.source,
            "version": spec.dataset.version,
            "coverage_start": serialize_datetime(spec.dataset.coverage_start),
            "coverage_end": serialize_datetime(spec.dataset.coverage_end),
            "market_universe_filter": dict(spec.dataset.market_universe_filter),
            "data_quality_gaps": [
                [serialize_datetime(start), serialize_datetime(end), reason]
                for start, end, reason in spec.dataset.data_quality_gaps
            ],
        },
        "execution_model": {
            "fee_rate": _serialize_float(spec.execution_model.fee_rate),
            "slippage_bps": _serialize_float(spec.execution_model.slippage_bps),
            "latency_ms": _serialize_float(spec.execution_model.latency_ms),
            "staleness_ms": _serialize_float(spec.execution_model.staleness_ms),
            "fill_policy": spec.execution_model.fill_policy,
            "order_ttl_ms": spec.execution_model.order_ttl_ms,
            "price_invalidation_streak": spec.execution_model.price_invalidation_streak,
            "replay_window_ms": spec.execution_model.replay_window_ms,
        },
        "risk_policy": {
            "max_position_notional_usdc": _serialize_float(
                spec.risk_policy.max_position_notional_usdc
            ),
            "max_daily_drawdown_pct": _serialize_float(
                spec.risk_policy.max_daily_drawdown_pct
            ),
            "min_order_size_usdc": _serialize_float(spec.risk_policy.min_order_size_usdc),
        },
        "date_range_start": serialize_datetime(spec.date_range_start),
        "date_range_end": serialize_datetime(spec.date_range_end),
    }


def serialize_execution_config(
    exec_config: BacktestExecutionConfig,
) -> dict[str, object]:
    return {
        "chunk_days": exec_config.chunk_days,
        "time_budget": exec_config.time_budget,
    }


def serialize_datetime(value: datetime) -> str:
    return value.isoformat()


def _deserialize_dataset(raw_value: object) -> BacktestDataset:
    payload = _json_object(raw_value)
    raw_gaps = payload.get("data_quality_gaps", [])
    if not isinstance(raw_gaps, list):
        msg = "BacktestDataset.data_quality_gaps must decode to a JSON array"
        raise TypeError(msg)
    gaps: list[tuple[datetime, datetime, str]] = []
    for item in raw_gaps:
        if not isinstance(item, list | tuple) or len(item) != 3:
            msg = "BacktestDataset.data_quality_gaps entries must be triples"
            raise TypeError(msg)
        gaps.append(
            (
                _deserialize_datetime(item[0]),
                _deserialize_datetime(item[1]),
                str(item[2]),
            )
        )
    market_universe_filter = payload.get("market_universe_filter", {})
    if not isinstance(market_universe_filter, Mapping):
        msg = "BacktestDataset.market_universe_filter must decode to a JSON object"
        raise TypeError(msg)
    return BacktestDataset(
        source=str(payload["source"]),
        version=str(payload["version"]),
        coverage_start=_deserialize_datetime(payload["coverage_start"]),
        coverage_end=_deserialize_datetime(payload["coverage_end"]),
        market_universe_filter=cast(Mapping[str, Any], dict(market_universe_filter)),
        data_quality_gaps=tuple(gaps),
    )


def _deserialize_execution_model(raw_value: object) -> ExecutionModel:
    payload = _json_object(raw_value)
    return ExecutionModel(
        fee_rate=_coerce_float(
            payload["fee_rate"],
            field_name="ExecutionModel.fee_rate",
        ),
        slippage_bps=_coerce_float(
            payload["slippage_bps"],
            field_name="ExecutionModel.slippage_bps",
        ),
        latency_ms=_coerce_float(
            payload["latency_ms"],
            field_name="ExecutionModel.latency_ms",
        ),
        staleness_ms=_coerce_float(
            payload["staleness_ms"],
            field_name="ExecutionModel.staleness_ms",
        ),
        fill_policy=_coerce_fill_policy(payload["fill_policy"]),
        order_ttl_ms=_coerce_int(
            payload.get("order_ttl_ms", 60_000),
            field_name="ExecutionModel.order_ttl_ms",
        ),
        price_invalidation_streak=_coerce_int(
            payload.get("price_invalidation_streak", 10),
            field_name="ExecutionModel.price_invalidation_streak",
        ),
        replay_window_ms=_coerce_int(
            payload.get("replay_window_ms", 86_400_000),
            field_name="ExecutionModel.replay_window_ms",
        ),
    )


def _deserialize_risk_policy(raw_value: object) -> RiskPolicy:
    payload = _json_object(raw_value)
    return RiskPolicy(
        max_position_notional_usdc=_coerce_float(
            payload["max_position_notional_usdc"],
            field_name="RiskPolicy.max_position_notional_usdc",
        ),
        max_daily_drawdown_pct=_coerce_float(
            payload["max_daily_drawdown_pct"],
            field_name="RiskPolicy.max_daily_drawdown_pct",
        ),
        min_order_size_usdc=_coerce_float(
            payload["min_order_size_usdc"],
            field_name="RiskPolicy.min_order_size_usdc",
        ),
    )


def _deserialize_datetime(raw_value: object) -> datetime:
    if not isinstance(raw_value, str):
        msg = "Backtest datetime fields must decode to ISO-8601 strings"
        raise TypeError(msg)
    value = datetime.fromisoformat(raw_value)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = "Backtest datetime fields must be timezone-aware"
        raise ValueError(msg)
    return value


def _coerce_int(raw_value: object, *, field_name: str) -> int:
    if not isinstance(raw_value, int) or isinstance(raw_value, bool):
        msg = f"{field_name} must decode to an integer"
        raise TypeError(msg)
    return raw_value


def _coerce_float(raw_value: object, *, field_name: str) -> float:
    if isinstance(raw_value, str):
        sentinel = raw_value.strip().lower()
        if sentinel in {".inf", "inf", "infinity"}:
            return float("inf")
        if sentinel in {"-.inf", "-inf", "-infinity"}:
            return float("-inf")
        if sentinel in {".nan", "nan"}:
            return float("nan")
    if not isinstance(raw_value, int | float) or isinstance(raw_value, bool):
        msg = f"{field_name} must decode to a numeric value"
        raise TypeError(msg)
    return float(raw_value)


def _coerce_fill_policy(raw_value: object) -> FillPolicy:
    if raw_value not in (
        "immediate_or_cancel",
        "limit_if_touched",
        "good_til_cancelled",
        "fill_or_kill",
    ):
        msg = "ExecutionModel.fill_policy must decode to a supported fill policy"
        raise TypeError(msg)
    return cast(FillPolicy, str(raw_value))


def _json_object(raw_value: object) -> dict[str, object]:
    decoded = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    if not isinstance(decoded, dict):
        msg = "Expected JSON object payload"
        raise TypeError(msg)
    return cast(dict[str, object], decoded)


def _serialize_float(value: float) -> float | str:
    if math.isnan(value):
        return ".nan"
    if math.isinf(value):
        return ".inf" if value > 0 else "-.inf"
    return value
