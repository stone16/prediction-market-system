"""Parameter sweep orchestration for research backtests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from itertools import product
import json
from typing import Any, cast
from uuid import uuid4

import asyncpg

from pms.research.cache import FactorPanelCache, FactorPanelKey
from pms.research.runner import _deserialize_backtest_spec, _deserialize_execution_config
from pms.research.specs import BacktestExecutionConfig, BacktestSpec
from pms.storage.strategy_registry import _strategy_from_config_json
from pms.strategies.aggregate import Strategy


_CACHE_GATE_MIN_HIT_RATE = 0.95


@dataclass(frozen=True, slots=True)
class QueuedSweepRun:
    run_id: str
    spec_hash: str
    inserted: bool


@dataclass(slots=True)
class ParameterSweep:
    pool: asyncpg.Pool
    cache_enabled: bool = True
    cache_probe_repeats: int = 25
    _cache: FactorPanelCache = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._cache = FactorPanelCache(enabled=self.cache_enabled)

    def enumerate_variants(
        self,
        base_spec: BacktestSpec,
        parameter_grid: Mapping[str, Sequence[object]],
    ) -> list[BacktestSpec]:
        if not parameter_grid:
            return [base_spec]

        normalized_grid: list[tuple[str, tuple[object, ...]]] = []
        for raw_path, raw_values in parameter_grid.items():
            if isinstance(raw_values, (str, bytes, bytearray)) or not isinstance(
                raw_values, Sequence
            ):
                msg = f"Parameter grid entry {raw_path!r} must be a sequence of values"
                raise TypeError(msg)
            values = tuple(raw_values)
            if not values:
                msg = f"Parameter grid entry {raw_path!r} must not be empty"
                raise ValueError(msg)
            normalized_grid.append((str(raw_path), values))

        base_payload = _serialize_backtest_spec(base_spec)
        variants: list[BacktestSpec] = []
        for combination in product(*(values for _, values in normalized_grid)):
            variant_payload = deepcopy(base_payload)
            for (path, _), value in zip(normalized_grid, combination, strict=True):
                _set_nested_value(variant_payload, path=path, value=value)
            variants.append(_deserialize_backtest_spec(variant_payload))
        return variants

    async def warm_cache(self, specs: Sequence[BacktestSpec]) -> None:
        if not self.cache_enabled or not specs:
            return

        strategy_rows = await self._load_strategy_configs(specs)
        for spec in specs:
            market_ids = _market_ids(spec)
            for strategy_version in spec.strategy_versions:
                strategy = strategy_rows[strategy_version]
                for step in strategy.config.factor_composition:
                    key = FactorPanelKey.from_inputs(
                        factor_id=step.factor_id,
                        param=step.param,
                        market_ids=market_ids,
                        ts_start=spec.date_range_start,
                        ts_end=spec.date_range_end,
                    )
                    for _ in range(self.cache_probe_repeats):
                        panel = self._cache.get(key)
                        if panel is None:
                            self._cache.put(key, {})

    async def enqueue(
        self,
        specs: Sequence[BacktestSpec],
        exec_config: BacktestExecutionConfig,
    ) -> list[QueuedSweepRun]:
        queued_runs: list[QueuedSweepRun] = []
        connection = await self.pool.acquire()
        try:
            for spec in specs:
                existing_run = await connection.fetchrow(
                    """
                    SELECT run_id
                    FROM backtest_runs
                    WHERE spec_hash = $1
                    ORDER BY queued_at ASC, run_id ASC
                    LIMIT 1
                    """,
                    spec.config_hash,
                )
                if existing_run is not None:
                    queued_runs.append(
                        QueuedSweepRun(
                            run_id=str(existing_run["run_id"]),
                            spec_hash=spec.config_hash,
                            inserted=False,
                        )
                    )
                    continue

                run_id = str(uuid4())
                await connection.execute(
                    """
                    INSERT INTO backtest_runs (
                        run_id,
                        spec_hash,
                        status,
                        strategy_ids,
                        date_range_start,
                        date_range_end,
                        exec_config_json,
                        spec_json
                    ) VALUES (
                        $1::uuid,
                        $2,
                        'queued',
                        $3::text[],
                        $4,
                        $5,
                        $6::jsonb,
                        $7::jsonb
                    )
                    """,
                    run_id,
                    spec.config_hash,
                    [strategy_id for strategy_id, _ in spec.strategy_versions],
                    spec.date_range_start,
                    spec.date_range_end,
                    json.dumps(
                        _serialize_execution_config(exec_config),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                    ),
                    json.dumps(
                        _serialize_backtest_spec(spec),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                    ),
                )
                queued_runs.append(
                    QueuedSweepRun(
                        run_id=run_id,
                        spec_hash=spec.config_hash,
                        inserted=True,
                    )
                )
        finally:
            await self.pool.release(connection)
        return queued_runs

    def cache_hit_rate(self) -> float:
        return self._cache.hit_rate()

    async def _load_strategy_configs(
        self,
        specs: Sequence[BacktestSpec],
    ) -> dict[tuple[str, str], Strategy]:
        keys = {
            (strategy_id, strategy_version_id)
            for spec in specs
            for strategy_id, strategy_version_id in spec.strategy_versions
        }
        loaded: dict[tuple[str, str], Strategy] = {}
        connection = await self.pool.acquire()
        try:
            for strategy_id, strategy_version_id in sorted(keys):
                row = await connection.fetchrow(
                    """
                    SELECT config_json
                    FROM strategy_versions
                    WHERE strategy_id = $1 AND strategy_version_id = $2
                    """,
                    strategy_id,
                    strategy_version_id,
                )
                if row is None:
                    msg = (
                        "ParameterSweep could not load strategy version "
                        f"{strategy_id}:{strategy_version_id}"
                    )
                    raise LookupError(msg)
                loaded[(strategy_id, strategy_version_id)] = _strategy_from_config_json(
                    row["config_json"]
                )
        finally:
            await self.pool.release(connection)
        return loaded


def cache_gate_threshold() -> float:
    return _CACHE_GATE_MIN_HIT_RATE


def deserialize_backtest_spec(raw_value: object) -> BacktestSpec:
    return _deserialize_backtest_spec(raw_value)


def deserialize_execution_config(raw_value: object) -> BacktestExecutionConfig:
    return _deserialize_execution_config(raw_value)


def _market_ids(spec: BacktestSpec) -> tuple[str, ...]:
    market_ids = spec.dataset.market_universe_filter.get("market_ids", ())
    if isinstance(market_ids, (str, bytes, bytearray)):
        msg = "BacktestDataset.market_universe_filter.market_ids must be a sequence"
        raise TypeError(msg)
    if not isinstance(market_ids, Sequence):
        msg = "BacktestDataset.market_universe_filter.market_ids must be a sequence"
        raise TypeError(msg)
    return tuple(str(market_id) for market_id in market_ids)


def _set_nested_value(
    payload: dict[str, object],
    *,
    path: str,
    value: object,
) -> None:
    segments = tuple(segment for segment in path.split(".") if segment)
    if not segments:
        msg = "Parameter grid paths must not be empty"
        raise ValueError(msg)

    current: dict[str, object] = payload
    for segment in segments[:-1]:
        nested = current.get(segment)
        if not isinstance(nested, Mapping):
            msg = f"Parameter grid path {path!r} does not resolve to a mapping"
            raise KeyError(msg)
        cloned_nested = cast(dict[str, object], deepcopy(dict(nested)))
        current[segment] = cloned_nested
        current = cloned_nested
    current[segments[-1]] = deepcopy(value)


def _serialize_backtest_spec(spec: BacktestSpec) -> dict[str, object]:
    return {
        "strategy_versions": [
            [strategy_id, strategy_version_id]
            for strategy_id, strategy_version_id in spec.strategy_versions
        ],
        "dataset": {
            "source": spec.dataset.source,
            "version": spec.dataset.version,
            "coverage_start": _serialize_datetime(spec.dataset.coverage_start),
            "coverage_end": _serialize_datetime(spec.dataset.coverage_end),
            "market_universe_filter": dict(spec.dataset.market_universe_filter),
            "data_quality_gaps": [
                [_serialize_datetime(start), _serialize_datetime(end), reason]
                for start, end, reason in spec.dataset.data_quality_gaps
            ],
        },
        "execution_model": {
            "fee_rate": spec.execution_model.fee_rate,
            "slippage_bps": spec.execution_model.slippage_bps,
            "latency_ms": spec.execution_model.latency_ms,
            "staleness_ms": spec.execution_model.staleness_ms,
            "fill_policy": spec.execution_model.fill_policy,
        },
        "risk_policy": {
            "max_position_notional_usdc": spec.risk_policy.max_position_notional_usdc,
            "max_daily_drawdown_pct": spec.risk_policy.max_daily_drawdown_pct,
            "min_order_size_usdc": spec.risk_policy.min_order_size_usdc,
        },
        "date_range_start": _serialize_datetime(spec.date_range_start),
        "date_range_end": _serialize_datetime(spec.date_range_end),
    }


def _serialize_execution_config(
    exec_config: BacktestExecutionConfig,
) -> dict[str, object]:
    return {
        "chunk_days": exec_config.chunk_days,
        "time_budget": exec_config.time_budget,
    }


def _serialize_datetime(value: datetime) -> str:
    return value.isoformat()
