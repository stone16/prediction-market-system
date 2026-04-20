"""Backtest/live comparison tooling for research workflows."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import json
from typing import Any, cast
from uuid import uuid4

import asyncpg

from pms.research.entities import deserialize_portfolio_target_json
from pms.research.policies import (
    SelectionDenominator,
    SelectionSimilarityMetric,
    SymbolNormalizationPolicy,
    TimeAlignmentPolicy,
)


@dataclass(frozen=True, slots=True)
class BacktestLiveComparison:
    comparison_id: str
    run_id: str
    strategy_id: str
    strategy_version_id: str
    live_window_start: datetime
    live_window_end: datetime
    denominator: SelectionDenominator
    equity_delta_json: tuple[Mapping[str, object], ...]
    overlap_ratio: float
    backtest_only_symbols: tuple[str, ...]
    live_only_symbols: tuple[str, ...]
    time_alignment_policy_json: Mapping[str, object]
    symbol_normalization_policy_json: Mapping[str, object]
    computed_at: datetime

    def __post_init__(self) -> None:
        if not self.strategy_id:
            msg = "BacktestLiveComparison.strategy_id must be non-empty"
            raise ValueError(msg)
        if not self.strategy_version_id:
            msg = "BacktestLiveComparison.strategy_version_id must be non-empty"
            raise ValueError(msg)
        for field_name in ("live_window_start", "live_window_end", "computed_at"):
            timestamp = getattr(self, field_name)
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                msg = f"BacktestLiveComparison.{field_name} must be timezone-aware"
                raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class BacktestLiveComparisonStore:
    pool: asyncpg.Pool

    async def insert(self, comparison: BacktestLiveComparison) -> BacktestLiveComparison:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO backtest_live_comparisons (
                    comparison_id,
                    run_id,
                    strategy_id,
                    strategy_version_id,
                    live_window_start,
                    live_window_end,
                    denominator,
                    equity_delta_json,
                    overlap_ratio,
                    backtest_only_symbols,
                    live_only_symbols,
                    time_alignment_policy_json,
                    symbol_normalization_policy_json,
                    computed_at
                ) VALUES (
                    $1::uuid,
                    $2::uuid,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8::jsonb,
                    $9,
                    $10::text[],
                    $11::text[],
                    $12::jsonb,
                    $13::jsonb,
                    $14
                )
                RETURNING
                    comparison_id,
                    run_id,
                    strategy_id,
                    strategy_version_id,
                    live_window_start,
                    live_window_end,
                    denominator,
                    equity_delta_json,
                    overlap_ratio,
                    backtest_only_symbols,
                    live_only_symbols,
                    time_alignment_policy_json,
                    symbol_normalization_policy_json,
                    computed_at
                """,
                comparison.comparison_id,
                comparison.run_id,
                comparison.strategy_id,
                comparison.strategy_version_id,
                comparison.live_window_start,
                comparison.live_window_end,
                comparison.denominator,
                json.dumps(
                    [dict(entry) for entry in comparison.equity_delta_json],
                    separators=(",", ":"),
                    ensure_ascii=True,
                ),
                comparison.overlap_ratio,
                list(comparison.backtest_only_symbols),
                list(comparison.live_only_symbols),
                json.dumps(
                    dict(comparison.time_alignment_policy_json),
                    separators=(",", ":"),
                    ensure_ascii=True,
                ),
                json.dumps(
                    dict(comparison.symbol_normalization_policy_json),
                    separators=(",", ":"),
                    ensure_ascii=True,
                ),
                comparison.computed_at,
            )
        assert row is not None
        return _row_to_backtest_live_comparison(row)


@dataclass(frozen=True, slots=True)
class BacktestLiveComparisonTool:
    pool: asyncpg.Pool
    time_alignment_policy: TimeAlignmentPolicy
    symbol_normalization_policy: SymbolNormalizationPolicy

    async def compute(
        self,
        *,
        run_id: str,
        strategy_id: str,
        strategy_version_id: str,
        live_window_start: datetime,
        live_window_end: datetime,
        denominator: SelectionDenominator,
    ) -> BacktestLiveComparison:
        if live_window_start.tzinfo is None or live_window_start.utcoffset() is None:
            msg = "live_window_start must be timezone-aware"
            raise ValueError(msg)
        if live_window_end.tzinfo is None or live_window_end.utcoffset() is None:
            msg = "live_window_end must be timezone-aware"
            raise ValueError(msg)
        if live_window_end < live_window_start:
            msg = "live_window_end must be greater than or equal to live_window_start"
            raise ValueError(msg)

        metric = SelectionSimilarityMetric(denominator=denominator)
        padded_window_start, padded_window_end = _padded_window(
            live_window_start,
            live_window_end,
            self.time_alignment_policy,
        )

        async with self.pool.acquire() as connection:
            strategy_row = await connection.fetchrow(
                """
                SELECT pnl_cum, portfolio_target_json
                FROM strategy_runs
                WHERE run_id = $1::uuid
                  AND strategy_id = $2
                  AND strategy_version_id = $3
                """,
                run_id,
                strategy_id,
                strategy_version_id,
            )
            if strategy_row is None:
                msg = (
                    "BacktestLiveComparisonTool could not find strategy run "
                    f"{run_id}:{strategy_id}:{strategy_version_id}"
                )
                raise LookupError(msg)

            eval_rows = await connection.fetch(
                """
                SELECT recorded_at, pnl
                FROM eval_records
                WHERE strategy_id = $1
                  AND strategy_version_id = $2
                  AND recorded_at BETWEEN $3 AND $4
                ORDER BY recorded_at ASC
                """,
                strategy_id,
                strategy_version_id,
                padded_window_start,
                padded_window_end,
            )
            opportunity_rows = await connection.fetch(
                """
                SELECT market_id, token_id, created_at
                FROM opportunities
                WHERE strategy_id = $1
                  AND strategy_version_id = $2
                  AND created_at BETWEEN $3 AND $4
                ORDER BY created_at ASC
                """,
                strategy_id,
                strategy_version_id,
                padded_window_start,
                padded_window_end,
            )
        backtest_symbols = _backtest_symbols(
            raw_portfolio_target=strategy_row["portfolio_target_json"],
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            normalization_policy=self.symbol_normalization_policy,
        )
        live_symbols = _live_symbols(
            opportunity_rows=opportunity_rows,
            window_start=live_window_start,
            window_end=live_window_end,
            time_alignment_policy=self.time_alignment_policy,
            normalization_policy=self.symbol_normalization_policy,
        )
        backtest_equity = float(cast(float | None, strategy_row["pnl_cum"]) or 0.0)
        comparison = BacktestLiveComparison(
            comparison_id=str(uuid4()),
            run_id=run_id,
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            live_window_start=live_window_start,
            live_window_end=live_window_end,
            denominator=denominator,
            equity_delta_json=_equity_delta_rows(
                backtest_equity=backtest_equity,
                eval_rows=eval_rows,
                window_start=live_window_start,
                window_end=live_window_end,
                time_alignment_policy=self.time_alignment_policy,
            ),
            overlap_ratio=metric.compute(backtest_symbols, live_symbols),
            backtest_only_symbols=tuple(sorted(backtest_symbols - live_symbols)),
            live_only_symbols=tuple(sorted(live_symbols - backtest_symbols)),
            time_alignment_policy_json=_time_alignment_policy_json(self.time_alignment_policy),
            symbol_normalization_policy_json=_symbol_normalization_policy_json(
                self.symbol_normalization_policy
            ),
            computed_at=datetime.now(tz=UTC),
        )
        saved = await BacktestLiveComparisonStore(self.pool).insert(comparison)
        warning = _policy_warning(
            time_alignment_policy=self.time_alignment_policy,
            symbol_normalization_policy=self.symbol_normalization_policy,
        )
        if warning is not None:
            await _append_warning(
                self.pool,
                run_id=run_id,
                ranking_metric="pnl_cum",
                warning=warning,
            )
        return saved


def _backtest_symbols(
    *,
    raw_portfolio_target: object,
    strategy_id: str,
    strategy_version_id: str,
    normalization_policy: SymbolNormalizationPolicy,
) -> frozenset[str]:
    if raw_portfolio_target is None:
        return frozenset()
    portfolio_target = deserialize_portfolio_target_json(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        raw_value=raw_portfolio_target,
    )
    symbols = {
        _normalized_symbol(
            market_id=market_id,
            token_id=token_id,
            normalization_policy=normalization_policy,
        )
        for market_id, token_id, _side, _timestamp in portfolio_target.targets
    }
    return frozenset(symbols)


def _live_symbols(
    *,
    opportunity_rows: Sequence[asyncpg.Record],
    window_start: datetime,
    window_end: datetime,
    time_alignment_policy: TimeAlignmentPolicy,
    normalization_policy: SymbolNormalizationPolicy,
) -> frozenset[str]:
    symbols: set[str] = set()
    for row in opportunity_rows:
        shifted_timestamp = time_alignment_policy.apply_generated(cast(datetime, row["created_at"]))
        if not _within_window(shifted_timestamp, window_start, window_end):
            continue
        symbols.add(
            _normalized_symbol(
                market_id=cast(str, row["market_id"]),
                token_id=cast(str, row["token_id"]),
                normalization_policy=normalization_policy,
            )
        )
    return frozenset(symbols)


def _equity_delta_rows(
    *,
    backtest_equity: float,
    eval_rows: Sequence[asyncpg.Record],
    window_start: datetime,
    window_end: datetime,
    time_alignment_policy: TimeAlignmentPolicy,
) -> tuple[Mapping[str, object], ...]:
    daily_live_increments: dict[date, float] = defaultdict(float)
    for row in eval_rows:
        shifted_timestamp = time_alignment_policy.apply_evaluation(
            cast(datetime, row["recorded_at"])
        )
        if not _within_window(shifted_timestamp, window_start, window_end):
            continue
        pnl = row["pnl"]
        if isinstance(pnl, (int, float)) and not isinstance(pnl, bool):
            daily_live_increments[shifted_timestamp.date()] += float(pnl)

    live_equity = 0.0
    rows: list[Mapping[str, object]] = []
    for day in _days_in_window(window_start, window_end):
        live_equity += daily_live_increments.get(day, 0.0)
        rows.append(
            {
                "day": day.isoformat(),
                "backtest_equity": backtest_equity,
                "live_equity": live_equity,
                "delta": backtest_equity - live_equity,
            }
        )
    return tuple(rows)


def _normalized_symbol(
    *,
    market_id: str,
    token_id: str,
    normalization_policy: SymbolNormalizationPolicy,
) -> str:
    normalized_market = normalization_policy.normalize_market_id(market_id)
    normalized_token = normalization_policy.normalize_token_id(token_id)
    return f"{normalized_market}::{normalized_token}"


def _days_in_window(window_start: datetime, window_end: datetime) -> tuple[date, ...]:
    current_day = window_start.date()
    end_day = window_end.date()
    days: list[date] = []
    while current_day <= end_day:
        days.append(current_day)
        current_day += timedelta(days=1)
    return tuple(days)


def _within_window(timestamp: datetime, window_start: datetime, window_end: datetime) -> bool:
    return window_start <= timestamp <= window_end


def _padded_window(
    window_start: datetime,
    window_end: datetime,
    policy: TimeAlignmentPolicy,
) -> tuple[datetime, datetime]:
    max_offset_s = max(
        abs(policy.generated_offset_s),
        abs(policy.exchange_offset_s),
        abs(policy.ingest_offset_s),
        abs(policy.evaluation_offset_s),
    )
    padding = timedelta(seconds=max_offset_s)
    return (window_start - padding, window_end + padding)


def _time_alignment_policy_json(policy: TimeAlignmentPolicy) -> Mapping[str, object]:
    return {
        "generated_offset_s": policy.generated_offset_s,
        "exchange_offset_s": policy.exchange_offset_s,
        "ingest_offset_s": policy.ingest_offset_s,
        "evaluation_offset_s": policy.evaluation_offset_s,
    }


def _symbol_normalization_policy_json(
    policy: SymbolNormalizationPolicy,
) -> Mapping[str, object]:
    return {
        "token_id_aliases": dict(policy.token_id_aliases),
        "market_id_aliases": dict(policy.market_id_aliases),
    }


def _policy_warning(
    *,
    time_alignment_policy: TimeAlignmentPolicy,
    symbol_normalization_policy: SymbolNormalizationPolicy,
) -> str | None:
    identity_time = TimeAlignmentPolicy()
    identity_symbol = SymbolNormalizationPolicy()
    if (
        time_alignment_policy == identity_time
        and symbol_normalization_policy == identity_symbol
    ):
        return None
    return (
        "non-identity comparison policy applied: "
        f"time_alignment={json.dumps(_time_alignment_policy_json(time_alignment_policy), sort_keys=True)}; "
        "symbol_normalization="
        f"{json.dumps(_symbol_normalization_policy_json(symbol_normalization_policy), sort_keys=True)}"
    )


async def _append_warning(
    pool: asyncpg.Pool,
    *,
    run_id: str,
    ranking_metric: str,
    warning: str,
) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            UPDATE evaluation_reports
            SET warnings = warnings || $2::jsonb
            WHERE run_id = $1::uuid
              AND ranking_metric = $3
              AND NOT (warnings @> $2::jsonb)
            """,
            run_id,
            json.dumps([warning], separators=(",", ":"), ensure_ascii=True),
            ranking_metric,
        )


def _json_array(raw_value: object) -> list[object]:
    decoded = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    if not isinstance(decoded, list):
        return []
    return list(decoded)


def _row_to_backtest_live_comparison(row: asyncpg.Record) -> BacktestLiveComparison:
    return BacktestLiveComparison(
        comparison_id=str(cast(object, row["comparison_id"])),
        run_id=str(cast(object, row["run_id"])),
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        live_window_start=cast(datetime, row["live_window_start"]),
        live_window_end=cast(datetime, row["live_window_end"]),
        denominator=cast(SelectionDenominator, row["denominator"]),
        equity_delta_json=_deserialize_equity_delta_json(row["equity_delta_json"]),
        overlap_ratio=float(cast(float, row["overlap_ratio"])),
        backtest_only_symbols=tuple(cast(list[str], row["backtest_only_symbols"])),
        live_only_symbols=tuple(cast(list[str], row["live_only_symbols"])),
        time_alignment_policy_json=_json_object(row["time_alignment_policy_json"]),
        symbol_normalization_policy_json=_json_object(row["symbol_normalization_policy_json"]),
        computed_at=cast(datetime, row["computed_at"]),
    )


def _deserialize_equity_delta_json(raw_value: object) -> tuple[Mapping[str, object], ...]:
    decoded = _json_array(raw_value)
    rows: list[Mapping[str, object]] = []
    for item in decoded:
        if isinstance(item, Mapping):
            rows.append(cast(Mapping[str, object], item))
    return tuple(rows)


def _json_object(raw_value: object) -> Mapping[str, object]:
    decoded = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    if not isinstance(decoded, Mapping):
        return {}
    return cast(Mapping[str, object], decoded)


__all__ = [
    "BacktestLiveComparison",
    "BacktestLiveComparisonStore",
    "BacktestLiveComparisonTool",
]
