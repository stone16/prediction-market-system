from __future__ import annotations

from dataclasses import FrozenInstanceError, is_dataclass
from datetime import UTC, datetime
from inspect import Parameter, signature

import pytest

from pms.research.comparison import BacktestLiveComparison, BacktestLiveComparisonTool
from pms.research.policies import SymbolNormalizationPolicy, TimeAlignmentPolicy


def test_backtest_live_comparison_types_are_frozen_dataclasses() -> None:
    assert is_dataclass(BacktestLiveComparison)
    assert is_dataclass(BacktestLiveComparisonTool)

    comparison = BacktestLiveComparison(
        comparison_id="cmp-1",
        run_id="run-1",
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        live_window_start=datetime(2026, 4, 10, 0, 0, tzinfo=UTC),
        live_window_end=datetime(2026, 4, 12, 23, 59, 59, tzinfo=UTC),
        denominator="union",
        equity_delta_json=(),
        overlap_ratio=0.25,
        backtest_only_symbols=("market-a::token-a",),
        live_only_symbols=("market-c::token-c",),
        time_alignment_policy_json={},
        symbol_normalization_policy_json={},
        computed_at=datetime(2026, 4, 12, 0, 0, tzinfo=UTC),
    )

    with pytest.raises(FrozenInstanceError):
        comparison.overlap_ratio = 0.5  # type: ignore[misc]


def test_backtest_live_comparison_tool_requires_explicit_policy_args() -> None:
    parameters = signature(BacktestLiveComparisonTool).parameters

    assert parameters["pool"].default is Parameter.empty
    assert parameters["time_alignment_policy"].default is Parameter.empty
    assert parameters["symbol_normalization_policy"].default is Parameter.empty

    tool = BacktestLiveComparisonTool(
        pool=object(),
        time_alignment_policy=TimeAlignmentPolicy(),
        symbol_normalization_policy=SymbolNormalizationPolicy(),
    )

    assert tool.time_alignment_policy == TimeAlignmentPolicy()
    assert tool.symbol_normalization_policy == SymbolNormalizationPolicy()
