from __future__ import annotations

from dataclasses import dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
import importlib
from typing import Any, Callable

import pytest


UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class DummyExecutionModel:
    profile: str
    fee_rate: float
    latency_ms: float


def _load_symbol(module_name: str, symbol_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in red phase
        pytest.fail(f"{module_name} is missing: {exc}")

    return getattr(module, symbol_name)


def _build_dataset(
    *,
    version: str = "outer-ring-v1",
    market_universe_filter: dict[str, object] | None = None,
) -> Any:
    dataset_cls = _load_symbol("pms.research.specs", "BacktestDataset")
    assert callable(dataset_cls)
    return dataset_cls(
        source="postgresql",
        version=version,
        coverage_start=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        coverage_end=datetime(2026, 3, 31, 23, 59, tzinfo=UTC),
        market_universe_filter=market_universe_filter
        or {"venue": "polymarket", "min_volume_usdc": 500.0},
        data_quality_gaps=(
            (
                datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
                datetime(2026, 3, 10, 6, 0, tzinfo=UTC),
                "websocket-replay-gap",
            ),
        ),
    )


def _build_risk_policy(*, max_daily_drawdown_pct: float = 2.5) -> Any:
    risk_policy_cls = _load_symbol("pms.research.specs", "RiskPolicy")
    assert callable(risk_policy_cls)
    return risk_policy_cls(
        max_position_notional_usdc=100.0,
        max_daily_drawdown_pct=max_daily_drawdown_pct,
        min_order_size_usdc=1.0,
    )


def _build_spec(
    *,
    strategy_versions: tuple[tuple[str, str], ...] = (("alpha", "v1"), ("beta", "v2")),
    dataset: object | None = None,
    execution_model: object | None = None,
    risk_policy: object | None = None,
    date_range_start: datetime = datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
    date_range_end: datetime = datetime(2026, 3, 31, 23, 59, tzinfo=UTC),
) -> Any:
    spec_cls = _load_symbol("pms.research.specs", "BacktestSpec")
    assert callable(spec_cls)
    return spec_cls(
        strategy_versions=strategy_versions,
        dataset=dataset or _build_dataset(),
        execution_model=execution_model or DummyExecutionModel(
            profile="paper",
            fee_rate=0.0,
            latency_ms=0.0,
        ),
        risk_policy=risk_policy or _build_risk_policy(),
        date_range_start=date_range_start,
        date_range_end=date_range_end,
    )


def test_research_package_exposes_cp01a_modules() -> None:
    research_module = importlib.import_module("pms.research")
    entities_module = importlib.import_module("pms.research.entities")

    assert hasattr(research_module, "BacktestSpec")
    assert hasattr(research_module, "BacktestExecutionConfig")
    assert hasattr(research_module, "BacktestDataset")
    assert entities_module.__doc__ is not None


def test_cp01a_dataclasses_are_frozen_and_execution_config_defaults_are_stable() -> None:
    dataset_cls = _load_symbol("pms.research.specs", "BacktestDataset")
    spec_cls = _load_symbol("pms.research.specs", "BacktestSpec")
    config_cls = _load_symbol("pms.research.specs", "BacktestExecutionConfig")

    for cls in (dataset_cls, spec_cls, config_cls):
        assert isinstance(cls, type)
        assert is_dataclass(cls)
        assert getattr(cls, "__dataclass_params__").frozen is True

    exec_config = config_cls()
    assert exec_config.chunk_days == 7
    assert exec_config.time_budget == 1800


def test_backtest_spec_config_hash_is_deterministic_for_equivalent_inputs() -> None:
    dataset = _build_dataset(
        market_universe_filter={"min_volume_usdc": 500.0, "venue": "polymarket"},
    )
    execution_model = DummyExecutionModel(profile="paper", fee_rate=0.0, latency_ms=0.0)
    risk_policy = _build_risk_policy()

    spec_one = _build_spec(
        strategy_versions=(("alpha", "v1"), ("beta", "v2")),
        dataset=dataset,
        execution_model=execution_model,
        risk_policy=risk_policy,
    )
    spec_two = _build_spec(
        risk_policy=risk_policy,
        execution_model=execution_model,
        dataset=_build_dataset(
            market_universe_filter={"venue": "polymarket", "min_volume_usdc": 500.0},
        ),
        strategy_versions=(("beta", "v2"), ("alpha", "v1")),
    )

    assert spec_one.config_hash == spec_two.config_hash
    assert len(spec_one.config_hash) == 64


@pytest.mark.parametrize(
    ("field_name", "variant_spec"),
    [
        (
            "strategy_versions",
            lambda: _build_spec(strategy_versions=(("alpha", "v1"), ("gamma", "v1"))),
        ),
        (
            "dataset",
            lambda: _build_spec(dataset=_build_dataset(version="outer-ring-v2")),
        ),
        (
            "execution_model",
            lambda: _build_spec(
                execution_model=DummyExecutionModel(
                    profile="live-estimate",
                    fee_rate=0.04,
                    latency_ms=250.0,
                )
            ),
        ),
        (
            "risk_policy",
            lambda: _build_spec(risk_policy=_build_risk_policy(max_daily_drawdown_pct=4.0)),
        ),
        (
            "date_range_start",
            lambda: _build_spec(date_range_start=datetime(2026, 3, 2, 0, 0, tzinfo=UTC)),
        ),
        (
            "date_range_end",
            lambda: _build_spec(date_range_end=datetime(2026, 4, 1, 23, 59, tzinfo=UTC)),
        ),
    ],
)
def test_backtest_spec_config_hash_changes_when_any_direct_field_changes(
    field_name: str,
    variant_spec: Callable[[], Any],
) -> None:
    baseline = _build_spec()

    assert variant_spec().config_hash != baseline.config_hash, field_name


def test_backtest_spec_config_hash_changes_when_nested_dataset_field_changes() -> None:
    baseline = _build_spec()
    variant = _build_spec(
        dataset=_build_dataset(
            market_universe_filter={
                "venue": "polymarket",
                "min_volume_usdc": 500.0,
                "resolution_horizon_days": 7,
            }
        )
    )

    assert variant.config_hash != baseline.config_hash


def test_backtest_execution_config_differences_do_not_affect_spec_hash() -> None:
    config_cls = _load_symbol("pms.research.specs", "BacktestExecutionConfig")
    assert callable(config_cls)

    baseline = _build_spec()
    short_chunks = config_cls(chunk_days=7, time_budget=1800)
    wide_chunks = config_cls(chunk_days=14, time_budget=900)

    assert short_chunks != wide_chunks
    assert baseline.config_hash == _build_spec().config_hash
    assert baseline.config_hash == _build_spec(
        date_range_end=datetime(2026, 3, 31, 23, 59, tzinfo=UTC)
    ).config_hash


def test_backtest_spec_requires_timezone_aware_datetimes_for_hashing() -> None:
    spec_cls = _load_symbol("pms.research.specs", "BacktestSpec")
    assert callable(spec_cls)

    with pytest.raises(ValueError, match="timezone-aware"):
        spec_cls(
            strategy_versions=(("alpha", "v1"),),
            dataset=_build_dataset(),
            execution_model=DummyExecutionModel(
                profile="paper",
                fee_rate=0.0,
                latency_ms=0.0,
            ),
            risk_policy=_build_risk_policy(),
            date_range_start=datetime(2026, 3, 1, 0, 0),
            date_range_end=datetime(2026, 3, 1, 0, 0, tzinfo=UTC) + timedelta(days=1),
        )
