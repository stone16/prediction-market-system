from __future__ import annotations

from dataclasses import is_dataclass
from datetime import UTC, datetime
import importlib
import inspect
import math
import re
from typing import Any, Mapping, cast

import pytest


SOURCE_COMMENT_PATTERN = re.compile(r"^\s*#\s*source:\s+", re.MULTILINE)
LIVE_PROFILE_FIELDS = (
    "fee_rate",
    "slippage_bps",
    "latency_ms",
    "staleness_ms",
    "fill_policy",
)


def _load_execution_model() -> type[Any]:
    module = importlib.import_module("pms.research.specs")
    execution_model = getattr(module, "ExecutionModel")
    assert isinstance(execution_model, type)
    return execution_model


@pytest.mark.parametrize(
    ("notional_usdc", "fill_price", "fee_rate", "expected_fee"),
    [
        (100.0, 0.25, 0.04, 3.0),
        (100.0, 0.28, 0.04, 2.88),
        (50.0, 0.25, 0.04, 1.5),
        (50.0, 0.28, 0.04, 1.44),
        (200.0, 0.10, 0.072, 12.96),
    ],
)
def test_execution_model_compute_fee_matches_notional_formula(
    notional_usdc: float,
    fill_price: float,
    fee_rate: float,
    expected_fee: float,
) -> None:
    execution_model_cls = _load_execution_model()
    execution_model = execution_model_cls(
        fee_rate=fee_rate,
        slippage_bps=1.0,
        latency_ms=1.0,
        staleness_ms=1.0,
        fill_policy="immediate_or_cancel",
    )

    assert execution_model.compute_fee(
        notional_usdc=notional_usdc,
        fill_price=fill_price,
    ) == pytest.approx(expected_fee)


def test_execution_model_compute_fee_returns_expected_boundary_values() -> None:
    execution_model_cls = _load_execution_model()
    execution_model = execution_model_cls(
        fee_rate=0.04,
        slippage_bps=1.0,
        latency_ms=1.0,
        staleness_ms=1.0,
        fill_policy="immediate_or_cancel",
    )

    assert execution_model.compute_fee(notional_usdc=100.0, fill_price=0.0) == pytest.approx(4.0)
    assert execution_model.compute_fee(notional_usdc=100.0, fill_price=1.0) == pytest.approx(0.0)


def test_execution_model_is_frozen_dataclass() -> None:
    execution_model_cls = _load_execution_model()

    assert is_dataclass(execution_model_cls)
    assert getattr(execution_model_cls, "__dataclass_params__").frozen is True


def test_polymarket_paper_profile_is_idealized() -> None:
    execution_model_cls = _load_execution_model()

    paper = execution_model_cls.polymarket_paper()

    assert paper.fee_rate == pytest.approx(0.0)
    assert paper.slippage_bps == pytest.approx(0.0)
    assert paper.latency_ms == pytest.approx(0.0)
    assert math.isinf(paper.staleness_ms)
    assert paper.fill_policy == "immediate_or_cancel"


def test_polymarket_live_estimate_uses_representative_costs() -> None:
    execution_model_cls = _load_execution_model()

    live_estimate = execution_model_cls.polymarket_live_estimate()

    assert live_estimate.fee_rate == pytest.approx(0.04)
    assert live_estimate.slippage_bps > 0.0
    assert live_estimate.latency_ms > 0.0
    assert live_estimate.staleness_ms > 0.0
    assert live_estimate.fill_policy == "immediate_or_cancel"


def test_polymarket_live_estimate_assignments_have_source_comments() -> None:
    execution_model_cls = _load_execution_model()

    live_profile_source = inspect.getsource(execution_model_cls.polymarket_live_estimate)
    live_profile_lines = live_profile_source.splitlines()

    for field_name in LIVE_PROFILE_FIELDS:
        assignment_index = next(
            index
            for index, line in enumerate(live_profile_lines)
            if re.match(rf"^\s*{re.escape(field_name)}\s*=", line)
        )
        comment_window = live_profile_lines[max(0, assignment_index - 4) : assignment_index]
        assert any(
            SOURCE_COMMENT_PATTERN.match(line) for line in comment_window
        ), field_name


def test_paper_profile_round_trips_through_spec_codec_with_new_fields() -> None:
    from pms.research.spec_codec import deserialize_backtest_spec, serialize_backtest_spec
    from pms.research.specs import BacktestDataset, BacktestSpec, ExecutionModel
    from pms.strategies.projections import RiskParams

    spec = BacktestSpec(
        strategy_versions=(("alpha", "alpha-v1"),),
        dataset=BacktestDataset(
            source="fixture",
            version="v1",
            coverage_start=datetime(2026, 4, 1, tzinfo=UTC),
            coverage_end=datetime(2026, 4, 30, tzinfo=UTC),
            market_universe_filter={"market_ids": ["market-a"]},
            data_quality_gaps=(),
        ),
        execution_model=ExecutionModel.polymarket_paper(),
        risk_policy=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        date_range_start=datetime(2026, 4, 1, tzinfo=UTC),
        date_range_end=datetime(2026, 4, 30, tzinfo=UTC),
    )

    payload = serialize_backtest_spec(spec)
    execution_model_payload = cast(Mapping[str, object], payload["execution_model"])

    assert execution_model_payload["staleness_ms"] == ".inf"
    assert execution_model_payload["order_ttl_ms"] == 60_000
    assert execution_model_payload["price_invalidation_streak"] == 10
    assert execution_model_payload["replay_window_ms"] == 86_400_000
    assert len(spec.config_hash) == 64

    decoded = deserialize_backtest_spec(payload)

    assert math.isinf(decoded.execution_model.staleness_ms)
    assert decoded.execution_model.order_ttl_ms == 60_000
    assert decoded.execution_model.price_invalidation_streak == 10
    assert decoded.execution_model.replay_window_ms == 86_400_000
