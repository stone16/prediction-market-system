from __future__ import annotations

from dataclasses import is_dataclass
import importlib
import inspect
import math
import re
from typing import Any

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
    ("price", "shares", "fee_rate", "expected_fee"),
    [
        (0.5, 100.0, 0.04, 1.0),
        (0.25, 80.0, 0.04, 0.6),
        (0.9, 50.0, 0.03, 0.135),
        (0.1, 200.0, 0.072, 1.296),
        (0.5, 1.0, 0.05, 0.0125),
    ],
)
def test_execution_model_fee_curve_matches_polymarket_formula(
    price: float,
    shares: float,
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

    assert execution_model.fee_curve(price=price, shares=shares) == pytest.approx(
        expected_fee
    )


def test_execution_model_is_frozen_dataclass() -> None:
    execution_model_cls = _load_execution_model()

    assert is_dataclass(execution_model_cls)
    assert getattr(execution_model_cls, "__dataclass_params__").frozen is True


def test_execution_model_fee_curve_returns_zero_at_probability_boundaries() -> None:
    execution_model_cls = _load_execution_model()
    execution_model = execution_model_cls(
        fee_rate=0.04,
        slippage_bps=1.0,
        latency_ms=1.0,
        staleness_ms=1.0,
        fill_policy="immediate_or_cancel",
    )

    assert execution_model.fee_curve(price=0.0, shares=100.0) == pytest.approx(0.0)
    assert execution_model.fee_curve(price=1.0, shares=100.0) == pytest.approx(0.0)


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
