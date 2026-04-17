from __future__ import annotations

import importlib
from dataclasses import FrozenInstanceError, fields, is_dataclass
from typing import Any, get_args, get_origin, get_type_hints

import pytest


PROJECTION_NAMES = [
    "StrategyConfig",
    "RiskParams",
    "EvalSpec",
    "ForecasterSpec",
    "MarketSelectionSpec",
]

EXPECTED_FIELD_TYPES: dict[str, dict[str, object]] = {
    "StrategyConfig": {
        "strategy_id": str,
        "factor_composition": tuple[tuple[str, float], ...],
        "metadata": tuple[tuple[str, str], ...],
    },
    "RiskParams": {
        "max_position_notional_usdc": float,
        "max_daily_drawdown_pct": float,
        "min_order_size_usdc": float,
    },
    "EvalSpec": {
        "metrics": tuple[str, ...],
    },
    "ForecasterSpec": {
        "forecasters": tuple[tuple[str, tuple[tuple[str, str], ...]], ...],
    },
    "MarketSelectionSpec": {
        "venue": str,
        "resolution_time_max_horizon_days": int | None,
        "volume_min_usdc": float,
    },
}


def _load_projections_module() -> Any:
    try:
        return importlib.import_module("pms.strategies.projections")
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in red phase
        pytest.fail(f"pms.strategies.projections is missing: {exc}")


def _sample_projection_values() -> dict[str, object]:
    module = _load_projections_module()
    return {
        "StrategyConfig": module.StrategyConfig(
            strategy_id="default",
            factor_composition=(("factor-a", 0.6), ("factor-b", 0.4)),
            metadata=(("owner", "system"), ("tier", "default")),
        ),
        "RiskParams": module.RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=5.0,
            min_order_size_usdc=1.0,
        ),
        "EvalSpec": module.EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        "ForecasterSpec": module.ForecasterSpec(
            forecasters=(
                ("rules", (("threshold", "0.55"),)),
                ("stats", (("window", "15m"),)),
            )
        ),
        "MarketSelectionSpec": module.MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    }


def _assert_no_mutable_containers(type_hint: object) -> None:
    origin = get_origin(type_hint)
    if type_hint in {list, dict, set} or origin in {list, dict, set}:
        pytest.fail(f"mutable container type is not allowed: {type_hint!r}")

    if origin is None:
        return

    for nested in get_args(type_hint):
        if nested is type(None):
            continue
        _assert_no_mutable_containers(nested)


def test_strategy_projection_dataclasses_are_frozen_slotted_and_ordered() -> None:
    module = _load_projections_module()

    projection_names = [
        name for name in module.__dict__ if name in PROJECTION_NAMES and is_dataclass(module.__dict__[name])
    ]

    assert projection_names == PROJECTION_NAMES

    for projection_name in projection_names:
        projection = getattr(module, projection_name)
        params = projection.__dataclass_params__

        assert params.frozen
        assert hasattr(projection, "__slots__")


def test_strategy_projection_field_types_match_checkpoint_spec() -> None:
    module = _load_projections_module()

    for projection_name, expected_fields in EXPECTED_FIELD_TYPES.items():
        projection = getattr(module, projection_name)
        hints = get_type_hints(projection)

        assert [field.name for field in fields(projection)] == list(expected_fields)
        for field_name, expected_type in expected_fields.items():
            assert hints[field_name] == expected_type


def test_strategy_projection_field_types_exclude_mutable_collections() -> None:
    module = _load_projections_module()

    for projection_name in PROJECTION_NAMES:
        projection = getattr(module, projection_name)
        hints = get_type_hints(projection)

        for field_name in [field.name for field in fields(projection)]:
            _assert_no_mutable_containers(hints[field_name])


def test_strategy_projections_are_immutable_instances() -> None:
    projection_instances = _sample_projection_values()

    for projection_name, projection in projection_instances.items():
        first_field = fields(type(projection))[0].name
        replacement_value = "mutated" if projection_name != "RiskParams" else 999.0

        with pytest.raises(FrozenInstanceError):
            setattr(projection, first_field, replacement_value)
