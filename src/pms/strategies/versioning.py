"""Deterministic strategy-version hashing.

Canonicalization uses ``json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=True)`` before hashing. The ``default=`` hook handles ``Enum``
values via ``.value`` and nested frozen dataclasses via recursive
``dataclasses.asdict(..., dict_factory=<sorted-dict>)`` normalization.

The resulting SHA-256 hex digest must remain byte-identical across Python minor-version bumps and process restarts. Any accidental drift is an Invariant 3 violation.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
from hashlib import sha256
import json
from typing import Any

from .projections import (
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


def _sorted_dict_factory(items: list[tuple[str, Any]]) -> dict[str, Any]:
    return {key: value for key, value in sorted(items)}


def _json_sort_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _normalize_value(value[key])
            for key in sorted(value)
        }
    if isinstance(value, (tuple, list)):
        normalized_items = [_normalize_value(item) for item in value]
        return sorted(normalized_items, key=_json_sort_key)
    if isinstance(value, frozenset):
        normalized_items = [_normalize_value(item) for item in value]
        return sorted(normalized_items, key=_json_sort_key)
    return value


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return _normalize_value(asdict(value, dict_factory=_sorted_dict_factory))
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def compute_strategy_version_id(
    config: StrategyConfig,
    risk: RiskParams,
    eval_spec: EvalSpec,
    forecaster: ForecasterSpec,
    market_selection: MarketSelectionSpec,
) -> str:
    canonical_payload = {
        "config": config,
        "risk": risk,
        "eval_spec": eval_spec,
        "forecaster": forecaster,
        "market_selection": market_selection,
    }
    canonical_json = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_json_default,
    )
    return sha256(canonical_json.encode("utf-8")).hexdigest()
