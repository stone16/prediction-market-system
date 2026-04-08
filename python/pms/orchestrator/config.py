"""Pipeline config loader.

A pipeline is specified by a YAML file that lists the concrete class to
use for each pluggable module — one or more connectors, zero or more
strategies, and a singleton executor, risk manager, metrics collector,
and feedback engine. Each entry is a ``ModuleSpec`` with:

- ``class`` — a fully-qualified dotted class path, e.g.
  ``pms.connectors.polymarket.PolymarketConnector``
- ``kwargs`` — a dict of keyword arguments forwarded to the class
  constructor by :class:`~pms.orchestrator.registry.ModuleRegistry`.

See ``config.yaml.example`` at the repo root for a fully worked example.

This module intentionally does **not** resolve classes to instances —
that is the job of :class:`~pms.orchestrator.registry.ModuleRegistry` so
that the config loader stays a pure, deterministic transformation of
YAML text into immutable dataclasses and can be unit-tested without
importing any concrete implementation module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class ModuleSpec:
    """Specification of one module implementation to instantiate.

    Attributes:
        class_path: Fully-qualified dotted class path.
        kwargs: Keyword arguments to forward to the class constructor.
    """

    class_path: str
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved pipeline configuration loaded from a YAML file.

    All collection fields are kept as plain ``list[ModuleSpec]`` rather
    than tuples so the frozen dataclass contract (no field mutation) is
    still enforced at the dataclass level while keeping the internal
    structure convenient for downstream iteration. Downstream code should
    treat these lists as read-only.
    """

    connectors: list[ModuleSpec]
    strategies: list[ModuleSpec]
    executor: ModuleSpec
    risk_manager: ModuleSpec
    metrics: ModuleSpec
    feedback_engine: ModuleSpec


def load_config(path: Path) -> PipelineConfig:
    """Parse ``path`` as a pipeline config YAML and return a ``PipelineConfig``.

    Args:
        path: Path to a YAML file on disk.

    Returns:
        A fully populated ``PipelineConfig``.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        KeyError: If a required top-level section is missing.
        yaml.YAMLError: If the file is not valid YAML.
    """
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError(
            f"Pipeline config at {path} must be a YAML mapping, got "
            f"{type(data).__name__}"
        )

    return PipelineConfig(
        connectors=[
            _parse_module_spec(item)
            for item in (data.get("connectors") or [])
        ],
        strategies=[
            _parse_module_spec(item)
            for item in (data.get("strategies") or [])
        ],
        executor=_parse_module_spec(data["executor"]),
        risk_manager=_parse_module_spec(data["risk_manager"]),
        metrics=_parse_module_spec(data["metrics"]),
        feedback_engine=_parse_module_spec(data["feedback_engine"]),
    )


def _parse_module_spec(raw: Any) -> ModuleSpec:
    """Validate and convert a raw dict into a ``ModuleSpec``."""
    if not isinstance(raw, dict):
        raise ValueError(
            f"Module spec must be a mapping, got {type(raw).__name__}: {raw!r}"
        )
    if "class" not in raw:
        raise KeyError(f"Module spec is missing required key 'class': {raw!r}")

    class_path = raw["class"]
    if not isinstance(class_path, str):
        raise ValueError(
            f"Module spec 'class' must be a string, got "
            f"{type(class_path).__name__}: {class_path!r}"
        )

    kwargs_raw = raw.get("kwargs") or {}
    if not isinstance(kwargs_raw, dict):
        raise ValueError(
            f"Module spec 'kwargs' must be a mapping, got "
            f"{type(kwargs_raw).__name__}: {kwargs_raw!r}"
        )
    # Narrow for mypy strict: YAML keys are always strings in our schema.
    kwargs: dict[str, Any] = {str(k): v for k, v in kwargs_raw.items()}

    return ModuleSpec(class_path=class_path, kwargs=kwargs)
