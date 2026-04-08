"""Trading pipeline orchestration layer.

This package wires together the Protocol implementations defined in
:mod:`pms.protocols` and runs the main sense → strategy → risk → execute
→ evaluate → feedback loop. The orchestration layer is deliberately
module-agnostic: it accepts any object conforming to the relevant
Protocol via constructor injection, and resolves concrete implementations
from a YAML config file through :class:`~pms.orchestrator.registry.ModuleRegistry`.
"""

from .config import ModuleSpec, PipelineConfig, load_config
from .pipeline import CycleReport, TradingPipeline
from .registry import ModuleRegistry

__all__ = [
    "CycleReport",
    "ModuleRegistry",
    "ModuleSpec",
    "PipelineConfig",
    "TradingPipeline",
    "load_config",
]
