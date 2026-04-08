"""Module registry — resolve dotted class paths to instances.

Given a :class:`~pms.orchestrator.config.ModuleSpec`, the registry
imports the module, looks up the class attribute, and instantiates it
with the spec's kwargs. This indirection keeps the orchestrator
decoupled from any concrete implementation module — a new connector,
strategy, or risk manager can be added by writing a class and adding an
entry to the pipeline's YAML config, with zero changes to the
orchestrator code.

The registry deliberately does not cache instances. A single ``ModuleSpec``
should yield a fresh object on every call so tests and multi-pipeline
setups do not accidentally share state.
"""

from __future__ import annotations

import importlib
from typing import Any

from pms.orchestrator.config import ModuleSpec


class ModuleRegistry:
    """Resolves ``ModuleSpec.class_path`` strings to class instances."""

    def instantiate(self, spec: ModuleSpec) -> Any:
        """Import, look up, and instantiate the class described by ``spec``.

        Args:
            spec: The :class:`ModuleSpec` to resolve.

        Returns:
            A newly constructed instance of the referenced class.

        Raises:
            ValueError: If ``spec.class_path`` has no dotted prefix
                (e.g. ``"NoModuleHere"``) — such a string cannot identify
                an importable module.
            ImportError: If the module portion of the path cannot be
                imported.
            AttributeError: If the module is importable but does not
                expose the referenced class attribute.
        """
        module_path, _, class_name = spec.class_path.rpartition(".")
        if not module_path:
            raise ValueError(
                f"Invalid class_path {spec.class_path!r}: expected a "
                f"dotted path like 'pkg.module.ClassName', but got a "
                f"bare name with no module prefix."
            )

        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise ImportError(
                f"Cannot import module {module_path!r} for spec "
                f"{spec.class_path!r}: {exc}"
            ) from exc

        try:
            cls = getattr(module, class_name)
        except AttributeError as exc:
            raise AttributeError(
                f"Module {module_path!r} has no attribute {class_name!r} "
                f"(from spec {spec.class_path!r})"
            ) from exc

        instance: Any = cls(**spec.kwargs)
        return instance
