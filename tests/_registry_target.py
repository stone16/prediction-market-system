"""A tiny plain-dataclass used as the target for ModuleRegistry tests.

Why is this in its own file rather than inside ``tests/test_pipeline.py``?

``tests/`` has no ``__init__.py`` (the other tests rely on pytest's
rootdir-based test collection), which means pytest imports
``test_pipeline.py`` under the top-level name ``test_pipeline``, not
``tests.test_pipeline``. If the ModuleRegistry test tried to resolve
``FakeRegistryTarget`` via ``tests.test_pipeline.FakeRegistryTarget``,
``importlib`` would load a *second* copy of the module in a different
namespace — the registry's instance would then not ``isinstance`` match
the class object that pytest imported for the assertion.

Putting the target in this standalone module sidesteps the duplicate-
load because ``tests._registry_target`` is imported exactly once (by
the test module's own ``from ... import`` statement) and
``importlib.import_module("tests._registry_target")`` resolves to the
same cached entry. The leading underscore keeps pytest from collecting
this file as a test module.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FakeRegistryTarget:
    """Plain dataclass target used by ModuleRegistry instantiation tests."""

    x: int
    label: str = "default"
