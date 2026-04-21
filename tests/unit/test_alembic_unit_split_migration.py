from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "alembic"
    / "versions"
    / "0002_unit_split.py"
)


def _load_migration_module() -> ModuleType:
    assert MIGRATION_PATH.exists(), f"migration file missing: {MIGRATION_PATH}"
    module_name = "test_alembic_0002_unit_split"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_unit_split_migration_declares_expected_revision_metadata() -> None:
    module = _load_migration_module()

    assert module.revision == "0002_unit_split"
    assert module.down_revision == "0001_baseline"
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_unit_split_migration_docstring_mentions_shell_and_legacy_paths() -> None:
    module = _load_migration_module()

    assert module.__doc__ is not None
    assert "shell-table baseline" in module.__doc__
    assert "filled_contracts" in module.__doc__
    assert "100k" in module.__doc__
