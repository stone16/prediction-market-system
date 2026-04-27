from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "alembic"
    / "versions"
    / "0011_live_reconcile_audit.py"
)


def _load_migration_module() -> ModuleType:
    assert MIGRATION_PATH.exists(), f"migration file missing: {MIGRATION_PATH}"
    module_name = "test_alembic_0011_live_reconcile_audit"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_live_reconcile_audit_migration_revision_metadata_fits_alembic_table() -> None:
    module = _load_migration_module()

    assert module.revision == "0011_live_reconcile_audit"
    assert len(module.revision) <= 32
    assert module.down_revision == "0010_order_intent_key"
    assert callable(module.upgrade)
    assert callable(module.downgrade)
