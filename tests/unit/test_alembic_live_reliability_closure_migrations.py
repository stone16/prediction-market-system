from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[2]


def _load_migration(filename: str) -> ModuleType:
    path = ROOT / "alembic" / "versions" / filename
    assert path.exists(), f"migration file missing: {path}"
    module_name = f"test_alembic_{filename.replace('.', '_')}"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_order_intent_cancelled_market_resolved_migration_metadata() -> None:
    module = _load_migration("0012_order_intent_cancelled_market_resolved.py")

    assert module.revision == "0012_cancelled_market_resolved"
    assert len(module.revision) <= 32
    assert module.down_revision == "0011_live_reconcile_audit"
    assert "cancelled_market_resolved" in module.ORDER_INTENT_OUTCOMES
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_markets_live_status_fields_migration_metadata() -> None:
    module = _load_migration("0013_markets_live_status_fields.py")

    assert module.revision == "0013_market_status_fields"
    assert len(module.revision) <= 32
    assert module.down_revision == "0012_cancelled_market_resolved"
    assert callable(module.upgrade)
    assert callable(module.downgrade)
