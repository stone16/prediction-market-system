from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "alembic"
    / "versions"
    / "0001_baseline.py"
)


def _load_migration_module() -> ModuleType:
    module_name = "test_alembic_0001_baseline"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_baseline_migration_declares_expected_revision_metadata() -> None:
    module = _load_migration_module()

    assert module.revision == "0001_baseline"
    assert module.down_revision is None
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_baseline_migration_strips_outer_transaction_wrapper() -> None:
    module = _load_migration_module()

    schema_sql = module._migration_schema_sql()

    assert not schema_sql.lstrip().startswith("BEGIN;")
    assert not schema_sql.rstrip().endswith("COMMIT;")
    assert "CREATE TABLE IF NOT EXISTS markets" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS backtest_live_comparisons" in schema_sql
