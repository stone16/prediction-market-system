from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
import re
from types import ModuleType

import pytest


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


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def exec_driver_sql(self, statement: str) -> None:
        self.statements.append(statement)


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
    assert "CREATE TABLE IF NOT EXISTS strategy_judgement_artifacts" not in schema_sql
    assert "CREATE TABLE IF NOT EXISTS strategy_execution_artifacts" not in schema_sql
    assert "idx_strategy_judgement_artifacts_strategy_created_at" not in schema_sql


def test_baseline_migration_upgrade_executes_schema_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    assert len(fake_connection.statements) == 1
    assert "CREATE TABLE IF NOT EXISTS markets" in fake_connection.statements[0]
    assert not fake_connection.statements[0].lstrip().startswith("BEGIN;")


def test_baseline_downgrade_list_matches_baseline_created_tables() -> None:
    module = _load_migration_module()
    schema_table_names = set(
        re.findall(
            r"CREATE TABLE IF NOT EXISTS ([a-z_]+)",
            module._migration_schema_sql(),
        )
    )

    assert set(module._CREATED_TABLES) == schema_table_names


def test_baseline_deferred_filter_preserves_dollar_quoted_blocks() -> None:
    module = _load_migration_module()
    statements = module._split_sql_statements(
        """
        DO $$
        BEGIN
            RAISE NOTICE 'strategy_judgement_artifacts is mentioned only in text';
        END
        $$;

        CREATE TABLE IF NOT EXISTS strategy_judgement_artifacts (
            artifact_id TEXT PRIMARY KEY
        );
        """
    )

    kept = [
        statement
        for statement in statements
        if not module._is_deferred_schema_statement(statement)
    ]

    assert len(statements) == 2
    assert kept == [statements[0]]
    assert kept[0].lstrip().startswith("DO $$")


def test_baseline_statement_splitter_ignores_semicolons_in_sql_comments() -> None:
    module = _load_migration_module()

    statements = module._split_sql_statements(
        """
        -- CP00 resolved Q2a to allow duplicates; replay order is (ts, id).
        CREATE TABLE IF NOT EXISTS price_changes (
            id TEXT PRIMARY KEY
        );

        /* Strategy factors are empty in S2; populated by S3. */
        CREATE TABLE IF NOT EXISTS strategy_factors (
            id TEXT PRIMARY KEY
        );
        """
    )

    assert len(statements) == 2
    assert "replay order is (ts, id)" in statements[0]
    assert statements[0].count("CREATE TABLE") == 1
    assert "populated by S3" in statements[1]
    assert statements[1].count("CREATE TABLE") == 1


def test_baseline_migration_downgrade_drops_all_tables_in_reverse_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    assert fake_connection.statements[0] == (
        'DROP TABLE IF EXISTS "backtest_live_comparisons" CASCADE'
    )
    assert fake_connection.statements[-1] == 'DROP TABLE IF EXISTS "markets" CASCADE'
    assert 'DROP TABLE IF EXISTS "market_subscriptions" CASCADE' in (
        fake_connection.statements
    )
    assert 'DROP TABLE IF EXISTS "market_price_snapshots" CASCADE' in (
        fake_connection.statements
    )
    assert len(fake_connection.statements) == len(module._CREATED_TABLES)
