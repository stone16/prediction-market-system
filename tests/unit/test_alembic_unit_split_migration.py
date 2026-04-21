from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType

import pytest


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


class _FakeConnection:
    def __init__(
        self,
        *,
        columns: dict[str, set[str]],
        indexes: dict[str, list[str]],
    ) -> None:
        self.columns = {table: set(names) for table, names in columns.items()}
        self.indexes = {table: list(names) for table, names in indexes.items()}
        self.statements: list[str] = []

    def execute(
        self,
        statement: object,
        params: dict[str, object] | None = None,
    ) -> list[tuple[str]]:
        sql = str(statement)
        self.statements.append(sql)
        if "SELECT column_name" in sql:
            assert params is not None
            table_name = str(params["table_name"])
            return [(name,) for name in sorted(self.columns.get(table_name, set()))]
        if "SELECT indexname" in sql:
            assert params is not None
            table_name = str(params["table_name"])
            return [(name,) for name in sorted(self.indexes.get(table_name, []))]

        rename_column = re.search(
            r'ALTER TABLE "([^"]+)" RENAME COLUMN "([^"]+)" TO "([^"]+)"',
            sql,
        )
        if rename_column is not None:
            table_name, old_name, new_name = rename_column.groups()
            self.columns[table_name].remove(old_name)
            self.columns[table_name].add(new_name)
            return []

        add_column = re.search(r'ALTER TABLE "([^"]+)" ADD COLUMN "([^"]+)"', sql)
        if add_column is not None:
            table_name, column_name = add_column.groups()
            self.columns[table_name].add(column_name)
            return []

        drop_column = re.search(r'ALTER TABLE "([^"]+)" DROP COLUMN "([^"]+)"', sql)
        if drop_column is not None:
            table_name, column_name = drop_column.groups()
            self.columns[table_name].remove(column_name)
            return []

        rename_index = re.search(
            r'ALTER INDEX "([^"]+)" RENAME TO "([^"]+)"',
            sql,
        )
        if rename_index is not None:
            old_name, new_name = rename_index.groups()
            for index_names in self.indexes.values():
                if old_name in index_names:
                    index_names[index_names.index(old_name)] = new_name
                    break
            return []

        drop_index = re.search(r'DROP INDEX "([^"]+)"', sql)
        if drop_index is not None:
            index_name = drop_index.group(1)
            for index_names in self.indexes.values():
                if index_name in index_names:
                    index_names.remove(index_name)
                    break
            return []

        create_index = re.search(
            r'CREATE INDEX IF NOT EXISTS "([^"]+)" ON "([^"]+)"',
            sql,
        )
        if create_index is not None:
            index_name, table_name = create_index.groups()
            if index_name not in self.indexes[table_name]:
                self.indexes[table_name].append(index_name)
            return []

        return []


def test_unit_split_upgrade_on_shell_tables_adds_split_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection(
        columns={
            "orders": {
                "order_id",
                "market_id",
                "ts",
                "strategy_id",
                "strategy_version_id",
            },
            "fills": {
                "fill_id",
                "order_id",
                "market_id",
                "ts",
                "strategy_id",
                "strategy_version_id",
            },
        },
        indexes={"orders": [], "fills": []},
    )
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    assert {
        "requested_notional_usdc",
        "filled_notional_usdc",
        "remaining_notional_usdc",
        "filled_quantity",
    }.issubset(fake_connection.columns["orders"])
    assert {
        "fill_notional_usdc",
        "fill_quantity",
    }.issubset(fake_connection.columns["fills"])
    assert "idx_orders_requested_notional_usdc" in fake_connection.indexes["orders"]
    assert "idx_fills_fill_notional_usdc" in fake_connection.indexes["fills"]


def test_unit_split_upgrade_absorbs_legacy_columns_when_head_columns_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection(
        columns={
            "orders": {
                "order_id",
                "requested_size",
                "filled_size",
                "remaining_size",
                "requested_notional_usdc",
                "filled_notional_usdc",
                "remaining_notional_usdc",
                "filled_quantity",
            },
            "fills": {
                "fill_id",
                "fill_size",
                "filled_contracts",
                "fill_notional_usdc",
                "fill_quantity",
            },
        },
        indexes={
            "orders": [
                "idx_orders_requested_size",
                "idx_orders_requested_notional_usdc",
            ],
            "fills": [
                "idx_fills_fill_size",
                "idx_fills_fill_notional_usdc",
            ],
        },
    )
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    assert "requested_size" not in fake_connection.columns["orders"]
    assert "filled_size" not in fake_connection.columns["orders"]
    assert "remaining_size" not in fake_connection.columns["orders"]
    assert "fill_size" not in fake_connection.columns["fills"]
    assert "filled_contracts" not in fake_connection.columns["fills"]
    assert "idx_orders_requested_size" not in fake_connection.indexes["orders"]
    assert "idx_fills_fill_size" not in fake_connection.indexes["fills"]
    assert any(
        "UPDATE \"orders\" SET \"requested_notional_usdc\" = \"requested_size\"" in statement
        for statement in fake_connection.statements
    )
    assert any(
        "UPDATE fills\n                SET fill_quantity = filled_contracts" in statement
        or "UPDATE fills SET fill_quantity = filled_contracts" in statement
        for statement in fake_connection.statements
    )


def test_unit_split_downgrade_restores_legacy_column_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection(
        columns={
            "orders": {
                "order_id",
                "requested_notional_usdc",
                "filled_notional_usdc",
                "remaining_notional_usdc",
                "filled_quantity",
            },
            "fills": {
                "fill_id",
                "fill_notional_usdc",
                "fill_quantity",
            },
        },
        indexes={
            "orders": ["idx_orders_requested_notional_usdc"],
            "fills": ["idx_fills_fill_notional_usdc"],
        },
    )
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    assert {
        "requested_size",
        "filled_size",
        "remaining_size",
    }.issubset(fake_connection.columns["orders"])
    assert "filled_quantity" not in fake_connection.columns["orders"]
    assert "fill_size" in fake_connection.columns["fills"]
    assert "filled_contracts" in fake_connection.columns["fills"]
    assert "fill_notional_usdc" not in fake_connection.columns["fills"]
    assert "fill_quantity" not in fake_connection.columns["fills"]
