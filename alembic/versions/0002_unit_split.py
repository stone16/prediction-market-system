"""Migrate order/fill unit columns to the notional/quantity split.

Supports both the shell-table baseline from 0001_baseline and legacy pre-split
databases that already carry requested_size / fill_size / filled_contracts
columns. The filled_contracts backfill is expected to touch <=10k rows in
dev/CI; if an ops database exceeds 100k rows, batch the UPDATE by ctid range.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import Connection


revision = "0002_unit_split"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None

_ORDER_RENAMES: Final[tuple[tuple[str, str], ...]] = (
    ("requested_size", "requested_notional_usdc"),
    ("filled_size", "filled_notional_usdc"),
    ("remaining_size", "remaining_notional_usdc"),
)
_FILL_RENAMES: Final[tuple[tuple[str, str], ...]] = (
    ("fill_size", "fill_notional_usdc"),
)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _column_names(connection: Connection, table_name: str) -> set[str]:
    result = connection.execute(
        sa.text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table_name
            """
        ),
        {"table_name": table_name},
    )
    return {str(row[0]) for row in result}


def _index_names(connection: Connection, table_name: str) -> list[str]:
    result = connection.execute(
        sa.text(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = :table_name
            ORDER BY indexname
            """
        ),
        {"table_name": table_name},
    )
    return [str(row[0]) for row in result]


def _rename_column(connection: Connection, table_name: str, old_name: str, new_name: str) -> None:
    connection.execute(
        sa.text(
            f"ALTER TABLE {_quoted(table_name)} "
            f"RENAME COLUMN {_quoted(old_name)} TO {_quoted(new_name)}"
        )
    )


def _add_column(connection: Connection, table_name: str, column_sql: str) -> None:
    connection.execute(
        sa.text(f"ALTER TABLE {_quoted(table_name)} ADD COLUMN {column_sql}")
    )


def _drop_column(connection: Connection, table_name: str, column_name: str) -> None:
    connection.execute(
        sa.text(
            f"ALTER TABLE {_quoted(table_name)} DROP COLUMN {_quoted(column_name)}"
        )
    )


def _drop_index(connection: Connection, index_name: str) -> None:
    connection.execute(sa.text(f"DROP INDEX {_quoted(index_name)}"))


def _copy_column_values(
    connection: Connection,
    *,
    table_name: str,
    source_column: str,
    target_column: str,
) -> None:
    connection.execute(
        sa.text(
            f"UPDATE {_quoted(table_name)} "
            f"SET {_quoted(target_column)} = {_quoted(source_column)}"
        )
    )


def _rename_indexes(
    connection: Connection,
    *,
    table_name: str,
    replacements: Iterable[tuple[str, str]],
) -> None:
    current_index_names = _index_names(connection, table_name)
    for index_name in list(current_index_names):
        renamed = index_name
        for old_fragment, new_fragment in replacements:
            renamed = renamed.replace(old_fragment, new_fragment)
        if renamed == index_name:
            continue
        if renamed in current_index_names:
            _drop_index(connection, index_name)
            current_index_names.remove(index_name)
            continue
        connection.execute(
            sa.text(
                f"ALTER INDEX {_quoted(index_name)} RENAME TO {_quoted(renamed)}"
            )
        )
        current_index_names.remove(index_name)
        current_index_names.append(renamed)


def _ensure_index(connection: Connection, index_name: str, table_name: str, column_name: str) -> None:
    connection.execute(
        sa.text(
            f"CREATE INDEX IF NOT EXISTS {_quoted(index_name)} "
            f"ON {_quoted(table_name)} ({_quoted(column_name)})"
        )
    )


def upgrade() -> None:
    connection = op.get_bind()

    order_columns = _column_names(connection, "orders")
    for old_name, new_name in _ORDER_RENAMES:
        if old_name in order_columns and new_name in order_columns:
            _copy_column_values(
                connection,
                table_name="orders",
                source_column=old_name,
                target_column=new_name,
            )
            _drop_column(connection, "orders", old_name)
            order_columns.remove(old_name)
        elif old_name in order_columns and new_name not in order_columns:
            _rename_column(connection, "orders", old_name, new_name)
            order_columns.remove(old_name)
            order_columns.add(new_name)
        elif new_name not in order_columns:
            _add_column(
                connection,
                "orders",
                f"{_quoted(new_name)} DOUBLE PRECISION NOT NULL DEFAULT 0.0",
            )
            order_columns.add(new_name)
    if "filled_quantity" not in order_columns:
        _add_column(
            connection,
            "orders",
            '"filled_quantity" DOUBLE PRECISION NOT NULL DEFAULT 0.0',
        )
    _rename_indexes(connection, table_name="orders", replacements=_ORDER_RENAMES)
    _ensure_index(
        connection,
        "idx_orders_requested_notional_usdc",
        "orders",
        "requested_notional_usdc",
    )

    fill_columns = _column_names(connection, "fills")
    for old_name, new_name in _FILL_RENAMES:
        if old_name in fill_columns and new_name in fill_columns:
            _copy_column_values(
                connection,
                table_name="fills",
                source_column=old_name,
                target_column=new_name,
            )
            _drop_column(connection, "fills", old_name)
            fill_columns.remove(old_name)
        elif old_name in fill_columns and new_name not in fill_columns:
            _rename_column(connection, "fills", old_name, new_name)
            fill_columns.remove(old_name)
            fill_columns.add(new_name)
        elif new_name not in fill_columns:
            _add_column(
                connection,
                "fills",
                f"{_quoted(new_name)} DOUBLE PRECISION NOT NULL DEFAULT 0.0",
            )
            fill_columns.add(new_name)
    if "fill_quantity" not in fill_columns:
        _add_column(
            connection,
            "fills",
            '"fill_quantity" DOUBLE PRECISION NOT NULL DEFAULT 0.0',
        )
        fill_columns.add("fill_quantity")
    if "filled_contracts" in fill_columns:
        connection.execute(
            sa.text(
                """
                UPDATE fills
                SET fill_quantity = filled_contracts
                WHERE filled_contracts IS NOT NULL
                """
            )
        )
        _drop_column(connection, "fills", "filled_contracts")
    _rename_indexes(
        connection,
        table_name="fills",
        replacements=(
            ("fill_size", "fill_notional_usdc"),
            ("filled_contracts", "fill_quantity"),
        ),
    )
    _ensure_index(
        connection,
        "idx_fills_fill_notional_usdc",
        "fills",
        "fill_notional_usdc",
    )


def downgrade() -> None:
    connection = op.get_bind()

    fill_columns = _column_names(connection, "fills")
    if "fill_quantity" in fill_columns:
        if "filled_contracts" not in fill_columns:
            _add_column(connection, "fills", '"filled_contracts" DOUBLE PRECISION')
        connection.execute(
            sa.text(
                """
                UPDATE fills
                SET filled_contracts = fill_quantity
                WHERE fill_quantity IS NOT NULL
                """
            )
        )
        _drop_column(connection, "fills", "fill_quantity")
        fill_columns.remove("fill_quantity")
        fill_columns.add("filled_contracts")
    if "fill_notional_usdc" in fill_columns and "fill_size" not in fill_columns:
        _rename_column(connection, "fills", "fill_notional_usdc", "fill_size")
        fill_columns.remove("fill_notional_usdc")
        fill_columns.add("fill_size")
    elif "fill_notional_usdc" in fill_columns and "fill_size" in fill_columns:
        _copy_column_values(
            connection,
            table_name="fills",
            source_column="fill_notional_usdc",
            target_column="fill_size",
        )
        _drop_column(connection, "fills", "fill_notional_usdc")
        fill_columns.remove("fill_notional_usdc")
    _rename_indexes(
        connection,
        table_name="fills",
        replacements=(
            ("fill_notional_usdc", "fill_size"),
            ("fill_quantity", "filled_contracts"),
        ),
    )

    order_columns = _column_names(connection, "orders")
    if "filled_quantity" in order_columns:
        _drop_column(connection, "orders", "filled_quantity")
        order_columns.remove("filled_quantity")
    for new_name, old_name in (
        ("remaining_notional_usdc", "remaining_size"),
        ("filled_notional_usdc", "filled_size"),
        ("requested_notional_usdc", "requested_size"),
    ):
        if new_name in order_columns and old_name in order_columns:
            _copy_column_values(
                connection,
                table_name="orders",
                source_column=new_name,
                target_column=old_name,
            )
            _drop_column(connection, "orders", new_name)
            order_columns.remove(new_name)
        elif new_name in order_columns and old_name not in order_columns:
            _rename_column(connection, "orders", new_name, old_name)
            order_columns.remove(new_name)
            order_columns.add(old_name)
    _rename_indexes(
        connection,
        table_name="orders",
        replacements=(
            ("requested_notional_usdc", "requested_size"),
            ("filled_notional_usdc", "filled_size"),
            ("remaining_notional_usdc", "remaining_size"),
        ),
    )
