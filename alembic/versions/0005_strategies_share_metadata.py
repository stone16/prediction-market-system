"""Add public share metadata columns to strategies.

Supports both fresh-clone databases where `schema.sql` already includes the
columns and upgraded databases coming from 0004_decisions_table.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import Connection


revision = "0005_strategies_share_metadata"
down_revision = "0004_decisions_table"
branch_labels = None
depends_on = None


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


def upgrade() -> None:
    connection = op.get_bind()
    columns = _column_names(connection, "strategies")

    if "title" not in columns:
        connection.execute(sa.text('ALTER TABLE strategies ADD COLUMN title TEXT'))
    if "description" not in columns:
        connection.execute(sa.text('ALTER TABLE strategies ADD COLUMN description TEXT'))
    if "archived" not in columns:
        connection.execute(
            sa.text(
                "ALTER TABLE strategies "
                "ADD COLUMN archived BOOLEAN NOT NULL DEFAULT FALSE"
            )
        )
    if "share_enabled" not in columns:
        connection.execute(
            sa.text(
                "ALTER TABLE strategies "
                "ADD COLUMN share_enabled BOOLEAN NOT NULL DEFAULT TRUE"
            )
        )


def downgrade() -> None:
    connection = op.get_bind()
    columns = _column_names(connection, "strategies")

    if "share_enabled" in columns:
        connection.execute(sa.text("ALTER TABLE strategies DROP COLUMN share_enabled"))
    if "archived" in columns:
        connection.execute(sa.text("ALTER TABLE strategies DROP COLUMN archived"))
    if "description" in columns:
        connection.execute(sa.text("ALTER TABLE strategies DROP COLUMN description"))
    if "title" in columns:
        connection.execute(sa.text("ALTER TABLE strategies DROP COLUMN title"))
