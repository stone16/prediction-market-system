from __future__ import annotations

from pathlib import Path
from typing import Final

from alembic import op
from sqlalchemy.engine import Connection


revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None

_CREATED_TABLES: Final[tuple[str, ...]] = (
    "backtest_live_comparisons",
    "evaluation_reports",
    "strategy_runs",
    "backtest_runs",
    "opportunities",
    "fills",
    "order_intents",
    "orders",
    "decisions",
    "eval_records",
    "feedback",
    "strategy_factors",
    "strategy_versions",
    "strategies",
    "factor_values",
    "factors",
    "trades",
    "price_changes",
    "book_levels",
    "book_snapshots",
    "market_subscriptions",
    "tokens",
    "market_price_snapshots",
    "markets",
)

_DEFERRED_TO_LATER_MIGRATIONS: Final[tuple[str, ...]] = (
    "strategy_judgement_artifacts",
    "strategy_execution_artifacts",
)


def _schema_sql_path() -> Path:
    return Path(__file__).resolve().parents[2] / "schema.sql"


def _migration_schema_sql() -> str:
    schema_sql = _schema_sql_path().read_text(encoding="utf-8").strip()
    lines = schema_sql.splitlines()
    if lines and lines[0].strip() == "BEGIN;":
        lines = lines[1:]
    if lines and lines[-1].strip() == "COMMIT;":
        lines = lines[:-1]
    statements = []
    for statement in "\n".join(lines).split(";"):
        normalized = statement.strip()
        if not normalized:
            continue
        if any(marker in normalized for marker in _DEFERRED_TO_LATER_MIGRATIONS):
            continue
        statements.append(f"{normalized};")
    return "\n\n".join(statements).strip() + "\n"


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(_migration_schema_sql())


def downgrade() -> None:
    connection: Connection = op.get_bind()
    for table_name in _CREATED_TABLES:
        connection.exec_driver_sql(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
