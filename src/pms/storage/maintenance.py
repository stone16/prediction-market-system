from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

import asyncpg


@dataclass(frozen=True)
class MaintenanceStatement:
    description: str
    sql: str
    args: tuple[object, ...] = ()
    runs_in_transaction: bool = True


@dataclass(frozen=True)
class MarketHistoryPrunePolicy:
    truncate_market_price_snapshots: bool = False
    price_changes_retention: timedelta | None = timedelta(days=7)
    vacuum_full: bool = False


def build_market_history_prune_plan(
    policy: MarketHistoryPrunePolicy,
) -> tuple[MaintenanceStatement, ...]:
    statements: list[MaintenanceStatement] = []
    if policy.truncate_market_price_snapshots:
        statements.append(
            MaintenanceStatement(
                description="truncate market_price_snapshots",
                sql="TRUNCATE TABLE market_price_snapshots",
            )
        )
    if policy.price_changes_retention is not None:
        statements.append(
            MaintenanceStatement(
                description="delete old price_changes",
                sql="DELETE FROM price_changes WHERE ts < (now() - $1::interval)",
                args=(policy.price_changes_retention,),
            )
        )
    if policy.vacuum_full:
        statements.extend(
            (
                MaintenanceStatement(
                    description="vacuum market_price_snapshots",
                    sql="VACUUM FULL market_price_snapshots",
                    runs_in_transaction=False,
                ),
                MaintenanceStatement(
                    description="vacuum price_changes",
                    sql="VACUUM FULL price_changes",
                    runs_in_transaction=False,
                ),
            )
        )
    return tuple(statements)


async def apply_market_history_prune_plan(
    pool: asyncpg.Pool,
    plan: Sequence[MaintenanceStatement],
) -> tuple[str, ...]:
    applied: list[str] = []
    async with pool.acquire() as connection:
        for statement in plan:
            if statement.runs_in_transaction:
                async with connection.transaction():
                    await connection.execute(statement.sql, *statement.args)
            else:
                await connection.execute(statement.sql, *statement.args)
            applied.append(statement.description)
    return tuple(applied)
