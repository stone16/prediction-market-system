"""Register the PAPER-only canary strategy in the configured PMS database."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import asyncpg

from pms.storage.strategy_registry import PostgresStrategyRegistry
from pms.strategies.paper_canary import build_paper_canary_strategy
from pms.strategies.projections import StrategyVersion


async def install_paper_canary_strategy(database_url: str) -> StrategyVersion:
    pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=2,
    )
    try:
        registry = PostgresStrategyRegistry(pool)
        return await registry.create_version(build_paper_canary_strategy())
    finally:
        await pool.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Register paper_canary_v1 as an active PAPER strategy."
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL DSN. Defaults to DATABASE_URL.",
    )
    args = parser.parse_args(argv)

    database_url = args.database_url
    if not database_url:
        print("error: DATABASE_URL is not set", file=sys.stderr)
        return 2

    version = _run_install(database_url)
    print(f"strategy_id: {version.strategy_id}")
    print(f"strategy_version_id: {version.strategy_version_id}")
    print(f"created_at: {version.created_at.isoformat()}")
    return 0


def _run_install(database_url: str) -> StrategyVersion:
    return asyncio.run(install_paper_canary_strategy(database_url))


if __name__ == "__main__":
    sys.exit(main())
