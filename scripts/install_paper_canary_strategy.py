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


async def install_paper_canary_strategy(
    database_url: str,
    *,
    archive_default: bool = False,
) -> StrategyVersion:
    pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=2,
    )
    try:
        registry = PostgresStrategyRegistry(pool)
        strategy = build_paper_canary_strategy()
        version = await registry.create_version(strategy, activate=False)
        if archive_default:
            try:
                await registry.archive_strategy("default")
            except LookupError:
                pass
        await registry.set_active(
            strategy.config.strategy_id,
            version.strategy_version_id,
        )
        return version
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
    parser.add_argument(
        "--archive-default",
        action="store_true",
        help=(
            "Archive the seeded default strategy so paper_canary_v1 is the "
            "only active plumbing-smoke controller."
        ),
    )
    args = parser.parse_args(argv)

    database_url = args.database_url
    if not database_url:
        print("error: DATABASE_URL is not set", file=sys.stderr)
        return 2

    version = _run_install(database_url, archive_default=args.archive_default)
    print(f"strategy_id: {version.strategy_id}")
    print(f"strategy_version_id: {version.strategy_version_id}")
    print(f"created_at: {version.created_at.isoformat()}")
    print(f"archived_default: {str(args.archive_default).lower()}")
    return 0


def _run_install(database_url: str, *, archive_default: bool = False) -> StrategyVersion:
    return asyncio.run(
        install_paper_canary_strategy(
            database_url,
            archive_default=archive_default,
        )
    )


if __name__ == "__main__":
    sys.exit(main())
