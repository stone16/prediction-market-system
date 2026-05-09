from __future__ import annotations

import argparse
import asyncio
from datetime import timedelta
import sys

import asyncpg

from pms.config import PMSSettings
from pms.storage.maintenance import (
    MarketHistoryPrunePolicy,
    apply_market_history_prune_plan,
    build_market_history_prune_plan,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Prune high-volume market history tables. Dry-run by default; "
            "pass --execute to apply the SQL."
        )
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="PMS YAML config path. Defaults to config.yaml.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually run the cleanup plan. Without this, only prints SQL.",
    )
    parser.add_argument(
        "--keep-price-changes-days",
        type=float,
        default=7.0,
        help="Retention window for price_changes. Defaults to 7 days.",
    )
    parser.add_argument(
        "--no-truncate-market-price-snapshots",
        action="store_true",
        help="Keep market_price_snapshots instead of truncating it.",
    )
    parser.add_argument(
        "--no-vacuum-full",
        action="store_true",
        help="Skip VACUUM FULL statements.",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    settings = PMSSettings.load(args.config)
    plan = build_market_history_prune_plan(
        MarketHistoryPrunePolicy(
            truncate_market_price_snapshots=(
                not args.no_truncate_market_price_snapshots
            ),
            price_changes_retention=timedelta(
                days=float(args.keep_price_changes_days)
            ),
            vacuum_full=not args.no_vacuum_full,
        )
    )

    for statement in plan:
        print(f"-- {statement.description}", file=sys.stdout)
        print(statement.sql + ";", file=sys.stdout)
        if statement.args:
            print(f"-- args: {statement.args!r}", file=sys.stdout)

    if not args.execute:
        print("-- dry-run only; pass --execute to apply", file=sys.stdout)
        return 0

    pool = await asyncpg.create_pool(
        settings.database.dsn,
        min_size=settings.database.pool_min_size,
        max_size=settings.database.pool_max_size,
    )
    try:
        applied = await apply_market_history_prune_plan(pool, plan)
    finally:
        await pool.close()

    print(f"applied {len(applied)} market history prune statements", file=sys.stdout)
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_run(_parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
