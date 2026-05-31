"""Register the PAPER-only Phase A multi-factor strategy."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import asyncpg

from pms.factors.catalog import ensure_factor_catalog
from pms.factors.definitions import REGISTERED
from pms.storage.strategy_registry import PostgresStrategyRegistry
from pms.strategies.aggregate import Strategy
from pms.strategies.paper_multifactor import build_paper_multi_factor_strategy
from pms.strategies.projections import FactorCompositionStep, StrategyVersion

_RAW_FACTOR_ROLES = frozenset(
    {
        "weighted",
        "precedence_rank",
        "threshold_edge",
        "posterior_prior",
        "posterior_success",
        "posterior_failure",
        "rule_delta",
    }
)
_REGISTERED_FACTOR_IDS = frozenset(factor_cls.factor_id for factor_cls in REGISTERED)


async def install_paper_multi_factor_strategy(
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
        strategy = build_paper_multi_factor_strategy()
        version = await registry.create_version(strategy)
        factor_steps = _strategy_factor_steps(strategy)
        await ensure_factor_catalog(
            pool,
            factor_ids=tuple(
                dict.fromkeys(step.factor_id for step in factor_steps)
            ),
        )
        await registry.populate_strategy_factors(
            strategy.config.strategy_id,
            version.strategy_version_id,
            factor_steps,
        )
        if archive_default:
            await registry.archive_strategy("default")
        return version
    finally:
        await pool.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Register paper_multi_factor_v1 as an active PAPER strategy."
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
            "Archive the seeded default strategy so this PAPER strategy is the "
            "only active paper-soak controller."
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
        install_paper_multi_factor_strategy(
            database_url,
            archive_default=archive_default,
        )
    )


def _strategy_factor_steps(strategy: Strategy) -> tuple[FactorCompositionStep, ...]:
    return tuple(
        step
        for step in strategy.config.factor_composition
        if step.role in _RAW_FACTOR_ROLES and step.factor_id in _REGISTERED_FACTOR_IDS
    )


if __name__ == "__main__":
    sys.exit(main())
