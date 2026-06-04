"""Register the PAPER-only canary strategy in the configured PMS database."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import asyncpg

from pms.storage.strategy_registry import PostgresStrategyRegistry
from pms.strategies.aggregate import Strategy
from pms.strategies.paper_canary import build_paper_canary_strategy
from pms.strategies.projections import ForecasterSpec, StrategyConfig, StrategyVersion


DEFAULT_SAMPLE_MODULUS = 25


async def install_paper_canary_strategy(
    database_url: str,
    *,
    archive_default: bool = False,
    sample_modulus: int = DEFAULT_SAMPLE_MODULUS,
) -> StrategyVersion:
    pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=2,
    )
    try:
        registry = PostgresStrategyRegistry(pool)
        strategy = _build_canary_strategy(sample_modulus=sample_modulus)
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
    parser.add_argument(
        "--sample-modulus",
        type=int,
        default=DEFAULT_SAMPLE_MODULUS,
        help=(
            "Sampling modulus for paper_canary_v1. Use 1 for short local "
            "plumbing smokes that should exercise the paper actuator; the "
            "default 25 keeps ordinary soak probes sparse."
        ),
    )
    args = parser.parse_args(argv)

    database_url = args.database_url
    if not database_url:
        print("error: DATABASE_URL is not set", file=sys.stderr)
        return 2

    try:
        version = _run_install(
            database_url,
            archive_default=args.archive_default,
            sample_modulus=args.sample_modulus,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"strategy_id: {version.strategy_id}")
    print(f"strategy_version_id: {version.strategy_version_id}")
    print(f"created_at: {version.created_at.isoformat()}")
    print(f"archived_default: {str(args.archive_default).lower()}")
    print(f"sample_modulus: {args.sample_modulus}")
    return 0


def _run_install(
    database_url: str,
    *,
    archive_default: bool = False,
    sample_modulus: int = DEFAULT_SAMPLE_MODULUS,
) -> StrategyVersion:
    return asyncio.run(
        install_paper_canary_strategy(
            database_url,
            archive_default=archive_default,
            sample_modulus=sample_modulus,
        )
    )


def _build_canary_strategy(*, sample_modulus: int) -> Strategy:
    if sample_modulus <= 0:
        msg = "sample_modulus must be positive"
        raise ValueError(msg)
    strategy = build_paper_canary_strategy()
    if sample_modulus == DEFAULT_SAMPLE_MODULUS:
        return strategy
    metadata = tuple(
        ("sample", f"0/{sample_modulus}") if key == "sample" else (key, value)
        for key, value in strategy.config.metadata
    )
    forecasters = tuple(
        (
            name,
            tuple(
                ("sample_modulus", str(sample_modulus))
                if key == "sample_modulus"
                else (key, value)
                for key, value in params
            ),
        )
        for name, params in strategy.forecaster.forecasters
    )
    return Strategy(
        config=StrategyConfig(
            strategy_id=strategy.config.strategy_id,
            factor_composition=strategy.config.factor_composition,
            metadata=metadata,
        ),
        risk=strategy.risk,
        eval_spec=strategy.eval_spec,
        forecaster=ForecasterSpec(forecasters=forecasters),
        market_selection=strategy.market_selection,
        calibration=strategy.calibration,
    )


if __name__ == "__main__":
    sys.exit(main())
