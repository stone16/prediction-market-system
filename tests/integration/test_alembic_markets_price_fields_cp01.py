from __future__ import annotations

import os
import subprocess
import uuid
from urllib.parse import urlsplit, urlunsplit

import pytest


PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
    pytest.mark.skipif(
        PMS_TEST_DATABASE_URL is None,
        reason="set PMS_TEST_DATABASE_URL to a PostgreSQL URI with CREATE DATABASE privileges",
    ),
]


def _replace_database(database_url: str, database_name: str) -> str:
    parts = urlsplit(database_url)
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            f"/{database_name}",
            parts.query,
            parts.fragment,
        )
    )


def _run_psql(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["psql", database_url, "--set", "ON_ERROR_STOP=1", *args],
        text=True,
        capture_output=True,
        check=True,
    )


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env.pop("PMS_DATABASE_URL", None)
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )


def _read_market_price_columns(
    database_url: str,
) -> subprocess.CompletedProcess[str]:
    return _run_psql(
        database_url,
        "-At",
        "-F",
        "|",
        "-c",
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'markets'
          AND column_name IN (
            'yes_price',
            'no_price',
            'best_bid',
            'best_ask',
            'last_trade_price',
            'liquidity',
            'spread_bps',
            'price_updated_at'
          )
        ORDER BY array_position(
          ARRAY[
            'yes_price',
            'no_price',
            'best_bid',
            'best_ask',
            'last_trade_price',
            'liquidity',
            'spread_bps',
            'price_updated_at'
          ],
          column_name
        )
        """,
    )


def test_migration_0006_apply_and_reverse() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_cp01_price_fields_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)
    expected_columns = [
        "yes_price|numeric|YES",
        "no_price|numeric|YES",
        "best_bid|numeric|YES",
        "best_ask|numeric|YES",
        "last_trade_price|numeric|YES",
        "liquidity|numeric|YES",
        "spread_bps|integer|YES",
        "price_updated_at|timestamp with time zone|YES",
    ]

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")

        upgrade = _run_alembic(temp_database_url, "upgrade", "0006_markets_price_fields")
        assert upgrade.returncode == 0, upgrade.stderr

        columns = _read_market_price_columns(temp_database_url)
        assert columns.stdout.splitlines() == expected_columns

        indexes = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'markets'
              AND indexname = 'idx_markets_price_updated_at'
            """,
        )
        assert "price_updated_at DESC" in indexes.stdout
        assert "WHERE (price_updated_at IS NOT NULL)" in indexes.stdout

        downgrade = _run_alembic(temp_database_url, "downgrade", "-1")
        assert downgrade.returncode == 0, downgrade.stderr

        removed_columns = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'markets'
              AND column_name IN (
                'yes_price',
                'no_price',
                'best_bid',
                'best_ask',
                'last_trade_price',
                'liquidity',
                'spread_bps',
                'price_updated_at'
              )
            """,
        )
        assert removed_columns.stdout.strip() == "0"

        reupgrade = _run_alembic(temp_database_url, "upgrade", "head")
        assert reupgrade.returncode == 0, reupgrade.stderr
        restored_columns = _read_market_price_columns(temp_database_url)
        assert restored_columns.stdout.splitlines() == expected_columns
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
