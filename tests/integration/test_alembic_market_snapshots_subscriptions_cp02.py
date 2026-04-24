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


def _run_psql_unchecked(
    database_url: str,
    *args: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["psql", database_url, *args],
        text=True,
        capture_output=True,
        check=False,
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


def _create_temp_database(prefix: str) -> tuple[str, str]:
    assert PMS_TEST_DATABASE_URL is not None
    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"{prefix}_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)
    _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
    return temp_database, temp_database_url


def _drop_temp_database(database_name: str) -> None:
    assert PMS_TEST_DATABASE_URL is not None
    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    _run_psql(
        admin_database_url,
        "-c",
        f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)",
    )


def test_migration_0007_0008_apply_and_reverse() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    temp_database, temp_database_url = _create_temp_database("pms_cp02_schema")

    try:
        upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr

        tables = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename IN ('market_price_snapshots', 'market_subscriptions')
            ORDER BY tablename
            """,
        )
        assert tables.stdout.splitlines() == [
            "market_price_snapshots",
            "market_subscriptions",
        ]

        snapshot_indexes = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'market_price_snapshots'
            ORDER BY indexname
            """,
        )
        assert snapshot_indexes.stdout.splitlines() == [
            "idx_price_snapshots_recent",
            "market_price_snapshots_pkey",
        ]

        downgrade_0008 = _run_alembic(temp_database_url, "downgrade", "-1")
        assert downgrade_0008.returncode == 0, downgrade_0008.stderr

        after_0008 = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename IN ('market_price_snapshots', 'market_subscriptions')
            ORDER BY tablename
            """,
        )
        assert after_0008.stdout.splitlines() == ["market_price_snapshots"]

        downgrade_0007 = _run_alembic(temp_database_url, "downgrade", "-1")
        assert downgrade_0007.returncode == 0, downgrade_0007.stderr

        after_0007 = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT COUNT(*)
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename IN ('market_price_snapshots', 'market_subscriptions')
            """,
        )
        assert after_0007.stdout.strip() == "0"
    finally:
        _drop_temp_database(temp_database)


def test_market_subscriptions_check_constraint_rejects_non_user(
) -> None:
    assert PMS_TEST_DATABASE_URL is not None

    temp_database, temp_database_url = _create_temp_database("pms_cp02_check")

    try:
        upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr
        result = _run_psql_unchecked(
            temp_database_url,
            "-v",
            "ON_ERROR_STOP=0",
            "-c",
            """
            INSERT INTO markets (
              condition_id, slug, question, venue, created_at, last_seen_at
            ) VALUES (
              'cp02-check-market', 'cp02-check', 'Will CP02 check pass?',
              'polymarket', NOW(), NOW()
            ) ON CONFLICT (condition_id) DO NOTHING;
            INSERT INTO tokens (token_id, condition_id, outcome)
            VALUES ('cp02-check-token', 'cp02-check-market', 'YES')
            ON CONFLICT (token_id) DO NOTHING;
            INSERT INTO market_subscriptions (token_id, source)
            VALUES ('cp02-check-token', 'selector');
            """,
        )
    finally:
        _drop_temp_database(temp_database)

    assert result.returncode != 0
    assert "market_subscriptions_source_check" in result.stderr


def test_market_subscriptions_pk_rejects_duplicate(
) -> None:
    assert PMS_TEST_DATABASE_URL is not None

    temp_database, temp_database_url = _create_temp_database("pms_cp02_pk")

    try:
        upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr
        result = _run_psql_unchecked(
            temp_database_url,
            "-v",
            "ON_ERROR_STOP=0",
            "-c",
            """
            INSERT INTO markets (
              condition_id, slug, question, venue, created_at, last_seen_at
            ) VALUES (
              'cp02-pk-market', 'cp02-pk', 'Will CP02 pk pass?',
              'polymarket', NOW(), NOW()
            ) ON CONFLICT (condition_id) DO NOTHING;
            INSERT INTO tokens (token_id, condition_id, outcome)
            VALUES ('cp02-pk-token', 'cp02-pk-market', 'YES')
            ON CONFLICT (token_id) DO NOTHING;
            DELETE FROM market_subscriptions WHERE token_id = 'cp02-pk-token';
            INSERT INTO market_subscriptions (token_id, source)
            VALUES ('cp02-pk-token', 'user');
            INSERT INTO market_subscriptions (token_id, source)
            VALUES ('cp02-pk-token', 'user');
            """,
        )
    finally:
        _drop_temp_database(temp_database)

    assert result.returncode != 0
    assert "duplicate key value violates unique constraint" in result.stderr


def test_market_price_snapshots_cascade_on_market_delete(
) -> None:
    assert PMS_TEST_DATABASE_URL is not None

    temp_database, temp_database_url = _create_temp_database("pms_cp02_cascade")
    market_id = f"cp02-cascade-market-{uuid.uuid4().hex[:8]}"

    try:
        upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr
        _run_psql(
            temp_database_url,
            "-c",
            f"""
            INSERT INTO markets (
              condition_id, slug, question, venue, created_at, last_seen_at
            ) VALUES (
              '{market_id}', '{market_id}', 'Will CP02 cascade pass?',
              'polymarket', NOW(), NOW()
            );
            INSERT INTO market_price_snapshots (
              condition_id, snapshot_at, yes_price, no_price
            ) VALUES (
              '{market_id}', NOW(), 0.5100, 0.4900
            );
            DELETE FROM markets WHERE condition_id = '{market_id}';
            """,
        )
        snapshot_count = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            f"""
            SELECT COUNT(*)
            FROM market_price_snapshots
            WHERE condition_id = '{market_id}'
            """,
        )
    finally:
        _drop_temp_database(temp_database)

    assert snapshot_count.stdout.strip() == "0"
