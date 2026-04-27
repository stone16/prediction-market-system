from __future__ import annotations

import difflib
import os
import shutil
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schema.sql"
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
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )


def _run_psql_allow_failure(
    database_url: str,
    *args: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["psql", database_url, "--set", "ON_ERROR_STOP=1", *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _run_pg_dump(database_url: str) -> str:
    command = [
        "pg_dump",
        database_url,
        "--schema-only",
        "--no-owner",
        "--no-privileges",
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        return result.stdout
    if "server version mismatch" not in result.stderr:
        result.check_returncode()

    server_version = subprocess.run(
        [
            "psql",
            database_url,
            "-At",
            "-c",
            "SHOW server_version",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    major_version = server_version.stdout.strip().split(".", 1)[0]
    candidate_paths = [
        shutil.which(f"pg_dump@{major_version}"),
        shutil.which(f"pg_dump-{major_version}"),
        f"/opt/homebrew/opt/postgresql@{major_version}/bin/pg_dump",
        f"/usr/local/opt/postgresql@{major_version}/bin/pg_dump",
        f"/opt/homebrew/opt/libpq/bin/pg_dump",
    ]
    pg_dump_binary = next(
        (path for path in candidate_paths if path and Path(path).exists()),
        None,
    )
    assert pg_dump_binary is not None, result.stderr

    matched_result = subprocess.run(
        [
            pg_dump_binary,
            database_url,
            "--schema-only",
            "--no-owner",
            "--no-privileges",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return matched_result.stdout


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env.pop("PMS_DATABASE_URL", None)
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _normalized_pg_dump_statements(schema_dump: str) -> list[str]:
    ignored_prefixes = (
        "SET ",
        "SELECT pg_catalog.set_config",
        "CREATE SCHEMA public",
        "COMMENT ON SCHEMA public",
    )
    statements: list[str] = []
    current: list[str] = []

    for raw_line in schema_dump.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--") or line.startswith("\\"):
            continue
        current.append(line)
        if line.endswith(";"):
            statement = " ".join(current)
            current = []
            if statement.startswith(ignored_prefixes):
                continue
            statement = " ".join(statement.split()).removesuffix(";")
            if "alembic_version" in statement:
                continue
            statements.append(statement)

    if current:
        statement = " ".join(" ".join(current).split()).removesuffix(";")
        if statement and "alembic_version" not in statement:
            statements.append(statement)

    return sorted(statements)


def _extract_block(schema_text: str, begin: str, end: str) -> str:
    start = schema_text.index(begin) + len(begin)
    finish = schema_text.index(end, start)
    return schema_text[start:finish]


def _fetch_value(database_url: str, sql: str) -> str:
    return _run_psql(database_url, "-At", "-F", "|", "-c", sql).stdout.strip()


def _fetch_lines(database_url: str, sql: str) -> list[str]:
    return [line for line in _fetch_value(database_url, sql).splitlines() if line]


def _parse_rows(output: str) -> list[tuple[str, ...]]:
    return [tuple(line.split("|")) for line in output.splitlines() if line]


def test_alembic_order_intents_matches_schema_sql_via_pg_dump() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    schema_database = f"pms_cp15_schema_{uuid.uuid4().hex[:8]}"
    alembic_database = f"pms_cp15_migration_{uuid.uuid4().hex[:8]}"
    schema_database_url = _replace_database(PMS_TEST_DATABASE_URL, schema_database)
    alembic_database_url = _replace_database(PMS_TEST_DATABASE_URL, alembic_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {schema_database}")
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {alembic_database}")

        _run_psql(schema_database_url, "-f", str(SCHEMA_PATH))
        upgrade = _run_alembic(alembic_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr

        schema_statements = _normalized_pg_dump_statements(_run_pg_dump(schema_database_url))
        alembic_statements = _normalized_pg_dump_statements(_run_pg_dump(alembic_database_url))
        diff_lines = list(
            difflib.unified_diff(
                schema_statements,
                alembic_statements,
                fromfile="schema.sql",
                tofile="alembic-upgrade-head",
                lineterm="",
            )
        )

        assert diff_lines == []
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {schema_database} WITH (FORCE)",
        )
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {alembic_database} WITH (FORCE)",
        )


def test_alembic_order_intents_table_shape_constraints_and_idempotency() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_cp15_runtime_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr

        columns_result = _run_psql(
            temp_database_url,
            "-At",
            "-F",
            "|",
            "-c",
            """
            SELECT column_name, data_type, is_nullable, COALESCE(column_default, '')
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'order_intents'
            ORDER BY ordinal_position
            """,
        )
        assert _parse_rows(columns_result.stdout) == [
            ("decision_id", "text", "NO", ""),
            ("intent_key", "text", "YES", ""),
            ("strategy_id", "text", "NO", ""),
            ("strategy_version_id", "text", "NO", ""),
            ("acquired_at", "timestamp with time zone", "NO", "now()"),
            ("released_at", "timestamp with time zone", "YES", ""),
            ("worker_host", "text", "YES", ""),
            ("worker_pid", "integer", "YES", ""),
            ("outcome", "text", "YES", ""),
            ("reconciled_at", "timestamp with time zone", "YES", ""),
            ("reconciliation_note", "text", "YES", ""),
            ("reconciled_by", "text", "YES", ""),
            ("venue_order_id", "text", "YES", ""),
            ("reconciliation_status", "text", "YES", ""),
        ]

        constraint_rows = _parse_rows(
            _run_psql(
                temp_database_url,
                "-At",
                "-F",
                "|",
                "-c",
                """
                SELECT conname, contype
                FROM pg_constraint
                WHERE conrelid = 'order_intents'::regclass
                ORDER BY conname
                """,
            ).stdout
        )
        assert constraint_rows == [
            ("order_intents_outcome_check", "c"),
            ("order_intents_pkey", "p"),
            ("order_intents_reconciliation_status_check", "c"),
            ("order_intents_strategy_id_check", "c"),
            ("order_intents_strategy_version_id_check", "c"),
        ]

        index_rows = _parse_rows(
            _run_psql(
                temp_database_url,
                "-At",
                "-F",
                "|",
                "-c",
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = 'public' AND tablename = 'order_intents'
                ORDER BY indexname
                """,
            ).stdout
        )
        assert [name for name, _ in index_rows] == [
            "idx_order_intents_intent_key_unique",
            "idx_order_intents_released_at_nulls_first",
            "idx_order_intents_strategy_acquired_at_desc",
            "idx_order_intents_submission_unknown_unresolved",
            "order_intents_pkey",
        ]
        assert any("intent_key" in definition for _, definition in index_rows)
        assert any("strategy_id, acquired_at DESC" in definition for _, definition in index_rows)
        assert any("released_at NULLS FIRST" in definition for _, definition in index_rows)
        assert any(
            "outcome = 'submission_unknown'::text" in definition
            and "reconciled_at IS NULL" in definition
            for _, definition in index_rows
        )

        empty_strategy_result = _run_psql_allow_failure(
            temp_database_url,
            "-c",
            """
            INSERT INTO order_intents (
                decision_id,
                strategy_id,
                strategy_version_id
            ) VALUES (
                'd-empty-strategy',
                '',
                'v1'
            )
            """,
        )
        assert empty_strategy_result.returncode != 0
        assert "order_intents_strategy_id_check" in empty_strategy_result.stderr

        empty_version_result = _run_psql_allow_failure(
            temp_database_url,
            "-c",
            """
            INSERT INTO order_intents (
                decision_id,
                strategy_id,
                strategy_version_id
            ) VALUES (
                'd-empty-version',
                's1',
                ''
            )
            """,
        )
        assert empty_version_result.returncode != 0
        assert "order_intents_strategy_version_id_check" in empty_version_result.stderr

        invalid_outcome_result = _run_psql_allow_failure(
            temp_database_url,
            "-c",
            """
            INSERT INTO order_intents (
                decision_id,
                strategy_id,
                strategy_version_id,
                outcome
            ) VALUES (
                'd-invalid-outcome',
                's1',
                'v1',
                'frobnicate'
            )
            """,
        )
        assert invalid_outcome_result.returncode != 0
        assert "order_intents_outcome_check" in invalid_outcome_result.stderr

        invalid_reconciliation_status_result = _run_psql_allow_failure(
            temp_database_url,
            "-c",
            """
            INSERT INTO order_intents (
                decision_id,
                strategy_id,
                strategy_version_id,
                outcome,
                reconciliation_status
            ) VALUES (
                'd-invalid-reconciliation-status',
                's1',
                'v1',
                'submission_unknown',
                'ambiguous'
            )
            """,
        )
        assert invalid_reconciliation_status_result.returncode != 0
        assert (
            "order_intents_reconciliation_status_check"
            in invalid_reconciliation_status_result.stderr
        )

        _run_psql(
            temp_database_url,
            "-c",
            """
            INSERT INTO order_intents (
                decision_id,
                strategy_id,
                strategy_version_id
            ) VALUES (
                'd-conflict',
                's1',
                'v1'
            )
            """,
        )
        conflict_result = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            WITH inserted AS (
                INSERT INTO order_intents (
                    decision_id,
                    strategy_id,
                    strategy_version_id
                ) VALUES (
                    'd-conflict',
                    's1',
                    'v1'
                )
                ON CONFLICT (decision_id) DO NOTHING
                RETURNING 1
            )
            SELECT COUNT(*)
            FROM inserted
            """,
        )
        assert conflict_result.stdout.strip() == "0"
        assert (
            _fetch_value(
                temp_database_url,
                """
                SELECT COUNT(*)
                FROM order_intents
                WHERE decision_id = 'd-conflict'
                """,
            )
            == "1"
        )
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )


def test_cp15_schema_sql_does_not_touch_outer_or_middle_ring_tables() -> None:
    schema_text = SCHEMA_PATH.read_text()
    outer_ring = _extract_block(schema_text, "-- BEGIN OUTER RING", "-- END OUTER RING")
    middle_ring = _extract_block(schema_text, "-- BEGIN MIDDLE RING", "-- END MIDDLE RING")
    inner_ring = _extract_block(
        schema_text,
        "-- BEGIN INNER-RING PRODUCT SHELLS",
        "-- END INNER-RING PRODUCT SHELLS",
    )
    touched_tables = (
        "markets",
        "tokens",
        "book_snapshots",
        "book_levels",
        "price_changes",
        "trades",
        "factors",
        "factor_values",
    )
    migration_text = (ROOT / "alembic" / "versions" / "0003_order_intents.py").read_text()

    assert "order_intents" in inner_ring
    assert "order_intents" not in outer_ring
    assert "order_intents" not in middle_ring
    for table_name in touched_tables:
        assert table_name not in migration_text
