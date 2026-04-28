from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest


ROOT = Path(__file__).resolve().parents[2]
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


def _run_psql(
    database_url: str,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["psql", database_url, "--set", "ON_ERROR_STOP=1", *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=check,
    )


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["PMS_AUTO_START"] = "0"
    env.pop("PMS_DATABASE_URL", None)
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_strategy_artifacts_schema_round_trip_and_constraints() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_strategy_artifacts_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr

        columns = _run_psql(
            temp_database_url,
            "-At",
            "-F",
            "|",
            "-c",
            """
            SELECT table_name, column_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN (
                'strategy_judgement_artifacts',
                'strategy_execution_artifacts'
              )
              AND column_name IN ('strategy_id', 'strategy_version_id')
            ORDER BY table_name ASC, column_name ASC
            """,
        )
        assert columns.stdout.splitlines() == [
            "strategy_execution_artifacts|strategy_id|NO",
            "strategy_execution_artifacts|strategy_version_id|NO",
            "strategy_judgement_artifacts|strategy_id|NO",
            "strategy_judgement_artifacts|strategy_version_id|NO",
        ]

        constraints = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT conname
            FROM pg_constraint
            WHERE conname IN (
                'strategy_judgement_artifacts_strategy_identity_check',
                'strategy_execution_artifacts_strategy_identity_check'
            )
            ORDER BY conname ASC
            """,
        )
        assert constraints.stdout.splitlines() == [
            "strategy_execution_artifacts_strategy_identity_check",
            "strategy_judgement_artifacts_strategy_identity_check",
        ]

        _run_psql(
            temp_database_url,
            "-c",
            """
            INSERT INTO strategy_judgement_artifacts (
                artifact_id,
                strategy_id,
                strategy_version_id,
                artifact_type,
                observation_refs,
                candidate_id,
                judgement_id,
                judgement_summary,
                evidence_refs,
                assumptions,
                rejection_reasons,
                intent_payload
            ) VALUES (
                'artifact-approved-intent',
                'default',
                'default-v1',
                'approved_intent',
                '["observation-1"]'::jsonb,
                'candidate-1',
                'judgement-1',
                'Approved intent summary',
                '["doc://edge-model"]'::jsonb,
                '["fees current"]'::jsonb,
                '[]'::jsonb,
                '{"intent_id":"intent-1"}'::jsonb
            )
            """,
        )
        _run_psql(
            temp_database_url,
            "-c",
            """
            INSERT INTO strategy_execution_artifacts (
                artifact_id,
                strategy_id,
                strategy_version_id,
                artifact_type,
                intent_id,
                plan_id,
                execution_policy,
                execution_plan_payload,
                risk_decision_payload,
                venue_response_ids,
                reconciliation_status,
                post_trade_status,
                evidence_refs,
                rejection_reasons
            ) VALUES (
                'artifact-rejected-plan',
                'default',
                'default-v1',
                'rejected_execution_plan',
                'intent-1',
                'plan-1',
                'all_or_none',
                '{"plan_id":"plan-1","rejection_reason":"stale_book"}'::jsonb,
                '{}'::jsonb,
                '[]'::jsonb,
                NULL,
                NULL,
                '["quote://book-1"]'::jsonb,
                '["stale_book"]'::jsonb
            )
            """,
        )

        failed = _run_psql(
            temp_database_url,
            "-c",
            """
            INSERT INTO strategy_execution_artifacts (
                artifact_id,
                strategy_id,
                strategy_version_id,
                artifact_type,
                intent_id,
                plan_id,
                execution_plan_payload,
                evidence_refs
            ) VALUES (
                'artifact-empty-strategy',
                '',
                '',
                'accepted_execution_plan',
                'intent-2',
                'plan-2',
                '{}'::jsonb,
                '["quote://book-2"]'::jsonb
            )
            """,
            check=False,
        )
        assert failed.returncode != 0
        assert "strategy_execution_artifacts_strategy_identity_check" in failed.stderr
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
