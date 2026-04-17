from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, cast

import asyncpg
import pytest

from pms.core.enums import FeedbackSource, FeedbackTarget, OrderStatus
from pms.core.models import EvalRecord, Feedback
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore


PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
    pytest.mark.skipif(
        PMS_TEST_DATABASE_URL is None,
        reason="set PMS_TEST_DATABASE_URL to the compose-backed PostgreSQL URI",
    ),
]


class _AcquireConnection:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> asyncpg.Connection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        return None


class _SingleConnectionPool:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    def acquire(self) -> _AcquireConnection:
        return _AcquireConnection(self._connection)


def _feedback(feedback_id: str) -> Feedback:
    return Feedback(
        feedback_id=feedback_id,
        target=FeedbackTarget.CONTROLLER.value,
        source=FeedbackSource.EVALUATOR.value,
        message="threshold crossed",
        severity="warning",
        created_at=datetime(2026, 4, 14, tzinfo=UTC),
        category="brier:model-a",
        metadata={"window": "cp09"},
    )


def _eval_record(decision_id: str) -> EvalRecord:
    return EvalRecord(
        market_id="market-cp09",
        decision_id=decision_id,
        prob_estimate=0.7,
        resolved_outcome=1.0,
        brier_score=0.09,
        fill_status=OrderStatus.MATCHED.value,
        recorded_at=datetime(2026, 4, 14, tzinfo=UTC),
        citations=["trade-cp09"],
        category="model-a",
        model_id="model-a",
        pnl=1.0,
        slippage_bps=10.0,
        filled=True,
    )


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")


def _run_migration(data_dir: Path) -> subprocess.CompletedProcess[str]:
    assert PMS_TEST_DATABASE_URL is not None
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    return subprocess.run(
        [
            sys.executable,
            "scripts/migrate_jsonl_to_pg.py",
            "--data-dir",
            str(data_dir),
            "--database-url",
            PMS_TEST_DATABASE_URL,
        ],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_feedback_store_persists_and_filters_rows_in_postgres(
    db_conn: asyncpg.Connection,
) -> None:
    store = FeedbackStore(pool=cast(Any, _SingleConnectionPool(db_conn)))

    await store.append(_feedback("fb-1"))
    await store.append(_feedback("fb-2"))
    await store.resolve("fb-1")

    unresolved = await store.list(resolved=False)
    resolved = await store.list(resolved=True)
    stored_row = await db_conn.fetchrow(
        """
        SELECT strategy_id, strategy_version_id
        FROM feedback
        WHERE feedback_id = $1
        """,
        "fb-1",
    )

    assert [item.feedback_id for item in unresolved] == ["fb-2"]
    assert [item.feedback_id for item in resolved] == ["fb-1"]
    assert resolved[0].resolved is True
    assert stored_row is not None
    assert stored_row["strategy_id"] == "default"
    assert stored_row["strategy_version_id"] == "default-v1"


@pytest.mark.asyncio(loop_scope="session")
async def test_eval_store_persists_rows_in_postgres(
    db_conn: asyncpg.Connection,
) -> None:
    store = EvalStore(pool=cast(Any, _SingleConnectionPool(db_conn)))

    await store.append(_eval_record("decision-cp09"))

    records = await store.all()
    stored_row = await db_conn.fetchrow(
        """
        SELECT strategy_id, strategy_version_id
        FROM eval_records
        WHERE decision_id = $1
        """,
        "decision-cp09",
    )

    assert [record.decision_id for record in records] == ["decision-cp09"]
    assert stored_row is not None
    assert stored_row["strategy_id"] == "default"
    assert stored_row["strategy_version_id"] == "default-v1"


@pytest.mark.asyncio(loop_scope="session")
async def test_jsonl_migration_script_copies_feedback_and_eval_rows(
    pg_pool: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / ".data"
    _write_jsonl(
        data_dir / "feedback.jsonl",
        [_jsonable(asdict(_feedback("fb-migrate")))],
    )
    _write_jsonl(
        data_dir / "eval_records.jsonl",
        [_jsonable(asdict(_eval_record("decision-migrate")))],
    )

    result = _run_migration(data_dir)

    assert result.returncode == 0, result.stderr
    async with pg_pool.acquire() as connection:
        feedback_count = await connection.fetchval("SELECT COUNT(*) FROM feedback")
        eval_count = await connection.fetchval("SELECT COUNT(*) FROM eval_records")
    assert feedback_count == 1
    assert eval_count == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_jsonl_migration_script_aborts_on_invalid_json_line(
    pg_pool: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / ".data"
    _write_jsonl(
        data_dir / "feedback.jsonl",
        [_jsonable(asdict(_feedback("fb-good")))],
    )
    with (data_dir / "eval_records.jsonl").open("w", encoding="utf-8") as stream:
        stream.write(json.dumps(_jsonable(asdict(_eval_record("decision-good")))) + "\n")
        stream.write("{bad json\n")

    result = _run_migration(data_dir)

    assert result.returncode != 0
    assert "eval_records.jsonl:2" in result.stderr
    async with pg_pool.acquire() as connection:
        feedback_count = await connection.fetchval("SELECT COUNT(*) FROM feedback")
        eval_count = await connection.fetchval("SELECT COUNT(*) FROM eval_records")
    assert feedback_count == 0
    assert eval_count == 0
