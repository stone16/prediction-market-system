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
from pms.core.models import EvalRecord, Feedback, Opportunity
from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.storage.opportunity_store import OpportunityStore
from pms.storage.strategy_registry import PostgresStrategyRegistry


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


def _opportunity(opportunity_id: str = "op-cp02") -> Opportunity:
    return Opportunity(
        opportunity_id=opportunity_id,
        market_id="market-cp02",
        token_id="token-cp02",
        side="yes",
        selected_factor_values={"fair_value": 0.61},
        expected_edge=0.21,
        rationale="rules:edge",
        target_size_usdc=12.5,
        expiry=datetime(2026, 4, 30, tzinfo=UTC),
        staleness_policy="market_signal_freshness",
        strategy_id="default",
        strategy_version_id="default-v1",
        created_at=datetime(2026, 4, 19, tzinfo=UTC),
    )


def _strategy(*, drawdown_pct: float = 2.5) -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id="default",
            factor_composition=(
                FactorCompositionStep(
                    factor_id="factor-a",
                    role="weighted",
                    param="",
                    weight=0.6,
                    threshold=None,
                ),
                FactorCompositionStep(
                    factor_id="factor-b",
                    role="weighted",
                    param="",
                    weight=0.4,
                    threshold=None,
                ),
            ),
            metadata=(("owner", "system"), ("tier", "default")),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=drawdown_pct,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        forecaster=ForecasterSpec(
            forecasters=(
                ("rules", (("threshold", "0.55"),)),
                ("stats", (("window", "15m"),)),
            )
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )


async def _seed_active_default_strategy(pool: asyncpg.Pool) -> None:
    await PostgresStrategyRegistry(pool).create_version(_strategy())


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
    active_version = await PostgresStrategyRegistry(cast(Any, _SingleConnectionPool(db_conn))).create_version(
        _strategy()
    )
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
    assert stored_row["strategy_version_id"] == active_version.strategy_version_id


@pytest.mark.asyncio(loop_scope="session")
async def test_eval_store_persists_rows_in_postgres(
    db_conn: asyncpg.Connection,
) -> None:
    active_version = await PostgresStrategyRegistry(cast(Any, _SingleConnectionPool(db_conn))).create_version(
        _strategy()
    )
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
    assert stored_row["strategy_version_id"] == active_version.strategy_version_id


@pytest.mark.asyncio(loop_scope="session")
async def test_opportunity_store_persists_rows_in_postgres(
    db_conn: asyncpg.Connection,
) -> None:
    await db_conn.execute(
        """
        CREATE TABLE opportunities (
            opportunity_id TEXT PRIMARY KEY,
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            side TEXT NOT NULL,
            selected_factor_values JSONB NOT NULL,
            expected_edge DOUBLE PRECISION NOT NULL,
            rationale TEXT NOT NULL,
            target_size_usdc DOUBLE PRECISION NOT NULL,
            expiry TIMESTAMPTZ,
            staleness_policy TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            strategy_version_id TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    store = OpportunityStore(pool=cast(Any, _SingleConnectionPool(db_conn)))
    opportunity = _opportunity()

    await store.insert(opportunity)

    stored_row = await db_conn.fetchrow(
        """
        SELECT
            opportunity_id,
            market_id,
            token_id,
            side,
            selected_factor_values,
            expected_edge,
            rationale,
            target_size_usdc,
            expiry,
            staleness_policy,
            strategy_id,
            strategy_version_id,
            created_at
        FROM opportunities
        WHERE opportunity_id = $1
        """,
        opportunity.opportunity_id,
    )

    assert stored_row is not None
    assert stored_row["opportunity_id"] == opportunity.opportunity_id
    assert stored_row["market_id"] == opportunity.market_id
    assert stored_row["token_id"] == opportunity.token_id
    assert stored_row["side"] == opportunity.side
    assert stored_row["selected_factor_values"] == {"fair_value": 0.61}
    assert stored_row["expected_edge"] == pytest.approx(opportunity.expected_edge)
    assert stored_row["rationale"] == opportunity.rationale
    assert stored_row["target_size_usdc"] == pytest.approx(opportunity.target_size_usdc)
    assert stored_row["expiry"] == opportunity.expiry
    assert stored_row["staleness_policy"] == opportunity.staleness_policy
    assert stored_row["strategy_id"] == opportunity.strategy_id
    assert stored_row["strategy_version_id"] == opportunity.strategy_version_id
    assert stored_row["created_at"] == opportunity.created_at


@pytest.mark.asyncio(loop_scope="session")
async def test_jsonl_migration_script_copies_feedback_and_eval_rows(
    pg_pool: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    await _seed_active_default_strategy(pg_pool)
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
    await _seed_active_default_strategy(pg_pool)
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
