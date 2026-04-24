from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import httpx
import pytest

from pms.api.app import create_app
from pms.config import PMSSettings, RiskSettings
from pms.core.enums import FeedbackSource, FeedbackTarget, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import (
    EvalRecord,
    Feedback,
    FillRecord,
    MarketSignal,
    TradeDecision,
)
from pms.metrics import (
    MARKETS_SNAPSHOT_LAG_SECONDS_MAX_METRIC,
    SENSOR_DISCOVERY_PRICE_FIELDS_POPULATED_RATIO_METRIC,
    SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC,
    set_metric,
)
from pms.runner import Runner
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.storage.schema_check import SchemaVersionMismatchError
from tests.support.fake_stores import InMemoryEvalStore, InMemoryFeedbackStore


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.BACKTEST,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _stub_schema_check(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_schema_check(_pool: object) -> None:
        return

    monkeypatch.setattr("pms.api.app.ensure_schema_current", _noop_schema_check)


def _stub_orphan_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_scan(_pool: object) -> None:
        return

    monkeypatch.setattr("pms.api.app.scan_orphaned_backtest_runs", _noop_scan)


def _runner_with_state() -> Runner:
    pending = _feedback("fb-pending")
    runner = Runner(
        config=_settings(),
        eval_store=cast(EvalStore, InMemoryEvalStore([_eval_record()])),
        feedback_store=cast(
            FeedbackStore,
            InMemoryFeedbackStore(
                [
                    pending,
                    replace(_feedback("fb-resolved"), resolved=True),
                ]
            ),
        ),
    )
    runner.state.runner_started_at = datetime(2026, 4, 14, tzinfo=UTC)
    runner.state.signals.append(_signal())
    runner.state.decisions.append(_decision())
    runner.state.fills.append(_fill())
    return runner


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="api-market",
        token_id="yes-token",
        venue="polymarket",
        title="Will the API expose state?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 20, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"metaculus_prob": 0.7},
        fetched_at=datetime(2026, 4, 14, tzinfo=UTC),
        market_status="open",
    )


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="decision-api",
        market_id="api-market",
        token_id="yes-token",
        venue="polymarket",
        side=Side.BUY.value,
        limit_price=0.4,
        notional_usdc=12.5,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["min_volume:100.00"],
        prob_estimate=0.7,
        expected_edge=0.3,
        time_in_force=TimeInForce.GTC,
        opportunity_id="opportunity-api",
        strategy_id="default",
        strategy_version_id="default-v1",
        action=Side.BUY.value,
        model_id="model-a",
    )


def _fill() -> FillRecord:
    now = datetime(2026, 4, 14, tzinfo=UTC)
    return FillRecord(
        trade_id="trade-api",
        order_id="order-api",
        decision_id="decision-api",
        market_id="api-market",
        token_id="yes-token",
        venue="polymarket",
        side=Side.BUY.value,
        fill_price=0.41,
        fill_notional_usdc=12.5,
        fill_quantity=12.5 / 0.41,
        executed_at=now,
        filled_at=now,
        status=OrderStatus.MATCHED.value,
        anomaly_flags=[],
        strategy_id="default",
        strategy_version_id="default-v1",
        resolved_outcome=1.0,
    )


def _eval_record() -> EvalRecord:
    return EvalRecord(
        market_id="api-market",
        decision_id="decision-api",
        strategy_id="default",
        strategy_version_id="default-v1",
        prob_estimate=0.7,
        resolved_outcome=1.0,
        brier_score=0.09,
        fill_status=OrderStatus.MATCHED.value,
        recorded_at=datetime(2026, 4, 14, tzinfo=UTC),
        citations=["trade-api"],
        category="model-a",
        model_id="model-a",
        pnl=1.0,
        slippage_bps=10.0,
        filled=True,
    )


def _feedback(feedback_id: str) -> Feedback:
    return Feedback(
        feedback_id=feedback_id,
        target=FeedbackTarget.CONTROLLER.value,
        source=FeedbackSource.EVALUATOR.value,
        message="threshold crossed",
        severity="warning",
        created_at=datetime(2026, 4, 14, tzinfo=UTC),
        category="brier:model-a",
    )


@pytest.mark.asyncio
async def test_api_routes_expose_mock_runner_state() -> None:
    set_metric(SENSOR_DISCOVERY_PRICE_FIELDS_POPULATED_RATIO_METRIC, 0.625)
    set_metric(SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC, 3.0)
    set_metric(MARKETS_SNAPSHOT_LAG_SECONDS_MAX_METRIC, 12.5)
    app = create_app(_runner_with_state())
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        status = (await client.get("/status")).json()
        signals = (await client.get("/signals?limit=1")).json()
        decisions = (await client.get("/decisions?limit=1")).json()
        metrics = (await client.get("/metrics")).json()
        feedback = (await client.get("/feedback?resolved=false")).json()

    assert status["mode"] == "backtest"
    assert status["runner_started_at"] == "2026-04-14T00:00:00+00:00"
    assert status["sensors"][0].keys() == {"name", "status", "last_signal_at"}
    assert status["controller"] == {"decisions_total": 1, "diagnostics_total": 0}
    assert status["actuator"] == {"fills_total": 1, "mode": "backtest"}
    assert status["evaluator"] == {"eval_records_total": 1, "brier_overall": 0.09}
    assert signals[0]["market_id"] == "api-market"
    assert decisions[0]["forecaster"] == "model-a"
    assert decisions[0]["prob_estimate"] == 0.7
    assert decisions[0]["expected_edge"] == 0.3
    assert decisions[0]["action"] == "BUY"
    assert decisions[0]["limit_price"] == 0.4
    assert decisions[0]["kelly_size"] == 12.5
    assert metrics["brier_overall"] == 0.09
    assert metrics[SENSOR_DISCOVERY_PRICE_FIELDS_POPULATED_RATIO_METRIC] == 0.625
    assert metrics[SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC] == 3.0
    assert metrics[MARKETS_SNAPSHOT_LAG_SECONDS_MAX_METRIC] == 12.5
    assert metrics["ops_view"]["brier_overall"] == 0.09
    assert metrics["ops_view"]["pnl"] == 1.0
    assert metrics["per_strategy"] == [
        {
            "strategy_id": "default",
            "strategy_version_id": "default-v1",
            "record_count": 1,
            "insufficient_samples": False,
            "brier_overall": 0.09,
            "pnl": 1.0,
            "fill_rate": 1.0,
            "slippage_bps": 10.0,
            "drawdown": 0.0,
        }
    ]
    assert [item["feedback_id"] for item in feedback] == ["fb-pending"]


@pytest.mark.asyncio
async def test_api_feedback_resolve_and_config_errors() -> None:
    runner = _runner_with_state()
    app = create_app(runner)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resolved = await client.post("/feedback/fb-pending/resolve")
        missing = await client.post("/feedback/not-found/resolve")
        blocked_live = await client.post("/config", json={"mode": "live"})

    assert resolved.status_code == 200
    assert resolved.json()["resolved"] is True
    assert missing.status_code == 404
    assert blocked_live.status_code == 400
    assert blocked_live.json() == {
        "detail": "Live trading is disabled. Set live_trading_enabled=true in config."
    }


@pytest.mark.asyncio
async def test_api_run_start_stop_cycle(tmp_path: Path) -> None:
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.BACKTEST,
            auto_migrate_default_v2=False,
            risk=RiskSettings(
                max_position_per_market=1000.0,
                max_total_exposure=10_000.0,
            ),
        ),
        historical_data_path=FIXTURE_PATH,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )
    app = create_app(runner)
    transport = httpx.ASGITransport(app=app)

    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            initial_status = (await client.get("/status")).json()
            start_resp = await client.post("/run/start")
            conflict_resp = await client.post("/run/start")
            await runner.wait_until_idle()
            running_status = (await client.get("/status")).json()
            stop_resp = await client.post("/run/stop")
            stopped_status = (await client.get("/status")).json()
    finally:
        await runner.stop()

    assert initial_status["running"] is False
    assert start_resp.status_code == 200
    assert start_resp.json()["status"] == "started"
    assert start_resp.json()["mode"] == "backtest"
    assert conflict_resp.status_code == 409
    assert running_status["controller"]["decisions_total"] > 0
    assert stop_resp.status_code == 200
    assert stop_resp.json() == {"status": "stopped"}
    assert stopped_status["running"] is False


@pytest.mark.asyncio
async def test_api_auto_start_lifespan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_schema_check(monkeypatch)
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.BACKTEST,
            auto_migrate_default_v2=False,
            risk=RiskSettings(
                max_position_per_market=1000.0,
                max_total_exposure=10_000.0,
            ),
        ),
        historical_data_path=FIXTURE_PATH,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )
    app = create_app(runner, auto_start=True)

    async with app.router.lifespan_context(app):
        assert any(not task.done() for task in runner.tasks)
        await runner.wait_until_idle()

    assert all(task.done() for task in runner.tasks)


@pytest.mark.asyncio
async def test_api_auto_start_disabled_keeps_runner_idle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_schema_check(monkeypatch)
    runner = Runner(
        config=PMSSettings(mode=RunMode.BACKTEST, auto_migrate_default_v2=False),
        historical_data_path=FIXTURE_PATH,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )
    app = create_app(runner, auto_start=False)

    async with app.router.lifespan_context(app):
        assert runner.tasks == ()


@pytest.mark.asyncio
async def test_api_backtest_lifespan_skips_schema_check_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema_check_calls: list[object] = []

    async def _record_schema_check(pool: object) -> None:
        schema_check_calls.append(pool)

    monkeypatch.setattr("pms.api.app.ensure_schema_current", _record_schema_check)
    _stub_orphan_scan(monkeypatch)
    runner = Runner(
        config=PMSSettings(mode=RunMode.BACKTEST, auto_migrate_default_v2=False),
        historical_data_path=FIXTURE_PATH,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )
    app = create_app(runner, auto_start=False)

    async with app.router.lifespan_context(app):
        assert runner.pg_pool is not None

    assert schema_check_calls == []


@pytest.mark.asyncio
async def test_api_backtest_lifespan_can_opt_into_schema_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema_check_calls: list[object] = []

    async def _record_schema_check(pool: object) -> None:
        schema_check_calls.append(pool)

    monkeypatch.setattr("pms.api.app.ensure_schema_current", _record_schema_check)
    _stub_orphan_scan(monkeypatch)
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.BACKTEST,
            auto_migrate_default_v2=False,
            enforce_schema_check=True,
        ),
        historical_data_path=FIXTURE_PATH,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )
    app = create_app(runner, auto_start=False)

    async with app.router.lifespan_context(app):
        assert runner.pg_pool is not None

    assert len(schema_check_calls) == 1


@pytest.mark.asyncio
async def test_api_lifespan_closes_owned_pool_when_schema_check_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _TrackingPool:
        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    pool = _TrackingPool()

    async def _fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> _TrackingPool:
        del dsn, min_size, max_size
        return pool

    async def _raise_schema_check(_pool: object) -> None:
        raise SchemaVersionMismatchError("schema behind")

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", _fake_create_pool)
    monkeypatch.setattr("pms.api.app.ensure_schema_current", _raise_schema_check)
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.BACKTEST,
            auto_migrate_default_v2=False,
            enforce_schema_check=True,
        ),
        historical_data_path=FIXTURE_PATH,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )
    app = create_app(runner, auto_start=False)

    with pytest.raises(SchemaVersionMismatchError, match="schema behind"):
        async with app.router.lifespan_context(app):
            pass

    assert pool.close_calls == 1
    assert runner.pg_pool is None


@pytest.mark.asyncio
async def test_api_lifespan_stops_runner_started_via_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression for codex-bot C1: /run/start-triggered runner must also be stopped on shutdown."""
    _stub_schema_check(monkeypatch)
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.BACKTEST,
            auto_migrate_default_v2=False,
            risk=RiskSettings(
                max_position_per_market=1000.0,
                max_total_exposure=10_000.0,
            ),
        ),
        historical_data_path=FIXTURE_PATH,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )
    app = create_app(runner, auto_start=False)
    transport = httpx.ASGITransport(app=app)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            started = await client.post("/run/start")
        assert started.status_code == 200
        assert any(not task.done() for task in runner.tasks)

    assert all(task.done() for task in runner.tasks)
