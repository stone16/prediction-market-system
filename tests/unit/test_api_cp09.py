from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import httpx
import pytest

from pms.api.app import create_app
from pms.config import PMSSettings, RiskSettings
from pms.core.enums import FeedbackSource, FeedbackTarget, OrderStatus, RunMode, Side
from pms.core.models import (
    EvalRecord,
    Feedback,
    FillRecord,
    MarketSignal,
    TradeDecision,
)
from pms.runner import Runner
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.BACKTEST,
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _runner_with_state() -> Runner:
    runner = Runner(
        config=_settings(),
        eval_store=EvalStore(path=None),
        feedback_store=FeedbackStore(path=None),
    )
    runner.state.runner_started_at = datetime(2026, 4, 14, tzinfo=UTC)
    runner.state.signals.append(_signal())
    runner.state.decisions.append(_decision())
    runner.state.fills.append(_fill())
    runner.eval_store.append(_eval_record())
    pending = _feedback("fb-pending")
    runner.feedback_store.append(pending)
    runner.feedback_store.append(replace(_feedback("fb-resolved"), resolved=True))
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
        price=0.4,
        size=12.5,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["model_id:model-a"],
        prob_estimate=0.7,
        expected_edge=0.3,
        time_in_force="GTC",
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
        fill_size=12.5,
        executed_at=now,
        filled_at=now,
        status=OrderStatus.MATCHED.value,
        anomaly_flags=[],
        resolved_outcome=1.0,
    )


def _eval_record() -> EvalRecord:
    return EvalRecord(
        market_id="api-market",
        decision_id="decision-api",
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
    assert status["controller"] == {"decisions_total": 1}
    assert status["actuator"] == {"fills_total": 1, "mode": "backtest"}
    assert status["evaluator"] == {"eval_records_total": 1, "brier_overall": 0.09}
    assert signals[0]["market_id"] == "api-market"
    assert decisions[0]["forecaster"] == "model-a"
    assert decisions[0]["prob_estimate"] == 0.7
    assert decisions[0]["expected_edge"] == 0.3
    assert decisions[0]["kelly_size"] == 12.5
    assert metrics["brier_overall"] == 0.09
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
