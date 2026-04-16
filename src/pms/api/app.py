from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any, TypeVar, cast

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from pms.core.enums import RunMode
from pms.core.models import MarketSignal, TradeDecision
from pms.evaluation.metrics import MetricsCollector, MetricsSnapshot
from pms.runner import Runner


T = TypeVar("T")
LIVE_DISABLED_DETAIL = (
    "Live trading is disabled. Set live_trading_enabled=true in config."
)
RUNNER_ALREADY_RUNNING_DETAIL = "Runner is already running."
logger = logging.getLogger(__name__)


class ConfigUpdate(BaseModel):
    mode: RunMode


def create_app(
    runner: Runner | None = None,
    *,
    auto_start: bool | None = None,
) -> FastAPI:
    active_runner = runner or Runner()
    if auto_start is None:
        auto_start = os.environ.get("PMS_AUTO_START", "").lower() in {"1", "true", "yes"}

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if auto_start and not _is_runner_running(active_runner):
            logger.info("PMS_AUTO_START enabled — starting runner in %s mode", active_runner.state.mode.value)
            await active_runner.start()
        try:
            yield
        finally:
            # Always stop a running runner on shutdown — covers both auto_start
            # and runners launched by callers via POST /run/start, so sensor
            # resources (for example venue HTTP clients) close cleanly.
            if _is_runner_running(active_runner):
                await active_runner.stop()

    app = FastAPI(title="PMS API", lifespan=lifespan)
    app.state.runner = active_runner

    @app.get("/status")
    async def status() -> dict[str, Any]:
        metrics_snapshot = _metrics(active_runner)
        return {
            "mode": active_runner.state.mode.value,
            "runner_started_at": _jsonable(active_runner.state.runner_started_at),
            "running": _is_runner_running(active_runner),
            "sensors": _sensor_statuses(active_runner),
            "controller": {"decisions_total": len(active_runner.state.decisions)},
            "actuator": {
                "fills_total": len(active_runner.state.fills),
                "mode": active_runner.state.mode.value,
            },
            "evaluator": {
                "eval_records_total": len(active_runner.eval_store.all()),
                "brier_overall": metrics_snapshot.brier_overall,
            },
        }

    @app.get("/signals")
    async def signals(limit: int = 50) -> list[dict[str, Any]]:
        return [
            cast(dict[str, Any], _jsonable(signal))
            for signal in _latest(active_runner.state.signals, limit)
        ]

    @app.get("/decisions")
    async def decisions(limit: int = 50) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for decision in _latest(active_runner.state.decisions, limit):
            payload = cast(dict[str, Any], _jsonable(decision))
            payload["forecaster"] = _forecaster(decision)
            payload["kelly_size"] = decision.size
            payloads.append(payload)
        return payloads

    @app.get("/metrics")
    async def metrics() -> dict[str, Any]:
        snapshot = _metrics(active_runner)
        records = sorted(
            active_runner.eval_store.all(),
            key=lambda record: record.recorded_at,
        )
        payload = cast(dict[str, Any], _jsonable(snapshot))
        payload["brier_series"] = [
            {
                "recorded_at": record.recorded_at.isoformat(),
                "brier_score": record.brier_score,
            }
            for record in records
        ]
        payload["calibration_curve"] = [
            {
                "prob_estimate": record.prob_estimate,
                "resolved_outcome": record.resolved_outcome,
            }
            for record in records
        ]
        cumulative_pnl = 0.0
        pnl_series: list[dict[str, Any]] = []
        for record in records:
            cumulative_pnl += record.pnl
            pnl_series.append(
                {
                    "recorded_at": record.recorded_at.isoformat(),
                    "pnl": cumulative_pnl,
                }
            )
        payload["pnl_series"] = pnl_series
        return payload

    @app.get("/feedback")
    async def feedback(resolved: bool | None = None) -> list[dict[str, Any]]:
        return [
            cast(dict[str, Any], _jsonable(item))
            for item in active_runner.feedback_store.list(resolved=resolved)
        ]

    @app.post("/feedback/{feedback_id}/resolve")
    async def resolve_feedback(feedback_id: str) -> dict[str, Any]:
        resolved = active_runner.feedback_store.resolve(feedback_id)
        if resolved is None:
            raise HTTPException(status_code=404, detail="Feedback not found")
        return cast(dict[str, Any], _jsonable(resolved))

    @app.post("/config")
    async def update_config(update: ConfigUpdate) -> dict[str, str]:
        if update.mode == RunMode.LIVE and not active_runner.config.live_trading_enabled:
            raise HTTPException(status_code=400, detail=LIVE_DISABLED_DETAIL)
        active_runner.switch_mode(update.mode)
        return {"mode": active_runner.state.mode.value}

    @app.post("/run/start")
    async def run_start() -> dict[str, Any]:
        if _is_runner_running(active_runner):
            raise HTTPException(status_code=409, detail=RUNNER_ALREADY_RUNNING_DETAIL)
        await active_runner.start()
        return {
            "status": "started",
            "mode": active_runner.state.mode.value,
            "runner_started_at": _jsonable(active_runner.state.runner_started_at),
        }

    @app.post("/run/stop")
    async def run_stop() -> dict[str, Any]:
        await active_runner.stop()
        return {"status": "stopped"}

    return app


def _latest(items: Sequence[T], limit: int) -> list[T]:
    bounded_limit = max(limit, 0)
    if bounded_limit == 0:
        return []
    return list(items[-bounded_limit:])


def _metrics(runner: Runner) -> MetricsSnapshot:
    return MetricsCollector(runner.eval_store.all()).snapshot()


def _is_runner_running(runner: Runner) -> bool:
    return any(not task.done() for task in runner.tasks)


def _sensor_statuses(runner: Runner) -> list[dict[str, Any]]:
    last_signal_at = _last_signal_at(runner.state.signals)
    if not runner.active_sensors:
        return [
            {
                "name": "unknown",
                "status": "stopped"
                if runner.state.runner_started_at is None
                else "idle",
                "last_signal_at": last_signal_at,
            }
        ]

    running = any(not task.done() for task in runner.sensor_stream.tasks)
    status = "running" if running else "idle"
    return [
        {
            "name": sensor.__class__.__name__,
            "status": status,
            "last_signal_at": last_signal_at,
        }
        for sensor in runner.active_sensors
    ]


def _last_signal_at(signals: Sequence[MarketSignal]) -> str | None:
    if not signals:
        return None
    return signals[-1].fetched_at.isoformat()


def _forecaster(decision: TradeDecision) -> str:
    for condition in decision.stop_conditions:
        if condition.startswith("model_id:"):
            return condition.removeprefix("model_id:")
    return "unknown"


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value
