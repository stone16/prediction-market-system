from __future__ import annotations

import logging
import os
from collections import defaultdict
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, TypeVar, cast

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

from pms.api.research_routes import (
    compute_backtest_live_comparison,
    enqueue_backtest_runs,
    fetch_backtest_run,
    list_backtest_runs,
    list_backtest_strategy_runs,
    scan_orphaned_backtest_runs,
)
from pms.api.routes.factors import list_factor_catalog, list_factor_series
from pms.api.routes.feedback import list_feedback as list_feedback_items
from pms.api.routes.feedback import resolve_feedback as resolve_feedback_item
from pms.api.routes.signals import SignalDepthNotFoundError, get_signal_depth
from pms.api.routes.strategies import list_strategy_metrics as list_strategy_metrics_items
from pms.api.routes.strategies import list_strategies as list_strategies_items
from pms.core.enums import RunMode
from pms.core.models import EvalRecord, MarketSignal, TradeDecision
from pms.evaluation.metrics import MetricsCollector, MetricsSnapshot
from pms.runner import Runner
from pms.storage.market_data_store import PostgresMarketDataStore


T = TypeVar("T")
LIVE_DISABLED_DETAIL = (
    "Live trading is disabled. Set live_trading_enabled=true in config."
)
RUNNER_ALREADY_RUNNING_DETAIL = "Runner is already running."
logger = logging.getLogger(__name__)


class ConfigUpdate(BaseModel):
    mode: RunMode


class SubscriptionStateResponse(BaseModel):
    asset_ids: list[str]
    count: int
    last_updated_at: str | None


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
        elif not auto_start:
            await _ensure_runner_pool(active_runner)
        if active_runner.pg_pool is not None:
            await scan_orphaned_backtest_runs(active_runner.pg_pool)
        try:
            yield
        finally:
            # Always stop a running runner on shutdown — covers both auto_start
            # and runners launched by callers via POST /run/start, so sensor
            # resources (for example venue HTTP clients) close cleanly.
            if _is_runner_running(active_runner):
                await active_runner.stop()
            else:
                await _close_runner_pool(active_runner)

    app = FastAPI(title="PMS API", lifespan=lifespan)
    app.state.runner = active_runner

    @app.get("/status")
    async def status() -> dict[str, Any]:
        records = await active_runner.eval_store.all()
        metrics_snapshot = MetricsCollector(records).global_ops_snapshot()
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
                "eval_records_total": len(records),
                "brier_overall": metrics_snapshot.brier_overall,
            },
        }

    @app.get("/signals")
    async def signals(limit: int = 50) -> list[dict[str, Any]]:
        return [
            cast(dict[str, Any], _jsonable(signal))
            for signal in _latest(active_runner.state.signals, limit)
        ]

    @app.get("/signals/{market_id}/depth")
    async def signal_depth(
        market_id: str,
        limit: int = Query(default=20, ge=1, le=200),
    ) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        try:
            payload = await get_signal_depth(
                PostgresMarketDataStore(active_runner.pg_pool),
                market_id=market_id,
                limit=limit,
                stale_snapshot_threshold_s=active_runner.config.dashboard.stale_snapshot_threshold_s,
            )
        except SignalDepthNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return cast(dict[str, Any], _jsonable(payload))

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
        records = sorted(
            await active_runner.eval_store.all(),
            key=lambda record: (record.recorded_at, record.decision_id),
        )
        return _metrics_payload(records)

    @app.get("/feedback")
    async def feedback(resolved: bool | None = None) -> list[dict[str, Any]]:
        return [
            cast(dict[str, Any], _jsonable(item))
            for item in await list_feedback_items(active_runner.feedback_store, resolved=resolved)
        ]

    @app.get("/subscriptions")
    async def subscriptions() -> dict[str, Any]:
        subscription_controller = active_runner.subscription_controller
        if (
            active_runner.state.runner_started_at is None
            or active_runner.state.mode == RunMode.BACKTEST
            or subscription_controller is None
        ):
            response = SubscriptionStateResponse(
                asset_ids=[],
                count=0,
                last_updated_at=None,
            )
            return response.model_dump(mode="json")

        asset_ids = sorted(subscription_controller.current_asset_ids)
        response = SubscriptionStateResponse(
            asset_ids=asset_ids,
            count=len(asset_ids),
            last_updated_at=_jsonable(subscription_controller.last_updated_at),
        )
        return response.model_dump(mode="json")

    @app.get("/strategies")
    async def strategies() -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        return await list_strategies_items(active_runner.pg_pool)

    @app.get("/strategies/metrics")
    async def strategy_metrics() -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        return await list_strategy_metrics_items(active_runner.pg_pool)

    @app.get("/factors/catalog")
    async def factors_catalog() -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        return await list_factor_catalog(active_runner.pg_pool)

    @app.get("/factors")
    async def factors(
        factor_id: str,
        market_id: str,
        param: str = "",
        since: datetime | None = None,
        limit: int = Query(default=500, ge=1, le=2000),
    ) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        return await list_factor_series(
            active_runner.pg_pool,
            factor_id=factor_id,
            market_id=market_id,
            param=param,
            since=since,
            limit=limit,
        )

    @app.post("/research/backtest")
    async def create_backtest_run(request: Request) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        try:
            sweep_yaml = (await request.body()).decode("utf-8")
            return await enqueue_backtest_runs(active_runner.pg_pool, sweep_yaml)
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/research/backtest")
    async def get_backtest_runs(limit: int = Query(default=25, ge=1, le=100)) -> list[dict[str, Any]]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        return await list_backtest_runs(active_runner.pg_pool, limit=limit)

    @app.get("/research/backtest/{run_id}")
    async def get_backtest_run(run_id: str) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        payload = await fetch_backtest_run(active_runner.pg_pool, run_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Backtest run not found")
        return payload

    @app.get("/research/backtest/{run_id}/strategies")
    async def get_backtest_strategy_runs(run_id: str) -> list[dict[str, Any]]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        return await list_backtest_strategy_runs(active_runner.pg_pool, run_id)

    @app.post("/research/backtest/{run_id}/compare")
    async def compare_backtest_run(run_id: str, request: Request) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        try:
            return await compute_backtest_live_comparison(
                active_runner.pg_pool,
                run_id,
                await request.json(),
            )
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/feedback/{feedback_id}/resolve")
    async def resolve_feedback(feedback_id: str) -> dict[str, Any]:
        resolved = await resolve_feedback_item(active_runner.feedback_store, feedback_id)
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


def _metrics_payload(records: list[EvalRecord]) -> dict[str, Any]:
    collector = MetricsCollector(records)
    ops_view = _metrics_aggregate_payload(records, collector.global_ops_snapshot())
    strategy_snapshots = collector.snapshot_by_strategy()
    grouped_records: dict[tuple[str, str], list[EvalRecord]] = defaultdict(list)
    for record in records:
        grouped_records[(record.strategy_id, record.strategy_version_id)].append(record)

    per_strategy = []
    for key in sorted(strategy_snapshots):
        snapshot = strategy_snapshots[key]
        strategy_records = grouped_records[key]
        per_strategy.append(
            {
                "strategy_id": key[0],
                "strategy_version_id": key[1],
                "record_count": len(strategy_records),
                "insufficient_samples": len(strategy_records) == 0,
                "brier_overall": snapshot.brier_overall,
                "pnl": snapshot.pnl,
                "fill_rate": snapshot.fill_rate,
                "slippage_bps": snapshot.slippage_bps,
                "drawdown": _max_drawdown(strategy_records),
            }
        )

    payload = dict(ops_view)
    payload["per_strategy"] = per_strategy
    payload["ops_view"] = ops_view
    return payload


def _metrics_aggregate_payload(
    records: list[EvalRecord],
    snapshot: MetricsSnapshot,
) -> dict[str, Any]:
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
    cumulative_pnl = Decimal("0")
    pnl_series: list[dict[str, Any]] = []
    for record in records:
        cumulative_pnl += Decimal(str(record.pnl))
        pnl_series.append(
            {
                "recorded_at": record.recorded_at.isoformat(),
                "pnl": float(cumulative_pnl),
            }
        )
    payload["pnl_series"] = pnl_series
    return payload


def _max_drawdown(records: list[EvalRecord]) -> float:
    cumulative_pnl = Decimal("0")
    peak_equity = Decimal("0")
    max_drawdown = Decimal("0")
    for record in records:
        cumulative_pnl += Decimal(str(record.pnl))
        peak_equity = max(peak_equity, cumulative_pnl)
        max_drawdown = max(max_drawdown, peak_equity - cumulative_pnl)
    return float(max_drawdown)


def _is_runner_running(runner: Runner) -> bool:
    return any(not task.done() for task in runner.tasks)


async def _ensure_runner_pool(runner: Runner) -> None:
    await runner.ensure_pg_pool()


async def _close_runner_pool(runner: Runner) -> None:
    await runner.close_pg_pool()


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
    return "unknown" if decision.model_id is None else decision.model_id


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
