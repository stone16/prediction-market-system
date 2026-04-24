from __future__ import annotations

import asyncio
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
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from pms.api.auth import require_api_token
from pms.api.routes.decisions import (
    AcceptDecisionRequest,
    DecisionMarketChangedError,
    DecisionNotFoundError,
    DecisionRiskRejectedError,
    accept_decision as accept_decision_item,
    get_decision as get_decision_item,
    list_decisions as list_decisions_items,
)
from pms.api.routes.events import encode_sse_event
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
from pms.api.routes.markets import (
    MarketNotFoundError,
    MarketPriceHistoryNotFoundError,
    MarketsFilterParams,
    SubscribedFilter,
    get_market as get_market_item,
    get_price_history as get_price_history_item,
    list_markets as list_markets_items,
)
from pms.api.routes.market_subscriptions import (
    UnknownSubscriptionTokenError,
    subscribe_market as subscribe_market_item,
    unsubscribe_market as unsubscribe_market_item,
)
from pms.api.routes.positions import list_positions as list_positions_items
from pms.api.routes.share import SHARE_NOT_FOUND_DETAIL, get_shared_strategy
from pms.api.routes.signals import SignalDepthNotFoundError, get_signal_depth
from pms.api.routes.strategies import list_strategy_metrics as list_strategy_metrics_items
from pms.api.routes.strategies import list_strategies as list_strategies_items
from pms.api.routes.trades import list_trades as list_trades_items
from pms.config import PMSSettings
from pms.core.enums import RunMode
from pms.core.models import EvalRecord, MarketSignal, TradeDecision
from pms.evaluation.metrics import MetricsCollector, MetricsSnapshot
from pms.metrics import metrics_snapshot
from pms.runner import Runner
from pms.storage.schema_check import ensure_schema_current
from pms.storage.decision_store import DecisionStore
from pms.storage.market_data_store import PostgresMarketDataStore
from pms.storage.market_subscription_store import PostgresMarketSubscriptionStore


T = TypeVar("T")
LIVE_DISABLED_DETAIL = (
    "Live trading is disabled. Set live_trading_enabled=true in config."
)
RUNNER_ALREADY_RUNNING_DETAIL = "Runner is already running."
FIRST_TRADE_TIME_SECONDS_METRIC = "pms.ui.first_trade_time_seconds"
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
        pool_was_bound = active_runner.pg_pool is not None
        runner_pool_initialized = False
        startup_complete = False
        try:
            await _ensure_runner_pool(active_runner)
            runner_pool_initialized = active_runner.pg_pool is not None and not pool_was_bound
            if (
                active_runner.pg_pool is not None
                and _should_enforce_schema_check(active_runner.config)
            ):
                await ensure_schema_current(active_runner.pg_pool)
            if auto_start and not _is_runner_running(active_runner):
                logger.info(
                    "PMS_AUTO_START enabled — starting runner in %s mode",
                    active_runner.state.mode.value,
                )
                await active_runner.start()
            if active_runner.pg_pool is not None:
                await scan_orphaned_backtest_runs(active_runner.pg_pool)
            startup_complete = True
            yield
        finally:
            # Always stop a running runner on shutdown — covers both auto_start
            # and runners launched by callers via POST /run/start, so sensor
            # resources (for example venue HTTP clients) close cleanly.
            if startup_complete:
                if _is_runner_running(active_runner):
                    await active_runner.stop()
                else:
                    await _close_runner_pool(active_runner)
            elif runner_pool_initialized and not _is_runner_running(active_runner):
                await _close_runner_pool(active_runner)

    app = FastAPI(title="PMS API", lifespan=lifespan)
    app.state.runner = active_runner
    app.state.settings = active_runner.config

    @app.get("/status")
    async def status() -> dict[str, Any]:
        records = await active_runner.eval_store.all()
        metrics_snapshot = MetricsCollector(records).global_ops_snapshot()
        return {
            "mode": active_runner.state.mode.value,
            "runner_started_at": _jsonable(active_runner.state.runner_started_at),
            "running": _is_runner_running(active_runner),
            "sensors": _sensor_statuses(active_runner),
            "controller": {
                "decisions_total": len(active_runner.state.decisions),
                "diagnostics_total": len(active_runner.state.controller_diagnostics),
            },
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

    @app.get("/markets")
    async def markets(
        limit: int = Query(default=20, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
        q: str = Query(default=""),
        volume_min: float = Query(default=0.0, ge=0.0),
        liquidity_min: float = Query(default=0.0, ge=0.0),
        spread_max_bps: int | None = Query(default=None, ge=0),
        yes_min: float = Query(default=0.0, ge=0.0, le=1.0),
        yes_max: float = Query(default=1.0, ge=0.0, le=1.0),
        resolves_within_days: int | None = Query(default=None, ge=0),
        subscribed: SubscribedFilter = Query(default="all"),
    ) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        if yes_min > yes_max:
            raise HTTPException(
                status_code=422,
                detail="yes_min must be less than or equal to yes_max",
            )
        payload = await list_markets_items(
            PostgresMarketDataStore(active_runner.pg_pool),
            current_asset_ids=_current_subscription_asset_ids(active_runner),
            limit=limit,
            offset=offset,
            filters=MarketsFilterParams(
                q=q,
                volume_min=volume_min,
                liquidity_min=liquidity_min,
                spread_max_bps=spread_max_bps,
                yes_min=yes_min,
                yes_max=yes_max,
                resolves_within_days=resolves_within_days,
                subscribed=subscribed,
            ),
        )
        return payload.model_dump(mode="json")

    @app.get("/markets/{condition_id}")
    async def market_detail(condition_id: str) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        try:
            payload = await get_market_item(
                PostgresMarketDataStore(active_runner.pg_pool),
                current_asset_ids=_current_subscription_asset_ids(active_runner),
                market_id=condition_id,
            )
        except MarketNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Market not found") from exc
        return payload.model_dump(mode="json")

    @app.get("/markets/{condition_id}/price-history")
    async def market_price_history(
        condition_id: str,
        since: datetime | None = None,
        limit: int = Query(default=1440, ge=1),
    ) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        try:
            payload = await get_price_history_item(
                PostgresMarketDataStore(active_runner.pg_pool),
                condition_id=condition_id,
                since=since,
                limit=limit,
            )
        except MarketPriceHistoryNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Market not found") from exc
        return payload.model_dump(mode="json")

    @app.post(
        "/markets/{token_id}/subscribe",
        dependencies=[Depends(require_api_token)],
    )
    async def subscribe_market(token_id: str, request: Request) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        try:
            payload = await subscribe_market_item(
                PostgresMarketSubscriptionStore(active_runner.pg_pool),
                token_id=token_id,
                request_metadata=_request_metadata(request),
            )
        except UnknownSubscriptionTokenError as exc:
            raise HTTPException(status_code=404, detail="Token not found") from exc
        return payload.model_dump(mode="json")

    @app.delete(
        "/markets/{token_id}/subscribe",
        dependencies=[Depends(require_api_token)],
    )
    async def unsubscribe_market(token_id: str, request: Request) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        payload = await unsubscribe_market_item(
            PostgresMarketSubscriptionStore(active_runner.pg_pool),
            token_id=token_id,
            request_metadata=_request_metadata(request),
        )
        return payload.model_dump(mode="json")

    @app.get("/positions", dependencies=[Depends(require_api_token)])
    async def positions() -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        payload = await list_positions_items(active_runner.fill_store)
        return payload.model_dump(mode="json")

    @app.get("/trades", dependencies=[Depends(require_api_token)])
    async def trades(limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        payload = await list_trades_items(active_runner.fill_store, limit=limit)
        return payload.model_dump(mode="json")

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

    @app.get("/decisions", dependencies=[Depends(require_api_token)])
    async def decisions(
        limit: int = Query(default=50, ge=1, le=200),
        status: str | None = None,
        include: str | None = None,
    ) -> list[dict[str, Any]]:
        include_opportunity = include == "opportunity"
        if _should_use_durable_decisions(active_runner):
            rows = await list_decisions_items(
                cast(Any, active_runner.decision_store),
                limit=limit,
                status=status,
                include_opportunity=include_opportunity,
            )
            return [item.model_dump(mode="json") for item in rows]

        payloads: list[dict[str, Any]] = []
        for decision in _latest(active_runner.state.decisions, limit):
            payload = cast(dict[str, Any], _jsonable(decision))
            payload["forecaster"] = _forecaster(decision)
            payload["kelly_size"] = decision.notional_usdc
            payloads.append(payload)
        return payloads

    @app.get("/decisions/{decision_id}", dependencies=[Depends(require_api_token)])
    async def decision_detail(
        decision_id: str,
        include: str | None = None,
    ) -> dict[str, Any]:
        if _should_use_durable_decisions(active_runner):
            row = await get_decision_item(
                cast(Any, active_runner.decision_store),
                decision_id=decision_id,
                include_opportunity=include == "opportunity",
            )
            if row is None:
                raise HTTPException(status_code=404, detail="Decision not found")
            return row.model_dump(mode="json")

        for decision in active_runner.state.decisions:
            if decision.decision_id != decision_id:
                continue
            payload = cast(dict[str, Any], _jsonable(decision))
            payload["forecaster"] = _forecaster(decision)
            payload["kelly_size"] = decision.notional_usdc
            return payload
        raise HTTPException(status_code=404, detail="Decision not found")

    @app.post(
        "/decisions/{decision_id}/accept",
        dependencies=[Depends(require_api_token)],
    )
    async def accept_decision(
        decision_id: str,
        body: AcceptDecisionRequest,
    ) -> Any:
        try:
            payload = await accept_decision_item(
                cast(Any, active_runner.decision_store),
                decision_id=decision_id,
                factor_snapshot_hash=body.factor_snapshot_hash,
                dedup_store=cast(Any, active_runner.actuator_executor.dedup_store),
                risk=cast(Any, active_runner.actuator_executor.risk),
                portfolio=active_runner.portfolio,
                enqueue=active_runner.enqueue_accepted_decision,
            )
        except DecisionNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except DecisionMarketChangedError as exc:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": str(exc),
                    "current_factor_snapshot_hash": exc.current_factor_snapshot_hash,
                },
            )
        except DecisionRiskRejectedError as exc:
            raise HTTPException(status_code=422, detail=exc.reason) from exc
        return payload.model_dump(mode="json")

    @app.get("/metrics")
    async def metrics() -> dict[str, Any]:
        records = sorted(
            await active_runner.eval_store.all(),
            key=lambda record: (record.recorded_at, record.decision_id),
        )
        first_trade_time_seconds = await _first_trade_time_seconds(active_runner.pg_pool)
        return _metrics_payload(
            records,
            first_trade_time_seconds=first_trade_time_seconds,
        )

    @app.get("/feedback")
    async def feedback(resolved: bool | None = None) -> list[dict[str, Any]]:
        return [
            cast(dict[str, Any], _jsonable(item))
            for item in await list_feedback_items(active_runner.feedback_store, resolved=resolved)
        ]

    @app.get("/stream/events")
    async def stream_events(
        request: Request,
        last_event_id: int | None = Query(default=None, ge=0),
    ) -> StreamingResponse:
        async def event_generator() -> AsyncIterator[str]:
            replay, subscriber = await active_runner.event_bus.subscribe(
                last_event_id=last_event_id
            )
            try:
                for event in replay:
                    yield encode_sse_event(event)

                while True:
                    if await request.is_disconnected():
                        return
                    try:
                        event = await asyncio.wait_for(subscriber.get(), timeout=10.0)
                    except TimeoutError:
                        yield ": keepalive\n\n"
                        continue
                    yield encode_sse_event(event)
            finally:
                await active_runner.event_bus.unsubscribe(subscriber)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "cache-control": "no-cache",
                "connection": "keep-alive",
            },
        )

    @app.get("/subscriptions")
    async def subscriptions() -> dict[str, Any]:
        subscription_controller = active_runner.subscription_controller
        asset_ids = sorted(_current_subscription_asset_ids(active_runner))
        response = SubscriptionStateResponse(
            asset_ids=asset_ids,
            count=len(asset_ids),
            last_updated_at=(
                _jsonable(subscription_controller.last_updated_at)
                if subscription_controller is not None and asset_ids
                else None
            ),
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

    @app.get("/share/{strategy_id}")
    async def share_strategy(strategy_id: str) -> dict[str, Any]:
        if active_runner.pg_pool is None:
            raise HTTPException(status_code=503, detail="Runner PostgreSQL pool is not initialized")
        payload = await get_shared_strategy(active_runner.pg_pool, strategy_id)
        if payload is None:
            raise HTTPException(status_code=404, detail=SHARE_NOT_FOUND_DETAIL)
        return payload.model_dump(mode="json")

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

    @app.post("/research/backtest", dependencies=[Depends(require_api_token)])
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

    @app.post(
        "/research/backtest/{run_id}/compare",
        dependencies=[Depends(require_api_token)],
    )
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

    @app.post(
        "/feedback/{feedback_id}/resolve",
        dependencies=[Depends(require_api_token)],
    )
    async def resolve_feedback(feedback_id: str) -> dict[str, Any]:
        resolved = await resolve_feedback_item(active_runner.feedback_store, feedback_id)
        if resolved is None:
            raise HTTPException(status_code=404, detail="Feedback not found")
        return cast(dict[str, Any], _jsonable(resolved))

    @app.post("/config", dependencies=[Depends(require_api_token)])
    async def update_config(update: ConfigUpdate) -> dict[str, str]:
        if update.mode == RunMode.LIVE and not active_runner.config.live_trading_enabled:
            raise HTTPException(status_code=400, detail=LIVE_DISABLED_DETAIL)
        active_runner.switch_mode(update.mode)
        return {"mode": active_runner.state.mode.value}

    @app.post("/run/start", dependencies=[Depends(require_api_token)])
    async def run_start() -> dict[str, Any]:
        if _is_runner_running(active_runner):
            raise HTTPException(status_code=409, detail=RUNNER_ALREADY_RUNNING_DETAIL)
        await active_runner.start()
        return {
            "status": "started",
            "mode": active_runner.state.mode.value,
            "runner_started_at": _jsonable(active_runner.state.runner_started_at),
        }

    @app.post("/run/stop", dependencies=[Depends(require_api_token)])
    async def run_stop() -> dict[str, Any]:
        await active_runner.stop()
        return {"status": "stopped"}

    return app


def _latest(items: Sequence[T], limit: int) -> list[T]:
    bounded_limit = max(limit, 0)
    if bounded_limit == 0:
        return []
    return list(items[-bounded_limit:])


def _metrics_payload(
    records: list[EvalRecord],
    *,
    first_trade_time_seconds: float | None = None,
) -> dict[str, Any]:
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
    payload[FIRST_TRADE_TIME_SECONDS_METRIC] = first_trade_time_seconds
    payload.update(metrics_snapshot())
    payload["per_strategy"] = per_strategy
    payload["ops_view"] = ops_view
    return payload


async def _first_trade_time_seconds(pg_pool: asyncpg.Pool | None) -> float | None:
    if pg_pool is None:
        return None

    async with pg_pool.acquire() as connection:
        fill_payloads_table_exists = await connection.fetchval(
            "SELECT to_regclass('public.fill_payloads') IS NOT NULL"
        )
        if not fill_payloads_table_exists:
            return None

        seconds = await connection.fetchval(
            """
            WITH first_fills AS (
                SELECT
                    decisions.decision_id,
                    EXTRACT(EPOCH FROM MIN(fills.ts) - decisions.created_at) AS elapsed_seconds
                FROM decisions
                INNER JOIN fill_payloads
                    ON fill_payloads.payload->>'decision_id' = decisions.decision_id
                INNER JOIN fills
                    ON fills.fill_id = fill_payloads.fill_id
                WHERE fills.ts >= decisions.created_at
                GROUP BY decisions.decision_id, decisions.created_at
            )
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY elapsed_seconds)
            FROM first_fills
            """
        )

    if seconds is None:
        return None
    return float(seconds)


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


def _should_use_durable_decisions(runner: Runner) -> bool:
    store = runner.decision_store
    if not hasattr(store, "read_decisions") or not hasattr(store, "get_decision"):
        return False
    if isinstance(store, DecisionStore) and store.pool is None:
        return False
    return True


def _is_runner_running(runner: Runner) -> bool:
    return any(not task.done() for task in runner.tasks)


async def _ensure_runner_pool(runner: Runner) -> None:
    await runner.ensure_pg_pool()


async def _close_runner_pool(runner: Runner) -> None:
    await runner.close_pg_pool()


def _should_enforce_schema_check(settings: PMSSettings) -> bool:
    if settings.enforce_schema_check is not None:
        return settings.enforce_schema_check
    return settings.mode in {RunMode.PAPER, RunMode.LIVE}


def _current_subscription_asset_ids(runner: Runner) -> frozenset[str]:
    subscription_controller = runner.subscription_controller
    if (
        runner.state.runner_started_at is None
        or runner.state.mode == RunMode.BACKTEST
        or subscription_controller is None
        or not _is_runner_running(runner)
    ):
        return frozenset()
    return subscription_controller.current_asset_ids


def _request_metadata(request: Request) -> dict[str, object]:
    return {
        "request_method": request.method,
        "request_path": request.url.path,
    }


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
