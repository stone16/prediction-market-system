from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, TypeVar, runtime_checkable

import asyncpg
import httpx
from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.adapters.polymarket import PolymarketActuator
from pms.actuator.executor import ActuatorAdapter, ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import PMSSettings
from pms.controller.pipeline import ControllerPipeline
from pms.core.enums import OrderStatus, RunMode
from pms.core.interfaces import ISensor
from pms.core.models import (
    FillRecord,
    MarketSignal,
    OrderState,
    Portfolio,
    Position,
    TradeDecision,
)
from pms.evaluation.adapters.scoring import Scorer
from pms.evaluation.spool import EvalSpool
from pms.sensor.adapters.historical import HistoricalSensor
from pms.sensor.adapters.market_data import MarketDataSensor
from pms.sensor.adapters.market_discovery import MarketDiscoverySensor
from pms.sensor.stream import SensorStream
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.storage.market_data_store import PostgresMarketDataStore


logger = logging.getLogger(__name__)

DEFAULT_BACKTEST_FIXTURE = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")
RUNNER_STATE_LIMIT = 1000
T = TypeVar("T")


@runtime_checkable
class AsyncCloseable(Protocol):
    async def aclose(self) -> None: ...


@dataclass
class RunnerState:
    mode: RunMode
    runner_started_at: datetime | None = None
    signals: list[MarketSignal] = field(default_factory=list)
    decisions: list[TradeDecision] = field(default_factory=list)
    orders: list[OrderState] = field(default_factory=list)
    fills: list[FillRecord] = field(default_factory=list)


@dataclass
class Runner:
    config: PMSSettings = field(default_factory=PMSSettings)
    historical_data_path: Path = DEFAULT_BACKTEST_FIXTURE
    sensors: Sequence[ISensor] | None = None
    eval_store: EvalStore = field(default_factory=EvalStore)
    feedback_store: FeedbackStore = field(default_factory=FeedbackStore)
    portfolio: Portfolio = field(default_factory=lambda: Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
    ))
    sensor_stream: SensorStream = field(default_factory=SensorStream)
    controller: ControllerPipeline | None = None
    state: RunnerState = field(init=False)
    actuator_executor: ActuatorExecutor = field(init=False)
    _evaluator_spool: EvalSpool = field(init=False)
    _decision_queue: asyncio.Queue[tuple[TradeDecision, MarketSignal]] = field(
        init=False,
    )
    _stop_event: asyncio.Event = field(init=False)
    _controller_task: asyncio.Task[None] | None = field(init=False, default=None)
    _actuator_task: asyncio.Task[None] | None = field(init=False, default=None)
    _task: asyncio.Task[None] | None = field(init=False, default=None)
    _pg_pool: asyncpg.Pool | None = field(init=False, default=None)
    _active_sensors: tuple[ISensor, ...] = field(init=False, default=())

    def __post_init__(self) -> None:
        self.state = RunnerState(mode=self.config.mode)
        self.controller = self.controller or ControllerPipeline(settings=self.config)
        self._evaluator_spool = EvalSpool(store=self.eval_store, scorer=Scorer())
        self._decision_queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self.actuator_executor = self._build_executor(self.config.mode)

    @property
    def controller_task(self) -> asyncio.Task[None] | None:
        return self._controller_task

    @property
    def actuator_task(self) -> asyncio.Task[None] | None:
        return self._actuator_task

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    @property
    def pg_pool(self) -> asyncpg.Pool | None:
        return self._pg_pool

    @property
    def evaluator_task(self) -> asyncio.Task[None] | None:
        return self._evaluator_spool._task

    @property
    def active_sensors(self) -> tuple[ISensor, ...]:
        return self._active_sensors

    @property
    def tasks(self) -> tuple[asyncio.Task[None], ...]:
        tasks: list[asyncio.Task[None]] = []
        tasks.extend(self.sensor_stream.tasks)
        if self._controller_task is not None:
            tasks.append(self._controller_task)
        if self._actuator_task is not None:
            tasks.append(self._actuator_task)
        if self.evaluator_task is not None:
            tasks.append(self.evaluator_task)
        return tuple(tasks)

    async def start(self) -> None:
        if any(not task.done() for task in self.tasks):
            msg = "Runner is already started"
            raise RuntimeError(msg)

        self._stop_event.clear()
        self.state.runner_started_at = datetime.now(tz=UTC)
        self._pg_pool = await asyncpg.create_pool(
            dsn=self.config.database.dsn,
            min_size=self.config.database.pool_min_size,
            max_size=self.config.database.pool_max_size,
        )

        try:
            self._assert_no_legacy_jsonl_paths()
            self._bind_runtime_stores()
            self._active_sensors = self._build_sensors()
            await self.sensor_stream.start(self._active_sensors)
            await self._evaluator_spool.start()
            self._controller_task = asyncio.create_task(self._controller_loop())
            self._actuator_task = asyncio.create_task(self._actuator_loop())
        except Exception:
            await self._cleanup_after_start_failure()
            raise

    async def stop(self) -> None:
        self._stop_event.set()
        error: BaseException | None = None

        try:
            await self.sensor_stream.stop()
        except BaseException as exc:  # pragma: no cover - exercised via unit tests
            error = exc

        for task in (self._controller_task, self._actuator_task):
            if task is not None and not task.done():
                task.cancel()

        try:
            await asyncio.gather(
                *(task for task in (self._controller_task, self._actuator_task) if task),
                return_exceptions=True,
            )
        except BaseException as exc:  # pragma: no cover - defensive
            if error is None:
                error = exc

        try:
            await self._evaluator_spool.stop()
        except BaseException as exc:  # pragma: no cover - defensive
            if error is None:
                error = exc

        try:
            await self._close_active_sensors()
        except BaseException as exc:  # pragma: no cover - defensive
            if error is None:
                error = exc

        self._controller_task = None
        self._actuator_task = None

        try:
            await self._close_pg_pool()
        except BaseException as exc:  # pragma: no cover - defensive
            if error is None:
                error = exc

        if error is not None:
            raise error

    async def run(self) -> None:
        self._task = asyncio.current_task()
        try:
            await self.start()
            await self.wait_until_idle()
        finally:
            try:
                await self.stop()
            finally:
                self._task = None

    def switch_mode(self, new_mode: RunMode) -> None:
        self.config.mode = new_mode
        self.state.mode = new_mode
        self.actuator_executor = self._build_executor(new_mode)

    async def wait_until_idle(self) -> None:
        if self.sensor_stream.tasks:
            await asyncio.gather(*self.sensor_stream.tasks, return_exceptions=True)
        await self.sensor_stream.queue.join()
        if self._controller_task is not None:
            await self._controller_task
        await self._decision_queue.join()
        if self._actuator_task is not None:
            await self._actuator_task
        await self._evaluator_spool.join()

    async def wait_for_signals(self, count: int) -> None:
        while len(self.state.signals) < count:
            await asyncio.sleep(0.1)

    def _build_sensors(self) -> tuple[ISensor, ...]:
        if self.sensors is not None:
            return tuple(self.sensors)
        if self.config.mode == RunMode.BACKTEST:
            return (HistoricalSensor(self.historical_data_path),)
        if self._pg_pool is None:
            msg = "Runner PostgreSQL pool is not initialized"
            raise RuntimeError(msg)
        market_data_sensor = MarketDataSensor(
            store=PostgresMarketDataStore(self._pg_pool),
            asset_ids=[],
        )
        market_data_sensor.max_reconnect_interval_s = (
            self.config.sensor.max_reconnect_interval_s
        )
        return (
            MarketDiscoverySensor(
                store=PostgresMarketDataStore(self._pg_pool),
                http_client=httpx.AsyncClient(
                    base_url="https://gamma-api.polymarket.com"
                ),
                poll_interval_s=self.config.sensor.poll_interval_s,
            ),
            market_data_sensor,
        )

    def _assert_no_legacy_jsonl_paths(self) -> None:
        for store in (self.eval_store, self.feedback_store):
            if isinstance(getattr(store, "path", None), Path):
                msg = "legacy JSONL path referenced"
                raise RuntimeError(msg)

    def _bind_runtime_stores(self) -> None:
        if self._pg_pool is None:
            msg = "Runner PostgreSQL pool is not initialized"
            raise RuntimeError(msg)
        if isinstance(self.eval_store, EvalStore):
            self.eval_store.bind_pool(self._pg_pool)
        if isinstance(self.feedback_store, FeedbackStore):
            self.feedback_store.bind_pool(self._pg_pool)

    def _build_executor(self, mode: RunMode) -> ActuatorExecutor:
        return ActuatorExecutor(
            adapter=self._build_adapter(mode),
            risk=RiskManager(self.config.risk),
            feedback=ActuatorFeedback(self.feedback_store),
        )

    def _build_adapter(self, mode: RunMode) -> ActuatorAdapter:
        if mode == RunMode.BACKTEST:
            return BacktestActuator(self.historical_data_path)
        if mode == RunMode.PAPER:
            return PaperActuator()
        return PolymarketActuator(self.config)

    async def _controller_loop(self) -> None:
        controller = self.controller
        if controller is None:
            msg = "Runner controller is not initialized"
            raise RuntimeError(msg)
        while True:
            if self._should_stop_controller():
                return
            try:
                signal = await asyncio.wait_for(self.sensor_stream.queue.get(), 0.05)
            except TimeoutError:
                continue

            try:
                _append_bounded(self.state.signals, signal)
                decision = await controller.decide(signal, portfolio=self.portfolio)
                if decision is not None:
                    _append_bounded(self.state.decisions, decision)
                    await self._decision_queue.put((decision, signal))
            finally:
                self.sensor_stream.queue.task_done()

    async def _actuator_loop(self) -> None:
        while True:
            if self._should_stop_actuator():
                return
            try:
                decision, signal = await asyncio.wait_for(self._decision_queue.get(), 0.05)
            except TimeoutError:
                continue

            try:
                order_state = await self.actuator_executor.execute(
                    decision,
                    self.portfolio,
                )
                _append_bounded(self.state.orders, order_state)
                fill = _fill_from_order(order_state, decision, signal)
                if fill is not None:
                    _append_bounded(self.state.fills, fill)
                    self.portfolio = _portfolio_with_fill(self.portfolio, fill)
                    self._evaluator_spool.enqueue(fill, decision)
            except Exception as error:
                logger.warning("actuator execution failed: %s", error)
            finally:
                self._decision_queue.task_done()

    def _should_stop_controller(self) -> bool:
        if self._stop_event.is_set() and self.sensor_stream.queue.empty():
            return True
        return self._sensors_finished() and self.sensor_stream.queue.empty()

    def _should_stop_actuator(self) -> bool:
        if self._stop_event.is_set() and self._decision_queue.empty():
            return True
        controller_done = self._controller_task is not None and self._controller_task.done()
        return controller_done and self._decision_queue.empty()

    def _sensors_finished(self) -> bool:
        return bool(self.sensor_stream.tasks) and all(
            task.done() for task in self.sensor_stream.tasks
        )

    async def _close_active_sensors(self) -> None:
        for sensor in self._active_sensors:
            if isinstance(sensor, AsyncCloseable):
                await sensor.aclose()
        self._active_sensors = ()

    async def _close_pg_pool(self) -> None:
        if self._pg_pool is None:
            return
        pool = self._pg_pool
        self._pg_pool = None
        await pool.close()

    async def _cleanup_after_start_failure(self) -> None:
        stop_error: BaseException | None = None
        try:
            await self.sensor_stream.stop()
        except BaseException as exc:  # pragma: no cover - defensive
            stop_error = exc

        try:
            await self._evaluator_spool.stop()
        except BaseException as exc:  # pragma: no cover - defensive
            if stop_error is None:
                stop_error = exc

        try:
            await self._close_active_sensors()
        except BaseException as exc:  # pragma: no cover - defensive
            if stop_error is None:
                stop_error = exc

        self._controller_task = None
        self._actuator_task = None
        await self._close_pg_pool()

        if stop_error is not None:
            raise stop_error


def _append_bounded(items: list[T], item: T) -> None:
    items.append(item)
    overflow = len(items) - RUNNER_STATE_LIMIT
    if overflow > 0:
        del items[:overflow]


def _fill_from_order(
    order_state: OrderState,
    decision: TradeDecision,
    signal: MarketSignal,
) -> FillRecord | None:
    if order_state.status != OrderStatus.MATCHED.value or order_state.fill_price is None:
        return None
    if order_state.filled_size <= 0.0:
        return None
    if order_state.fill_price <= 0.0:
        return None

    return FillRecord(
        trade_id=order_state.order_id,
        order_id=order_state.order_id,
        decision_id=decision.decision_id,
        market_id=order_state.market_id,
        token_id=order_state.token_id,
        venue=order_state.venue,
        side=decision.side,
        fill_price=order_state.fill_price,
        fill_size=order_state.filled_size,
        executed_at=order_state.submitted_at,
        filled_at=order_state.last_updated_at,
        status=order_state.status,
        anomaly_flags=[],
        resolved_outcome=_resolved_outcome(signal),
    )


def _portfolio_with_fill(portfolio: Portfolio, fill: FillRecord) -> Portfolio:
    positions = list(portfolio.open_positions)
    fill_size = fill.fill_size
    contracts = _filled_contracts(fill)
    for index, position in enumerate(positions):
        if _same_position(position, fill):
            new_shares = position.shares_held + contracts
            avg_entry_price = (
                position.avg_entry_price * position.shares_held
                + fill.fill_price * contracts
            ) / new_shares
            positions[index] = replace(
                position,
                shares_held=new_shares,
                avg_entry_price=avg_entry_price,
                locked_usdc=position.locked_usdc + fill_size,
            )
            break
    else:
        positions.append(
            Position(
                market_id=fill.market_id,
                token_id=fill.token_id,
                venue=fill.venue,
                side=fill.side,
                shares_held=contracts,
                avg_entry_price=fill.fill_price,
                unrealized_pnl=0.0,
                locked_usdc=fill_size,
            )
        )

    return replace(
        portfolio,
        free_usdc=portfolio.free_usdc - fill_size,
        locked_usdc=portfolio.locked_usdc + fill_size,
        open_positions=positions,
    )


def _filled_contracts(fill: FillRecord) -> float:
    if fill.filled_contracts is not None:
        return fill.filled_contracts
    return fill.fill_size / fill.fill_price


def _same_position(position: Position, fill: FillRecord) -> bool:
    return (
        position.market_id == fill.market_id
        and position.token_id == fill.token_id
        and position.venue == fill.venue
        and position.side == fill.side
    )


def _resolved_outcome(signal: MarketSignal) -> float | None:
    raw_outcome = signal.external_signal.get("resolved_outcome")
    if raw_outcome is not None:
        return min(max(float(raw_outcome), 0.0), 1.0)
    return None
