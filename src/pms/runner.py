from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, TypeVar, cast, runtime_checkable

import asyncpg
import httpx
from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.adapters.polymarket import PolymarketActuator
from pms.actuator.executor import ActuatorAdapter, ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import PMSSettings
from pms.controller.factory import ControllerPipelineFactory
from pms.controller.factor_snapshot import PostgresFactorSnapshotReader
from pms.controller.outcome_tokens import MarketDataOutcomeTokenResolver
from pms.controller.pipeline import ControllerPipeline
from pms.core.enums import OrderStatus, RunMode
from pms.core.interfaces import (
    DiscoveryPollCompleteSensor,
    IController,
    ISensor,
    MarketSelectorLike,
    SubscriptionControllerLike,
    SubscriptionManagedSensor,
)
from pms.core.models import (
    FillRecord,
    MarketSignal,
    Opportunity,
    OrderState,
    Portfolio,
    Position,
    TradeDecision,
)
from pms.evaluation.adapters.scoring import Scorer
from pms.evaluation.feedback import EvaluatorFeedback
from pms.evaluation.metrics import (
    MetricsCollector,
    StrategyMetricsSnapshot,
    StrategyVersionKey,
)
from pms.evaluation.spool import EvalSpool
from pms.factors.catalog import ensure_factor_catalog
from pms.factors.definitions import REGISTERED
from pms.factors.service import FactorService
from pms.market_selection import (
    MarketSelector,
    SensorSubscriptionController,
    UnionMergePolicy,
)
from pms.sensor.adapters.historical import HistoricalSensor
from pms.sensor.adapters.market_data import MarketDataSensor
from pms.sensor.adapters.market_discovery import MarketDiscoverySensor
from pms.sensor.stream import SensorStream
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.storage.market_data_store import PostgresMarketDataStore
from pms.storage.opportunity_store import OpportunityStore
from pms.storage.strategy_registry import PostgresStrategyRegistry
from pms.strategies.aggregate import Strategy
from pms.strategies.defaults import DEFAULT_STRATEGY_COMPOSITION
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.strategies.versioning import compute_strategy_version_id


logger = logging.getLogger(__name__)

DEFAULT_BACKTEST_FIXTURE = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")
RUNNER_STATE_LIMIT = 1000
RESELECTION_INTERVAL_S = 300.0
RAW_FACTOR_COMPOSITION_ROLES = frozenset(
    {
        "weighted",
        "precedence_rank",
        "threshold_edge",
        "posterior_prior",
        "posterior_success",
        "posterior_failure",
    }
)
REGISTERED_FACTOR_IDS = frozenset(factor_cls.factor_id for factor_cls in REGISTERED)
DEFAULT_V2_FACTOR_COMPOSITION = DEFAULT_STRATEGY_COMPOSITION
T = TypeVar("T")
ControllerReleaseCancelPoint = Literal[
    "before_first_cleanup_await",
    "between_cleanup_awaits",
    "after_last_cleanup_await",
]


@runtime_checkable
class AsyncCloseable(Protocol):
    async def aclose(self) -> None: ...


@runtime_checkable
class OpportunityAwareController(Protocol):
    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[Opportunity, TradeDecision] | None: ...


@dataclass(frozen=True)
class StrategyControllerRuntime:
    strategy_id: str
    strategy_version_id: str
    controller: IController
    asset_ids: frozenset[str] | None


@dataclass
class DetachedControllerRuntime:
    strategy_id: str
    runtime: StrategyControllerRuntime | None
    queue: asyncio.Queue[MarketSignal] | None
    task: asyncio.Task[None] | None


@dataclass
class RunnerState:
    mode: RunMode
    runner_started_at: datetime | None = None
    signals: list[MarketSignal] = field(default_factory=list)
    opportunities: list[Opportunity] = field(default_factory=list)
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
    opportunity_store: OpportunityStore = field(default_factory=OpportunityStore)
    portfolio: Portfolio = field(default_factory=lambda: Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
    ))
    sensor_stream: SensorStream = field(default_factory=SensorStream)
    controller: IController | None = None
    state: RunnerState = field(init=False)
    actuator_executor: ActuatorExecutor = field(init=False)
    _evaluator_spool: EvalSpool = field(init=False)
    _decision_queue: asyncio.Queue[tuple[TradeDecision, MarketSignal]] = field(
        init=False,
    )
    _stop_event: asyncio.Event = field(init=False)
    _controller_task: asyncio.Task[None] | None = field(init=False, default=None)
    _actuator_task: asyncio.Task[None] | None = field(init=False, default=None)
    _factor_service: FactorService | None = field(init=False, default=None)
    _factor_service_task: asyncio.Task[None] | None = field(init=False, default=None)
    _task: asyncio.Task[None] | None = field(init=False, default=None)
    _pg_pool: asyncpg.Pool | None = field(init=False, default=None)
    _owns_pg_pool: bool = field(init=False, default=False)
    _market_selector: MarketSelectorLike | None = field(init=False, default=None)
    _subscription_controller: SubscriptionControllerLike | None = field(
        init=False,
        default=None,
    )
    _strategy_registry: PostgresStrategyRegistry | None = field(
        init=False,
        default=None,
    )
    _controller_factory: ControllerPipelineFactory = field(init=False)
    _controller_runtimes: dict[str, StrategyControllerRuntime] = field(
        init=False,
        default_factory=dict,
    )
    _controller_signal_queues: dict[str, asyncio.Queue[MarketSignal]] = field(
        init=False,
        default_factory=dict,
    )
    _controller_pipeline_tasks: dict[str, asyncio.Task[None]] = field(
        init=False,
        default_factory=dict,
    )
    _controller_pipeline_error: BaseException | None = field(
        init=False,
        default=None,
    )
    _controller_lifecycle_lock: asyncio.Lock = field(init=False)
    _controller_release_cancel_point: ControllerReleaseCancelPoint | None = field(
        init=False,
        default=None,
    )
    _reselection_task: asyncio.Task[None] | None = field(init=False, default=None)
    _reselection_lock: asyncio.Lock = field(init=False)
    _reselection_requested: asyncio.Event = field(init=False)
    _active_sensors: tuple[ISensor, ...] = field(init=False, default=())
    _paper_orderbooks: dict[str, dict[str, Any]] = field(
        init=False, default_factory=dict
    )

    def __post_init__(self) -> None:
        self.state = RunnerState(mode=self.config.mode)
        self._controller_factory = ControllerPipelineFactory(settings=self.config)
        self._evaluator_spool = EvalSpool(
            store=self.eval_store,
            scorer=Scorer(),
            feedback_generator=EvaluatorFeedback(self.feedback_store),
            metrics_provider=self._metrics_by_strategy,
        )
        self._decision_queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self._controller_lifecycle_lock = asyncio.Lock()
        self._reselection_lock = asyncio.Lock()
        self._reselection_requested = asyncio.Event()
        self.actuator_executor = self._build_executor(self.config.mode)

    @property
    def controller_task(self) -> asyncio.Task[None] | None:
        return self._controller_task

    @property
    def actuator_task(self) -> asyncio.Task[None] | None:
        return self._actuator_task

    @property
    def controller_pipeline_tasks(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(self._controller_pipeline_tasks.values())

    @property
    def factor_service_task(self) -> asyncio.Task[None] | None:
        return self._factor_service_task

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    @property
    def pg_pool(self) -> asyncpg.Pool | None:
        return self._pg_pool

    async def ensure_pg_pool(self) -> None:
        if self._pg_pool is not None:
            return
        self._pg_pool = await asyncpg.create_pool(
            dsn=self.config.database.dsn,
            min_size=self.config.database.pool_min_size,
            max_size=self.config.database.pool_max_size,
        )
        self._owns_pg_pool = True
        self._bind_runtime_stores()

    def bind_pg_pool(self, pool: asyncpg.Pool) -> None:
        self._pg_pool = pool
        self._owns_pg_pool = False
        self._bind_runtime_stores()

    async def close_pg_pool(self) -> None:
        if self._pg_pool is None:
            return
        pool = self._pg_pool
        owns_pool = self._owns_pg_pool
        self._pg_pool = None
        self._owns_pg_pool = False
        self._unbind_runtime_stores()
        if owns_pool:
            await pool.close()

    @property
    def evaluator_task(self) -> asyncio.Task[None] | None:
        return self._evaluator_spool._task

    @property
    def active_sensors(self) -> tuple[ISensor, ...]:
        return self._active_sensors

    @property
    def subscription_controller(self) -> SubscriptionControllerLike | None:
        return self._subscription_controller

    @property
    def strategy_registry(self) -> PostgresStrategyRegistry | None:
        return self._strategy_registry

    @property
    def tasks(self) -> tuple[asyncio.Task[None], ...]:
        tasks: list[asyncio.Task[None]] = []
        tasks.extend(self.sensor_stream.tasks)
        if self._factor_service_task is not None:
            tasks.append(self._factor_service_task)
        if self._controller_task is not None:
            tasks.append(self._controller_task)
        tasks.extend(self._controller_pipeline_tasks.values())
        if self._actuator_task is not None:
            tasks.append(self._actuator_task)
        if self._reselection_task is not None:
            tasks.append(self._reselection_task)
        if self.evaluator_task is not None:
            tasks.append(self.evaluator_task)
        return tuple(tasks)

    async def start(self) -> None:
        if any(not task.done() for task in self.tasks):
            msg = "Runner is already started"
            raise RuntimeError(msg)

        self._stop_event.clear()
        self._controller_pipeline_error = None
        self.state.runner_started_at = datetime.now(tz=UTC)

        try:
            self._assert_no_legacy_jsonl_paths()
            if self._should_boot_postgres_runtime():
                await self.ensure_pg_pool()
                if self._pg_pool is None:
                    msg = "Runner PostgreSQL pool is not initialized"
                    raise RuntimeError(msg)
                self._strategy_registry = PostgresStrategyRegistry(self._pg_pool)
                await ensure_factor_catalog(self._pg_pool)
                await self._ensure_default_v2_version()
                if self._pg_pool is None:
                    msg = "Runner PostgreSQL pool is not initialized"
                    raise RuntimeError(msg)
                factor_signal_stream = self.sensor_stream.subscribe()
                self._factor_service = FactorService(
                    pool=self._pg_pool,
                    store=PostgresMarketDataStore(self._pg_pool),
                    cadence_s=self.config.factor_cadence_s,
                    factors=REGISTERED,
                    signal_stream=factor_signal_stream,
                )
                self._factor_service_task = asyncio.create_task(self._factor_service.run())
            self._active_sensors = self._build_sensors()
            self._wire_active_perception(self._active_sensors)
            await self._configure_controllers()
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

        factor_task = self._factor_service_task
        if factor_task is not None and not factor_task.done():
            factor_task.cancel()
        reselection_task = self._reselection_task
        self._reselection_requested.set()
        self._clear_discovery_poll_complete_hook()
        self._unregister_strategy_change_callbacks()
        if reselection_task is not None and not reselection_task.done():
            reselection_task.cancel()

        try:
            await self.sensor_stream.stop()
        except BaseException as exc:  # pragma: no cover - exercised via unit tests
            error = exc

        for task in (self._controller_task, *self._controller_pipeline_tasks.values(), self._actuator_task):
            if task is not None and not task.done():
                task.cancel()

        try:
            await asyncio.gather(
                *(
                    task
                    for task in (
                        factor_task,
                        self._controller_task,
                        *self._controller_pipeline_tasks.values(),
                        self._actuator_task,
                    )
                    if task
                ),
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

        try:
            await self._await_cancelled_task(reselection_task)
        except BaseException as exc:  # pragma: no cover - defensive
            if error is None:
                error = exc

        self._factor_service = None
        self._factor_service_task = None
        self._controller_task = None
        self._actuator_task = None
        self._market_selector = None
        self._subscription_controller = None
        self._strategy_registry = None
        self._controller_runtimes = {}
        self._controller_signal_queues = {}
        self._controller_pipeline_tasks = {}
        self._controller_release_cancel_point = None
        self._reselection_task = None

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
        if self._factor_service_task is not None:
            await self._factor_service_task
        if self._controller_task is not None:
            await self._controller_task
        controller_queues = tuple(self._controller_signal_queues.values())
        for queue in controller_queues:
            await queue.join()
        controller_tasks = tuple(self._controller_pipeline_tasks.values())
        if controller_tasks:
            await asyncio.gather(*controller_tasks, return_exceptions=True)
        if self._controller_pipeline_error is not None:
            raise self._controller_pipeline_error
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

    def _wire_active_perception(self, sensors: tuple[ISensor, ...]) -> None:
        self._clear_discovery_poll_complete_hook()
        self._market_selector = None
        self._subscription_controller = None
        self._reselection_task = None
        self._reselection_requested.clear()
        if self.config.mode == RunMode.BACKTEST:
            return
        if self._pg_pool is None:
            msg = "Runner PostgreSQL pool is not initialized"
            raise RuntimeError(msg)

        discovery_sensor = _find_discovery_sensor(sensors)
        subscription_sink = _find_subscription_sink(sensors)
        if discovery_sensor is None or subscription_sink is None:
            return

        if self._strategy_registry is None:
            msg = "Runner strategy registry is not initialized"
            raise RuntimeError(msg)
        self._market_selector = MarketSelector(
            PostgresMarketDataStore(self._pg_pool),
            self._strategy_registry,
            UnionMergePolicy(),
        )
        self._subscription_controller = SensorSubscriptionController(subscription_sink)
        self._register_strategy_change_callbacks()
        discovery_sensor.on_poll_complete = self._request_reselection
        self._reselection_task = asyncio.create_task(self._periodic_reselection_loop())

    def _register_strategy_change_callbacks(self) -> None:
        registry = self._strategy_registry
        if registry is None:
            msg = "Runner strategy registry is not initialized"
            raise RuntimeError(msg)
        registry.register_change_callback(self._request_reselection)
        registry.register_change_callback(self._sync_controller_runtimes)

    def _unregister_strategy_change_callbacks(self) -> None:
        registry = self._strategy_registry
        if registry is None:
            return
        registry.unregister_change_callback(self._request_reselection)
        registry.unregister_change_callback(self._sync_controller_runtimes)

    def _assert_no_legacy_jsonl_paths(self) -> None:
        for store in (self.eval_store, self.feedback_store):
            if isinstance(getattr(store, "path", None), Path):
                msg = "legacy JSONL path referenced"
                raise RuntimeError(msg)

    def _should_boot_postgres_runtime(self) -> bool:
        if self._pg_pool is not None:
            return True
        if self.config.mode != RunMode.BACKTEST:
            return True
        return "database" in self.config.model_fields_set

    async def _ensure_default_v2_version(self) -> None:
        if not self.config.auto_migrate_default_v2:
            return
        if self._pg_pool is None:
            msg = "Runner PostgreSQL pool is not initialized"
            raise RuntimeError(msg)

        async with self._pg_pool.acquire() as connection:
            active_version_id = await connection.fetchval(
                """
                SELECT active_version_id
                FROM strategies
                WHERE strategy_id = 'default'
                """
            )
        if active_version_id != "default-v1":
            return

        registry = self._strategy_registry
        if registry is None:
            msg = "Runner strategy registry is not initialized"
            raise RuntimeError(msg)
        strategy = await registry.get_by_id("default")
        if strategy is None:
            return
        if strategy.config.factor_composition == DEFAULT_V2_FACTOR_COMPOSITION:
            return

        migrated_strategy = Strategy(
            config=replace(
                strategy.config,
                factor_composition=DEFAULT_V2_FACTOR_COMPOSITION,
            ),
            risk=strategy.risk,
            eval_spec=strategy.eval_spec,
            forecaster=strategy.forecaster,
            market_selection=strategy.market_selection,
        )
        version = await registry.create_version(migrated_strategy)
        await registry.set_active("default", version.strategy_version_id)
        await registry.populate_strategy_factors(
            "default",
            version.strategy_version_id,
            _raw_factor_steps(migrated_strategy.config.factor_composition),
        )

    def _bind_runtime_stores(self) -> None:
        if self._pg_pool is None:
            msg = "Runner PostgreSQL pool is not initialized"
            raise RuntimeError(msg)
        if isinstance(self._controller_factory, ControllerPipelineFactory):
            market_data_store = PostgresMarketDataStore(self._pg_pool)
            self._controller_factory = ControllerPipelineFactory(
                settings=self.config,
                factor_reader=PostgresFactorSnapshotReader(self._pg_pool),
                outcome_token_resolver=MarketDataOutcomeTokenResolver(market_data_store),
            )
        if isinstance(self.eval_store, EvalStore):
            self.eval_store.bind_pool(self._pg_pool)
        if isinstance(self.feedback_store, FeedbackStore):
            self.feedback_store.bind_pool(self._pg_pool)
        if isinstance(self.opportunity_store, OpportunityStore):
            self.opportunity_store.bind_pool(self._pg_pool)

    def _unbind_runtime_stores(self) -> None:
        if isinstance(self._controller_factory, ControllerPipelineFactory):
            self._controller_factory = ControllerPipelineFactory(settings=self.config)
        if isinstance(self.eval_store, EvalStore):
            self.eval_store.pool = None
        if isinstance(self.feedback_store, FeedbackStore):
            self.feedback_store.pool = None
        if isinstance(self.opportunity_store, OpportunityStore):
            self.opportunity_store.pool = None

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
            return PaperActuator(orderbooks=self._paper_orderbooks)
        return PolymarketActuator(self.config)

    async def _controller_loop(self) -> None:
        while True:
            if self._should_stop_controller():
                return
            try:
                signal = await asyncio.wait_for(self.sensor_stream.queue.get(), 0.05)
            except TimeoutError:
                continue

            try:
                _append_bounded(self.state.signals, signal)
                for strategy_id, runtime in tuple(self._controller_runtimes.items()):
                    if not _matches_strategy_scope(runtime.asset_ids, signal):
                        continue
                    queue = self._controller_signal_queues.get(strategy_id)
                    if queue is None:
                        continue
                    await queue.put(signal)
            finally:
                self.sensor_stream.queue.task_done()

    async def _controller_pipeline_loop(self, strategy_id: str) -> None:
        runtime = self._controller_runtimes[strategy_id]
        queue = self._controller_signal_queues[strategy_id]
        try:
            while True:
                if self._should_stop_controller_pipeline(strategy_id):
                    return
                try:
                    signal = await asyncio.wait_for(queue.get(), 0.05)
                except TimeoutError:
                    continue

                try:
                    decision: TradeDecision | None = None
                    if isinstance(runtime.controller, OpportunityAwareController):
                        emission = await runtime.controller.on_signal(
                            signal,
                            portfolio=self.portfolio,
                        )
                        if emission is None:
                            continue
                        opportunity, decision = emission
                        await self.opportunity_store.insert(opportunity)
                        _append_bounded(self.state.opportunities, opportunity)
                    else:
                        decision = await runtime.controller.decide(
                            signal,
                            portfolio=self.portfolio,
                        )
                    if decision is not None:
                        _append_bounded(self.state.decisions, decision)
                        await self._decision_queue.put((decision, signal))
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            logger.warning(
                "controller pipeline failed for %s: %s",
                strategy_id,
                error,
            )
            if self._controller_pipeline_error is None:
                self._controller_pipeline_error = error
            raise
        finally:
            current_task = asyncio.current_task()
            if (
                not self._stop_event.is_set()
                and self._controller_pipeline_tasks.get(strategy_id) is current_task
            ):
                await self._release_controller_runtime(strategy_id)

    async def _actuator_loop(self) -> None:
        while True:
            if self._should_stop_actuator():
                return
            try:
                decision, signal = await asyncio.wait_for(self._decision_queue.get(), 0.05)
            except TimeoutError:
                continue

            try:
                if self.config.mode == RunMode.PAPER:
                    self._paper_orderbooks[decision.market_id] = signal.orderbook
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

    async def _metrics_by_strategy(
        self,
    ) -> dict[StrategyVersionKey, tuple[StrategyMetricsSnapshot, EvalSpec]]:
        records = await self.eval_store.all()
        snapshots = MetricsCollector(records).snapshot_by_strategy()
        eval_specs = await self._active_eval_specs()
        return {
            key: (snapshot, eval_specs[key])
            for key, snapshot in snapshots.items()
            if key in eval_specs
        }

    async def _active_eval_specs(self) -> dict[StrategyVersionKey, EvalSpec]:
        if self._strategy_registry is None or not hasattr(
            self._strategy_registry,
            "list_active_strategies",
        ):
            if self._controller_runtimes:
                default_eval_spec = _default_active_strategy(self.config).eval_spec
                return {
                    (runtime.strategy_id, runtime.strategy_version_id): default_eval_spec
                    for runtime in self._controller_runtimes.values()
                }
            default_strategy = _default_active_strategy(self.config)
            return {
                (
                    default_strategy.strategy_id,
                    default_strategy.strategy_version_id,
                ): default_strategy.eval_spec
            }

        active_strategies = await self._strategy_registry.list_active_strategies()
        return {
            (strategy.strategy_id, strategy.strategy_version_id): strategy.eval_spec
            for strategy in active_strategies
        }

    def _should_stop_controller(self) -> bool:
        if self._stop_event.is_set() and self.sensor_stream.queue.empty():
            return True
        return self._sensors_finished() and self.sensor_stream.queue.empty()

    def _should_stop_actuator(self) -> bool:
        if self._stop_event.is_set() and self._decision_queue.empty():
            return True
        controller_done = (
            self._controller_task is not None
            and self._controller_task.done()
            and all(task.done() for task in self._controller_pipeline_tasks.values())
        )
        return controller_done and self._decision_queue.empty()

    def _should_stop_controller_pipeline(self, strategy_id: str) -> bool:
        queue = self._controller_signal_queues.get(strategy_id)
        if queue is None:
            return True
        if self._stop_event.is_set() and queue.empty():
            return True
        dispatcher_done = self._controller_task is not None and self._controller_task.done()
        return dispatcher_done and queue.empty()

    def _sensors_finished(self) -> bool:
        return bool(self.sensor_stream.tasks) and all(
            task.done() for task in self.sensor_stream.tasks
        )

    async def _close_active_sensors(self) -> None:
        self._clear_discovery_poll_complete_hook()
        for sensor in self._active_sensors:
            if isinstance(sensor, AsyncCloseable):
                await sensor.aclose()
        self._active_sensors = ()

    async def _close_pg_pool(self) -> None:
        await self.close_pg_pool()

    async def _cleanup_after_start_failure(self) -> None:
        stop_error: BaseException | None = None
        if self._factor_service_task is not None and not self._factor_service_task.done():
            self._factor_service_task.cancel()
        for task in self._controller_pipeline_tasks.values():
            if not task.done():
                task.cancel()
        reselection_task = self._reselection_task
        self._reselection_requested.set()
        self._clear_discovery_poll_complete_hook()
        self._unregister_strategy_change_callbacks()
        if reselection_task is not None and not reselection_task.done():
            reselection_task.cancel()
        try:
            await asyncio.gather(
                *(
                    task
                    for task in (self._factor_service_task, *self._controller_pipeline_tasks.values())
                    if task is not None
                ),
                return_exceptions=True,
            )
        except BaseException as exc:  # pragma: no cover - defensive
            stop_error = exc
        try:
            await self._await_cancelled_task(reselection_task)
        except BaseException as exc:  # pragma: no cover - defensive
            if stop_error is None:
                stop_error = exc
        try:
            await self.sensor_stream.stop()
        except BaseException as exc:  # pragma: no cover - defensive
            if stop_error is None:
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

        self._factor_service = None
        self._factor_service_task = None
        self._controller_task = None
        self._actuator_task = None
        self._market_selector = None
        self._subscription_controller = None
        self._strategy_registry = None
        self._controller_runtimes = {}
        self._controller_signal_queues = {}
        self._controller_pipeline_tasks = {}
        self._controller_release_cancel_point = None
        self._reselection_task = None
        await self._close_pg_pool()

        if stop_error is not None:
            raise stop_error

    async def _reselect(self) -> None:
        selector = self._market_selector
        subscription_controller = self._subscription_controller
        if selector is None or subscription_controller is None:
            return
        async with self._reselection_lock:
            result = await selector.select()
            await subscription_controller.update(list(result.asset_ids))

    async def _periodic_reselection_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    self._reselection_requested.wait(),
                    timeout=RESELECTION_INTERVAL_S,
                )
                self._reselection_requested.clear()
                if self._stop_event.is_set():
                    return
                try:
                    await self._reselect()
                except Exception as error:  # noqa: BLE001
                    logger.warning("periodic reselection failed: %s", error)
            except TimeoutError:
                if self._stop_event.is_set():
                    return
                try:
                    await self._reselect()
                except Exception as error:  # noqa: BLE001
                    logger.warning("periodic reselection failed: %s", error)

    def _clear_discovery_poll_complete_hook(self) -> None:
        discovery_sensor = _find_discovery_sensor(self._active_sensors)
        if discovery_sensor is not None:
            discovery_sensor.on_poll_complete = None

    async def _request_reselection(self) -> None:
        self._reselection_requested.set()

    async def _await_cancelled_task(
        self,
        task: asyncio.Task[None] | None,
    ) -> None:
        if task is None:
            return
        with suppress(asyncio.CancelledError):
            await task

    def _attach_controller_runtime(self, runtime: StrategyControllerRuntime) -> None:
        signal_queue: asyncio.Queue[MarketSignal] = asyncio.Queue()
        self._controller_runtimes[runtime.strategy_id] = runtime
        self._controller_signal_queues[runtime.strategy_id] = signal_queue
        task = asyncio.create_task(
            self._controller_pipeline_loop(runtime.strategy_id),
            name=f"controller-pipeline:{runtime.strategy_id}",
        )
        task.add_done_callback(self._capture_controller_pipeline_exception)
        self._controller_pipeline_tasks[runtime.strategy_id] = task

    def _capture_controller_pipeline_exception(
        self,
        task: asyncio.Task[None],
    ) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and self._controller_pipeline_error is None:
            self._controller_pipeline_error = error

    async def _sync_controller_runtimes(self) -> None:
        if self.controller is not None or self._strategy_registry is None:
            return
        async with self._controller_lifecycle_lock:
            desired_runtimes = await self._build_controller_runtimes()
            desired_by_strategy = {
                runtime.strategy_id: runtime
                for runtime in desired_runtimes
            }
            current_strategy_ids = set(self._controller_runtimes)
            desired_strategy_ids = set(desired_by_strategy)

            for strategy_id in sorted(current_strategy_ids - desired_strategy_ids):
                await self._release_controller_runtime_locked(strategy_id)

            for strategy_id in sorted(current_strategy_ids & desired_strategy_ids):
                current_runtime = self._controller_runtimes.get(strategy_id)
                desired_runtime = desired_by_strategy[strategy_id]
                if current_runtime is None:
                    continue
                if (
                    current_runtime.strategy_version_id != desired_runtime.strategy_version_id
                    or current_runtime.asset_ids != desired_runtime.asset_ids
                ):
                    await self._release_controller_runtime_locked(strategy_id)
                    self._attach_controller_runtime(desired_runtime)

            for strategy_id in sorted(desired_strategy_ids - current_strategy_ids):
                self._attach_controller_runtime(desired_by_strategy[strategy_id])

            await self._refresh_subscription_assets_locked()

    async def _release_controller_runtime(self, strategy_id: str) -> None:
        async with self._controller_lifecycle_lock:
            await self._release_controller_runtime_locked(strategy_id)

    async def _release_controller_runtime_locked(self, strategy_id: str) -> None:
        detached = self._detach_controller_runtime(strategy_id)
        if detached is None:
            return
        try:
            await self._finalize_controller_runtime_release_locked(
                detached,
                allow_cancel_injection=True,
            )
        except asyncio.CancelledError:
            await asyncio.shield(
                self._finalize_controller_runtime_release_locked(
                    detached,
                    allow_cancel_injection=False,
                )
            )
            raise
        except Exception:
            try:
                await asyncio.shield(
                    self._finalize_controller_runtime_release_locked(
                        detached,
                        allow_cancel_injection=False,
                    )
                )
            except Exception as cleanup_error:  # noqa: BLE001
                logger.warning(
                    "controller runtime cleanup retry failed for %s: %s",
                    detached.strategy_id,
                    cleanup_error,
                )
            raise

    def _detach_controller_runtime(
        self,
        strategy_id: str,
    ) -> DetachedControllerRuntime | None:
        runtime = self._controller_runtimes.pop(strategy_id, None)
        queue = self._controller_signal_queues.pop(strategy_id, None)
        task = self._controller_pipeline_tasks.pop(strategy_id, None)
        if runtime is None and queue is None and task is None:
            return None
        current_task = asyncio.current_task()
        if task is not None and task is not current_task and not task.done():
            task.cancel()
        return DetachedControllerRuntime(
            strategy_id=strategy_id,
            runtime=runtime,
            queue=queue,
            task=task,
        )

    async def _finalize_controller_runtime_release_locked(
        self,
        detached: DetachedControllerRuntime,
        *,
        allow_cancel_injection: bool,
    ) -> None:
        if allow_cancel_injection:
            await self._maybe_inject_controller_release_cancel(
                "before_first_cleanup_await"
            )
        current_task = asyncio.current_task()
        if detached.task is not None and detached.task is not current_task:
            await self._await_cancelled_task(detached.task)
        if allow_cancel_injection:
            await self._maybe_inject_controller_release_cancel(
                "between_cleanup_awaits"
            )
        self._drain_controller_signal_queue(detached.queue)
        await self._refresh_subscription_assets_locked()
        if allow_cancel_injection:
            await self._maybe_inject_controller_release_cancel(
                "after_last_cleanup_await"
            )

    async def _refresh_subscription_assets_locked(self) -> None:
        subscription_controller = self._subscription_controller
        if subscription_controller is None:
            return
        scoped_asset_ids: list[frozenset[str]] = []
        for runtime in self._controller_runtimes.values():
            if runtime.asset_ids is None:
                return
            scoped_asset_ids.append(runtime.asset_ids)
        merged_asset_ids = sorted(
            {
                asset_id
                for asset_ids in scoped_asset_ids
                for asset_id in asset_ids
            }
        )
        async with self._reselection_lock:
            await subscription_controller.update(merged_asset_ids)

    async def _maybe_inject_controller_release_cancel(
        self,
        point: ControllerReleaseCancelPoint,
    ) -> None:
        if self._controller_release_cancel_point == point:
            self._controller_release_cancel_point = None
            raise asyncio.CancelledError

    def _drain_controller_signal_queue(
        self,
        queue: asyncio.Queue[MarketSignal] | None,
    ) -> None:
        if queue is None:
            return
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            queue.task_done()

    async def _configure_controllers(self) -> None:
        if self._controller_task is not None or self._controller_pipeline_tasks:
            msg = "Runner controllers already configured"
            raise RuntimeError(msg)
        runtimes = await self._build_controller_runtimes()
        for runtime in runtimes:
            self._attach_controller_runtime(runtime)

    async def _build_controller_runtimes(self) -> list[StrategyControllerRuntime]:
        if self.controller is not None:
            return [
                StrategyControllerRuntime(
                    strategy_id="default",
                    strategy_version_id="default-v1",
                    controller=self.controller,
                    asset_ids=None,
                )
            ]

        if (
            self._strategy_registry is not None
            and hasattr(self._strategy_registry, "list_active_strategies")
            and (
                (
                    self._market_selector is not None
                    and hasattr(self._market_selector, "select_per_strategy")
                )
                or not self._owns_pg_pool
            )
        ):
            active_strategies = await self._strategy_registry.list_active_strategies()
            scopes: dict[str, frozenset[str]] = {}
            if (
                self._market_selector is not None
                and hasattr(self._market_selector, "select_per_strategy")
            ):
                selections = await self._market_selector.select_per_strategy()
                scopes = {
                    selection.strategy_id: selection.asset_ids
                    for selection in selections
                }
            pipelines = self._controller_factory.build_many(active_strategies)
            return [
                StrategyControllerRuntime(
                    strategy_id=strategy.strategy_id,
                    strategy_version_id=strategy.strategy_version_id,
                    controller=pipelines[strategy.strategy_id],
                    asset_ids=scopes.get(strategy.strategy_id),
                )
                for strategy in active_strategies
            ]

        default_strategy = _default_active_strategy(self.config)
        pipeline = self._controller_factory.build(default_strategy)
        return [
            StrategyControllerRuntime(
                strategy_id=default_strategy.strategy_id,
                strategy_version_id=default_strategy.strategy_version_id,
                controller=pipeline,
                asset_ids=None,
            )
        ]


def _append_bounded(items: list[T], item: T) -> None:
    items.append(item)
    overflow = len(items) - RUNNER_STATE_LIMIT
    if overflow > 0:
        del items[:overflow]


def _find_discovery_sensor(
    sensors: Sequence[ISensor],
) -> DiscoveryPollCompleteSensor | None:
    for sensor in sensors:
        if hasattr(sensor, "on_poll_complete"):
            return cast(DiscoveryPollCompleteSensor, sensor)
    return None


def _find_subscription_sink(
    sensors: Sequence[ISensor],
) -> SubscriptionManagedSensor | None:
    for sensor in sensors:
        update_subscription = getattr(sensor, "update_subscription", None)
        if callable(update_subscription):
            return cast(SubscriptionManagedSensor, sensor)
    return None


def _matches_strategy_scope(
    asset_ids: frozenset[str] | None,
    signal: MarketSignal,
) -> bool:
    if asset_ids is None:
        return True
    candidate_id = signal.token_id or signal.market_id
    return candidate_id in asset_ids


def _default_active_strategy(settings: PMSSettings) -> ActiveStrategy:
    config = StrategyConfig(
        strategy_id="default",
        factor_composition=DEFAULT_V2_FACTOR_COMPOSITION,
        metadata=(("owner", "system"), ("tier", "default")),
    )
    risk = RiskParams(
        max_position_notional_usdc=settings.risk.max_position_per_market,
        max_daily_drawdown_pct=(
            0.0 if settings.risk.max_drawdown_pct is None else settings.risk.max_drawdown_pct
        ),
        min_order_size_usdc=settings.risk.min_order_usdc,
    )
    eval_spec = EvalSpec(metrics=("brier", "pnl", "fill_rate"))
    forecaster = ForecasterSpec(
        forecasters=(
            ("rules", (("threshold", "0.55"),)),
            ("stats", (("window", "15m"),)),
            ("llm", ()),
        )
    )
    market_selection = MarketSelectionSpec(
        venue="polymarket",
        resolution_time_max_horizon_days=7,
        volume_min_usdc=500.0,
    )
    strategy_version_id = compute_strategy_version_id(
        config,
        risk,
        eval_spec,
        forecaster,
        market_selection,
    )
    return ActiveStrategy(
        strategy_id="default",
        strategy_version_id=strategy_version_id,
        config=config,
        risk=risk,
        eval_spec=eval_spec,
        forecaster=forecaster,
        market_selection=market_selection,
    )


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
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
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


def _raw_factor_steps(
    steps: Sequence[FactorCompositionStep],
) -> tuple[FactorCompositionStep, ...]:
    raw_steps: list[FactorCompositionStep] = []
    for step in steps:
        role = getattr(step, "role", None)
        factor_id = getattr(step, "factor_id", None)
        if role not in RAW_FACTOR_COMPOSITION_ROLES:
            continue
        if factor_id not in REGISTERED_FACTOR_IDS:
            continue
        raw_steps.append(step)
    return tuple(raw_steps)
