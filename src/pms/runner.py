from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Iterator, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Protocol, TypeVar, cast, runtime_checkable

import asyncpg
import httpx
from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.adapters.polymarket import (
    DenyFirstLiveOrderGate,
    FileFirstLiveOrderGate,
    LiveQuoteProvider,
    MissingLiveQuoteProvider,
    PolymarketActuator,
    PolymarketBookQuoteProvider,
    PolymarketDirectQuoteProvider,
    PolymarketRoutingQuoteProvider,
    PolymarketSDKClient,
    PolymarketSubmissionUnknownError,
    PolymarketVenueAccountReconciler,
)
from pms.actuator.executor import ActuatorAdapter, ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import PMSSettings, validate_live_mode_ready
from pms.controller.diagnostics import ControllerDiagnostic
from pms.controller.factory import ControllerPipelineFactory
from pms.controller.factor_snapshot import PostgresFactorSnapshotReader
from pms.controller.outcome_tokens import MarketDataOutcomeTokenResolver
from pms.controller.sizers.kelly import KellySizer
from pms.core.enums import OrderStatus, RunMode, TimeInForce
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
    ReconciliationReport as ReconciliationReport,
    TradeDecision,
    Venue,
    VenueAccountSnapshot as VenueAccountSnapshot,
    VenueCredentials,
)
from pms.evaluation.adapters.scoring import Scorer
from pms.evaluation.feedback import EvaluatorFeedback
from pms.evaluation.metrics import (
    MetricsCollector,
    StrategyMetricsSnapshot,
    StrategyVersionKey,
)
from pms.evaluation.spool import EvalSpool
from pms.event_stream import RuntimeEventBus
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
from pms.storage.dedup_store import InMemoryDedupStore, PgDedupStore
from pms.storage.decision_store import DecisionStore
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.storage.fill_store import FillStore
from pms.storage.live_emergency_audit import LiveEmergencyAuditWriter
from pms.storage.market_data_store import PostgresMarketDataStore
from pms.storage.market_subscription_store import PostgresMarketSubscriptionStore
from pms.storage.opportunity_store import OpportunityStore
from pms.storage.order_store import OrderStore
from pms.storage.strategy_registry import PostgresStrategyRegistry
from pms.strategies.aggregate import Strategy
from pms.strategies.defaults import DEFAULT_STRATEGY_COMPOSITION
from pms.strategies.flb import FlbAgent, FlbController, FlbStrategyModule
from pms.strategies.flb.source import (
    FlbMarketSnapshot,
    FlbMarketSnapshotReader,
    LiveFlbSource,
)
from pms.strategies.intents import StrategyContext
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.strategies.runtime_bridge import StrategyRunResult
from pms.strategies.versioning import compute_strategy_version_id


logger = logging.getLogger(__name__)

DEFAULT_BACKTEST_FIXTURE = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")
RUNNER_STATE_LIMIT = 1000
RESELECTION_INTERVAL_S = 300.0
DECISION_PENDING_TTL = timedelta(minutes=15)
DECISION_SWEEP_INTERVAL_S = 5.0
DEFAULT_OPERATIONAL_MARKET_SELECTION_HORIZON_DAYS = 90
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


class LivePersistenceFailureError(RuntimeError):
    """Raised after a venue submit when LIVE persistence safety is broken."""


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


@dataclass(frozen=True)
class ActuatorWorkItem:
    decision: TradeDecision
    signal: MarketSignal | None
    dedup_acquired: bool = False

    def __iter__(self) -> Iterator[TradeDecision | MarketSignal | None]:
        yield self.decision
        yield self.signal


class VenueAccountReconciler(Protocol):
    async def snapshot(self, credentials: VenueCredentials) -> VenueAccountSnapshot: ...

    async def compare(
        self,
        db_portfolio: Portfolio,
        venue_snapshot: VenueAccountSnapshot,
    ) -> ReconciliationReport: ...


@dataclass
class RunnerState:
    mode: RunMode
    runner_started_at: datetime | None = None
    signals: list[MarketSignal] = field(default_factory=list)
    opportunities: list[Opportunity] = field(default_factory=list)
    decisions: list[TradeDecision] = field(default_factory=list)
    orders: list[OrderState] = field(default_factory=list)
    fills: list[FillRecord] = field(default_factory=list)
    controller_diagnostics: list[ControllerDiagnostic] = field(default_factory=list)


@dataclass
class Runner:
    config: PMSSettings = field(default_factory=PMSSettings)
    historical_data_path: Path = DEFAULT_BACKTEST_FIXTURE
    sensors: Sequence[ISensor] | None = None
    eval_store: EvalStore = field(default_factory=EvalStore)
    feedback_store: FeedbackStore = field(default_factory=FeedbackStore)
    decision_store: DecisionStore = field(default_factory=DecisionStore)
    order_store: OrderStore = field(default_factory=OrderStore)
    fill_store: FillStore = field(default_factory=FillStore)
    opportunity_store: OpportunityStore = field(default_factory=OpportunityStore)
    venue_account_reconciler: VenueAccountReconciler | None = None
    portfolio: Portfolio = field(default_factory=lambda: Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
    ))
    sensor_stream: SensorStream = field(default_factory=SensorStream)
    event_bus: RuntimeEventBus = field(default_factory=RuntimeEventBus)
    controller: IController | None = None
    state: RunnerState = field(init=False)
    actuator_executor: ActuatorExecutor = field(init=False)
    _evaluator_spool: EvalSpool = field(init=False)
    _decision_queue: asyncio.Queue[ActuatorWorkItem] = field(
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
    _decision_expiry_task: asyncio.Task[None] | None = field(init=False, default=None)
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
    _live_trading_suspended_reason: str | None = field(init=False, default=None)

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

    async def enqueue_accepted_decision(self, decision: TradeDecision) -> None:
        await self._enqueue_decision(decision, signal=None, dedup_acquired=True)

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
        if self._decision_expiry_task is not None:
            tasks.append(self._decision_expiry_task)
        if self._reselection_task is not None:
            tasks.append(self._reselection_task)
        if self.evaluator_task is not None:
            tasks.append(self.evaluator_task)
        return tuple(tasks)

    @property
    def live_trading_suspended(self) -> bool:
        return self._live_trading_suspended_reason is not None

    async def start(self) -> None:
        if any(not task.done() for task in self.tasks):
            msg = "Runner is already started"
            raise RuntimeError(msg)

        if self.config.mode == RunMode.LIVE:
            validate_live_mode_ready(self.config)

        self._stop_event.clear()
        self._controller_pipeline_error = None
        self._live_trading_suspended_reason = None
        self.state = RunnerState(
            mode=self.config.mode,
            runner_started_at=datetime.now(tz=UTC),
        )
        self._paper_orderbooks.clear()

        try:
            self._assert_no_legacy_jsonl_paths()
            if self._should_boot_postgres_runtime():
                await self.ensure_pg_pool()
                if self._pg_pool is None:
                    msg = "Runner PostgreSQL pool is not initialized"
                    raise RuntimeError(msg)
                self._strategy_registry = PostgresStrategyRegistry(self._pg_pool)
                await ensure_factor_catalog(self._pg_pool)
                if self.config.mode == RunMode.LIVE:
                    await self._assert_no_unresolved_submission_unknown_incidents()
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
                # Rebuild the portfolio from persisted fills BEFORE sensors
                # start producing signals. Without this, a restart in LIVE
                # mode would forget open Polymarket exposure: every new BUY
                # would size against the hardcoded `Portfolio(free_usdc=…)`
                # default while the venue actually holds real exposure. See
                # also `_reconcile_portfolio_from_db`.
                await self._reconcile_portfolio_from_db()
                if self.config.mode == RunMode.LIVE:
                    await self._reconcile_venue_account()
            self._active_sensors = self._build_sensors()
            self._wire_active_perception(self._active_sensors)
            await self._configure_controllers()
            await self.sensor_stream.start(self._active_sensors)
            await self._evaluator_spool.start()
            self._controller_task = asyncio.create_task(self._controller_loop())
            self._actuator_task = asyncio.create_task(self._actuator_loop())
            if self._pg_pool is not None:
                self._decision_expiry_task = asyncio.create_task(self._decision_expiry_loop())
        except Exception:
            await self._cleanup_after_start_failure()
            raise

    async def stop(self) -> None:
        self._stop_event.set()
        error: BaseException | None = None

        factor_task = self._factor_service_task
        if factor_task is not None and not factor_task.done():
            factor_task.cancel()
        decision_expiry_task = self._decision_expiry_task
        if decision_expiry_task is not None and not decision_expiry_task.done():
            decision_expiry_task.cancel()
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
                        decision_expiry_task,
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
        self._decision_expiry_task = None
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
        if (
            self._decision_expiry_task is not None
            and self.config.mode == RunMode.BACKTEST
        ):
            await self._decision_expiry_task
        await self._evaluator_spool.join()

    async def wait_for_signals(self, count: int) -> None:
        while len(self.state.signals) < count:
            await asyncio.sleep(0.1)

    def build_flb_strategy_module(
        self,
        *,
        strategy_id: str,
        strategy_version_id: str,
        market_ids: Sequence[str],
        market_reader: FlbMarketSnapshotReader | None = None,
    ) -> FlbStrategyModule:
        if not market_ids:
            msg = "market_ids must not be empty"
            raise ValueError(msg)
        reader = market_reader
        if reader is None:
            if self._pg_pool is None:
                msg = (
                    "Runner PostgreSQL pool is required to build the production "
                    "FLB market reader"
                )
                raise RuntimeError(msg)
            reader = _PostgresFlbMarketSnapshotReader(
                PostgresMarketDataStore(self._pg_pool)
            )

        return FlbStrategyModule(
            source=LiveFlbSource(
                market_ids=tuple(market_ids),
                market_reader=reader,
                position_sizer=KellySizer(self.config.risk),
                portfolio=self.portfolio,
                max_slippage_bps=self.config.controller.max_slippage_bps,
                time_in_force=TimeInForce(
                    self.config.controller.time_in_force.upper()
                ),
            ),
            controller=FlbController(),
            agent=FlbAgent(),
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
        )

    async def run_flb_strategy_once(
        self,
        *,
        strategy_id: str,
        strategy_version_id: str,
        market_ids: Sequence[str],
        as_of: datetime | None = None,
        market_reader: FlbMarketSnapshotReader | None = None,
    ) -> Sequence[StrategyRunResult]:
        module = self.build_flb_strategy_module(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            market_ids=market_ids,
            market_reader=market_reader,
        )
        return tuple(
            await module.run_with_artifacts(
                StrategyContext(
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    as_of=as_of or datetime.now(tz=UTC),
                )
            )
        )

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
            PostgresMarketSubscriptionStore(self._pg_pool),
        )
        self._subscription_controller = SensorSubscriptionController(subscription_sink)
        self._register_strategy_change_callbacks()
        discovery_sensor.on_poll_complete = self._handle_discovery_poll_complete
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
        migrated_market_selection = _operational_default_market_selection(
            strategy.market_selection
        )
        if (
            strategy.config.factor_composition == DEFAULT_V2_FACTOR_COMPOSITION
            and strategy.market_selection == migrated_market_selection
        ):
            return

        migrated_strategy = Strategy(
            config=replace(
                strategy.config,
                factor_composition=DEFAULT_V2_FACTOR_COMPOSITION,
            ),
            risk=strategy.risk,
            eval_spec=strategy.eval_spec,
            forecaster=strategy.forecaster,
            market_selection=migrated_market_selection,
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
        if isinstance(self.decision_store, DecisionStore):
            self.decision_store.bind_pool(self._pg_pool)
        if isinstance(self.order_store, OrderStore):
            self.order_store.bind_pool(self._pg_pool)
        if isinstance(self.fill_store, FillStore):
            self.fill_store.bind_pool(self._pg_pool)
        if isinstance(self.opportunity_store, OpportunityStore):
            self.opportunity_store.bind_pool(self._pg_pool)
        self.actuator_executor = self._build_executor(self.config.mode)

    def _unbind_runtime_stores(self) -> None:
        if isinstance(self._controller_factory, ControllerPipelineFactory):
            self._controller_factory = ControllerPipelineFactory(settings=self.config)
        if isinstance(self.eval_store, EvalStore):
            self.eval_store.pool = None
        if isinstance(self.feedback_store, FeedbackStore):
            self.feedback_store.pool = None
        if isinstance(self.decision_store, DecisionStore):
            self.decision_store.pool = None
        if isinstance(self.order_store, OrderStore):
            self.order_store.pool = None
        if isinstance(self.fill_store, FillStore):
            self.fill_store.pool = None
        if isinstance(self.opportunity_store, OpportunityStore):
            self.opportunity_store.pool = None
        self.actuator_executor = self._build_executor(self.config.mode)

    def _build_executor(self, mode: RunMode) -> ActuatorExecutor:
        return ActuatorExecutor(
            adapter=self._build_adapter(mode),
            risk=RiskManager(self.config.risk),
            feedback=ActuatorFeedback(self.feedback_store),
            dedup_store=self._build_dedup_store(),
        )

    def _build_dedup_store(self) -> PgDedupStore | InMemoryDedupStore:
        if self._pg_pool is not None:
            return PgDedupStore(self._pg_pool)
        return InMemoryDedupStore()

    def _build_adapter(self, mode: RunMode) -> ActuatorAdapter:
        if mode == RunMode.BACKTEST:
            return BacktestActuator(self.historical_data_path)
        if mode == RunMode.PAPER:
            return PaperActuator(orderbooks=self._paper_orderbooks)
        quote_provider: LiveQuoteProvider = MissingLiveQuoteProvider()
        if self._pg_pool is not None:
            quote_provider = PolymarketRoutingQuoteProvider(
                snapshot_provider=PolymarketBookQuoteProvider(
                    PostgresMarketDataStore(self._pg_pool),
                    allowed_clock_skew_ms=(
                        self.config.controller.allowed_book_clock_skew_ms
                    ),
                ),
                direct_provider=PolymarketDirectQuoteProvider(
                    book_client=PolymarketSDKClient(),
                    allowed_clock_skew_ms=(
                        self.config.controller.allowed_book_clock_skew_ms
                    ),
                ),
                settings=self.config,
            )
        return PolymarketActuator(
            self.config,
            client=PolymarketSDKClient(),
            operator_gate=_first_live_order_gate(self.config),
            quote_provider=quote_provider,
        )

    def _remember_paper_orderbook(self, signal: MarketSignal) -> None:
        if signal.token_id is not None:
            self._paper_orderbooks[signal.token_id] = signal.orderbook
            return
        self._paper_orderbooks[signal.market_id] = signal.orderbook

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
                if self.config.mode == RunMode.PAPER:
                    self._remember_paper_orderbook(signal)
                await self.event_bus.publish(
                    "sensor.signal",
                    _signal_event_summary(signal),
                    created_at=signal.fetched_at,
                    market_id=signal.market_id,
                )
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
                    opportunity: Opportunity | None = None
                    decision: TradeDecision | None = None
                    if isinstance(runtime.controller, OpportunityAwareController):
                        emission = await runtime.controller.on_signal(
                            signal,
                            portfolio=self.portfolio,
                        )
                        if emission is None:
                            diagnostic = _controller_diagnostic(runtime.controller)
                            if diagnostic is not None:
                                _append_bounded(self.state.controller_diagnostics, diagnostic)
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
                        created_at = _decision_created_at(signal, opportunity)
                        expires_at = _decision_expires_at(
                            signal,
                            opportunity,
                            created_at=created_at,
                        )
                        await self.decision_store.insert(
                            decision,
                            factor_snapshot_hash=(
                                opportunity.factor_snapshot_hash
                                if opportunity is not None
                                else None
                            ),
                            created_at=created_at,
                            expires_at=expires_at,
                        )
                        update_status = getattr(
                            self.decision_store,
                            "update_status",
                            None,
                        )
                        if callable(update_status):
                            await update_status(
                                decision.decision_id,
                                current_status="pending",
                                next_status="accepted",
                                updated_at=created_at,
                            )
                        _append_bounded(self.state.decisions, decision)
                        await self.event_bus.publish(
                            "controller.decision",
                            _decision_event_summary(decision),
                            created_at=created_at,
                            market_id=decision.market_id,
                            decision_id=decision.decision_id,
                        )
                        await self._enqueue_decision(
                            decision,
                            signal=signal,
                            queued_at=created_at,
                        )
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
            await self.event_bus.publish(
                "error",
                f"controller pipeline failed for {strategy_id}: {error}",
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
                raw_work_item = await asyncio.wait_for(self._decision_queue.get(), 0.05)
            except TimeoutError:
                continue

            try:
                work_item = _coerce_actuator_work_item(raw_work_item)
            except Exception as error:
                await self.event_bus.publish(
                    "error",
                    f"malformed actuator work item: {error}",
                )
                logger.warning("malformed actuator work item: %s", error)
                self._decision_queue.task_done()
                continue

            decision = work_item.decision
            signal = work_item.signal
            try:
                if (
                    self.config.mode == RunMode.LIVE
                    and self._live_trading_suspended_reason is not None
                ):
                    await self.event_bus.publish(
                        "error",
                        (
                            "live trading suspended: "
                            f"{self._live_trading_suspended_reason}"
                        ),
                        market_id=decision.market_id,
                        decision_id=decision.decision_id,
                    )
                    continue
                if self.config.mode == RunMode.PAPER and signal is not None:
                    self._remember_paper_orderbook(signal)
                await self._update_decision_status_if_supported(
                    decision.decision_id,
                    current_status="queued",
                    next_status="submitted",
                )
                order_state = await _execute_actuator_work_item(
                    self.actuator_executor,
                    decision,
                    self.portfolio,
                    dedup_acquired=work_item.dedup_acquired,
                )
                _append_bounded(self.state.orders, order_state)
                try:
                    await self.order_store.insert(order_state)
                except Exception as error:  # noqa: BLE001
                    if self.config.mode == RunMode.LIVE:
                        await self._hard_halt_live_persistence_failure(
                            phase="order_state",
                            decision=decision,
                            order_state=order_state,
                            error=error,
                        )
                    logger.warning("order persistence failed: %s", error)
                fill = _fill_from_order(order_state, decision, signal)
                if fill is not None:
                    _append_bounded(self.state.fills, fill)
                    try:
                        await self.fill_store.insert(fill)
                    except Exception as error:  # noqa: BLE001
                        if self.config.mode == RunMode.LIVE:
                            await self._hard_halt_live_persistence_failure(
                                phase="fill",
                                decision=decision,
                                order_state=order_state,
                                error=error,
                            )
                        logger.warning("fill persistence failed: %s", error)
                    self.portfolio = _portfolio_with_fill(self.portfolio, fill)
                    self._evaluator_spool.enqueue(fill, decision)
                    await self.event_bus.publish(
                        "actuator.fill",
                        _fill_event_summary(fill),
                        created_at=fill.filled_at,
                        market_id=fill.market_id,
                        decision_id=fill.decision_id,
                        fill_id=fill.fill_id or fill.order_id,
                    )
                await self._update_decision_status_if_supported(
                    decision.decision_id,
                    current_status="submitted",
                    next_status=_decision_status_from_order(order_state),
                )
            except PolymarketSubmissionUnknownError as error:
                self._live_trading_suspended_reason = "submission_unknown"
                self._stop_event.set()
                unknown_order_state = error.order_state
                if unknown_order_state is not None:
                    _append_bounded(self.state.orders, unknown_order_state)
                    try:
                        await self.order_store.insert(unknown_order_state)
                    except Exception as store_error:  # noqa: BLE001
                        if self.config.mode == RunMode.LIVE:
                            await self._hard_halt_live_persistence_failure(
                                phase="submission_unknown_order_state",
                                decision=decision,
                                order_state=unknown_order_state,
                                error=store_error,
                            )
                        logger.warning(
                            "submission_unknown order persistence failed: %s",
                            store_error,
                        )
                await self._update_decision_status_if_supported(
                    decision.decision_id,
                    current_status="submitted",
                    next_status="submission_unknown",
                )
                await self.event_bus.publish(
                    "error",
                    f"live trading suspended for submission_unknown: {error}",
                    market_id=decision.market_id,
                    decision_id=decision.decision_id,
                )
                logger.warning("live trading suspended for submission_unknown: %s", error)
            except LivePersistenceFailureError:
                raise
            except Exception as error:
                await self.event_bus.publish(
                    "error",
                    f"actuator execution failed: {error}",
                    market_id=decision.market_id,
                    decision_id=decision.decision_id,
                )
                logger.warning("actuator execution failed: %s", error)
            finally:
                self._decision_queue.task_done()

    async def _decision_expiry_loop(self) -> None:
        while True:
            if self._should_stop_decision_expiry():
                return
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=DECISION_SWEEP_INTERVAL_S,
                )
            except TimeoutError:
                pass
            if self._should_stop_decision_expiry():
                return
            try:
                await self._sweep_expired_decisions_once()
            except Exception as error:  # noqa: BLE001
                logger.warning("decision expiry sweep failed: %s", error)

    async def _enqueue_decision(
        self,
        decision: TradeDecision,
        *,
        signal: MarketSignal | None,
        dedup_acquired: bool = False,
        queued_at: datetime | None = None,
    ) -> None:
        await self._decision_queue.put(
            ActuatorWorkItem(
                decision=decision,
                signal=signal,
                dedup_acquired=dedup_acquired,
            )
        )
        await self._update_decision_status_if_supported(
            decision.decision_id,
            current_status="accepted",
            next_status="queued",
            updated_at=queued_at or (signal.fetched_at if signal is not None else None),
        )

    async def _update_decision_status_if_supported(
        self,
        decision_id: str,
        *,
        current_status: str,
        next_status: str,
        updated_at: datetime | None = None,
    ) -> bool:
        update_status = getattr(self.decision_store, "update_status", None)
        if not callable(update_status):
            return False
        try:
            return bool(
                await update_status(
                    decision_id,
                    current_status=current_status,
                    next_status=next_status,
                    updated_at=updated_at or datetime.now(tz=UTC),
                )
            )
        except Exception as error:  # noqa: BLE001
            logger.warning(
                "decision status update failed for decision_id=%s %s->%s: %s",
                decision_id,
                current_status,
                next_status,
                error,
            )
            return False

    async def _hard_halt_live_persistence_failure(
        self,
        *,
        phase: str,
        decision: TradeDecision,
        order_state: OrderState | None,
        error: BaseException,
    ) -> None:
        self._live_trading_suspended_reason = "persistence_failure"
        self._stop_event.set()
        audit_writer = LiveEmergencyAuditWriter(
            Path(self.config.live_emergency_audit_path)
        )
        try:
            await audit_writer.append(
                phase=phase,
                decision=decision,
                order_state=order_state,
                error=error,
            )
        except Exception as audit_error:  # noqa: BLE001
            logger.warning("live emergency audit append failed: %s", audit_error)
        await self.event_bus.publish(
            "error",
            f"live trading suspended: persistence failure during {phase}: {error}",
            market_id=decision.market_id,
            decision_id=decision.decision_id,
        )
        msg = f"LIVE persistence failure during {phase}: {error}"
        raise LivePersistenceFailureError(msg) from error

    async def _sweep_expired_decisions_once(
        self,
        *,
        now: datetime | None = None,
    ) -> int:
        return await self.decision_store.expire_pending(
            before=now or datetime.now(tz=UTC)
        )

    async def _assert_no_unresolved_submission_unknown_incidents(self) -> None:
        pool = self._pg_pool
        if pool is None:
            return
        acquire = getattr(pool, "acquire", None)
        if acquire is None:
            return
        async with acquire() as connection:
            count = await connection.fetchval(
                """
                SELECT COUNT(*)
                FROM order_intents
                WHERE outcome = 'submission_unknown'
                  AND reconciled_at IS NULL
                """
            )
        if int(count or 0) > 0:
            msg = (
                "LIVE start refused: unresolved submission_unknown incident "
                "requires venue reconciliation before resume"
            )
            raise RuntimeError(msg)

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

    def _should_stop_decision_expiry(self) -> bool:
        if self._stop_event.is_set():
            return True
        return self.config.mode == RunMode.BACKTEST and self._should_stop_actuator()

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
        decision_expiry_task = self._decision_expiry_task
        if decision_expiry_task is not None and not decision_expiry_task.done():
            decision_expiry_task.cancel()
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
                    for task in (
                        self._factor_service_task,
                        *self._controller_pipeline_tasks.values(),
                        decision_expiry_task,
                    )
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
        self._decision_expiry_task = None
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

    async def _reconcile_portfolio_from_db(self) -> None:
        """Rebuild the in-memory portfolio from persisted fills.

        On a fresh process start the default Runner portfolio is hardcoded
        (`Portfolio(total_usdc=1000, free_usdc=1000, locked_usdc=0,
        open_positions=[])`). Without this step, restarting in LIVE mode
        forgets all open Polymarket exposure: every new BUY decision sizes
        against the hardcoded `free_usdc` while the venue actually holds
        real positions. Empirically observed during the 2026-04-25 PAPER
        soak — DB held 44 positions / $1984 locked while a fresh runner
        instance reported `locked_usdc=0`.

        Reconciliation aggregates fills via `FillStore.read_positions`,
        sums `locked_usdc`, and rebuilds the in-memory `Portfolio`. We
        clamp `free_usdc` to 0 if reconciled exposure exceeds
        `total_usdc` and surface a warning so operators tighten risk
        caps before resuming.
        """
        pool = self._pg_pool
        if pool is None:
            return
        try:
            persisted_positions = await self.fill_store.read_positions()
        except Exception as error:  # noqa: BLE001
            if self.config.mode == RunMode.LIVE:
                # Fail closed in LIVE: silent degradation here recreates
                # the exact restart-exposure bug this method exists to
                # prevent. Operator gets a clear signal via the
                # autostart_error path on /status (see api/app.py
                # lifespan). Do not start trading without a verified
                # baseline.
                msg = (
                    "LIVE portfolio reconciliation failed "
                    f"({type(error).__name__}: {error}); refusing to start "
                    "without a verified portfolio"
                )
                raise RuntimeError(msg) from error
            logger.warning("portfolio reconciliation failed: %s", error)
            return
        open_positions = [p for p in persisted_positions if p.shares_held > 0.0]
        if not open_positions:
            # Reset to baseline so a stop/start cycle on a runner that
            # previously held positions does not leave stale `locked_usdc`,
            # `free_usdc`, or `open_positions` in `self.portfolio`. Without
            # this, a second `start()` on the same Runner instance after
            # all positions closed would still report the old locked
            # exposure and undercount free budget, even though the DB
            # is empty.
            total_budget = self.portfolio.total_usdc
            self.portfolio = replace(
                self.portfolio,
                locked_usdc=0.0,
                free_usdc=total_budget,
                open_positions=[],
            )
            logger.info(
                "Reconciled portfolio from DB: 0 positions, $0.00 locked of $%.2f total",
                total_budget,
            )
            return
        total_locked = sum(position.locked_usdc for position in open_positions)
        total_budget = self.portfolio.total_usdc
        if total_locked > total_budget:
            logger.warning(
                "Reconciled locked exposure $%.2f exceeds total budget $%.2f"
                " — clamping free_usdc=0; review risk caps before resuming",
                total_locked,
                total_budget,
            )
            free_usdc = 0.0
        else:
            free_usdc = total_budget - total_locked
        self.portfolio = replace(
            self.portfolio,
            locked_usdc=total_locked,
            free_usdc=free_usdc,
            open_positions=open_positions,
        )
        logger.info(
            "Reconciled portfolio from DB: %d positions, $%.2f locked of $%.2f total",
            len(open_positions),
            total_locked,
            total_budget,
        )

    async def _reconcile_venue_account(self) -> None:
        if self.config.mode != RunMode.LIVE:
            return
        reconciler = self.venue_account_reconciler
        if reconciler is None:
            if not self.config.live_account_reconciliation_required:
                logger.warning("LIVE venue account reconciliation is not configured")
                return
            reconciler = PolymarketVenueAccountReconciler()
        credentials = validate_live_mode_ready(self.config)
        snapshot = await reconciler.snapshot(credentials)
        report = await reconciler.compare(self.portfolio, snapshot)
        if not report.ok:
            details = "; ".join(report.mismatches) or "unknown mismatch"
            msg = f"LIVE venue account reconciliation mismatch: {details}"
            raise RuntimeError(msg)

    async def _reselect(self) -> None:
        selector = self._market_selector
        subscription_controller = self._subscription_controller
        if selector is None or subscription_controller is None:
            return
        async with self._reselection_lock:
            result = await selector.select()
            await subscription_controller.update(
                _cap_subscription_asset_ids(list(result.asset_ids), self.config)
            )

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

    async def _handle_discovery_poll_complete(self) -> None:
        try:
            await self._sync_controller_runtimes()
        except Exception as error:  # noqa: BLE001
            logger.warning("discovery-driven controller sync failed: %s", error)
        await self._request_reselection()

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
            if not current_strategy_ids and not desired_strategy_ids:
                return

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
            await subscription_controller.update(
                _cap_subscription_asset_ids(merged_asset_ids, self.config)
            )

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
            if not active_strategies:
                return []
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


@dataclass(frozen=True)
class _PostgresFlbMarketSnapshotReader:
    store: PostgresMarketDataStore

    async def latest(
        self,
        market_id: str,
        *,
        as_of: datetime,
    ) -> FlbMarketSnapshot | None:
        del as_of
        query = """
        SELECT
            markets.condition_id AS market_id,
            markets.question,
            markets.venue,
            markets.resolves_at,
            markets.last_seen_at,
            markets.yes_price,
            markets.no_price,
            markets.best_bid,
            markets.best_ask,
            markets.price_updated_at,
            markets.closed,
            markets.accepting_orders,
            MAX(CASE WHEN tokens.outcome = 'YES' THEN tokens.token_id END) AS yes_token_id,
            MAX(CASE WHEN tokens.outcome = 'NO' THEN tokens.token_id END) AS no_token_id
        FROM markets
        LEFT JOIN tokens
            ON tokens.condition_id = markets.condition_id
        WHERE markets.condition_id = $1
           OR EXISTS (
                SELECT 1
                FROM tokens AS lookup_tokens
                WHERE lookup_tokens.token_id = $1
                  AND lookup_tokens.condition_id = markets.condition_id
           )
        GROUP BY
            markets.condition_id,
            markets.question,
            markets.venue,
            markets.resolves_at,
            markets.last_seen_at,
            markets.yes_price,
            markets.no_price,
            markets.best_bid,
            markets.best_ask,
            markets.price_updated_at,
            markets.closed,
            markets.accepting_orders
        LIMIT 1
        """
        async with self.store.pool.acquire() as connection:
            row = await connection.fetchrow(query, market_id)
        if row is None:
            return None
        if row["closed"] is True or row["accepting_orders"] is False:
            return None

        yes_price = _open_probability_or_none(row["yes_price"])
        yes_token_id = row["yes_token_id"]
        no_token_id = row["no_token_id"]
        if (
            yes_price is None
            or not isinstance(yes_token_id, str)
            or not isinstance(no_token_id, str)
        ):
            return None

        observed_at = row["price_updated_at"] or row["last_seen_at"]
        if not isinstance(observed_at, datetime):
            return None

        return FlbMarketSnapshot(
            market_id=cast(str, row["market_id"]),
            title=cast(str, row["question"]),
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            yes_price=yes_price,
            yes_best_ask=_open_probability_or_none(row["best_ask"]),
            no_best_ask=_no_best_ask(
                no_price=row["no_price"],
                yes_best_bid=row["best_bid"],
            ),
            observed_at=observed_at,
            resolves_at=cast(datetime | None, row["resolves_at"]),
            venue=cast(Venue, row["venue"]),
        )


def _open_probability_or_none(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(cast(Any, value))
    except (TypeError, ValueError):
        return None
    if parsed <= 0.0 or parsed >= 1.0:
        return None
    return parsed


def _no_best_ask(
    *,
    no_price: object,
    yes_best_bid: object,
) -> float | None:
    parsed_no_price = _open_probability_or_none(no_price)
    if parsed_no_price is not None:
        return parsed_no_price
    parsed_yes_best_bid = _open_probability_or_none(yes_best_bid)
    if parsed_yes_best_bid is None:
        return None
    return _open_probability_or_none(1.0 - parsed_yes_best_bid)


def _append_bounded(items: list[T], item: T) -> None:
    items.append(item)
    overflow = len(items) - RUNNER_STATE_LIMIT
    if overflow > 0:
        del items[:overflow]


def _first_live_order_gate(
    settings: PMSSettings,
) -> DenyFirstLiveOrderGate | FileFirstLiveOrderGate:
    approval_path = settings.polymarket.first_live_order_approval_path
    if approval_path is None or approval_path.strip() == "":
        return DenyFirstLiveOrderGate()
    return FileFirstLiveOrderGate(Path(approval_path))


def _controller_diagnostic(controller: object) -> ControllerDiagnostic | None:
    diagnostic = getattr(controller, "last_diagnostic", None)
    if isinstance(diagnostic, ControllerDiagnostic):
        return diagnostic
    return None


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
        resolution_time_max_horizon_days=(
            DEFAULT_OPERATIONAL_MARKET_SELECTION_HORIZON_DAYS
        ),
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


def _operational_default_market_selection(
    market_selection: MarketSelectionSpec,
) -> MarketSelectionSpec:
    if market_selection.resolution_time_max_horizon_days != 7:
        return market_selection
    return replace(
        market_selection,
        resolution_time_max_horizon_days=(
            DEFAULT_OPERATIONAL_MARKET_SELECTION_HORIZON_DAYS
        ),
    )


def _cap_subscription_asset_ids(
    asset_ids: list[str],
    settings: PMSSettings,
) -> list[str]:
    limit = settings.sensor.max_subscription_asset_ids
    if limit is None or len(asset_ids) <= limit:
        return asset_ids
    logger.warning(
        "subscription asset set capped at %d of %d selected assets",
        limit,
        len(asset_ids),
    )
    return asset_ids[:limit]


def _fill_from_order(
    order_state: OrderState,
    decision: TradeDecision,
    signal: MarketSignal | None,
) -> FillRecord | None:
    # Emit a FillRecord whenever the venue reports a non-zero positive
    # fill, regardless of the order's terminal status. Polymarket can
    # return PARTIAL/LIVE statuses with positive `filled_notional_usdc`
    # (e.g. an IOC limit that filled half its size and cancelled the
    # rest). Pre-fix, only `MATCHED` produced a FillRecord — every
    # partial fill was silently dropped from the inner ring, breaking
    # both portfolio accounting and evaluator metrics.
    if order_state.fill_price is None or order_state.fill_price <= 0.0:
        return None
    if order_state.filled_notional_usdc <= 0.0:
        return None

    return FillRecord(
        trade_id=order_state.order_id,
        order_id=order_state.order_id,
        decision_id=decision.decision_id,
        market_id=order_state.market_id,
        token_id=order_state.token_id,
        venue=order_state.venue,
        side=decision.action if decision.action is not None else decision.side,
        fill_price=order_state.fill_price,
        fill_notional_usdc=order_state.filled_notional_usdc,
        fill_quantity=order_state.filled_quantity,
        executed_at=order_state.submitted_at,
        filled_at=order_state.last_updated_at,
        status=order_state.status,
        anomaly_flags=[],
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        resolved_outcome=_resolved_outcome(signal),
    )


def _decision_status_from_order(order_state: OrderState) -> str:
    normalized_status = order_state.status.lower()
    raw_status = order_state.raw_status.lower()
    if (
        order_state.filled_notional_usdc > 0.0
        and order_state.remaining_notional_usdc > 1e-9
    ):
        return "partially_filled"
    if order_state.filled_notional_usdc > 0.0:
        return "filled"
    if normalized_status == OrderStatus.MATCHED.value:
        return "filled"
    if normalized_status in {
        OrderStatus.CANCELLED.value,
        OrderStatus.CANCELED_MARKET_RESOLVED.value,
        "canceled",
    }:
        return "cancelled"
    if raw_status == "venue_rejection" or normalized_status == "rejected":
        return "venue_rejected"
    if normalized_status == OrderStatus.INVALID.value:
        return "rejected"
    return "submitted"


def _portfolio_with_fill(portfolio: Portfolio, fill: FillRecord) -> Portfolio:
    positions = list(portfolio.open_positions)
    fill_size = fill.fill_notional_usdc
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
    return fill.fill_quantity


def _signal_event_summary(signal: MarketSignal) -> str:
    return f"Signal {signal.market_id} @ {(signal.yes_price * 100):.1f}¢"


def _decision_event_summary(decision: TradeDecision) -> str:
    return f"Accepted {decision.side} ${decision.notional_usdc:.2f} on {decision.market_id}"


def _fill_event_summary(fill: FillRecord) -> str:
    return f"Filled {fill.side} ${fill.fill_notional_usdc:.2f} on {fill.market_id}"


def _same_position(position: Position, fill: FillRecord) -> bool:
    return (
        position.market_id == fill.market_id
        and position.token_id == fill.token_id
        and position.venue == fill.venue
        and position.side == fill.side
    )


def _resolved_outcome(signal: MarketSignal | None) -> float | None:
    if signal is None:
        return None
    raw_outcome = signal.external_signal.get("resolved_outcome")
    if raw_outcome is not None:
        return min(max(float(raw_outcome), 0.0), 1.0)
    return None


def _coerce_actuator_work_item(
    work_item: object,
) -> ActuatorWorkItem:
    if isinstance(work_item, ActuatorWorkItem):
        return work_item
    if isinstance(work_item, tuple) and len(work_item) == 2:
        decision, signal = work_item
        return ActuatorWorkItem(
            decision=cast(TradeDecision, decision),
            signal=cast(MarketSignal | None, signal),
            dedup_acquired=False,
        )
    if isinstance(work_item, tuple) and len(work_item) == 3:
        decision, signal, dedup_acquired = work_item
        return ActuatorWorkItem(
            decision=cast(TradeDecision, decision),
            signal=cast(MarketSignal | None, signal),
            dedup_acquired=bool(dedup_acquired),
        )
    msg = f"unsupported actuator work item: {type(work_item)!r}"
    raise TypeError(msg)


async def _execute_actuator_work_item(
    executor: Any,
    decision: TradeDecision,
    portfolio: Portfolio,
    *,
    dedup_acquired: bool,
) -> OrderState:
    if dedup_acquired and _executor_accepts_dedup_acquired(executor):
        return cast(
            OrderState,
            await executor.execute(
                decision,
                portfolio,
                dedup_acquired=True,
            ),
        )
    return cast(OrderState, await executor.execute(decision, portfolio))


def _executor_accepts_dedup_acquired(executor: Any) -> bool:
    execute = getattr(executor, "execute", None)
    if execute is None:
        return False
    try:
        parameters = inspect.signature(execute).parameters.values()
    except (TypeError, ValueError):
        return True

    for parameter in parameters:
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.name == "dedup_acquired":
            return True
    return False


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


def _decision_created_at(
    signal: MarketSignal,
    opportunity: Opportunity | None,
) -> datetime:
    del signal
    if opportunity is not None:
        return opportunity.created_at
    return datetime.now(tz=UTC)


def _decision_expires_at(
    signal: MarketSignal,
    opportunity: Opportunity | None,
    *,
    created_at: datetime,
) -> datetime:
    candidates = [created_at + DECISION_PENDING_TTL]
    if opportunity is not None and opportunity.expiry is not None:
        candidates.append(opportunity.expiry)
    if signal.resolves_at is not None:
        candidates.append(signal.resolves_at)
    return min(candidates)
