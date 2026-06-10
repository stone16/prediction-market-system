"""Unified worker supervision for runner-owned asyncio tasks.

`WorkerSupervisor.spawn` wraps a worker coroutine factory in a wrapper task
with a declarative restart policy (`WorkerSpec`). The wrapper has exactly
four exit paths, each tested:

- **clean return** — the worker finished its work; the wrapper returns
  without respawning (preserves backtest wind-down semantics where a
  completed task means "done", not "dead").
- **listed transient within budget** — restart with exponential backoff;
  the wrapper task stays pending, so ``task.done()`` predicates downstream
  never observe a restart as a shutdown.
- **cancellation** — propagates unchanged (shutdown path).
- **unlisted exception or exhausted budget** — permanent failure: health is
  marked ``failed``, an ``error`` event is published, and the wrapper
  re-raises so the task completes loudly.

`observe` registers watch-only health tracking for tasks whose lifecycle is
owned elsewhere (sensor consume tasks, controller pipeline tasks).

The supervisor is strategy-agnostic: it knows nothing beyond coroutine
factories and exception types.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

logger = logging.getLogger(__name__)

WorkerState = Literal["running", "restarting", "stopped", "cancelled", "failed"]

ALIVE_WORKER_STATES: frozenset[str] = frozenset({"running", "restarting"})

WorkerFactory = Callable[[], Coroutine[Any, Any, None]]
EventPublisher = Callable[[str, str], Awaitable[None]]


@dataclass(frozen=True)
class WorkerSpec:
    name: str
    factory: WorkerFactory
    transient: tuple[type[BaseException], ...]
    max_restarts: int = 5
    restart_window_s: float = 600.0
    backoff_initial_s: float = 1.0
    backoff_max_s: float = 60.0


@dataclass(frozen=True)
class WorkerHealth:
    state: WorkerState
    restarts: int = 0
    last_error_class: str | None = None
    last_transition_at: datetime = field(
        default_factory=lambda: datetime.now(tz=UTC)
    )


@dataclass
class WorkerSupervisor:
    event_publisher: EventPublisher | None = None
    _health: dict[str, WorkerHealth] = field(init=False, default_factory=dict)
    _stop_requested: asyncio.Event = field(init=False)

    def __post_init__(self) -> None:
        self._stop_requested = asyncio.Event()

    def spawn(self, spec: WorkerSpec) -> asyncio.Task[None]:
        self._transition(spec.name, "running", restarts=0)
        task = asyncio.create_task(
            self._supervise(spec),
            name=f"supervised:{spec.name}",
        )
        # A task cancelled before its first tick never executes the wrapper
        # body, so the in-coroutine transitions cannot run. Reconcile the
        # terminal state from the done callback in that case.
        task.add_done_callback(
            lambda done: self._reconcile_wrapper_outcome(done, spec.name)
        )
        return task

    def observe(self, task: asyncio.Task[None], name: str) -> None:
        """Track health for a task owned elsewhere. Watch-only: no restart."""
        self._transition(name, "running")
        task.add_done_callback(
            lambda done: self._record_observed_outcome(done, name)
        )

    def stop(self) -> None:
        """Block all respawns. Call before cancelling worker tasks so a
        restart cannot race the teardown."""
        self._stop_requested.set()

    def snapshot(self) -> dict[str, WorkerHealth]:
        return dict(self._health)

    def component_payload(self) -> dict[str, object]:
        return {
            name: {
                "state": health.state,
                "restarts": health.restarts,
                "last_error_class": health.last_error_class,
            }
            for name, health in self._health.items()
        }

    async def _supervise(self, spec: WorkerSpec) -> None:
        try:
            await self._supervise_attempts(spec)
        except asyncio.CancelledError:
            self._transition(spec.name, "cancelled")
            raise

    async def _supervise_attempts(self, spec: WorkerSpec) -> None:
        restarts = 0
        restart_times: deque[float] = deque()
        while True:
            self._transition(spec.name, "running", restarts=restarts)
            try:
                await spec.factory()
            except asyncio.CancelledError:
                raise
            except spec.transient as error:
                now = time.monotonic()
                while (
                    restart_times
                    and now - restart_times[0] > spec.restart_window_s
                ):
                    restart_times.popleft()
                if len(restart_times) >= spec.max_restarts:
                    await self._record_permanent_failure(
                        spec.name,
                        error,
                        restarts=restarts,
                        reason="restart budget exhausted",
                    )
                    raise
                restart_times.append(now)
                restarts += 1
                self._transition(
                    spec.name,
                    "restarting",
                    restarts=restarts,
                    error=error,
                )
                backoff_s = min(
                    spec.backoff_initial_s * 2 ** (len(restart_times) - 1),
                    spec.backoff_max_s,
                )
                logger.warning(
                    "worker %s hit transient %s; restart %d/%d in %.1fs",
                    spec.name,
                    type(error).__name__,
                    restarts,
                    spec.max_restarts,
                    backoff_s,
                )
                await self._publish_event(
                    f"worker {spec.name} restarting after "
                    f"{type(error).__name__} (restart {restarts})",
                )
                stop_requested = await self._backoff_wait(backoff_s)
                if stop_requested or self._stop_requested.is_set():
                    self._transition(spec.name, "stopped", restarts=restarts)
                    return
            except BaseException as error:
                await self._record_permanent_failure(
                    spec.name,
                    error,
                    restarts=restarts,
                    reason="unlisted exception",
                )
                raise
            else:
                self._transition(spec.name, "stopped", restarts=restarts)
                return

    async def _backoff_wait(self, delay_s: float) -> bool:
        """Sleep for `delay_s` unless stop is requested earlier. Returns
        True when the wait ended because stop was requested."""
        try:
            await asyncio.wait_for(self._stop_requested.wait(), timeout=delay_s)
        except TimeoutError:
            return False
        return True

    async def _record_permanent_failure(
        self,
        name: str,
        error: BaseException,
        *,
        restarts: int,
        reason: str,
    ) -> None:
        self._transition(name, "failed", restarts=restarts, error=error)
        logger.error(
            "worker %s failed permanently (%s): %s",
            name,
            reason,
            error,
        )
        await self._publish_event(
            f"worker {name} failed permanently ({reason}): "
            f"{type(error).__name__}",
        )

    async def _publish_event(self, message: str) -> None:
        publisher = self.event_publisher
        if publisher is None:
            return
        try:
            await publisher("error", message)
        except Exception as publish_error:  # noqa: BLE001
            logger.warning(
                "worker event publish failed: %s",
                publish_error,
            )

    def _reconcile_wrapper_outcome(
        self,
        task: asyncio.Task[None],
        name: str,
    ) -> None:
        health = self._health.get(name)
        if health is not None and health.state not in ALIVE_WORKER_STATES:
            return
        self._record_observed_outcome(task, name)

    def _record_observed_outcome(
        self,
        task: asyncio.Task[None],
        name: str,
    ) -> None:
        if task.cancelled():
            self._transition(name, "cancelled")
            return
        error = task.exception()
        if error is not None:
            self._transition(name, "failed", error=error)
            return
        self._transition(name, "stopped")

    def _transition(
        self,
        name: str,
        state: WorkerState,
        *,
        restarts: int | None = None,
        error: BaseException | None = None,
    ) -> None:
        previous = self._health.get(name)
        self._health[name] = WorkerHealth(
            state=state,
            restarts=(
                restarts
                if restarts is not None
                else (previous.restarts if previous is not None else 0)
            ),
            last_error_class=(
                type(error).__name__
                if error is not None
                else (
                    previous.last_error_class
                    if previous is not None
                    else None
                )
            ),
            last_transition_at=datetime.now(tz=UTC),
        )
