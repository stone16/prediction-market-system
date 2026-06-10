from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from pms.supervision import (
    ALIVE_WORKER_STATES,
    WorkerSpec,
    WorkerSupervisor,
)


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_s: float = 2.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while not predicate():
        if loop.time() >= deadline:
            msg = "timed out waiting for supervised worker state"
            raise AssertionError(msg)
        await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_clean_return_completes_wrapper_without_restart() -> None:
    """A worker that returns cleanly must not be restarted: backtest
    wind-down relies on wrapper completion meaning 'work is finished'."""
    calls = 0

    async def worker() -> None:
        nonlocal calls
        calls += 1

    supervisor = WorkerSupervisor()
    task = supervisor.spawn(
        WorkerSpec(name="w", factory=worker, transient=(OSError,))
    )

    await asyncio.wait_for(task, timeout=1.0)

    assert calls == 1
    health = supervisor.snapshot()["w"]
    assert health.state == "stopped"
    assert health.restarts == 0
    assert health.last_error_class is None


@pytest.mark.asyncio
async def test_listed_transient_restarts_with_exponential_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Listed transients within budget restart the worker with exponential
    backoff; the wrapper task stays pending so downstream done()-predicates
    never observe a restart as a shutdown."""
    backoffs: list[float] = []

    async def fake_backoff(self: WorkerSupervisor, delay_s: float) -> bool:
        del self
        backoffs.append(delay_s)
        return False

    monkeypatch.setattr(WorkerSupervisor, "_backoff_wait", fake_backoff)

    calls = 0
    healthy = asyncio.Event()

    async def worker() -> None:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise OSError("transient")
        healthy.set()
        await asyncio.Event().wait()

    supervisor = WorkerSupervisor()
    task = supervisor.spawn(
        WorkerSpec(
            name="w",
            factory=worker,
            transient=(OSError,),
            backoff_initial_s=0.5,
            backoff_max_s=8.0,
        )
    )

    await asyncio.wait_for(healthy.wait(), timeout=1.0)
    await _wait_until(lambda: supervisor.snapshot()["w"].state == "running")

    assert calls == 3
    assert not task.done()
    health = supervisor.snapshot()["w"]
    assert health.state == "running"
    assert health.restarts == 2
    assert health.last_error_class == "OSError"
    assert backoffs == [0.5, 1.0]

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_unlisted_exception_fails_loud_and_reraises() -> None:
    """An unlisted exception is permanent: state `failed`, an error event is
    published, and the wrapper re-raises so today's cascade semantics are
    preserved — but loud."""
    events: list[tuple[str, str]] = []

    async def publish(kind: str, message: str) -> None:
        events.append((kind, message))

    async def worker() -> None:
        msg = "not transient"
        raise ValueError(msg)

    supervisor = WorkerSupervisor(event_publisher=publish)
    task = supervisor.spawn(
        WorkerSpec(name="w", factory=worker, transient=(OSError,))
    )

    with pytest.raises(ValueError, match="not transient"):
        await asyncio.wait_for(task, timeout=1.0)

    health = supervisor.snapshot()["w"]
    assert health.state == "failed"
    assert health.last_error_class == "ValueError"
    assert health.state not in ALIVE_WORKER_STATES
    assert any(kind == "error" and "w" in message for kind, message in events)


@pytest.mark.asyncio
async def test_restart_budget_exhausted_fails_and_reraises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_backoff(self: WorkerSupervisor, delay_s: float) -> bool:
        del self, delay_s
        return False

    monkeypatch.setattr(WorkerSupervisor, "_backoff_wait", fake_backoff)

    events: list[tuple[str, str]] = []

    async def publish(kind: str, message: str) -> None:
        events.append((kind, message))

    calls = 0

    async def worker() -> None:
        nonlocal calls
        calls += 1
        msg = "always transient"
        raise OSError(msg)

    supervisor = WorkerSupervisor(event_publisher=publish)
    task = supervisor.spawn(
        WorkerSpec(
            name="w",
            factory=worker,
            transient=(OSError,),
            max_restarts=2,
        )
    )

    with pytest.raises(OSError, match="always transient"):
        await asyncio.wait_for(task, timeout=1.0)

    assert calls == 3  # initial attempt + 2 restarts
    health = supervisor.snapshot()["w"]
    assert health.state == "failed"
    assert health.restarts == 2
    assert health.last_error_class == "OSError"
    assert any(kind == "error" for kind, _ in events)


@pytest.mark.asyncio
async def test_cancellation_propagates_without_respawn() -> None:
    calls = 0

    async def worker() -> None:
        nonlocal calls
        calls += 1
        await asyncio.Event().wait()

    supervisor = WorkerSupervisor()
    task = supervisor.spawn(
        WorkerSpec(name="w", factory=worker, transient=(OSError,))
    )
    await _wait_until(lambda: calls == 1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert calls == 1
    assert supervisor.snapshot()["w"].state == "cancelled"


@pytest.mark.asyncio
async def test_stop_during_backoff_prevents_respawn() -> None:
    calls = 0

    async def worker() -> None:
        nonlocal calls
        calls += 1
        msg = "transient"
        raise OSError(msg)

    supervisor = WorkerSupervisor()
    task = supervisor.spawn(
        WorkerSpec(
            name="w",
            factory=worker,
            transient=(OSError,),
            backoff_initial_s=30.0,
        )
    )
    await _wait_until(lambda: supervisor.snapshot()["w"].state == "restarting")

    supervisor.stop()
    await asyncio.wait_for(task, timeout=1.0)

    assert calls == 1
    assert supervisor.snapshot()["w"].state == "stopped"


@pytest.mark.asyncio
async def test_stop_before_respawn_check_prevents_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even with the backoff wait already satisfied, a stop request wins:
    no worker may respawn during teardown."""

    async def fake_backoff(self: WorkerSupervisor, delay_s: float) -> bool:
        del delay_s
        self.stop()
        return True

    monkeypatch.setattr(WorkerSupervisor, "_backoff_wait", fake_backoff)

    calls = 0

    async def worker() -> None:
        nonlocal calls
        calls += 1
        msg = "transient"
        raise OSError(msg)

    supervisor = WorkerSupervisor()
    task = supervisor.spawn(
        WorkerSpec(name="w", factory=worker, transient=(OSError,))
    )

    await asyncio.wait_for(task, timeout=1.0)

    assert calls == 1
    assert supervisor.snapshot()["w"].state == "stopped"


@pytest.mark.asyncio
async def test_cancel_before_first_tick_still_records_cancelled() -> None:
    """A wrapper cancelled before its coroutine ever runs cannot execute
    its in-coroutine transitions; the done callback must still reconcile
    the terminal state so health never reads `running` for a dead task."""

    async def worker() -> None:
        await asyncio.Event().wait()

    supervisor = WorkerSupervisor()
    task = supervisor.spawn(
        WorkerSpec(name="w", factory=worker, transient=(OSError,))
    )
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await _wait_until(lambda: supervisor.snapshot()["w"].state == "cancelled")


@pytest.mark.asyncio
async def test_observe_records_watch_only_outcomes() -> None:
    """observe() tracks health without restart semantics: sensor _consume
    tasks and pipeline tasks have their own lifecycle machinery."""

    async def clean() -> None:
        return None

    async def failing() -> None:
        msg = "boom"
        raise RuntimeError(msg)

    async def forever() -> None:
        await asyncio.Event().wait()

    supervisor = WorkerSupervisor()
    clean_task = asyncio.create_task(clean())
    failing_task = asyncio.create_task(failing())
    cancelled_task = asyncio.create_task(forever())
    supervisor.observe(clean_task, name="clean")
    supervisor.observe(failing_task, name="failing")
    supervisor.observe(cancelled_task, name="cancelled")

    assert supervisor.snapshot()["clean"].state == "running"

    cancelled_task.cancel()
    await asyncio.gather(
        clean_task,
        failing_task,
        cancelled_task,
        return_exceptions=True,
    )
    await _wait_until(
        lambda: supervisor.snapshot()["cancelled"].state == "cancelled"
    )

    snapshot = supervisor.snapshot()
    assert snapshot["clean"].state == "stopped"
    assert snapshot["failing"].state == "failed"
    assert snapshot["failing"].last_error_class == "RuntimeError"
    assert snapshot["cancelled"].state == "cancelled"


@pytest.mark.asyncio
async def test_component_payload_is_json_shaped() -> None:
    async def worker() -> None:
        return None

    supervisor = WorkerSupervisor()
    task = supervisor.spawn(
        WorkerSpec(name="w", factory=worker, transient=(OSError,))
    )
    await asyncio.wait_for(task, timeout=1.0)

    payload = supervisor.component_payload()
    assert payload == {
        "w": {
            "state": "stopped",
            "restarts": 0,
            "last_error_class": None,
        }
    }
