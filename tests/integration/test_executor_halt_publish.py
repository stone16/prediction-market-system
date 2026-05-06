from __future__ import annotations

import os
from typing import cast

import pytest

from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import RiskSettings
from pms.core.enums import OrderStatus
from pms.event_stream import RuntimeEventBus
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryFeedbackStore
from tests.unit.test_executor_publishes_halt import RecordingAdapter, _decision, _portfolio


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
    ),
]


async def test_executor_halt_publish_integration() -> None:
    bus = RuntimeEventBus()
    _, queue = await bus.subscribe()
    manager = RiskManager(RiskSettings(max_drawdown_pct=20.0))
    executor = ActuatorExecutor(
        adapter=RecordingAdapter(),
        risk=manager,
        feedback=ActuatorFeedback(cast(FeedbackStore, InMemoryFeedbackStore())),
        event_bus=bus,
    )

    state = await executor.execute(_decision(), _portfolio())
    event = queue.get_nowait()

    assert state.status == OrderStatus.INVALID.value
    assert manager.halt_events[-1].state.reason == "drawdown_circuit_breaker"
    assert event.event_type == "pms.halt.drawdown_circuit_breaker"
