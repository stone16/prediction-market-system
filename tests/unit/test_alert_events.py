from __future__ import annotations

from datetime import UTC, datetime

from pms.alerting.events import halt_event_from_runtime
from pms.event_stream import RuntimeEvent


def test_halt_event_reason_preserves_inner_colons() -> None:
    event = RuntimeEvent(
        event_id=1,
        event_type="pms.halt.drawdown_circuit_breaker",
        created_at=datetime(2026, 5, 6, 8, 0, tzinfo=UTC),
        summary="Auto-halt drawdown_circuit_breaker: drawdown 15%: above 10% limit",
    )

    halt = halt_event_from_runtime(event)

    assert halt is not None
    assert halt.reason == "drawdown 15%: above 10% limit"


def test_halt_event_from_runtime_preserves_daily_loss_trigger() -> None:
    event = RuntimeEvent(
        event_id=2,
        event_type="pms.halt.daily_loss_limit",
        created_at=datetime(2026, 5, 6, 8, 0, tzinfo=UTC),
        summary="Auto-halt daily_loss_limit: daily_loss_limit",
    )

    halt = halt_event_from_runtime(event)

    assert halt is not None
    assert halt.trigger_kind == "daily_loss_limit"
    assert halt.reason == "daily_loss_limit"
