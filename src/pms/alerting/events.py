from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from pms.actuator.risk import HaltTriggerKind
from pms.event_stream import RuntimeEvent


Severity = Literal["info", "warning", "critical"]
HALT_EVENT_PREFIX = "pms.halt."


@dataclass(frozen=True)
class HaltEvent:
    reason: str
    trigger_kind: HaltTriggerKind
    triggered_at: datetime
    market_id: str | None = None
    decision_id: str | None = None
    trace_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> str:
        return f"{HALT_EVENT_PREFIX}{self.trigger_kind}"

    @property
    def summary(self) -> str:
        return f"Auto-halt {self.trigger_kind}: {self.reason}"


@dataclass(frozen=True)
class AlertEvent:
    event_type: str
    severity: Severity
    message: str
    emitted_at: datetime
    details: dict[str, Any] = field(default_factory=dict)


def halt_event_from_runtime(event: RuntimeEvent) -> HaltEvent | None:
    if not event.event_type.startswith(HALT_EVENT_PREFIX):
        return None
    trigger_kind = event.event_type.removeprefix(HALT_EVENT_PREFIX)
    reason = event.summary.split(": ", maxsplit=1)[-1]
    return HaltEvent(
        reason=reason,
        trigger_kind=_coerce_trigger_kind(trigger_kind),
        triggered_at=event.created_at,
        market_id=event.market_id,
        decision_id=event.decision_id,
        details={"runtime_event_id": event.event_id, "summary": event.summary},
    )


def alert_from_halt(halt: HaltEvent) -> AlertEvent:
    message = (
        f"PMS auto-halt: `{halt.trigger_kind}`\n"
        f"Reason: `{halt.reason}`\n"
        f"Market: `{halt.market_id or 'unknown'}`\n"
        f"Decision: `{halt.decision_id or 'unknown'}`"
    )
    return AlertEvent(
        event_type=halt.event_type,
        severity="critical",
        message=message,
        emitted_at=datetime.now(tz=UTC),
        details={
            "reason": halt.reason,
            "trigger_kind": halt.trigger_kind,
            "market_id": halt.market_id,
            "decision_id": halt.decision_id,
        },
    )


def _coerce_trigger_kind(value: str) -> HaltTriggerKind:
    allowed: set[str] = {
        "none",
        "consecutive_losses",
        "slippage_spike",
        "credential_failure",
        "order_without_fill",
        "rate_limit_exceeded",
        "drawdown_circuit_breaker",
    }
    if value not in allowed:
        return "none"
    return value  # type: ignore[return-value]
