from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal


DiagnosticSeverity = Literal["info", "warning", "error"]


@dataclass(frozen=True, slots=True)
class ControllerDiagnostic:
    code: str
    message: str
    market_id: str
    strategy_id: str
    strategy_version_id: str
    token_id: str | None
    severity: DiagnosticSeverity = "warning"
    metadata: Mapping[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
