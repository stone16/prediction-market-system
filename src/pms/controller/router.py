from __future__ import annotations

from dataclasses import dataclass, field
import logging
from math import inf, isfinite
from typing import Any

from pms.config import ControllerSettings
from pms.core.models import MarketSignal


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Router:
    controller: ControllerSettings = field(default_factory=ControllerSettings)

    def gate(self, signal: MarketSignal) -> bool:
        passed = self._gate(signal)
        logger.info(
            "router funnel market_id=%s routed=%d",
            signal.market_id,
            int(passed),
            extra={
                "event": "funnel_router",
                "market_id": signal.market_id,
                "routed_count": int(passed),
            },
        )
        return passed

    def _gate(self, signal: MarketSignal) -> bool:
        if (
            signal.volume_24h is not None
            and signal.volume_24h < self.controller.min_volume
        ):
            return False
        if (
            not isfinite(signal.yes_price)
            or signal.yes_price < 0.02
            or signal.yes_price > 0.98
        ):
            return False
        if signal.resolves_at is not None and signal.resolves_at <= signal.timestamp:
            return False
        market_status = str(
            signal.external_signal.get("market_status", signal.market_status)
        ).lower()
        if market_status not in {"open", "active"}:
            return False
        spread_bps = _optional_float(signal.external_signal.get("spread_bps"))
        if spread_bps is not None and spread_bps > self.controller.max_spread_bps:
            return False
        book_age_ms = _optional_float(signal.external_signal.get("book_age_ms"))
        if book_age_ms is not None and book_age_ms > self.controller.max_book_age_ms:
            return False
        return True

    def stop_conditions(self, signal: MarketSignal) -> list[str]:
        conditions = [
            f"min_volume:{self.controller.min_volume:.2f}",
            "near_resolution_price_band:0.02-0.98",
        ]
        if signal.resolves_at is not None:
            conditions.append(f"resolves_at:{signal.resolves_at.isoformat()}")
        return conditions


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return inf
    return parsed if isfinite(parsed) else inf
