from __future__ import annotations

from dataclasses import dataclass, field

from pms.config import ControllerSettings
from pms.core.models import MarketSignal


@dataclass(frozen=True)
class Router:
    controller: ControllerSettings = field(default_factory=ControllerSettings)

    def gate(self, signal: MarketSignal) -> bool:
        if (
            signal.volume_24h is not None
            and signal.volume_24h < self.controller.min_volume
        ):
            return False
        return not (signal.yes_price < 0.02 or signal.yes_price > 0.98)

    def stop_conditions(self, signal: MarketSignal) -> list[str]:
        conditions = [
            f"min_volume:{self.controller.min_volume:.2f}",
            "near_resolution_price_band:0.02-0.98",
        ]
        if signal.resolves_at is not None:
            conditions.append(f"resolves_at:{signal.resolves_at.isoformat()}")
        return conditions
