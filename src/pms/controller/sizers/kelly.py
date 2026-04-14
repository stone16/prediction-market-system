from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from pms.config import PMSSettings, RiskSettings
from pms.core.models import Portfolio

# Kelly sizing is implemented directly because the checkpoint's fee-aware
# binary formula is authoritative; third-party packages may use incompatible
# odds conventions.


@dataclass(frozen=True)
class KellySizer:
    risk: RiskSettings | None = None
    fraction: Decimal = Decimal("0.25")

    def __post_init__(self) -> None:
        if self.risk is None:
            object.__setattr__(self, "risk", PMSSettings().risk)

    def size(self, *, prob: float, market_price: float, portfolio: Portfolio) -> float:
        if market_price <= 0.0 or market_price >= 1.0:
            return 0.0
        p = Decimal(str(prob))
        q = Decimal("1") - p
        price = Decimal(str(market_price))
        b = (Decimal("1") - price) / price
        kelly_fraction = (p * b - q) / b
        if kelly_fraction <= 0:
            return 0.0
        scaled_fraction = kelly_fraction * self.fraction
        raw_size = Decimal(str(portfolio.free_usdc)) * scaled_fraction
        assert self.risk is not None
        capped = min(raw_size, Decimal(str(self.risk.max_position_per_market)))
        return float(capped)
