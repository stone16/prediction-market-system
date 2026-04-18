from __future__ import annotations

from pms.core.models import MarketSignal
from pms.factors.base import FactorDefinition, FactorValueRow, OuterRingReader


class SubsetPricingViolation(FactorDefinition):
    factor_id = "subset_pricing_violation"
    required_inputs = (
        "external_signal.subset_price",
        "external_signal.superset_price",
    )

    def compute(
        self,
        signal: MarketSignal,
        outer_ring: OuterRingReader,
    ) -> FactorValueRow | None:
        del outer_ring

        raw_subset = signal.external_signal.get("subset_price")
        raw_superset = signal.external_signal.get("superset_price")
        if raw_subset is None or raw_superset is None:
            return None

        subset_price = float(raw_subset)
        superset_price = float(raw_superset)
        return FactorValueRow(
            factor_id=self.factor_id,
            param="",
            market_id=signal.market_id,
            ts=signal.timestamp,
            value=subset_price - superset_price,
        )
