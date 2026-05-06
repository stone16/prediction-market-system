from __future__ import annotations

from math import sqrt

from pms.core.models import EvalRecord, PriceChange
from pms.meta_evidence.models import RegimeClassification, ValidationRegime


def classify_regime(
    *,
    eval_records: list[EvalRecord],
    price_changes: list[PriceChange],
    volatility_threshold: float,
    drift_threshold: float,
    min_resolved_samples: int,
) -> RegimeClassification:
    resolved_records = [record for record in eval_records if record.filled]
    if len(resolved_records) >= min_resolved_samples:
        pnl_values = [record.pnl for record in resolved_records]
        volatility = _sample_stddev(pnl_values)
        drift = pnl_values[-1] - pnl_values[0] if len(pnl_values) >= 2 else 0.0
        return RegimeClassification(
            validation_regime=_classify(
                volatility=volatility,
                drift=drift,
                volatility_threshold=volatility_threshold,
                drift_threshold=drift_threshold,
            ),
            regime_source="eval_records",
            regime_sample_count=len(resolved_records),
            volatility=volatility,
            drift=drift,
        )

    prices = [change.price for change in sorted(price_changes, key=lambda item: item.ts)]
    if len(prices) >= 2:
        returns = [
            (prices[index] - prices[index - 1]) / prices[index - 1]
            for index in range(1, len(prices))
            if prices[index - 1] > 0.0
        ]
        volatility = _sample_stddev(returns)
        drift = prices[-1] - prices[0]
        return RegimeClassification(
            validation_regime=_classify(
                volatility=volatility,
                drift=drift,
                volatility_threshold=volatility_threshold,
                drift_threshold=drift_threshold,
            ),
            regime_source="price_changes",
            regime_sample_count=len(prices),
            volatility=volatility,
            drift=drift,
        )

    return RegimeClassification(
        validation_regime="other",
        regime_source="insufficient_data",
        regime_sample_count=len(prices),
        volatility=None,
        drift=None,
    )


def _classify(
    *,
    volatility: float,
    drift: float,
    volatility_threshold: float,
    drift_threshold: float,
) -> ValidationRegime:
    low_volatility = volatility < volatility_threshold
    if low_volatility and drift > drift_threshold:
        return "low_vol_bull"
    if not low_volatility and drift < -drift_threshold:
        return "high_vol_bear"
    if low_volatility and abs(drift) <= drift_threshold:
        return "range_bound"
    return "other"


def _sample_stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return sqrt(variance)
