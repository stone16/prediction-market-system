"""Execution fee helpers shared by controller and simulated fills."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any


_FEE_RATE_BPS_KEYS = ("fee_rate_bps", "feeRateBps")
_BPS_DENOMINATOR = Decimal("10000")
_MAX_FEE_RATE_BPS = Decimal("10000")


def market_fee_rate_from_metadata(
    metadata: Mapping[str, Any],
    *,
    fallback_rate: float,
) -> float:
    """Return a unit-interval fee rate from signal metadata, else fallback.

    Polymarket fee evidence arrives as basis points on market/trade payloads.
    The configured fallback remains useful for backfills and feeds that do not
    expose fee data yet.
    """

    for key in _FEE_RATE_BPS_KEYS:
        fee_rate = _fee_rate_from_bps(metadata.get(key))
        if fee_rate is not None:
            return fee_rate
    return fallback_rate


def _fee_rate_from_bps(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        bps = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    if not bps.is_finite() or bps < 0 or bps > _MAX_FEE_RATE_BPS:
        return None
    return float(bps / _BPS_DENOMINATOR)
