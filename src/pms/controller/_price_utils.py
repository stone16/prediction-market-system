from __future__ import annotations

from math import isfinite
from typing import Any

from pms.core.models import MarketSignal


def best_ask(signal: MarketSignal) -> float | None:
    raw_external_ask = signal.external_signal.get("best_ask")
    external_ask = open_probability_or_none(raw_external_ask)
    if external_ask is not None:
        return external_ask

    raw_asks = signal.orderbook.get("asks")
    if not isinstance(raw_asks, list):
        return None
    asks: list[float] = []
    for raw_level in raw_asks:
        if not isinstance(raw_level, dict):
            continue
        price = open_probability_or_none(raw_level.get("price"))
        size = positive_float_or_none(raw_level.get("size"))
        if price is not None and size is not None:
            asks.append(price)
    if not asks:
        return None
    return min(asks)


def best_bid(signal: MarketSignal) -> float | None:
    raw_external_bid = signal.external_signal.get("best_bid")
    external_bid = open_probability_or_none(raw_external_bid)
    if external_bid is not None:
        return external_bid

    raw_bids = signal.orderbook.get("bids")
    if not isinstance(raw_bids, list):
        return None
    bids: list[float] = []
    for raw_level in raw_bids:
        if not isinstance(raw_level, dict):
            continue
        price = open_probability_or_none(raw_level.get("price"))
        size = positive_float_or_none(raw_level.get("size"))
        if price is not None and size is not None:
            bids.append(price)
    if not bids:
        return None
    return max(bids)


def spread_bps_at_decision(signal: MarketSignal) -> int | None:
    explicit_spread = nonnegative_float_or_none(signal.external_signal.get("spread_bps"))
    if explicit_spread is not None:
        return int(round(explicit_spread))

    bid = best_bid(signal)
    ask = best_ask(signal)
    if bid is None or ask is None or ask < bid:
        return None
    mid = (ask + bid) / 2.0
    if mid <= 0.0:
        return None
    return int(round((ask - bid) / mid * 10_000))


def open_probability_or_none(value: Any) -> float | None:
    parsed = positive_float_or_none(value)
    if parsed is None or parsed >= 1.0:
        return None
    return parsed


def nonnegative_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed < 0.0:
        return None
    return parsed


def positive_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed <= 0.0:
        return None
    return parsed
