from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from math import isfinite
from typing import Any

from pms.core.models import MarketSignal


def best_ask(signal: MarketSignal) -> float | None:
    orderbook_ask = _best_orderbook_price(signal, "asks")
    if orderbook_ask is not None:
        return orderbook_ask
    raw_external_ask = signal.external_signal.get("best_ask")
    return open_probability_or_none(raw_external_ask)


def best_bid(signal: MarketSignal) -> float | None:
    orderbook_bid = _best_orderbook_price(signal, "bids")
    if orderbook_bid is not None:
        return orderbook_bid
    raw_external_bid = signal.external_signal.get("best_bid")
    return open_probability_or_none(raw_external_bid)


def _best_orderbook_price(signal: MarketSignal, side: str) -> float | None:
    raw_levels = signal.orderbook.get(side)
    if side not in {"bids", "asks"} or not isinstance(raw_levels, list):
        return None
    prices: list[float] = []
    for raw_level in raw_levels:
        if not isinstance(raw_level, dict):
            continue
        price = open_probability_or_none(raw_level.get("price"))
        size = positive_float_or_none(raw_level.get("size"))
        if price is not None and size is not None:
            prices.append(price)
    if not prices:
        return None
    return max(prices) if side == "bids" else min(prices)


def spread_bps_at_decision(
    signal: MarketSignal,
    *,
    token_id: str | None = None,
    outcome: str | None = None,
    yes_token_id: str | None = None,
) -> int | None:
    target_token_id = token_id or signal.token_id
    uses_signal_token = target_token_id is None or target_token_id == signal.token_id

    explicit_spread = nonnegative_float_or_none(signal.external_signal.get("spread_bps"))
    if uses_signal_token and explicit_spread is not None:
        return _rounded_decimal_int(Decimal(str(explicit_spread)))

    bid = best_bid(signal)
    ask = best_ask(signal)
    if bid is None or ask is None or ask < bid:
        return None

    if not uses_signal_token:
        if outcome != "NO" or (yes_token_id is not None and signal.token_id != yes_token_id):
            return None
        no_bid = 1.0 - ask
        no_ask = 1.0 - bid
        return _spread_bps_from_bid_ask(no_bid, no_ask)

    return _spread_bps_from_bid_ask(bid, ask)


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


def _spread_bps_from_bid_ask(bid: float, ask: float) -> int | None:
    bid_dec = _decimal_or_none(bid)
    ask_dec = _decimal_or_none(ask)
    if bid_dec is None or ask_dec is None or ask_dec < bid_dec:
        return None
    mid = (ask_dec + bid_dec) / Decimal("2")
    if mid <= 0:
        return None
    spread_bps = (ask_dec - bid_dec) / mid * Decimal("10000")
    return _rounded_decimal_int(spread_bps)


def _decimal_or_none(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _rounded_decimal_int(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_HALF_EVEN))
