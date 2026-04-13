from __future__ import annotations

from enum import StrEnum


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    INVALID = "invalid"
    LIVE = "live"
    DELAYED = "delayed"
    MATCHED = "matched"
    UNMATCHED = "unmatched"
    CANCELED = "canceled"
    CANCELED_MARKET_RESOLVED = "canceled_market_resolved"


class MarketStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    SETTLED = "settled"
    UNOPENED = "unopened"


class FeedbackTarget(StrEnum):
    SENSOR = "sensor"
    CONTROLLER = "controller"
    ACTUATOR = "actuator"
    EVALUATOR = "evaluator"


class FeedbackSource(StrEnum):
    ACTUATOR = "actuator"
    EVALUATOR = "evaluator"
    HUMAN = "human"


class RunMode(StrEnum):
    BACKTEST = "backtest"
    PAPER = "paper"
    LIVE = "live"

