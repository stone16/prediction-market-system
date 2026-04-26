"""Core immutable entities for the v2 cybernetic architecture.

Entity financial fields use Python ``float`` at the entity boundary, matching
the schema-spec field definitions. ``Decimal`` remains reserved for adapter and
calculation internals before values cross into these dataclasses.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, cast

from pms.core.enums import TimeInForce


Venue = Literal["polymarket", "kalshi"]
Outcome = Literal["YES", "NO"]
BookSide = Literal["BUY", "SELL"]
BookSource = Literal["subscribe", "reconnect", "checkpoint"]


class LiveTradingDisabledError(RuntimeError):
    """Raised when live execution is requested while live trading is disabled."""


@dataclass(frozen=True)
class MarketSignal:
    market_id: str
    token_id: str | None
    venue: Venue
    title: str
    yes_price: float
    volume_24h: float | None
    resolves_at: datetime | None
    orderbook: dict[str, Any]
    external_signal: dict[str, Any]
    fetched_at: datetime
    market_status: str

    @property
    def timestamp(self) -> datetime:
        return self.fetched_at


@dataclass(frozen=True)
class Opportunity:
    opportunity_id: str
    market_id: str
    token_id: str
    side: Literal["yes", "no"]
    selected_factor_values: Mapping[str, float]
    expected_edge: float
    rationale: str
    target_size_usdc: float
    expiry: datetime | None
    staleness_policy: str
    strategy_id: str
    strategy_version_id: str
    created_at: datetime
    factor_snapshot_hash: str | None = None
    composition_trace: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TradeDecision:
    decision_id: str
    market_id: str
    token_id: str | None
    venue: Venue
    side: BookSide
    notional_usdc: float
    order_type: str
    max_slippage_bps: int
    stop_conditions: list[str]
    prob_estimate: float
    expected_edge: float
    time_in_force: TimeInForce
    opportunity_id: str
    strategy_id: str
    strategy_version_id: str
    limit_price: float
    action: BookSide | None = None
    outcome: Outcome = "YES"
    model_id: str | None = None
    intent_key: str | None = None

    def __post_init__(self) -> None:
        if self.action is not None and self.side != self.action:
            msg = "TradeDecision.side/action mismatch"
            raise ValueError(msg)
        if self.notional_usdc <= 0.0:
            msg = "TradeDecision.notional_usdc must be > 0.0"
            raise ValueError(msg)
        if self.limit_price <= 0.0 or self.limit_price >= 1.0:
            msg = "TradeDecision.limit_price must satisfy 0.0 < limit_price < 1.0"
            raise ValueError(msg)
        raw_time_in_force = cast(object, self.time_in_force)
        if not isinstance(raw_time_in_force, TimeInForce):
            try:
                normalized = TimeInForce(str(raw_time_in_force).upper())
            except ValueError as exc:
                msg = "TradeDecision.time_in_force must be one of GTC, IOC, or FOK"
                raise ValueError(msg) from exc
            object.__setattr__(self, "time_in_force", normalized)


@dataclass(frozen=True)
class OrderState:
    order_id: str
    decision_id: str
    status: str
    market_id: str
    token_id: str | None
    venue: Venue
    requested_notional_usdc: float
    filled_notional_usdc: float
    remaining_notional_usdc: float
    fill_price: float | None
    submitted_at: datetime
    last_updated_at: datetime
    raw_status: str
    strategy_id: str
    strategy_version_id: str
    filled_quantity: float = 0.0
    pre_submit_quote: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FillRecord:
    trade_id: str
    order_id: str
    decision_id: str
    market_id: str
    token_id: str | None
    venue: Venue
    side: str
    fill_price: float
    fill_notional_usdc: float
    fill_quantity: float
    executed_at: datetime
    filled_at: datetime
    status: str
    anomaly_flags: list[str]
    strategy_id: str
    strategy_version_id: str
    fill_id: str | None = None
    fee_bps: int | None = None
    fees: float | None = None
    liquidity_side: str | None = None
    transaction_ref: str | None = None
    resolved_outcome: float | None = None


@dataclass(frozen=True)
class Position:
    market_id: str
    token_id: str | None
    venue: Venue
    side: str
    shares_held: float
    avg_entry_price: float
    unrealized_pnl: float
    locked_usdc: float


@dataclass(frozen=True)
class Portfolio:
    total_usdc: float
    free_usdc: float
    locked_usdc: float
    open_positions: list[Position]
    max_drawdown_pct: float | None = None
    max_open_positions: int | None = None


@dataclass(frozen=True)
class VenueCredentials:
    venue: Venue
    host: str
    private_key: str | None = field(default=None, repr=False)
    api_key: str | None = field(default=None, repr=False)
    api_secret: str | None = field(default=None, repr=False)
    api_passphrase: str | None = field(default=None, repr=False)
    signature_type: int | None = None
    funder_address: str | None = None
    api_key_id: str | None = field(default=None, repr=False)
    private_key_pem: str | None = field(default=None, repr=False)
    chain_id: int | None = None


@dataclass(frozen=True)
class Market:
    condition_id: str
    slug: str
    question: str
    venue: Venue
    resolves_at: datetime | None
    created_at: datetime
    last_seen_at: datetime
    volume_24h: float | None = None
    yes_price: float | None = None
    no_price: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    last_trade_price: float | None = None
    liquidity: float | None = None
    spread_bps: int | None = None
    price_updated_at: datetime | None = None


@dataclass(frozen=True)
class Token:
    token_id: str
    condition_id: str
    outcome: Outcome


@dataclass(frozen=True)
class BookSnapshot:
    id: int
    market_id: str
    token_id: str
    ts: datetime
    hash: str | None
    source: BookSource


@dataclass(frozen=True)
class BookLevel:
    snapshot_id: int
    market_id: str
    side: BookSide
    price: float
    size: float


@dataclass(frozen=True)
class PriceChange:
    id: int
    market_id: str
    token_id: str
    ts: datetime
    side: BookSide
    price: float
    size: float
    best_bid: float | None
    best_ask: float | None
    hash: str | None


@dataclass(frozen=True)
class Trade:
    id: int
    market_id: str
    token_id: str
    ts: datetime
    price: float


@dataclass(frozen=True)
class EvalRecord:
    market_id: str
    decision_id: str
    strategy_id: str
    strategy_version_id: str
    prob_estimate: float
    resolved_outcome: float
    brier_score: float
    fill_status: str
    recorded_at: datetime
    citations: list[str]
    category: str | None = None
    model_id: str | None = None
    pnl: float = 0.0
    slippage_bps: float = 0.0
    filled: bool = True


@dataclass(frozen=True)
class Feedback:
    feedback_id: str
    target: str
    source: str
    message: str
    severity: str
    created_at: datetime
    resolved: bool = False
    resolved_at: datetime | None = None
    category: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    strategy_id: str = "default"
    strategy_version_id: str = "default-v1"
