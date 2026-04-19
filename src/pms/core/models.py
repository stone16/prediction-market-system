"""Core immutable entities for the v2 cybernetic architecture.

Entity financial fields use Python ``float`` at the entity boundary, matching
the schema-spec field definitions. ``Decimal`` remains reserved for adapter and
calculation internals before values cross into these dataclasses.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


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


@dataclass(frozen=True)
class TradeDecision:
    decision_id: str
    market_id: str
    token_id: str | None
    venue: Venue
    side: str
    price: float
    size: float
    order_type: str
    max_slippage_bps: int
    stop_conditions: list[str]
    prob_estimate: float
    expected_edge: float
    time_in_force: str
    opportunity_id: str
    strategy_id: str
    strategy_version_id: str
    model_id: str | None = None


@dataclass(frozen=True)
class OrderState:
    order_id: str
    decision_id: str
    status: str
    market_id: str
    token_id: str | None
    venue: Venue
    requested_size: float
    filled_size: float
    remaining_size: float
    fill_price: float | None
    submitted_at: datetime
    last_updated_at: datetime
    raw_status: str
    strategy_id: str
    strategy_version_id: str


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
    fill_size: float
    executed_at: datetime
    filled_at: datetime
    status: str
    anomaly_flags: list[str]
    strategy_id: str
    strategy_version_id: str
    fill_id: str | None = None
    filled_contracts: float | None = None
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
