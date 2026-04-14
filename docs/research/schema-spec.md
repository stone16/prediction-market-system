catalog_version: 2026-04-12
topic: Prediction Market Platform - Schema Design and Entity Catalog
schema_version: v1
status: draft-5

context:
  system: 'Single-process Python monorepo; sensor, controller, actuator, evaluation run in one asyncio loop.'
  venues: ['polymarket', 'kalshi']
  key_constraint: 'Core entities stay venue-agnostic; adapters own raw-schema translation.'

decimal_handling_invariant:
  applies_to:
    - 'kalshi_to_internal.market.orderbook.yes and orderbook.no size ladders'
    - 'kalshi_to_internal.order.count_fp -> requested_size'
    - 'kalshi_to_internal.trade.count_fp -> filled_contracts'
    - 'kalshi_to_internal.trade.count_fp + yes_price_dollars/no_price_dollars -> fill_size'
    - 'FillRecord.filled_contracts'
    - 'FillRecord.fill_size'
    - 'FillRecord.fees'
    - 'actuator.adapters.kalshi.KalshiActuatorAdapter'
  rules:
    - 'Parse count_fp, fee_cost, yes_price_dollars, and no_price_dollars with Decimal at adapter ingress.'
    - 'Keep Kalshi contract counts and side-relevant price operands in Decimal through multiplication, reconciliation, and fee attribution.'
    - 'Convert to float only at the final internal presentation boundary when the entity field type requires float.'
    - 'Never use Python float() for intermediate Kalshi arithmetic or orderbook-size aggregation.'
  field_lists:
    count_fields: ['count_fp', 'yes_bid_size_fp', 'yes_ask_size_fp', 'last_trade_size_fp', 'queue_position_fp']
    price_fields: ['yes_bid_dollars', 'yes_ask_dollars', 'no_bid_dollars', 'no_ask_dollars', 'yes_price_dollars', 'no_price_dollars', 'yes_price_fixed', 'no_price_fixed']
    fee_fields: ['fee_cost']
  rationale: 'Source-02 defines Kalshi quantities as fixed-point strings, and source-03 K4 warns that float coercion causes reconciliation drift. The invariant centralizes that rule so every Kalshi adapter path applies the same Decimal discipline.'

entities:
  - name: MarketSignal
    description: 'Normalized sensor output.'
    layer_boundary: 'sensor->controller'
    fields:
      - {name: market_id, type: str, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 GET /markets/{ticker}', source_field: 'OrderBookSummary.market (condition_id) | ticker', why_kept: 'cross-venue join key', when_used: 'all layers', notes: 'PM=condition_id; Kalshi=ticker'}
      - {name: token_id, type: str, required: false, nullable: true, source_repo: 'Polymarket/agents/agents/utils/objects.py | py-clob-client/py_clob_client/clob_types.py | source-02 GET /markets/{ticker}', source_field: 'clobTokenIds[0] / asset_id | ticker', why_kept: 'exact PM leg', when_used: 'controller->actuator for PM', notes: 'null on Kalshi; PM YES token is index 0'}
      - {name: venue, type: 'Literal[polymarket,kalshi]', required: true, nullable: false, source_repo: 'source-01-polymarket-schemas | source-02-kalshi-schemas', source_field: 'venue namespace', why_kept: 'adapter dispatch', when_used: 'controller, actuator', notes: 'never infer from market_id'}
      - {name: title, type: str, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 GET /markets/{ticker}', source_field: 'question | title', why_kept: 'human label', when_used: 'logs, eval', notes: 'Kalshi subtitle stays in external_signal'}
      - {name: yes_price, type: float, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 GET /markets/{ticker}', source_field: 'outcomePrices[0] | yes_bid_dollars', why_kept: 'normalized YES quote', when_used: 'edge, sizing', notes: '0.0-1.0 float'}
      - {name: volume_24h, type: float, required: false, nullable: true, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 GET /markets/{ticker}', source_field: 'volume24hr | volume_24h', why_kept: 'liquidity filter', when_used: 'controller gates', notes: 'numeric only'}
      - {name: resolves_at, type: datetime, required: false, nullable: true, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 GET /markets/{ticker}', source_field: 'endDateIso | expiration_time', why_kept: 'resolution horizon', when_used: 'forecast + eval timing', notes: 'parse ISO8601'}
      - {name: orderbook, type: dict, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 GET /markets/{ticker}/orderbook', source_field: 'bids, asks | orderbook.yes, orderbook.no', why_kept: 'executable depth', when_used: 'slippage, replay', notes: 'Kalshi asks are reconstructed reciprocals, and Kalshi sizes must be Decimal-parsed before arithmetic.'}
      - {name: external_signal, type: dict, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | py-clob-client/py_clob_client/clob_types.py | source-02 GET /markets/{ticker}', source_field: 'negRisk, spread, orderPriceMinTickSize, orderMinSize, acceptingOrders, enableOrderBook, event_ticker, open_interest', why_kept: 'venue extras', when_used: 'rules, adapters', notes: 'only sanctioned venue-specific bag'}
      - {name: fetched_at, type: datetime, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-04-architecture-patterns', source_field: 'OrderBookSummary.timestamp | timestamp', why_kept: 'staleness clock', when_used: 'watchdog, replay', notes: 'Kalshi stamps adapter time'}
      - {name: market_status, type: str, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 GET /markets/{ticker}', source_field: 'active/closed/acceptingOrders | status', why_kept: 'tradability gate', when_used: 'sensor/controller', notes: 'PM composed from booleans; Kalshi direct'}

  - name: TradeDecision
    description: 'Normalized controller intent.'
    layer_boundary: 'controller->actuator'
    fields:
      - {name: decision_id, type: str, required: true, nullable: false, source_repo: 'source-02 POST /portfolio/orders | source-04-architecture-patterns', source_field: 'client_order_id | clientOrderId', why_kept: 'idempotency key', when_used: 'retry suppression', notes: 'deterministic hash of normalized payload'}
      - {name: market_id, type: str, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 POST /portfolio/orders', source_field: 'market | ticker', why_kept: 'market bind', when_used: 'routing, joins', notes: 'matches MarketSignal.market_id'}
      - {name: token_id, type: str, required: false, nullable: true, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 POST /portfolio/orders', source_field: 'token_id | side', why_kept: 'PM leg id', when_used: 'PM actuator', notes: 'null on Kalshi'}
      - {name: venue, type: 'Literal[polymarket,kalshi]', required: true, nullable: false, source_repo: 'source-01-polymarket-schemas | source-02-kalshi-schemas', source_field: 'adapter context', why_kept: 'adapter dispatch', when_used: 'runner, actuator', notes: 'explicit not inferred'}
      - {name: side, type: 'Literal[BUY,SELL]', required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02-kalshi-schemas', source_field: 'side | side+action', why_kept: 'one exposure convention', when_used: 'sizing, execution', notes: 'relative to YES direction'}
      - {name: price, type: float, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 POST /portfolio/orders', source_field: 'price | yes_price_dollars', why_kept: 'normalized quote', when_used: 'native order build', notes: '0.0-1.0 float'}
      - {name: size, type: float, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 POST /portfolio/orders | source-03-issues-and-gotchas', source_field: 'size / amount | count_fp | SELL size semantics', why_kept: 'internal notional', when_used: 'adapter conversion', notes: 'USDC-equivalent; PM SELL -> shares=size/price'}
      - {name: order_type, type: 'Literal[limit,market]', required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 POST /portfolio/orders', source_field: 'LimitOrderArgs / MarketOrderArgs | type', why_kept: 'execution style', when_used: 'request selection', notes: 'limit vs market'}
      - {name: max_slippage_bps, type: int, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | py-clob-client/py_clob_client/clob_types.py | source-02-kalshi-schemas', source_field: 'orderPriceMinTickSize / tick_size | yes_bid_dollars / yes_ask_dollars', why_kept: 'price guardrail', when_used: 'risk check', notes: 'internal risk metadata'}
      - {name: stop_conditions, type: list, required: true, nullable: false, source_repo: 'source-03-issues-and-gotchas | source-02 POST /portfolio/orders', source_field: 'status != LIVE within 5s | expiration_ts | reduce_only', why_kept: 'abort rules', when_used: 'risk/watchdog', notes: 'unverified submit, stale book, min size, resolved market'}
      - {name: prob_estimate, type: float, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: 'predicted_prob', why_kept: 'forecast value', when_used: 'eval + attribution', notes: '0.0-1.0'}
      - {name: expected_edge, type: float, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: 'predicted_prob | min_edge', why_kept: 'explicit alpha', when_used: 'gating, reports', notes: 'typically prob_estimate - yes_price'}
      - {name: time_in_force, type: str, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 POST /portfolio/orders', source_field: 'order_type | expiration_ts', why_kept: 'persistence policy', when_used: 'adapter maps to FOK/FAK/GTC/GTD or expiry', notes: 'Kalshi has no raw TIF enum'}

  - name: OrderState
    description: 'Actuator-owned order lifecycle.'
    layer_boundary: internal
    fields:
      - {name: order_id, type: str, required: true, nullable: false, source_repo: 'nautilus_trader/adapters/polymarket/schemas/order.py | source-02 POST /portfolio/orders', source_field: 'PolymarketMakerOrder.order_id | order_id', why_kept: 'venue handle', when_used: 'poll, cancel', notes: 'stored with decision_id'}
      - {name: decision_id, type: str, required: true, nullable: false, source_repo: 'source-02 POST /portfolio/orders | source-04-architecture-patterns', source_field: 'client_order_id | clientOrderId', why_kept: 'intent join', when_used: 'dedupe, eval', notes: 'local map on PM'}
      - {name: status, type: str, required: true, nullable: false, source_repo: 'nautilus_trader/adapters/polymarket/common/enums.py | source-02-kalshi-schemas', source_field: 'PolymarketOrderStatus | status', why_kept: 'state machine', when_used: 'executor control', notes: 'PM: INVALID/LIVE/DELAYED/MATCHED/UNMATCHED/CANCELED/CANCELED_MARKET_RESOLVED'}
      - {name: market_id, type: str, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 POST /portfolio/orders', source_field: 'market | ticker', why_kept: 'market scan', when_used: 'risk, portfolio', notes: 'same as decision'}
      - {name: token_id, type: str, required: false, nullable: true, source_repo: 'nautilus_trader/adapters/polymarket/schemas/order.py | source-02 POST /portfolio/orders', source_field: 'tokenId | side', why_kept: 'leg id', when_used: 'PM cancel/replace', notes: 'null on Kalshi'}
      - {name: venue, type: 'Literal[polymarket,kalshi]', required: true, nullable: false, source_repo: 'source-01-polymarket-schemas | source-02-kalshi-schemas', source_field: 'adapter context', why_kept: 'status mapping', when_used: 'state transitions', notes: 'order_id not globally unique'}
      - {name: requested_size, type: float, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 POST /portfolio/orders', source_field: 'size / amount | count_fp', why_kept: 'remaining calc', when_used: 'partial fill logic', notes: 'internal notional'}
      - {name: filled_size, type: float, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 POST /portfolio/orders', source_field: 'size | fill_count', why_kept: 'execution progress', when_used: 'position updates', notes: 'Kalshi contracts convert by fill price'}
      - {name: remaining_size, type: float, required: true, nullable: false, source_repo: 'internal-design', source_field: 'derived', why_kept: 'residual logic', when_used: 'partial fill handling', notes: 'requested_size - filled_size when missing upstream'}
      - {name: fill_price, type: float, required: false, nullable: true, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 POST /portfolio/orders', source_field: 'price | yes_price_dollars / no_price_dollars', why_kept: 'pnl basis', when_used: 'portfolio + eval', notes: 'null until fill'}
      - {name: submitted_at, type: datetime, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: 'timestamp', why_kept: 'timeout anchor', when_used: 'verify loop', notes: 'adapter stamped'}
      - {name: last_updated_at, type: datetime, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-04-architecture-patterns', source_field: 'last_update | timestamp', why_kept: 'stale-order check', when_used: 'poll backoff', notes: 'adapter stamped on Kalshi'}
      - {name: raw_status, type: str, required: true, nullable: false, source_repo: 'nautilus_trader/adapters/polymarket/common/enums.py | source-02-kalshi-schemas', source_field: 'PolymarketOrderStatus | status', why_kept: 'debug fidelity', when_used: 'logs/tests', notes: 'never collapsed away'}

  - name: FillRecord
    description: 'Immutable execution fact.'
    layer_boundary: 'actuator->evaluation'
    fields:
      - {name: fill_id, type: str, required: false, nullable: true, source_repo: 'docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'fill_id', why_kept: 'Kalshi-specific immutable fill handle', when_used: 'reconciliation, duplicate suppression', notes: 'null on Polymarket'}
      - {name: trade_id, type: str, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'id | trade_id', why_kept: 'stable execution identifier across both venues', when_used: 'audit joins, replay dedupe', notes: 'Kalshi uses trade_id; Polymarket uses Trade.id'}
      - {name: order_id, type: str, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 POST /portfolio/orders | docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'taker_order_id / PolymarketMakerOrder.order_id | order_id', why_kept: 'order join', when_used: 'eval, portfolio', notes: 'Kalshi uses executed order_id'}
      - {name: decision_id, type: str, required: true, nullable: false, source_repo: 'source-02 POST /portfolio/orders | docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'client_order_id', why_kept: 'intent join', when_used: 'attribution', notes: 'local map on PM when venue response lacks client_order_id'}
      - {name: market_id, type: str, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 POST /portfolio/orders | docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'market | ticker | market_ticker', why_kept: 'market join', when_used: 'eval + positions', notes: 'Kalshi prefers market_ticker when present'}
      - {name: token_id, type: str, required: false, nullable: true, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 POST /portfolio/orders', source_field: 'asset_id | side', why_kept: 'leg id', when_used: 'position updates', notes: 'null on Kalshi'}
      - {name: venue, type: 'Literal[polymarket,kalshi]', required: true, nullable: false, source_repo: 'source-01-polymarket-schemas | source-02-kalshi-schemas', source_field: 'adapter context', why_kept: 'settlement semantics', when_used: 'eval + reconciliation', notes: 'off-chain vs on-chain'}
      - {name: side, type: 'Literal[BUY,SELL]', required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02-kalshi-schemas | docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'side | side+action', why_kept: 'exposure delta', when_used: 'portfolio', notes: 'YES-relative convention'}
      - {name: fill_price, type: float, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 POST /portfolio/orders | docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'price | yes_price_dollars / no_price_dollars', why_kept: 'execution price', when_used: 'pnl + eval', notes: 'normalized float'}
      - {name: filled_contracts, type: float, required: false, nullable: true, source_repo: 'docs.kalshi.com/api-reference/portfolio/get-fills | Polymarket/agents/agents/utils/objects.py', source_field: 'count_fp | size', why_kept: 'exact pre-normalization quantity', when_used: 'reconciliation, venue-specific reporting', notes: 'Kalshi count_fp stays Decimal through multiplication; PM mirrors share count'}
      - {name: fill_size, type: float, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 POST /portfolio/orders | docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'size | fill_count | count_fp', why_kept: 'executed notional', when_used: 'position + weighting', notes: 'Kalshi uses Decimal(count_fp) * Decimal(side-relevant *_dollars price)'}
      - {name: fee_bps, type: int, required: false, nullable: true, source_repo: 'py-clob-client/py_clob_client/clob_types.py | Polymarket/agents/agents/utils/objects.py', source_field: 'fee_rate_bps', why_kept: 'net pnl on Polymarket', when_used: evaluation, notes: 'nullable on Kalshi'}
      - {name: fees, type: float, required: false, nullable: true, source_repo: 'docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'fee_cost', why_kept: 'absolute fee cashflow for Kalshi fills', when_used: 'net pnl, venue fee attribution', notes: 'Decimal(fee_cost) -> float'}
      - {name: liquidity_side, type: 'Literal[TAKER,MAKER]', required: false, nullable: true, source_repo: 'docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'is_taker', why_kept: 'maker/taker affects fee schedule', when_used: 'fee attribution, execution quality analysis', notes: 'true -> TAKER, false -> MAKER'}
      - {name: executed_at, type: datetime, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'match_time | created_time', why_kept: 'canonical execution timestamp', when_used: 'time ordering, replay, evaluation', notes: 'parse ISO8601'}
      - {name: filled_at, type: datetime, required: true, nullable: false, source_repo: 'internal-design', source_field: 'derived', why_kept: 'backward-compatible alias for executed_at', when_used: 'replay + eval', notes: 'mirror executed_at'}
      - {name: transaction_ref, type: str, required: false, nullable: true, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 POST /portfolio/orders', source_field: 'transaction_hash | order_id', why_kept: 'audit handle', when_used: 'incident review', notes: 'PM tx hash; Kalshi falls back to order_id'}
      - {name: status, type: str, required: true, nullable: false, source_repo: 'nautilus_trader/adapters/polymarket/common/enums.py | source-02-kalshi-schemas | docs.kalshi.com/api-reference/portfolio/get-fills', source_field: 'PolymarketTradeStatus | status (joined by order_id from order poll)', why_kept: 'finality state', when_used: 'scoring gate, fill reconciliation', notes: 'Kalshi GET /portfolio/fills is joined with order status so the execution row carries final state'}
      - {name: anomaly_flags, type: list, required: true, nullable: false, source_repo: 'source-03-issues-and-gotchas', source_field: '#258, #292, #294, #345, K5', why_kept: 'typed anomalies', when_used: 'alerts + eval', notes: 'submission_unverified, ws_watchdog_reconnect, sell_size_converted, partial_fill_residual, kalshi_seq_gap_resubscribe'}

  - name: Position
    description: 'Current exposure state.'
    layer_boundary: internal
    fields:
      - {name: market_id, type: str, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 GET /markets/{ticker}', source_field: 'market | ticker', why_kept: 'market join', when_used: portfolio, notes: 'same as signal'}
      - {name: token_id, type: str, required: false, nullable: true, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02-kalshi-schemas', source_field: 'asset_id | side', why_kept: 'PM leg split', when_used: 'PM netting', notes: 'null on Kalshi'}
      - {name: venue, type: 'Literal[polymarket,kalshi]', required: true, nullable: false, source_repo: 'source-01-polymarket-schemas | source-02-kalshi-schemas', source_field: 'adapter context', why_kept: 'settlement rules', when_used: 'portfolio reporting', notes: 'venue-specific close flows'}
      - {name: side, type: 'Literal[LONG,SHORT]', required: true, nullable: false, source_repo: 'internal-design', source_field: 'derived', why_kept: 'LONG = YES exposure, SHORT = NO exposure', when_used: 'Portfolio.open_positions aggregation, risk manager exposure calc', notes: 'LONG = bought YES or sold NO; SHORT = bought NO or sold YES'}
      - {name: shares_held, type: float, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02-kalshi-schemas', source_field: 'size | count_fp', why_kept: quantity, when_used: 'unwind, pnl', notes: 'Kalshi contracts; PM shares'}
      - {name: avg_entry_price, type: float, required: true, nullable: false, source_repo: 'Polymarket/agents/agents/utils/objects.py | source-02 POST /portfolio/orders', source_field: 'price | yes_price_dollars / no_price_dollars', why_kept: 'cost basis', when_used: 'unrealized pnl', notes: '0.0-1.0 float'}
      - {name: unrealized_pnl, type: float, required: true, nullable: false, source_repo: 'internal-design', source_field: 'derived', why_kept: 'mark-to-market', when_used: 'risk dashboards', notes: 'quote minus basis times quantity'}
      - {name: locked_usdc, type: float, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: 'locked_usdc', why_kept: 'reserved capital', when_used: 'free capital calc', notes: 'resting risk notional'}

  - name: Portfolio
    description: 'Capital snapshot.'
    layer_boundary: internal
    fields:
      - {name: total_usdc, type: float, required: true, nullable: false, source_repo: 'internal-design', source_field: 'derived', why_kept: 'top-level capital', when_used: sizing, notes: 'derived free+locked'}
      - {name: free_usdc, type: float, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: 'free_usdc', why_kept: 'deployable capital', when_used: 'pre-trade checks', notes: 'updated after verify/fill'}
      - {name: locked_usdc, type: float, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: 'locked_usdc', why_kept: 'reserved capital', when_used: 'risk caps', notes: 'sum of open reserves'}
      - {name: open_positions, type: list, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: 'open_positions', why_kept: 'materialized exposure list', when_used: 'controller + actuator', notes: 'list[Position]'}
      - {name: max_drawdown_pct, type: float, required: false, nullable: true, source_repo: 'internal-design', source_field: 'derived', why_kept: 'Circuit breaker: if drawdown exceeds this, halt new orders', when_used: 'risk.py checks this before passing decisions to actuator', notes: 'e.g. 0.20 = halt if down 20% from peak. null = no limit'}
      - {name: max_open_positions, type: int, required: false, nullable: true, source_repo: 'internal-design', source_field: 'derived', why_kept: 'Prevents over-diversification / excessive capital lock', when_used: 'risk.py validates position count before new order', notes: 'null = no limit'}

  - name: VenueCredentials
    description: 'Venue auth config union.'
    layer_boundary: internal
    fields:
      - {name: venue, type: 'Literal[polymarket,kalshi]', required: true, nullable: false, source_repo: 'source-01-polymarket-schemas | source-02-kalshi-schemas', source_field: 'auth namespace', why_kept: 'shape switch', when_used: 'config validation', notes: 'PM vs Kalshi subsets'}
      - {name: private_key, type: str, required: false, nullable: true, source_repo: 'source-01-polymarket-schemas', source_field: 'POLYGON_WALLET_PRIVATE_KEY', why_kept: 'PM L1 signer', when_used: 'EIP-712 startup', notes: 'PM only'}
      - {name: api_key, type: str, required: false, nullable: true, source_repo: 'source-01-polymarket-schemas', source_field: 'CLOB_API_KEY', why_kept: 'PM L2 key', when_used: 'REST auth', notes: 'PM only'}
      - {name: api_secret, type: str, required: false, nullable: true, source_repo: 'source-01-polymarket-schemas', source_field: 'CLOB_SECRET', why_kept: 'PM L2 secret', when_used: 'REST auth', notes: 'PM only; redact logs'}
      - {name: api_passphrase, type: str, required: false, nullable: true, source_repo: 'source-01-polymarket-schemas', source_field: 'CLOB_PASS_PHRASE', why_kept: 'PM L2 passphrase', when_used: 'REST auth', notes: 'PM only; redact logs'}
      - {name: signature_type, type: int, required: false, nullable: true, source_repo: 'py-clob-client/py_clob_client/clob_types.py | nautilus_trader/adapters/polymarket/common/enums.py', source_field: 'BalanceAllowanceParams.signature_type | PolymarketSignatureType', why_kept: 'PM signing mode', when_used: 'startup validation', notes: '0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE'}
      - {name: funder_address, type: str, required: false, nullable: true, source_repo: 'source-01-polymarket-schemas', source_field: 'POLYMARKET_PROXY_ADDRESS', why_kept: 'PM proxy funder', when_used: 'proxy mode orders', notes: 'PM only'}
      - {name: api_key_id, type: str, required: false, nullable: true, source_repo: 'source-02-kalshi-schemas', source_field: 'api_key_id | KALSHI-ACCESS-KEY', why_kept: 'Kalshi access id', when_used: 'header signing', notes: 'Kalshi only'}
      - {name: private_key_pem, type: str, required: false, nullable: true, source_repo: 'source-02-kalshi-schemas', source_field: 'private_key_pem', why_kept: 'Kalshi signer', when_used: 'header signing', notes: 'Kalshi only'}
      - {name: host, type: str, required: true, nullable: false, source_repo: 'source-01-polymarket-schemas | source-02-kalshi-schemas', source_field: 'https://clob.polymarket.com | https://api.elections.kalshi.com/trade-api/v2', why_kept: 'endpoint selection', when_used: 'client init', notes: 'Kalshi may use demo host'}
      - {name: chain_id, type: int, required: false, nullable: true, source_repo: 'source-03-issues-and-gotchas | source-04-architecture-patterns', source_field: 'chain_id=137', why_kept: 'PM signing safety', when_used: 'startup health check', notes: 'assert 137 on PM'}

  - name: EvalRecord
    description: 'Forecast-to-outcome evaluation row.'
    layer_boundary: internal
    fields:
      - {name: market_id, type: str, required: true, nullable: false, source_repo: 'py-clob-client/py_clob_client/clob_types.py | source-02 GET /markets/{ticker}', source_field: 'market | ticker', why_kept: 'score rollup key', when_used: metrics, notes: 'same as fill/signal'}
      - {name: decision_id, type: str, required: true, nullable: false, source_repo: 'source-02 POST /portfolio/orders | source-04-architecture-patterns', source_field: 'client_order_id | clientOrderId', why_kept: 'forecast join', when_used: attribution, notes: 'same as TradeDecision'}
      - {name: prob_estimate, type: float, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: 'predicted_prob', why_kept: 'brier input', when_used: 'metrics.py', notes: 'copied from decision'}
      - {name: resolved_outcome, type: float, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: outcome, why_kept: 'brier label', when_used: 'metrics.py', notes: '1.0 YES, 0.0 NO'}
      - {name: brier_score, type: float, required: true, nullable: false, source_repo: 'source-04-architecture-patterns', source_field: 'brier_score(predicted_prob, outcome)', why_kept: 'main score', when_used: 'evaluation reports', notes: '(predicted_prob-outcome)^2'}
      - {name: fill_status, type: str, required: true, nullable: false, source_repo: 'nautilus_trader/adapters/polymarket/common/enums.py | source-02-kalshi-schemas', source_field: 'PolymarketTradeStatus | status', why_kept: 'finality gate', when_used: 'score filtering', notes: 'avoid scoring non-final fills'}
      - {name: recorded_at, type: datetime, required: true, nullable: false, source_repo: 'internal-design', source_field: derived, why_kept: 'durability order', when_used: 'spool + replay', notes: 'spool timestamp'}
      - {name: citations, type: list, required: true, nullable: false, source_repo: 'README.md | source-04-architecture-patterns', source_field: 'benchmark_answers | evaluation architecture', why_kept: auditability, when_used: 'export/review', notes: 'must be non-empty'}

venue_mappings:
  polymarket_to_internal:
    market:
      - {external_field: condition_id, internal_field: market_id, transform: 'direct copy', notes: 'primary cross-venue market id'}
      - {external_field: clobTokenIds[0], internal_field: token_id, transform: 'json.loads if needed, index 0', notes: 'YES token'}
      - {external_field: clobTokenIds[1], internal_field: external_signal.no_token_id, transform: 'json.loads if needed, index 1', notes: 'NO token'}
      - {external_field: question, internal_field: title, transform: 'copy', notes: 'market label'}
      - {external_field: description, internal_field: external_signal.description, transform: 'copy', notes: 'operator context'}
      - {external_field: outcomes, internal_field: external_signal.outcomes, transform: 'copy/json.loads', notes: 'YES/NO order preserved'}
      - {external_field: outcomePrices[0], internal_field: yes_price, transform: 'parse float', notes: 'YES quote'}
      - {external_field: outcomePrices[1], internal_field: external_signal.no_price, transform: 'parse float', notes: 'NO quote'}
      - {external_field: endDateIso, internal_field: resolves_at, transform: 'ISO8601 -> datetime', notes: 'resolution time'}
      - {external_field: volume24hr, internal_field: volume_24h, transform: 'copy', notes: '24h volume'}
      - {external_field: volume, internal_field: external_signal.volume_lifetime, transform: 'copy', notes: 'lifetime volume'}
      - {external_field: liquidity, internal_field: external_signal.liquidity, transform: 'copy', notes: 'venue liquidity metric'}
      - {external_field: negRisk, internal_field: external_signal.neg_risk, transform: 'copy', notes: 'multi-outcome marker'}
      - {external_field: orderPriceMinTickSize, internal_field: external_signal.price_tick_size, transform: 'copy', notes: 'tick rule'}
      - {external_field: orderMinSize, internal_field: external_signal.min_order_size_usdc, transform: 'copy', notes: 'min notional'}
      - {external_field: spread, internal_field: external_signal.spread, transform: 'copy', notes: 'spread metric'}
      - {external_field: acceptingOrders, internal_field: external_signal.accepting_orders, transform: 'copy', notes: 'tradability flag'}
      - {external_field: enableOrderBook, internal_field: external_signal.orderbook_enabled, transform: 'copy', notes: 'book enabled'}
      - {external_field: active/closed, internal_field: market_status, transform: 'compose', notes: 'open/closed status'}
      - {external_field: restricted, internal_field: external_signal.restricted, transform: 'copy', notes: 'restriction flag'}
      - {external_field: funded, internal_field: external_signal.funded, transform: 'copy', notes: 'funded flag'}
      - {external_field: startDateIso, internal_field: external_signal.starts_at, transform: 'ISO8601 -> datetime', notes: 'market start'}
      - {external_field: bids, internal_field: orderbook.bids, transform: 'parse OrderSummary[]', notes: 'explicit bids'}
      - {external_field: asks, internal_field: orderbook.asks, transform: 'parse OrderSummary[]', notes: 'explicit asks'}
      - {external_field: timestamp, internal_field: fetched_at, transform: 'parse timestamp', notes: 'sensor clock'}
      - {external_field: asset_id, internal_field: token_id, transform: 'copy', notes: 'book leg id'}
      - {external_field: min_order_size, internal_field: external_signal.book_min_order_size_shares, transform: 'parse float', notes: 'book min in shares'}
      - {external_field: tick_size, internal_field: external_signal.book_tick_size, transform: 'parse float', notes: 'book tick'}
      - {external_field: last_trade_price, internal_field: external_signal.last_trade_price, transform: 'parse float', notes: 'recent trade'}
      - {external_field: hash, internal_field: external_signal.orderbook_hash, transform: 'copy', notes: 'change detector'}
    order:
      - {external_field: tokenId, internal_field: token_id, transform: 'copy', notes: 'PM leg id'}
      - {external_field: side, internal_field: side, transform: 'copy', notes: 'BUY/SELL'}
      - {external_field: price, internal_field: fill_price, transform: 'parse float', notes: 'execution/request price'}
      - {external_field: feeRateBps / fee_rate_bps, internal_field: fee_bps, transform: 'int()', notes: 'fee metadata'}
      - {external_field: expiration, internal_field: time_in_force, transform: '0=>GTC else expiry-bound', notes: 'TIF bridge'}
      - {external_field: nonce, internal_field: external_signal.nonce, transform: 'copy', notes: 'cancel/debug'}
      - {external_field: signatureType, internal_field: external_signal.signature_type, transform: 'map enum', notes: 'EOA/proxy/safe'}
      - {external_field: maker, internal_field: external_signal.funder_address, transform: 'copy', notes: 'maker wallet'}
      - {external_field: signer, internal_field: external_signal.signer_address, transform: 'copy', notes: 'signer wallet'}
      - {external_field: taker, internal_field: external_signal.taker_address, transform: 'copy', notes: 'counterparty restriction'}
      - {external_field: makerAmount, internal_field: requested_size, transform: 'BUY=USDC; SELL requires conversion', notes: 'do not assume symmetry'}
      - {external_field: takerAmount, internal_field: external_signal.requested_shares, transform: 'parse float', notes: 'share amount'}
      - {external_field: PolymarketMakerOrder.order_id, internal_field: order_id, transform: 'copy', notes: 'external order handle'}
      - {external_field: PolymarketOrderStatus, internal_field: status, transform: 'enum passthrough', notes: 'strict state machine'}
    trade:
      - {external_field: id, internal_field: external_signal.trade_id, transform: 'copy', notes: 'trade id'}
      - {external_field: taker_order_id, internal_field: order_id, transform: 'copy', notes: 'order join'}
      - {external_field: market, internal_field: market_id, transform: 'copy', notes: 'condition_id'}
      - {external_field: asset_id, internal_field: token_id, transform: 'copy', notes: 'leg token'}
      - {external_field: side, internal_field: side, transform: 'copy', notes: 'BUY/SELL'}
      - {external_field: size, internal_field: fill_size, transform: 'shares * price -> internal notional', notes: 'normalize unit'}
      - {external_field: fee_rate_bps, internal_field: fee_bps, transform: 'int()', notes: fee}
      - {external_field: price, internal_field: fill_price, transform: 'parse float', notes: 'execution price'}
      - {external_field: status, internal_field: status, transform: 'enum passthrough', notes: 'trade finality'}
      - {external_field: match_time, internal_field: filled_at, transform: 'ISO8601 -> datetime', notes: 'fill time'}
      - {external_field: last_update, internal_field: last_updated_at, transform: 'ISO8601 -> datetime', notes: 'status update'}
      - {external_field: outcome, internal_field: external_signal.outcome_leg, transform: 'copy', notes: 'YES/NO'}
      - {external_field: maker_address, internal_field: external_signal.maker_address, transform: 'copy', notes: audit}
      - {external_field: owner, internal_field: external_signal.owner, transform: 'copy', notes: audit}
      - {external_field: transaction_hash, internal_field: transaction_ref, transform: 'copy', notes: 'on-chain ref'}
      - {external_field: bucket_index, internal_field: external_signal.bucket_index, transform: 'copy', notes: diagnostic}
      - {external_field: maker_orders, internal_field: external_signal.maker_orders, transform: 'copy', notes: diagnostic}
      - {external_field: type, internal_field: external_signal.raw_type, transform: 'copy', notes: 'expected TRADE'}
    dropped_fields:
      - {external_field: id, reason: 'Gamma row id is weaker than condition_id'}
      - {external_field: questionID, reason: 'not needed after market_id normalization'}
      - {external_field: archived, reason: 'covered by market_status'}
      - {external_field: featured, reason: 'UI-only'}
      - {external_field: new, reason: 'UI-only'}
      - {external_field: volumeNum, reason: 'redundant with volume'}
      - {external_field: volumeClob, reason: 'redundant diagnostic split'}
      - {external_field: volume24hrClob, reason: 'redundant diagnostic split'}
      - {external_field: liquidityNum, reason: 'redundant with liquidity'}
      - {external_field: liquidityClob, reason: 'diagnostic split only'}
      - {external_field: umaBond, reason: 'settlement-governance detail'}
      - {external_field: umaReward, reason: 'settlement-governance detail'}
      - {external_field: marketMakerAddress, reason: 'operational metadata'}
      - {external_field: resolvedBy, reason: 'resolver metadata not needed live'}
      - {external_field: createdAt, reason: 'not part of runtime signal'}
      - {external_field: updatedAt, reason: 'fetched_at covers freshness'}
      - {external_field: groupItemTitle, reason: 'UI grouping'}
      - {external_field: groupItemThreshold, reason: 'UI grouping'}
      - {external_field: events, reason: 'large nested payload; keep out of core'}
      - {external_field: ready, reason: 'overlaps tradability flags'}
      - {external_field: deployed, reason: 'operational flag only'}

  kalshi_to_internal:
    market:
      - {external_field: ticker, internal_field: market_id, transform: 'copy', notes: 'primary market id'}
      - {external_field: event_ticker, internal_field: external_signal.event_ticker, transform: 'copy', notes: 'event group'}
      - {external_field: title, internal_field: title, transform: 'copy', notes: label}
      - {external_field: subtitle, internal_field: external_signal.subtitle, transform: 'copy', notes: 'secondary label'}
      - {external_field: status, internal_field: market_status, transform: 'copy', notes: 'open/closed/settled/unopened'}
      - {external_field: expiration_time, internal_field: resolves_at, transform: 'ISO8601 -> datetime', notes: 'resolution time'}
      - {external_field: yes_bid_dollars, internal_field: yes_price, transform: 'parse float', notes: 'current YES quote'}
      - {external_field: yes_ask_dollars, internal_field: external_signal.yes_ask, transform: 'parse float', notes: 'YES ask'}
      - {external_field: no_bid_dollars, internal_field: external_signal.no_bid, transform: 'parse float', notes: 'NO bid'}
      - {external_field: no_ask_dollars, internal_field: external_signal.no_ask, transform: 'parse float', notes: 'NO ask'}
      - {external_field: yes_bid_size_fp, internal_field: external_signal.best_yes_bid_size, transform: 'Decimal parse', notes: 'best YES bid size'}
      - {external_field: yes_ask_size_fp, internal_field: external_signal.best_yes_ask_size, transform: 'Decimal parse', notes: 'best YES ask size'}
      - {external_field: last_trade_size_fp, internal_field: external_signal.last_trade_size, transform: 'Decimal parse', notes: 'recent trade size'}
      - {external_field: volume, internal_field: external_signal.volume_lifetime, transform: 'copy', notes: 'lifetime contracts'}
      - {external_field: volume_24h, internal_field: volume_24h, transform: 'copy', notes: '24h contracts'}
      - {external_field: open_interest, internal_field: external_signal.open_interest, transform: 'copy', notes: 'outstanding contracts'}
      - {external_field: orderbook.yes, internal_field: orderbook.bids, transform: '[price_cents,size] -> float+Decimal', notes: 'explicit YES bids; sizes stay Decimal until arithmetic is finished'}
      - {external_field: orderbook.no, internal_field: orderbook.asks, transform: 'yes_ask = 1.00 - no_bid', notes: 'asks are reciprocal, not explicit; NO ladder sizes stay Decimal until arithmetic is finished'}
    order:
      - {external_field: ticker, internal_field: market_id, transform: 'copy', notes: 'market key'}
      - {external_field: side + action, internal_field: side, transform: 'yes+buy=>BUY, yes+sell=>SELL, no+buy=>SELL, no+sell=>BUY', notes: 'YES-relative normalization'}
      - {external_field: count_fp, internal_field: requested_size, transform: 'Decimal(count_fp) * Decimal(price)', notes: 'internal notional'}
      - {external_field: type, internal_field: order_type, transform: 'copy', notes: 'limit/market'}
      - {external_field: yes_price_dollars / no_price_dollars, internal_field: fill_price, transform: 'parse float', notes: 'side-relevant price'}
      - {external_field: client_order_id, internal_field: decision_id, transform: 'copy', notes: 'native idempotency'}
      - {external_field: expiration_ts, internal_field: time_in_force, transform: 'null=>GTC-like else expiry-bound', notes: 'no raw TIF enum'}
      - {external_field: reduce_only, internal_field: external_signal.reduce_only, transform: 'copy', notes: 'risk metadata'}
      - {external_field: self_trade_prevention_type, internal_field: external_signal.self_trade_prevention_type, transform: 'copy', notes: 'risk metadata'}
      - {external_field: order_id, internal_field: order_id, transform: 'copy', notes: 'external order id'}
      - {external_field: status, internal_field: status, transform: 'resting=>LIVE, canceled=>CANCELED, executed=>MATCHED', notes: 'internal state bridge'}
      - {external_field: fill_count, internal_field: filled_size, transform: 'Decimal(fill_count) * Decimal(fill price)', notes: 'normalize contracts to notional'}
      - {external_field: queue_position_fp, internal_field: external_signal.queue_position, transform: 'Decimal parse', notes: diagnostic}
    trade:
      - {external_field: fill_id, internal_field: fill_id, transform: 'copy', notes: 'Kalshi-specific fill id from GET /portfolio/fills'}
      - {external_field: trade_id, internal_field: trade_id, transform: 'copy', notes: 'trade id from GET /portfolio/fills'}
      - {external_field: market_ticker, internal_field: market_id, transform: 'copy (preferred over ticker when present)', notes: 'canonical market id on fills'}
      - {external_field: ticker, internal_field: market_id, transform: 'copy only when market_ticker is absent', notes: 'fallback market id'}
      - {external_field: side + action, internal_field: side, transform: 'same as order mapping', notes: 'YES-relative normalization'}
      - {external_field: count_fp, internal_field: filled_contracts, transform: 'Decimal(count_fp)', notes: 'exact contract count from fills; keep Decimal(count_fp) in Decimal form until reconciliation and notional multiplication are complete'}
      - {external_field: count_fp + yes_price_dollars/no_price_dollars, internal_field: fill_size, transform: 'Decimal(count_fp) * Decimal(side-relevant dollars price)', notes: 'normalized notional; compute in Decimal and only cast after the multiplication'}
      - {external_field: yes_price_dollars / no_price_dollars, internal_field: fill_price, transform: 'parse float from side-relevant price', notes: 'execution price'}
      - {external_field: yes_price_dollars, internal_field: external_signal.yes_price_dollars, transform: 'copy', notes: 'raw YES fill price preserved for reconciliation'}
      - {external_field: no_price_dollars, internal_field: external_signal.no_price_dollars, transform: 'copy', notes: 'raw NO fill price preserved for reconciliation'}
      - {external_field: yes_price_fixed, internal_field: external_signal.yes_price_fixed, transform: 'copy', notes: 'raw fixed-point YES price from fills preserved for reconciliation'}
      - {external_field: no_price_fixed, internal_field: external_signal.no_price_fixed, transform: 'copy', notes: 'raw fixed-point NO price from fills preserved for reconciliation'}
      - {external_field: order_id, internal_field: order_id, transform: 'copy', notes: 'audit handle'}
      - {external_field: client_order_id, internal_field: decision_id, transform: 'copy', notes: 'idempotent intent'}
      - {external_field: is_taker, internal_field: liquidity_side, transform: 'true=>TAKER, false=>MAKER', notes: 'critical for fee attribution'}
      - {external_field: fee_cost, internal_field: fees, transform: 'Decimal(fee_cost) -> float', notes: 'absolute fee cashflow from fills'}
      - {external_field: created_time, internal_field: executed_at, transform: 'ISO8601 -> datetime', notes: 'execution timestamp'}
      - {external_field: ts, internal_field: external_signal.exchange_ts, transform: 'copy', notes: 'exchange timestamp from fills'}
      - {external_field: subaccount_number, internal_field: external_signal.subaccount_number, transform: 'copy', notes: 'account partition for reconciliation'}
      - {external_field: status, internal_field: status, transform: 'executed=>MATCHED, canceled=>CANCELED, resting=>LIVE', notes: 'joined from order poll by order_id so fill reconciliation carries final state'}
      - {external_field: fill_count, internal_field: external_signal.fill_contracts, transform: 'copy/Decimal parse', notes: 'order-response contract count used for order-state reconciliation'}
      - {external_field: queue_position_fp, internal_field: external_signal.queue_position, transform: 'Decimal parse', notes: 'order-response diagnostic'}
    dropped_fields:
      - {external_field: yes_bid, reason: 'removed Jan 2026; silent break risk'}
      - {external_field: yes_ask, reason: 'removed Jan 2026; use _dollars'}
      - {external_field: no_bid, reason: 'removed Jan 2026; use _dollars'}
      - {external_field: no_ask, reason: 'removed Jan 2026; use _dollars'}
      - {external_field: tick_size, reason: 'removed Jan 2026'}
      - {external_field: previous_yes_bid, reason: 'removed Jan 2026'}
      - {external_field: previous_yes_ask, reason: 'removed Jan 2026'}
      - {external_field: previous_price, reason: 'removed Jan 2026'}
      - {external_field: response_price_units, reason: 'removed Jan 2026'}
      - {external_field: notional_value, reason: 'compute from count_fp * price internally'}
      - {external_field: liquidity, reason: 'removed Jan 2026; use depth/open_interest'}

directory_structure: |
  pms/
    pyproject.toml                 # deps, pytest, CLI entrypoints
    README.md                      # operator runbook and runtime semantics
    src/pms/
      __init__.py                  # package marker
      runner.py                    # asyncio runtime wiring all four layers
      config.py                    # Pydantic settings for VenueCredentials and runtime config
      core/
        __init__.py                # core exports
        models.py                  # stdlib-only entities and enums
        interfaces.py              # IForecaster, ISizer, IActuator, IEvaluator, sensor interfaces
        enums.py                   # strict internal enums
        mapping.py                 # pure transform helpers
      sensor/
        __init__.py                # sensor exports
        watchdog.py                # inactivity and seq-gap watchdogs
        stream.py                  # websocket/REST orchestration and queue fan-out
        adapters/
          __init__.py              # adapter namespace
          polymarket.py            # Gamma + CLOB -> MarketSignal
          kalshi.py                # Kalshi market/orderbook -> MarketSignal
      controller/
        __init__.py                # controller exports
        pipeline.py                # compose forecaster, calibrator, sizer, router
        router.py                  # gating, venue selection, stop-condition assembly
        forecasters/
          __init__.py              # forecaster namespace
          rules.py                 # rule-based forecaster
          statistical.py           # classical model forecaster
          llm.py                   # LLM forecaster behind IForecaster
        calibrators/
          __init__.py              # calibrator namespace
          netcal.py                # probability calibration wrapper
        sizers/
          __init__.py              # sizer namespace
          kelly.py                 # fractional Kelly sizing and min-order floor
      actuator/
        __init__.py                # actuator exports
        executor.py                # submit->verify->poll loop
        risk.py                    # slippage, min size, residual-order checks
        adapters/
          __init__.py              # adapter namespace
          polymarket.py            # TradeDecision -> LimitOrderArgs/MarketOrderArgs
          kalshi.py                # TradeDecision -> ticker+side+action+count_fp
          paper.py                 # paper trading implementation
      evaluation/
        __init__.py                # evaluation exports
        spool.py                   # async durable queue for eval work
        metrics.py                 # Brier, calibration, anomaly metrics
        adapters/
          __init__.py              # adapter namespace
          scoring.py               # FillRecord -> EvalRecord
    tests/
      conftest.py                  # shared fixtures and loop helpers
      fixtures/
        polymarket_market.json     # captured PM payloads
        kalshi_market.json         # captured Kalshi payloads
      unit/
        test_models.py             # entity and enum validation
        test_venue_mapping.py      # raw->internal transform tests
        test_kelly.py              # sizing math tests
        test_watchdog.py           # inactivity + seq-gap tests
        test_risk.py               # pre-submit risk tests
      integration/
        test_polymarket_sensor.py  # PM sensor adapter checks
        test_kalshi_sensor.py      # Kalshi reciprocal-book checks
        test_polymarket_actuator.py # PM SELL conversion and LIVE verification
        test_kalshi_actuator.py    # Kalshi side/action and idempotency checks
        test_paper_actuator.py     # paper mode checks

adapter_contracts:
  - adapter: sensor.adapters.polymarket.PolymarketSensorAdapter
    input_shape: ['Gamma Market fields', 'CLOB OrderBookSummary fields']
    output_shape: ['MarketSignal with market_id=condition_id, token_id=YES clobTokenIds[0], yes_price, resolves_at, orderbook, fetched_at, external_signal']
    transforms: ['json.loads clobTokenIds/outcomePrices', 'normalize price strings to float', 'keep explicit bids+asks', 'carry negRisk/orderPriceMinTickSize/orderMinSize/acceptingOrders/enableOrderBook into external_signal', '120s websocket inactivity watchdog with REST fallback']
    dropped_fields: ['featured/new/groupItem*/uma*/resolvedBy/marketMakerAddress/createdAt/updatedAt/events']
    why: ['controller sees one normalized signal object while adapter absorbs PM quirks']
  - adapter: sensor.adapters.kalshi.KalshiSensorAdapter
    input_shape: ['GET /markets/{ticker}', 'GET /markets/{ticker}/orderbook']
    output_shape: ['MarketSignal with market_id=ticker, token_id=null, yes_price, volume_24h, resolves_at, reconstructed orderbook, fetched_at, external_signal']
    transforms: ['parse _dollars to float', 'parse *_fp with Decimal', 'reconstruct YES asks as 1.00 - NO bid', 'resubscribe on sid/seq gap', 'never read removed legacy yes_bid/yes_ask/no_bid/no_ask fields']
    dropped_fields: ['removed legacy price/liquidity fields']
    why: ['same MarketSignal shape as PM without leaking reciprocal-book details']
  - adapter: actuator.adapters.polymarket.PolymarketActuatorAdapter
    input_shape: ['TradeDecision', 'Polymarket VenueCredentials subset']
    output_shape: ['LimitOrderArgs or MarketOrderArgs', 'OrderState', 'FillRecord']
    transforms: ['startup auth: L1 private_key -> L2 api creds -> optional proxy funder', 'store decision_id -> order_id local idempotency map', 'BUY size is USDC; SELL shares = size / price', 'map order_type/time_in_force to LimitOrderArgs/MarketOrderArgs + FOK/FAK/GTC/GTD', 'post -> verify LIVE within 5s else UNMATCHED', 'redact auth fields in all exceptions']
    dropped_fields: ['none from TradeDecision']
    why: ['rest of system never sees EIP-712, proxy-wallet, or SELL asymmetry details']
  - adapter: actuator.adapters.kalshi.KalshiActuatorAdapter
    input_shape: ['TradeDecision', 'Kalshi VenueCredentials subset']
    output_shape: ['POST /portfolio/orders request', 'OrderState', 'FillRecord']
    transforms: ['map internal BUY/SELL back to exact side+action', 'count_fp = Decimal(size) / Decimal(price)', 'copy decision_id to client_order_id', 'format yes_price_dollars to 4dp', 'collapse time_in_force into expiration_ts behavior', 'poll GET /portfolio/fills and map fill_id/trade_id/market_ticker/yes_price_dollars/no_price_dollars/yes_price_fixed/no_price_fixed/is_taker/fee_cost/created_time/count_fp/ts/subaccount_number into FillRecord', 'for fills, keep Decimal(count_fp) and Decimal(side-relevant *_dollars price) in Decimal form until notional multiplication completes', 'join status from order poll by order_id before emitting FillRecord']
    dropped_fields: ['token_id']
    why: ['controller never handles side/action confusion, count_fp precision, or Kalshi fill reconciliation joins']
  - adapter: actuator.adapters.paper.PaperActuator
    input_shape: ['TradeDecision', 'MarketSignal', 'Portfolio']
    output_shape: ['synthetic OrderState', 'synthetic FillRecord']
    transforms: ['reuse slippage and sizing rules', 'simulate fills from orderbook levels', 'never touch live credentials']
    dropped_fields: ['all venue credentials']
    why: ['required safe test mode; avoids live-only anti-pattern']

architecture_decisions:
  - {id: AD-01, choice: 'single-process asyncio event loop', rejected: 'microservices / Actor MessageBus', rationale: 'source-04 says coroutines are enough and Actor/MessageBus is overkill here'}
  - {id: AD-02, choice: 'stdlib-only core + thin adapters', rejected: 'SDK calls or business logic in core/controller', rationale: 'source-04: core has zero third-party imports; adapters only call SDKs and translate'}
  - {id: AD-03, choice: 'normalize price to float 0.0-1.0 and size to internal USDC-equivalent', rejected: 'leaking cents strings, count_fp, or PM share-vs-USDC asymmetry upward', rationale: 'source-02 and source-04 require cross-venue normalization; source-03 #294 proves raw PM size is unsafe'}
  - {id: AD-04, choice: 'strict enums for status, side, signature type, anomaly flags', rejected: 'free-form strings', rationale: 'source-04 recommends enum state machines; source-01 gives authoritative PM enums'}
  - {id: AD-05, choice: 'IForecaster abstraction for rules/statistical/LLM models', rejected: 'one hard-wired forecasting method', rationale: 'topic requires interchangeable forecasters and source-04 shows pipeline composition'}
  - {id: AD-06, choice: 'async evaluation with spool', rejected: 'blocking scoring on execution path', rationale: 'source-04 says evaluator writes should not block the main loop'}
  - {id: AD-07, choice: 'typed config and startup health checks', rejected: 'hard-coded strategy params or implicit auth assumptions', rationale: 'source-04 supplies config pattern; source-03 documents auth/min-size failure modes'}

anti_patterns:
  - {id: AP-01, title: 'Auth Header Logging', issue: '#327', what_goes_wrong: 'SDK errors can dump live auth headers into logs', prevention: 'wrap SDK calls and redact secrets before logging'}
  - {id: AP-02, title: 'Trusting Success Without Verification', issue: '#258', what_goes_wrong: 'post_order success can still yield no LIVE order and no on-chain effect', prevention: 'submit -> verify -> poll; require LIVE within 5s or mark UNMATCHED'}
  - {id: AP-03, title: 'Reconnect Without Inactivity Watchdog', issue: '#292', what_goes_wrong: 'socket stays open but sends zero data for hours', prevention: '120s inactivity watchdog plus REST fallback'}
  - {id: AP-04, title: 'Ignoring SELL Size Asymmetry', issue: '#294', what_goes_wrong: 'treating BUY and SELL size the same breaks PM signatures and amounts', prevention: 'store internal size as notional and convert SELL to shares before request build'}
  - {id: AP-05, title: 'Kalshi Side/Action Confusion', issue: 'K1', what_goes_wrong: 'economically similar yes/no trades get sent with wrong side/action pair', prevention: 'normalize to internal BUY/SELL and remap only in the adapter'}
  - {id: AP-06, title: 'Floating count_fp Too Early', issue: 'K4', what_goes_wrong: 'Parsing Kalshi count_fp or the side-relevant *_dollars price as float before multiplying introduces rounding drift that compounds across fills and breaks reconciliation against order_id, fill_id, fees, and position totals', prevention: 'parse count_fp, fee_cost, and yes_price_dollars/no_price_dollars as Decimal; keep them in Decimal through the multiplication and reconciliation path; convert to float only at the final presentation boundary if required'}
