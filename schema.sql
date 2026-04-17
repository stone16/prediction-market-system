BEGIN;

-- BEGIN OUTER RING

CREATE TABLE IF NOT EXISTS markets (
    condition_id TEXT PRIMARY KEY,
    slug TEXT NOT NULL,
    question TEXT NOT NULL,
    venue TEXT NOT NULL CHECK (venue IN ('polymarket', 'kalshi')),
    resolves_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS tokens (
    token_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL REFERENCES markets(condition_id) ON DELETE CASCADE,
    outcome TEXT NOT NULL CHECK (outcome IN ('YES', 'NO'))
);

CREATE INDEX IF NOT EXISTS idx_tokens_condition_id
    ON tokens(condition_id);

CREATE TABLE IF NOT EXISTS book_snapshots (
    id BIGSERIAL PRIMARY KEY,
    market_id TEXT NOT NULL REFERENCES markets(condition_id) ON DELETE CASCADE,
    token_id TEXT NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    hash TEXT,
    source TEXT NOT NULL CHECK (source IN ('subscribe', 'reconnect', 'checkpoint'))
);

CREATE INDEX IF NOT EXISTS idx_book_snapshots_market_token_ts
    ON book_snapshots(market_id, token_id, ts DESC);

CREATE TABLE IF NOT EXISTS book_levels (
    snapshot_id BIGINT NOT NULL REFERENCES book_snapshots(id) ON DELETE CASCADE,
    market_id TEXT NOT NULL REFERENCES markets(condition_id) ON DELETE CASCADE,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_book_levels_snapshot_id
    ON book_levels(snapshot_id);

CREATE INDEX IF NOT EXISTS idx_book_levels_market_side_price
    ON book_levels(market_id, side, price);

CREATE TABLE IF NOT EXISTS price_changes (
    id BIGSERIAL PRIMARY KEY,
    market_id TEXT NOT NULL REFERENCES markets(condition_id) ON DELETE CASCADE,
    token_id TEXT NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    best_bid DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    hash TEXT
);

-- CP00 resolved Q2a to allow duplicates; replay order is (ts, id).
CREATE INDEX IF NOT EXISTS idx_price_changes_market_token_ts_id
    ON price_changes(market_id, token_id, ts ASC, id ASC);

CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL PRIMARY KEY,
    market_id TEXT NOT NULL REFERENCES markets(condition_id) ON DELETE CASCADE,
    token_id TEXT NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_market_token_ts
    ON trades(market_id, token_id, ts DESC);

-- END OUTER RING

-- BEGIN INNER-RING PRODUCT SHELLS

CREATE TABLE IF NOT EXISTS feedback (
    feedback_id TEXT PRIMARY KEY,
    target TEXT NOT NULL,
    source TEXT NOT NULL,
    message TEXT NOT NULL,
    severity TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMPTZ,
    category TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    strategy_id TEXT NULL,
    strategy_version_id TEXT NULL
);

CREATE TABLE IF NOT EXISTS eval_records (
    decision_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    prob_estimate DOUBLE PRECISION NOT NULL,
    resolved_outcome DOUBLE PRECISION NOT NULL,
    brier_score DOUBLE PRECISION NOT NULL,
    fill_status TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    citations JSONB NOT NULL DEFAULT '[]'::jsonb,
    category TEXT,
    model_id TEXT,
    pnl DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    slippage_bps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    filled BOOLEAN NOT NULL DEFAULT TRUE,
    strategy_id TEXT NULL,
    strategy_version_id TEXT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    strategy_id TEXT NULL,
    strategy_version_id TEXT NULL
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    strategy_id TEXT NULL,
    strategy_version_id TEXT NULL
);

-- END INNER-RING PRODUCT SHELLS

COMMIT;
