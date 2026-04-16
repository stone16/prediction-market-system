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

COMMIT;
