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

-- BEGIN MIDDLE RING

CREATE TABLE IF NOT EXISTS factors (
    factor_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    input_schema_hash TEXT NOT NULL,
    default_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_type TEXT NOT NULL CHECK (output_type IN ('scalar', 'probability')),
    direction TEXT NOT NULL CHECK (direction IN ('long', 'short', 'neutral')),
    owner TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS factor_values (
    id BIGSERIAL PRIMARY KEY,
    factor_id TEXT NOT NULL REFERENCES factors(factor_id) ON DELETE CASCADE,
    param TEXT NOT NULL DEFAULT '',
    market_id TEXT NOT NULL REFERENCES markets(condition_id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_factor_values_factor_param_market_ts
    ON factor_values(factor_id, param, market_id, ts DESC);

-- END MIDDLE RING

-- strategies: inner-ring identity table (Invariants 3, 8)

CREATE TABLE IF NOT EXISTS strategies (
    strategy_id TEXT PRIMARY KEY,
    active_version_id TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- strategy_versions: immutable hash-keyed version rows (Invariant 3)

CREATE TABLE IF NOT EXISTS strategy_versions (
    strategy_version_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    config_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (strategy_id, strategy_version_id)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'strategies_active_version_id_fkey'
    ) THEN
        ALTER TABLE strategies
            ADD CONSTRAINT strategies_active_version_id_fkey
            FOREIGN KEY (active_version_id)
            REFERENCES strategy_versions(strategy_version_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

-- strategy_factors: link table (empty in S2; populated by S3). Columns declared so S3 inserts do not require a schema change (Invariants 2, 4, 8)
-- Invariant 4: raw factor rows are stored in factor_values (S3); composite factors belong in StrategyConfig.factor_composition, not as first-class rows here.
CREATE TABLE IF NOT EXISTS strategy_factors (
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
    factor_id TEXT NOT NULL,
    param JSONB NOT NULL DEFAULT '{}'::jsonb,
    weight DOUBLE PRECISION NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('long', 'short')),
    PRIMARY KEY (strategy_id, strategy_version_id, factor_id),
    FOREIGN KEY (strategy_id, strategy_version_id)
        REFERENCES strategy_versions(strategy_id, strategy_version_id)
        ON DELETE CASCADE
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'strategy_factors_factor_id_fkey'
    ) THEN
        ALTER TABLE strategy_factors
            ADD CONSTRAINT strategy_factors_factor_id_fkey
            FOREIGN KEY (factor_id)
            REFERENCES factors(factor_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

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

-- default strategy seed (Invariant 3 NULLABLE→seed pattern).
-- load-bearing legacy bootstrap row: changing `default-v1` requires a coordinated
-- migration of every existing inner-ring product row tagged by the pre-S5 runtime.
INSERT INTO strategies (strategy_id, active_version_id)
    VALUES ('default', 'default-v1')
    ON CONFLICT (strategy_id) DO NOTHING;

INSERT INTO strategy_versions (
    strategy_version_id,
    strategy_id,
    config_json
) VALUES (
    'default-v1',
    'default',
    '{"config":{"factor_composition":[["factor-a",0.6],["factor-b",0.4]],"metadata":[["owner","system"],["tier","default"]],"strategy_id":"default"},"eval_spec":{"metrics":["brier","pnl","fill_rate"]},"forecaster":{"forecasters":[["rules",[["threshold","0.55"]]],["stats",[["window","15m"]]]]},"market_selection":{"resolution_time_max_horizon_days":7,"venue":"polymarket","volume_min_usdc":500.0},"risk":{"max_daily_drawdown_pct":2.5,"max_position_notional_usdc":100.0,"min_order_size_usdc":1.0}}'::jsonb
)
ON CONFLICT (strategy_version_id) DO NOTHING;

COMMIT;
