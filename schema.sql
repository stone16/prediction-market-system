BEGIN;

-- BEGIN OUTER RING

CREATE TABLE IF NOT EXISTS markets (
    condition_id TEXT PRIMARY KEY,
    slug TEXT NOT NULL,
    question TEXT NOT NULL,
    venue TEXT NOT NULL CHECK (venue IN ('polymarket', 'kalshi')),
    resolves_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    volume_24h DOUBLE PRECISION
);

ALTER TABLE markets
    ADD COLUMN IF NOT EXISTS volume_24h DOUBLE PRECISION;

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

CREATE UNIQUE INDEX IF NOT EXISTS idx_factor_values_factor_param_market_ts
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
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL
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
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS opportunities (
    opportunity_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('yes', 'no')),
    selected_factor_values JSONB NOT NULL,
    expected_edge DOUBLE PRECISION NOT NULL,
    rationale TEXT NOT NULL,
    target_size_usdc DOUBLE PRECISION NOT NULL,
    expiry TIMESTAMPTZ,
    staleness_policy TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- research backtest inner-ring tables (Invariants 3, 8)

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id UUID PRIMARY KEY,
    spec_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    strategy_ids TEXT[] NOT NULL,
    date_range_start TIMESTAMPTZ NOT NULL,
    date_range_end TIMESTAMPTZ NOT NULL,
    exec_config_json JSONB NOT NULL,
    spec_json JSONB NOT NULL,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    failure_reason TEXT,
    worker_pid INTEGER,
    worker_host TEXT,
    CONSTRAINT backtest_runs_status_check
        CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled'))
);

CREATE TABLE IF NOT EXISTS strategy_runs (
    strategy_run_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
    brier DOUBLE PRECISION,
    pnl_cum DOUBLE PRECISION,
    drawdown_max DOUBLE PRECISION,
    fill_rate DOUBLE PRECISION,
    slippage_bps DOUBLE PRECISION,
    opportunity_count INTEGER,
    decision_count INTEGER,
    fill_count INTEGER,
    portfolio_target_json JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT strategy_runs_strategy_identity_check
        CHECK (strategy_id != '' AND strategy_version_id != '')
);

CREATE TABLE IF NOT EXISTS evaluation_reports (
    report_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    ranking_metric TEXT NOT NULL,
    ranked_strategies JSONB NOT NULL,
    benchmark_rows JSONB NOT NULL DEFAULT '[]'::jsonb,
    attribution_commentary TEXT,
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    next_action TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT evaluation_reports_ranking_metric_check
        CHECK (ranking_metric IN ('brier', 'sharpe', 'pnl_cum')),
    CONSTRAINT evaluation_reports_run_id_ranking_metric_key
        UNIQUE (run_id, ranking_metric)
);

CREATE TABLE IF NOT EXISTS backtest_live_comparisons (
    comparison_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
    live_window_start TIMESTAMPTZ NOT NULL,
    live_window_end TIMESTAMPTZ NOT NULL,
    denominator TEXT NOT NULL,
    equity_delta_json JSONB NOT NULL,
    overlap_ratio DOUBLE PRECISION NOT NULL,
    backtest_only_symbols TEXT[] NOT NULL,
    live_only_symbols TEXT[] NOT NULL,
    time_alignment_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    symbol_normalization_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT backtest_live_comparisons_denominator_check
        CHECK (denominator IN ('backtest_set', 'live_set', 'union')),
    CONSTRAINT backtest_live_comparisons_strategy_identity_check
        CHECK (strategy_id != '' AND strategy_version_id != '')
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
    '{"config":{"factor_composition":[["factor-a",0.6],["factor-b",0.4]],"metadata":[["owner","system"],["tier","default"]],"strategy_id":"default"},"eval_spec":{"max_brier_score":0.3,"metrics":["brier","pnl","fill_rate"],"min_win_rate":0.5,"slippage_threshold_bps":50.0},"forecaster":{"forecasters":[["rules",[["threshold","0.55"]]],["stats",[["window","15m"]]],["llm",[]]]},"market_selection":{"resolution_time_max_horizon_days":7,"venue":"polymarket","volume_min_usdc":500.0},"risk":{"max_daily_drawdown_pct":2.5,"max_position_notional_usdc":100.0,"min_order_size_usdc":1.0}}'::jsonb
)
ON CONFLICT (strategy_version_id) DO NOTHING;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM feedback
        WHERE strategy_id IS NULL OR strategy_version_id IS NULL
    ) OR EXISTS (
        SELECT 1
        FROM eval_records
        WHERE strategy_id IS NULL OR strategy_version_id IS NULL
    ) OR EXISTS (
        SELECT 1
        FROM orders
        WHERE strategy_id IS NULL OR strategy_version_id IS NULL
    ) OR EXISTS (
        SELECT 1
        FROM fills
        WHERE strategy_id IS NULL OR strategy_version_id IS NULL
    ) THEN
        RAISE EXCEPTION $message$
CP04 remediation required before enforcing strategy identity columns.
Run:
UPDATE feedback SET strategy_id = 'default', strategy_version_id = 'default-v1' WHERE strategy_id IS NULL OR strategy_version_id IS NULL;
UPDATE eval_records SET strategy_id = 'default', strategy_version_id = 'default-v1' WHERE strategy_id IS NULL OR strategy_version_id IS NULL;
UPDATE orders SET strategy_id = 'default', strategy_version_id = 'default-v1' WHERE strategy_id IS NULL OR strategy_version_id IS NULL;
UPDATE fills SET strategy_id = 'default', strategy_version_id = 'default-v1' WHERE strategy_id IS NULL OR strategy_version_id IS NULL;
Then re-run schema.sql.
$message$;
    END IF;
END
$$;

ALTER TABLE feedback
    ALTER COLUMN strategy_id SET NOT NULL,
    ALTER COLUMN strategy_version_id SET NOT NULL;

ALTER TABLE eval_records
    ALTER COLUMN strategy_id SET NOT NULL,
    ALTER COLUMN strategy_version_id SET NOT NULL;

ALTER TABLE orders
    ALTER COLUMN strategy_id SET NOT NULL,
    ALTER COLUMN strategy_version_id SET NOT NULL;

ALTER TABLE fills
    ALTER COLUMN strategy_id SET NOT NULL,
    ALTER COLUMN strategy_version_id SET NOT NULL;

ALTER TABLE opportunities
    ALTER COLUMN strategy_id SET NOT NULL,
    ALTER COLUMN strategy_version_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'feedback_strategy_identity_check'
    ) THEN
        ALTER TABLE feedback
            ADD CONSTRAINT feedback_strategy_identity_check
            CHECK (strategy_id != '' AND strategy_version_id != '');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'eval_records_strategy_identity_check'
    ) THEN
        ALTER TABLE eval_records
            ADD CONSTRAINT eval_records_strategy_identity_check
            CHECK (strategy_id != '' AND strategy_version_id != '');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'orders_strategy_identity_check'
    ) THEN
        ALTER TABLE orders
            ADD CONSTRAINT orders_strategy_identity_check
            CHECK (strategy_id != '' AND strategy_version_id != '');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fills_strategy_identity_check'
    ) THEN
        ALTER TABLE fills
            ADD CONSTRAINT fills_strategy_identity_check
            CHECK (strategy_id != '' AND strategy_version_id != '');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'opportunities_strategy_identity_check'
    ) THEN
        ALTER TABLE opportunities
            ADD CONSTRAINT opportunities_strategy_identity_check
            CHECK (strategy_id != '' AND strategy_version_id != '');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'backtest_runs_status_check'
    ) THEN
        ALTER TABLE backtest_runs
            ADD CONSTRAINT backtest_runs_status_check
            CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled'));
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'strategy_runs_strategy_identity_check'
    ) THEN
        ALTER TABLE strategy_runs
            ADD CONSTRAINT strategy_runs_strategy_identity_check
            CHECK (strategy_id != '' AND strategy_version_id != '');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'evaluation_reports_ranking_metric_check'
    ) THEN
        ALTER TABLE evaluation_reports
            ADD CONSTRAINT evaluation_reports_ranking_metric_check
            CHECK (ranking_metric IN ('brier', 'sharpe', 'pnl_cum'));
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'evaluation_reports_run_id_ranking_metric_key'
    ) THEN
        ALTER TABLE evaluation_reports
            ADD CONSTRAINT evaluation_reports_run_id_ranking_metric_key
            UNIQUE (run_id, ranking_metric);
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'backtest_live_comparisons_denominator_check'
    ) THEN
        ALTER TABLE backtest_live_comparisons
            ADD CONSTRAINT backtest_live_comparisons_denominator_check
            CHECK (denominator IN ('backtest_set', 'live_set', 'union'));
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'backtest_live_comparisons_strategy_identity_check'
    ) THEN
        ALTER TABLE backtest_live_comparisons
            ADD CONSTRAINT backtest_live_comparisons_strategy_identity_check
            CHECK (strategy_id != '' AND strategy_version_id != '');
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_feedback_strategy_identity
    ON feedback(strategy_id, strategy_version_id);

CREATE INDEX IF NOT EXISTS idx_eval_records_strategy_identity
    ON eval_records(strategy_id, strategy_version_id);

CREATE INDEX IF NOT EXISTS idx_orders_strategy_identity
    ON orders(strategy_id, strategy_version_id);

CREATE INDEX IF NOT EXISTS idx_fills_strategy_identity
    ON fills(strategy_id, strategy_version_id);

CREATE INDEX IF NOT EXISTS idx_opportunities_strategy_identity
    ON opportunities(strategy_id, strategy_version_id);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_status
    ON backtest_runs(status);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_queued_at_desc
    ON backtest_runs(queued_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_runs_run_id
    ON strategy_runs(run_id);

CREATE INDEX IF NOT EXISTS idx_backtest_live_comparisons_run_strategy_identity
    ON backtest_live_comparisons(run_id, strategy_id, strategy_version_id);

INSERT INTO factors (
    factor_id,
    name,
    description,
    input_schema_hash,
    output_type,
    direction,
    owner
) VALUES (
    'orderbook_imbalance',
    'Orderbook Imbalance',
    'Normalized bid-versus-ask depth imbalance from the current orderbook signal.',
    '97e885bf8b2edd8ce9fff149334dbe1706358eb4fb8b8c51a4b42561878c5963',
    'scalar',
    'neutral',
    'system'
)
ON CONFLICT (factor_id) DO NOTHING;

INSERT INTO factors (
    factor_id,
    name,
    description,
    input_schema_hash,
    output_type,
    direction,
    owner
) VALUES (
    'fair_value_spread',
    'Fair Value Spread',
    'Signed difference between external fair value and the current YES price.',
    'adb923abb80bbd30efa4db61ba846660317f138ef12c3ae521891df2831d64f9',
    'scalar',
    'neutral',
    'system'
)
ON CONFLICT (factor_id) DO NOTHING;

INSERT INTO factors (
    factor_id,
    name,
    description,
    input_schema_hash,
    output_type,
    direction,
    owner
) VALUES (
    'subset_pricing_violation',
    'Subset Pricing Violation',
    'Signed difference between subset and superset prices from external signals.',
    'c9e66b836e6fe6a9981ee6419aa38acb39de607e84fb1ff643b46bb9ac446891',
    'scalar',
    'neutral',
    'system'
)
ON CONFLICT (factor_id) DO NOTHING;

INSERT INTO factors (
    factor_id,
    name,
    description,
    input_schema_hash,
    output_type,
    direction,
    owner
) VALUES (
    'metaculus_prior',
    'Metaculus Prior',
    'Raw Metaculus probability from the external signal payload.',
    '4f62fec15fd5abaf2ff76810596268d1e14b46d346ff6e9f38b259c370a3ed71',
    'probability',
    'neutral',
    'system'
)
ON CONFLICT (factor_id) DO NOTHING;

INSERT INTO factors (
    factor_id,
    name,
    description,
    input_schema_hash,
    output_type,
    direction,
    owner
) VALUES (
    'yes_count',
    'Yes Count',
    'Raw external yes_count observation count from the signal payload.',
    'afbc921285acc81f1289beca8dd64114c18f49068a8904c651a887c5ba8c178f',
    'scalar',
    'neutral',
    'system'
)
ON CONFLICT (factor_id) DO NOTHING;

INSERT INTO factors (
    factor_id,
    name,
    description,
    input_schema_hash,
    output_type,
    direction,
    owner
) VALUES (
    'no_count',
    'No Count',
    'Raw external no_count observation count from the signal payload.',
    '2871d6bf945e3ed4407b8b1f1beeb484cd8bd455a156e939094fbcc6a455c317',
    'scalar',
    'neutral',
    'system'
)
ON CONFLICT (factor_id) DO NOTHING;

COMMIT;
