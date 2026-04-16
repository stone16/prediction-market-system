# 2026-04-16 Repo Issues: Controller and Evaluator First

Status: discovery note, not an implementation spec.

This records the issues we jointly identified on April 16 while validating the
current `prediction-market-system` repo after PR #5. The next step is to turn
this into an evaluation tech spec, then a Harness spec, before implementation.

Companion note: `docs/notes/2026-04-16-evaluator-entity-abstraction.md`
captures the Evaluator-specific gaps, entity abstraction direction, and proposed
first-class citizens such as `Factor`, `Strategy`, `Opportunity`,
`BacktestRun`, and `StrategyRun`.

## Target Operating Model

The product flow should be:

1. **Observe trading data at scale.** The system should ingest enough market,
   orderbook, price, liquidity, external-signal, and historical outcome data to
   identify where prediction markets appear systematically overvalued or
   undervalued.
2. **Find and validate strategies.** Controller strategies should be first-class
   objects that can be configured, replayed, compared, and understood before any
   live execution path is considered.
3. **Apply strategies through the actuator.** The Actuator should execute only
   after a Controller strategy has produced an executable, risk-checked decision.
4. **Evaluate strategy quality.** The Evaluator should close the loop with
   strategy-level calibration, P&L, drawdown, fill-rate, slippage, and opportunity
   diagnostics.

The repo's layer boundaries are directionally right:

- Sensor normalizes incoming venue data into `MarketSignal`.
- Controller is the core layer users should understand and configure.
- Actuator consumes Controller output and handles execution.
- Evaluator scores decisions and feeds results back to Controller.

The main gap is not the basic data flow. The main gap is that Controller and
Evaluator are still too thin for strategy discovery and validation.

## Architecture: How The Stages Are Stitched Together

Understanding the integration mechanisms is prerequisite to changing any layer.
The repo uses three orthogonal stitching primitives:

### Protocol — compile-time shape contracts

`src/pms/core/interfaces.py` defines 7 `Protocol` types (`ISensor`,
`IController`, `IActuator`, `IEvaluator`, `IForecaster`, `ICalibrator`,
`ISizer`). These are structural types — no base class inheritance required.
Any object with the right method signature satisfies the protocol. This is
what allows swapping venue adapters without touching the orchestrator.

### Queue — runtime decoupling via asyncio.Queue

`runner.py` connects stages through two queues:

- `SensorStream.queue` → consumed by `_controller_loop()`
- `_decision_queue` → consumed by `_actuator_loop()`

Each stage is an independent `asyncio.Task`. Queues provide backpressure
(fast sensor won't overwhelm slow controller) and lifecycle independence
(sensor can finish before controller drains its queue).

### Store — cross-restart decoupling via durable storage

`FeedbackStore` and `EvalStore` persist to disk (currently JSONL under
`.data/`; per the persistence decisions recorded below, both will migrate
to PostgreSQL tables). `FeedbackStore.reload()` loads history on startup.
This is how evaluator output survives restarts and can influence future
controller decisions.

### Exception: scatter-gather inside Controller

`ControllerPipeline.on_signal()` does NOT use queues between forecasters.
It uses `asyncio.gather()` — a synchronous barrier that waits for all
forecasters before averaging probabilities. This is because the controller
must aggregate all forecasts before producing a single `TradeDecision`.
Unlike the runner's queue-based CSP pattern, this is a scatter-gather
(fan-out / fan-in) pattern with a mandatory synchronization point.

## Current Findings

### 1. Sensor: Normalized Flow Exists, Observation Depth Is Thin

Current files:

- `src/pms/sensor/adapters/polymarket_rest.py`
- `src/pms/sensor/adapters/historical.py`
- `src/pms/runner.py`
- `src/pms/core/models.py`

What works:

- Sensor adapters emit standardized `MarketSignal` objects.
- Paper mode can ingest live Polymarket Gamma market data.
- Historical fixtures can replay a deterministic backtest path.

Gaps:

- There is no durable market-universe observation store. Runtime state keeps only
  bounded in-memory lists under `RunnerState`.
- `MarketSignal.external_signal` is an untyped bag. It can carry `fair_value`,
  `metaculus_prob`, or `resolved_outcome`, but there is no first-class schema for
  opportunity features.
- The dashboard exposes recent signals, but not market-universe analytics such as
  repeated mispricing, liquidity-adjusted opportunity, or venue/category cohorts.
- Historical replay is fixture-oriented, not a general backtest data source that
  can be queried by strategy.

Implication:

The current Sensor layer can feed the loop, but it does not yet support the
"大量数据观测 -> 系统性高估/低估发现" product goal.

### 1a. Orderbook Data: Simulated, Not Real

The most immediate Sensor gap is orderbook depth. As of 2026-04-16, the code
path is:

1. `PolymarketRestSensor.poll_once()` calls Gamma API `GET /markets` — returns
   market metadata including `liquidity` (scalar) and `outcomePrices`, but **no
   orderbook depth**.
2. `_simulated_paper_orderbook()` in `polymarket_rest.py:131-146` fabricates a
   single-level bid/ask from the scalar `liquidity` value: `bid = price - 0.01`,
   `ask = price`, both with `size = liquidity`. This assumes all liquidity
   sits at best bid/ask — unrealistic for any market with real depth.
3. `PolymarketStreamSensor` connects to
   `wss://ws-subscriptions-clob.polymarket.com/ws/` but only parses
   `price`-type messages. It ignores `book` (full snapshot) and
   `price_change` (delta) event types that carry real orderbook data.
4. `runner.py:177-184` (`_build_sensors()`) never instantiates
   `PolymarketStreamSensor` — only `PolymarketRestSensor` is wired for
   non-backtest modes.

Consequence: the dashboard, the actuator's paper-mode fill simulation, and any
risk/sizing logic that reads `signal.orderbook` are all operating on fabricated
depth.

#### Available Polymarket APIs for real orderbook data

**REST — CLOB API `GET /book`** (no auth required):

```
GET https://clob.polymarket.com/book?token_id={token_id}

Response:
{
  "market": "0x...",         // condition ID
  "asset_id": "0x...",      // token ID
  "timestamp": "1234567890",
  "hash": "0x...",
  "bids": [{"price": "0.52", "size": "100.0"}, ...],
  "asks": [{"price": "0.53", "size": "80.0"}, ...],
  "min_order_size": "5",
  "tick_size": "0.01",
  "neg_risk": false,
  "last_trade_price": "0.525"
}
```

Known issue: GitHub Polymarket/py-clob-client#180 reports that `/book` can
return stale snapshots while `/price` stays live. Mitigation: cross-check
`last_trade_price` from `/book` against the Gamma `outcomePrices` to detect
staleness.

**WebSocket — Market Channel** (no auth required):

```
Connect: wss://ws-subscriptions-clob.polymarket.com/ws/market

Subscribe:
{
  "assets_ids": ["token_id_1", "token_id_2"],
  "type": "market",
  "initial_dump": true,
  "level": 2
}

Events received:
- "book"         — full orderbook snapshot (bids + asks arrays)
- "price_change" — delta: asset_id, price, size, side, best_bid, best_ask
- "last_trade_price" — trade execution notification
- "tick_size_change" — market tick adjustment

Heartbeat: send "PING" every 10s, expect "PONG".
```

#### Target: Route B — upgrade PolymarketStreamSensor

Decision (2026-04-16): the target is Route B (WebSocket). Rationale:

1. The WebSocket connection already exists in `PolymarketStreamSensor`.
2. Route B provides near-realtime orderbook updates without extra HTTP round
   trips per market.
3. The dashboard benefits most from streaming depth — poll-based REST at 5s
   intervals is too coarse for orderbook visualization.
4. Route A (REST `GET /book`) remains useful as a fallback or initial-snapshot
   source.

Implementation scope (not a spec — requires Harness spec before execution):

- Parse `book` events in `PolymarketStreamSensor._message_to_signal()` to
  populate `MarketSignal.orderbook` with real bids/asks.
- Parse `price_change` events to maintain a local orderbook mirror per
  `asset_id`, applying deltas to the last known snapshot.
- Wire `PolymarketStreamSensor` into `runner.py:_build_sensors()` for paper
  and live modes (alongside or replacing `PolymarketRestSensor`).
- Add heartbeat (10s PING) to `PolymarketStreamSensor._iterate()`.
- Add `SensorWatchdog` integration: the watchdog already exists in
  `sensor/watchdog.py` but is not wired to the stream sensor.
- Expose real orderbook depth on the dashboard `/signals` page.

Open questions for the spec:

1. Should the stream sensor maintain a full local orderbook mirror (snapshot +
   deltas), or treat each `price_change` as an independent signal?
2. How many `assets_ids` can a single WebSocket subscription handle before
   needing connection fan-out?
3. Should `PolymarketRestSensor` remain as a fallback for initial market
   discovery (it provides `title`, `volume_24h`, `resolves_at` that the
   WebSocket does not), with `PolymarketStreamSensor` layered on top for
   real-time depth?
4. What is the reconnection strategy when the WebSocket drops? Current code
   has exponential backoff but no snapshot re-request on reconnect.

#### Persistence Decision: PostgreSQL, All Environments

Decision (2026-04-16, revised): **PostgreSQL from the start.** No SQLite
detour, no dual-dialect schema, no migration seam. All environments
(local dev, CI, production) run the same PostgreSQL version.

Context: an earlier version of this note proposed "SQLite now, PostgreSQL
later with an `IMarketDataStore` Protocol as migration seam." That proposal
was rejected in conversation because:

1. The end-state is already known to be PostgreSQL. "Start with SQLite"
   only delays the PG-specific learning and introduces real abstraction
   cost today (Protocol, dual-dialect SQL, eventual data migration).
2. The PM data model needs capabilities that are PG strengths and SQLite
   weaknesses: window functions for time-series analysis (`LAG`, `LEAD`,
   `OVER PARTITION BY`), concurrent writes (multiple sensors or backfill
   jobs), and native `TIMESTAMPTZ` + `INTERVAL` arithmetic.
3. Managed PG free tiers (Supabase 500MB, Neon 3GB, Fly Postgres 256MB)
   are sufficient for dev and early production.
4. Docker Compose or `brew install postgresql@16` covers local dev with
   a one-time setup step, documented in `CLAUDE.md`.

Rationale:

1. Existing `EvalStore` and `FeedbackStore` use JSONL — fine for low-volume
   records. Orderbook `price_change` events can arrive at tens per second
   per market; JSONL at that volume is not greppable in practice and does
   not support the queries strategies will need.
2. PostgreSQL offers the query surface strategies will rely on: range
   scans on `timestamp`, partial/expression indexes, window functions, and
   JSONB where structured blobs are unavoidable.
3. Strict typing catches category errors at insert time instead of at read
   time — important when the Polymarket WebSocket payload shape evolves.

Non-goals:

- No ORM. Raw SQL via `asyncpg` or `psycopg[async]`, returning frozen
  dataclasses. Matches the frozen-dataclass convention in `core/models.py`.
- No migration framework initially. A single `schema.sql` applied on
  startup if target tables do not exist. Explicit migrations (Alembic or
  Sqitch) can come later if needed.
- No analytics queries inside the sensor write path. Writes only during
  ingestion; reads happen from evaluator, API, or offline scripts.
- No `IMarketDataStore` Protocol abstraction layer. A single
  `PostgresMarketDataStore` class with typed methods is sufficient; the
  Protocol pattern only pays off when there are multiple implementations.

Open configuration questions:

- Which async driver: `asyncpg` (fastest, non-DBAPI, custom API) or
  `psycopg 3` (DBAPI-compatible, async support)? `asyncpg` is preferred
  for raw throughput; `psycopg 3` is easier to mix with sync code paths.
- Connection pool sizing: PG connection pool in the Runner, shared across
  sensor / controller / actuator / evaluator / API tasks. Default 10
  connections is a reasonable start; the API + sensor workload is
  write-heavy.
- Local dev DB provisioning: Docker Compose (committed `compose.yml`),
  `brew services`, or a README note pointing to a free managed tier.
  Docker Compose is most reproducible across contributors.

Follow-on decisions:

- **Feedback and eval storage migration** — RESOLVED (2026-04-16): all
  persistent state moves to PostgreSQL, including `feedback.jsonl` and
  `eval_records.jsonl`. One consistent storage backend. Rationale:
  1. Evaluator queries benefit from cross-table JOINs (e.g. "decisions
     rejected by risk at timestamps when best-ask spread was > N").
     JSONL cannot JOIN against SQL-backed tables.
  2. Dashboard API can serve unified queries instead of merging two
     storage formats.
  3. Backup, retention, and observability apply uniformly.
  4. Existing `FeedbackStore.reload()` and `EvalStore.append()` call
     surfaces are small — rewriting as SQL-backed stores is bounded
     work, not a sprawling change.

  Consequence:
  - `FeedbackStore` and `EvalStore` become thin wrappers over SQL queries
    against a shared PG connection pool.
  - `.data/` directory is no longer part of the runtime contract. Dev
    state isolation moves to per-shell PG databases (e.g.
    `DATABASE_URL=postgres://localhost/pms_dev_$(whoami)`), not env-var
    directory overrides.
  - Existing `.data/feedback.jsonl` / `.data/eval_records.jsonl` in dev
    environments: not load-bearing (gitignored, dev-only). A one-off
    migration script can be written if any contributor has state worth
    preserving; otherwise drop and restart.

- **Test strategy** — RESOLVED (2026-04-16): **transaction-rollback
  fixture against a shared test DB**. Each test acquires a connection
  from the pool, opens a transaction, yields the connection, and rolls
  back on teardown. No `pytest-postgresql` dependency.

  Shape of the primary fixture:

  ```python
  @pytest_asyncio.fixture
  async def db_conn(pg_pool):
      async with pg_pool.acquire() as conn:
          tr = conn.transaction()
          await tr.start()
          try:
              yield conn
          finally:
              await tr.rollback()
  ```

  Scope and escape hatches:
  - Storage-layer unit tests (`FeedbackStore`, `EvalStore`, the new
    `PostgresMarketDataStore`): use `db_conn` fixture. Extremely fast,
    naturally isolated, safe to parallelize as long as each test sticks
    to its own connection.
  - Cross-connection integration tests (e.g. "Runner writes, API reads
    from a different connection"): the transaction-rollback pattern does
    NOT isolate because other connections cannot see uncommitted rows.
    For those, fall back to a per-test `TRUNCATE` on the affected tables
    inside an `autouse` fixture.
  - Tests that exercise code which itself calls
    `async with conn.transaction():`: the inner block becomes a
    SAVEPOINT inside the outer test transaction. That works correctly;
    the outer rollback still discards everything.

  Local + CI provisioning:
  - Local: `docker compose up -d postgres` with a committed
    `compose.yml`. Test DSN:
    `postgres://postgres:postgres@localhost:5432/pms_test`.
  - CI (GitHub Actions): `services.postgres.image: postgres:16`,
    matching the local compose image tag.
  - Schema applied per test session (not per test) via a
    `session`-scoped fixture that runs `schema.sql` against the test DB
    once, before any transaction-rollback fixture runs.

#### Schema Design: Open Questions

The PostgreSQL schema has several unresolved trade-offs. These must be
answered before any implementation starts — each choice propagates to
query patterns, storage size, and replay semantics.

**Q1: Snapshots vs. event log vs. both?** — RESOLVED (2026-04-16)

Decision: **Both**. Store each `book` event as a snapshot row and each
`price_change` event as a delta row.

Rationale:

- The system will run many strategies over time. At decision time we do not
  know what query shapes future strategies need. Storing both guarantees
  lossless replay.
- Polymarket already pushes a `book` on subscribe/reconnect, so snapshots
  arrive "for free" at the start of every WebSocket session.
- Between subscription-triggered snapshots, a periodic checkpoint (every
  N minutes or M deltas) caps the replay window when reconstructing historic
  state.

Note on delta semantics: Polymarket's `price_change.size` is the NEW total
size at that price level, not an increment. `size=0` means the level is
removed. The ingestion code must treat the field accordingly when
reconstructing state.

**Q2: How to store bids/asks arrays?** — RESOLVED (2026-04-16)

Decision: **Fully row-structured.** Snapshots are expanded into per-level
rows (no JSON blobs). Deltas stay as their natural single-row shape.

Rationale:

- Strategies need to query both the delta stream (time-series price paths)
  AND snapshot internals (depth patterns, spread distributions, best-price
  thresholds). JSON-blob snapshots cannot be indexed by price without a
  full-table scan.
- `book_levels` and `price_changes` end up structurally identical
  (`side, price, size`), making union-style analytics trivial.
- Snapshot writes become "1 metadata row + N level rows" (N ≈ 20-50), but
  snapshots are rare (subscribe / reconnect / periodic checkpoint), so the
  cost is amortized against the frequent delta writes.
- Prices stored as `DOUBLE PRECISION` (Python float on read), matching the
  repo convention of "float at entity boundary" (see CLAUDE.md). `Decimal`
  is reserved for calculation internals. Prediction market ticks are
  0.01, well within double precision; if tick ever tightens, `NUMERIC(10,4)`
  is the escape hatch.

Draft schema (PostgreSQL dialect — goes into the Harness spec for review,
not final):

```sql
CREATE TABLE markets (
  condition_id  TEXT        PRIMARY KEY,
  slug          TEXT,
  question      TEXT,
  venue         TEXT        NOT NULL CHECK (venue IN ('polymarket','kalshi')),
  resolves_at   TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tokens (
  token_id      TEXT PRIMARY KEY,
  condition_id  TEXT NOT NULL REFERENCES markets(condition_id),
  outcome       TEXT NOT NULL CHECK (outcome IN ('YES','NO'))
);

CREATE TABLE book_snapshots (
  id         BIGSERIAL   PRIMARY KEY,
  market_id  TEXT        NOT NULL,
  token_id   TEXT        NOT NULL,
  ts         TIMESTAMPTZ NOT NULL,
  hash       TEXT,
  source     TEXT        NOT NULL
                          CHECK (source IN ('subscribe','reconnect','checkpoint'))
);
CREATE INDEX idx_snap_market_ts ON book_snapshots(market_id, ts DESC);

CREATE TABLE book_levels (
  snapshot_id BIGINT           NOT NULL REFERENCES book_snapshots(id)
                                        ON DELETE CASCADE,
  market_id   TEXT             NOT NULL,      -- denormalized for index-only scans
  side        TEXT             NOT NULL CHECK (side IN ('BUY','SELL')),
  price       DOUBLE PRECISION NOT NULL,
  size        DOUBLE PRECISION NOT NULL
);
CREATE INDEX idx_levels_snap       ON book_levels(snapshot_id);
CREATE INDEX idx_levels_price_side ON book_levels(market_id, side, price);

CREATE TABLE price_changes (
  id          BIGSERIAL        PRIMARY KEY,
  market_id   TEXT             NOT NULL,
  token_id    TEXT             NOT NULL,
  ts          TIMESTAMPTZ      NOT NULL,
  side        TEXT             NOT NULL CHECK (side IN ('BUY','SELL')),
  price       DOUBLE PRECISION NOT NULL,
  size        DOUBLE PRECISION NOT NULL,   -- new total at level (0 = removed)
  best_bid    DOUBLE PRECISION,
  best_ask    DOUBLE PRECISION,
  hash        TEXT
);
CREATE INDEX idx_pc_market_ts   ON price_changes(market_id, ts DESC);
CREATE INDEX idx_pc_price_side  ON price_changes(market_id, side, price);

CREATE TABLE trades (
  id          BIGSERIAL        PRIMARY KEY,
  market_id   TEXT             NOT NULL,
  token_id    TEXT             NOT NULL,
  ts          TIMESTAMPTZ      NOT NULL,
  price       DOUBLE PRECISION NOT NULL
);
CREATE INDEX idx_trades_market_ts ON trades(market_id, ts DESC);

-- Migrated from feedback.jsonl
CREATE TABLE feedback (
  feedback_id   TEXT        PRIMARY KEY,
  target        TEXT        NOT NULL,
  source        TEXT        NOT NULL,
  message       TEXT        NOT NULL,
  severity      TEXT        NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL,
  resolved      BOOLEAN     NOT NULL DEFAULT FALSE,
  resolved_at   TIMESTAMPTZ,
  category      TEXT,
  metadata      JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX idx_feedback_created  ON feedback(created_at DESC);
CREATE INDEX idx_feedback_resolved ON feedback(resolved) WHERE resolved = FALSE;

-- Migrated from eval_records.jsonl
CREATE TABLE eval_records (
  id              BIGSERIAL        PRIMARY KEY,
  market_id       TEXT             NOT NULL,
  decision_id     TEXT             NOT NULL,
  prob_estimate   DOUBLE PRECISION NOT NULL,
  resolved_outcome DOUBLE PRECISION NOT NULL,
  brier_score     DOUBLE PRECISION NOT NULL,
  fill_status     TEXT             NOT NULL,
  recorded_at     TIMESTAMPTZ      NOT NULL,
  citations       JSONB            NOT NULL DEFAULT '[]'::jsonb,
  category        TEXT,
  model_id        TEXT,
  pnl             DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  slippage_bps    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  filled          BOOLEAN          NOT NULL DEFAULT TRUE
);
CREATE INDEX idx_eval_decision  ON eval_records(decision_id);
CREATE INDEX idx_eval_recorded  ON eval_records(recorded_at DESC);
CREATE INDEX idx_eval_model     ON eval_records(model_id) WHERE model_id IS NOT NULL;
```

Open sub-questions for the spec:

- Does the in-memory `MarketSignal.orderbook` model need to change? For
  backward compatibility the sensor can still populate the existing dict
  shape from the latest book state; the new tables become the authoritative
  durable record.
- Should `price_changes` have a UNIQUE constraint on
  `(market_id, timestamp, price, side)` for idempotent replay, or allow
  duplicates with an insertion-order tie-break?
- Does the schema need a `sensor_sessions` table to record WebSocket
  connection lifecycle (connected_at, disconnected_at, subscribe_reason)?
  This would let evaluators distinguish "no event" from "sensor was down".

**Q3: What granularity for tables?** — SUPERSEDED by Q2

The draft schema under Q2 defines the table granularity. Q3 is folded into
Q2's resolution. One additional table to consider at spec time:

- `signals` — derived `MarketSignal` records for compatibility with the
  existing in-memory `RunnerState.signals` list. Optional: the sensor may
  derive `MarketSignal` on-the-fly from the latest book state instead of
  persisting it separately, since the underlying data is already captured
  across `book_snapshots` + `book_levels` + `markets` + `tokens`.

**Q4: Retention policy?**

PostgreSQL handles double-digit GB fine on a single instance, but
`price_changes` is the high-volume table and there's no point keeping
every event from a year ago if strategies never query that far back.
Options:

- Keep everything indefinitely (until disk pressure forces action).
- Rolling window: drop events older than N days via a background task.
- Tiered: keep snapshots forever, drop deltas older than N days (can still
  replay coarse history).

**Q5: Migration seam to PostgreSQL?** — OBSOLETE (2026-04-16)

There is no migration seam because there is nothing to migrate from. The
project goes directly to PostgreSQL in all environments (see "Persistence
Decision: PostgreSQL, All Environments" above).

Consequence: no `IMarketDataStore` Protocol is required purely for
portability. A single concrete `PostgresMarketDataStore` class with typed
methods is sufficient. If a second backend is ever needed (read replica,
analytics warehouse, test double), the Protocol can be extracted at that
point from the existing concrete class with minimal effort.

**Q6: Write path concurrency?** — RESOLVED (2026-04-16)

The original framing (SQLite sync writes vs. asyncio event loop) is moot
after the PostgreSQL-first decision. PostgreSQL has real async drivers;
writes never block the event loop.

Decisions:

1. **Driver: `asyncpg`**. Chosen for alignment with the repo's existing
   async-native dependency stack (`httpx`, `websockets`). DBAPI
   compatibility is not needed — there is no SQLAlchemy / pandas in the
   runtime dependency set. If future offline analysis needs pandas, a
   separate `psycopg` connection for notebooks is acceptable; the runtime
   stays on `asyncpg`.

2. **Connection pool**: a single `asyncpg.Pool` owned by the `Runner`,
   created in `start()`, closed in `stop()`. Starting defaults:
   `min_size=2`, `max_size=10`. All sensor / controller / actuator /
   evaluator / API tasks acquire from the same pool via
   `async with pool.acquire() as conn`.

3. **Write mode: one INSERT per event**. Initial implementation writes
   each `price_change` / `book_level` with a single INSERT. No batching,
   no COPY, no background flusher. Rationale: simplicity, easier fault
   recovery, observable per-event latency. The Store method signatures
   stay batch-friendly (`write_price_change` takes one event; a future
   `write_price_changes` can wrap `copy_records_to_table` without
   breaking callers) so upgrading to bulk ingestion is a local change if
   the single-row path ever becomes the bottleneck.

4. **Transaction granularity**: one transaction per event (implicit via
   `asyncpg.Connection.execute`). No cross-event transactions in the
   ingestion path — each `price_change` is independently durable. When
   the evaluator needs atomic multi-row writes (e.g. `book_snapshot` +
   N `book_levels`), it uses `async with conn.transaction():` explicitly.

5. **Backpressure**: if the DB ever falls behind, asyncpg's pool will
   block `pool.acquire()` until a connection frees up. This naturally
   back-pressures the WebSocket consumer loop. If this ever becomes
   visible (dropped messages, rising queue depth), add a bounded
   in-memory buffer with a "drop oldest" or "batch flush" escape. Not
   needed initially.

## Summary: Decisions Captured On 2026-04-16

For the future spec writer: these are the load-bearing decisions from the
Socratic walkthrough on 2026-04-16. The rest of the document is context.

1. **Product goal**: observe data at scale → find mispriced markets →
   validate strategies → execute → evaluate. Controller and Evaluator are
   the weakest layers; fix those before live-trading polish.
2. **Data source target: Route B** — upgrade `PolymarketStreamSensor` to
   parse `book` and `price_change` events from the CLOB market WebSocket.
   Wire it into `runner._build_sensors()` for paper and live modes. REST
   `GET /book` is available as a fallback but not the primary path.
3. **Persistence target: PostgreSQL in all environments** (local dev, CI,
   production) via `asyncpg`. No SQLite detour. No `IMarketDataStore`
   Protocol for portability. No ORM. No migration framework initially.
   Raw SQL returning frozen dataclasses. Single connection pool owned by
   Runner (`min_size=2, max_size=10`). One INSERT per event in the
   ingestion path; bulk `COPY` deferred until a bottleneck is measured.
4. **Schema shape** (Q1/Q2 resolved): both `book` snapshots and
   `price_change` deltas stored. Snapshots expanded into `book_levels`
   rows (no JSON blobs). All prices as `DOUBLE PRECISION`, all times as
   `TIMESTAMPTZ`, all `side`/`source`/`venue`/`outcome` columns have
   `CHECK` constraints.
5. **Storage unification**: `feedback.jsonl` and `eval_records.jsonl` both
   migrate to PostgreSQL tables. `FeedbackStore` and `EvalStore` are
   rewritten as thin wrappers over SQL. No JSONL remains in the runtime
   contract. Dev state isolation moves from `PMS_DATA_DIR` to per-shell PG
   databases (e.g. `DATABASE_URL=postgres://localhost/pms_dev_$(whoami)`).
6. **Test strategy**: transaction-rollback fixture against a shared test
   PostgreSQL DB. Each test opens a transaction, yields the connection,
   rolls back on teardown. No `pytest-postgresql` dependency. Schema
   loaded once per session. Cross-connection integration tests fall back
   to per-test `TRUNCATE`.
7. **Stitching primitives** recorded under "Architecture: How The Stages
   Are Stitched Together". Any new layer (market data store) should use
   the same Protocol + Queue + Store pattern — not a fourth mechanism.
8. **Scope of this note**: design discovery only. No implementation until
   a Harness spec is written, reviewed, and approved per the repo's retro
   process. Remaining open questions: Q4 retention policy, the Q2
   sub-questions on `price_changes` UNIQUE constraint and
   `sensor_sessions` lifecycle table, and the `MarketSignal.orderbook`
   backward-compatibility question.

### 2. Controller: First-Class Layer, But Strategies Are Not First-Class Yet

Current files:

- `src/pms/controller/pipeline.py`
- `src/pms/controller/router.py`
- `src/pms/controller/forecasters/rules.py`
- `src/pms/controller/forecasters/statistical.py`
- `src/pms/controller/forecasters/llm.py`
- `src/pms/controller/calibrators/netcal.py`
- `src/pms/controller/sizers/kelly.py`

What works:

- `ControllerPipeline` composes forecasters, calibrator, sizer, and router.
- `RulesForecaster` can detect simple `fair_value` spread and subset-pricing
  violations.
- `StatisticalForecaster` can produce a probability from simple priors/counts.
- `NetcalCalibrator` and `KellySizer` provide basic calibration/sizing seams.

Gaps:

- There is no `Strategy` abstraction. A strategy is currently implicit inside a
  mix of forecasters, router gates, Kelly sizing, and stop-condition strings.
- There is no strategy registry, strategy config schema, strategy id, version, or
  explainable strategy output.
- The Controller currently averages all successful forecaster probabilities. It
  does not choose among strategies, rank opportunities, or produce comparable
  strategy candidates.
- `TradeDecision.stop_conditions` is doing too much: routing conditions,
  model-id attribution, and audit hints are all encoded as strings.
- `TradeDecision` does not preserve enough strategy provenance for serious
  backtesting: no `strategy_id`, `strategy_version`, `signal_features`,
  `raw_forecasts`, `calibrated_probability`, `confidence`, or reasoned
  opportunity classification.
- Dashboard `/decisions` shows recent decisions, but not strategy configuration,
  strategy candidates, strategy performance, or why an opportunity was accepted
  or rejected.

Implication:

Controller is structurally in the right place, but it needs a strategy API before
live trading or credential work becomes the highest-leverage task.

### 3. Evaluator: Decision Scoring Exists, Strategy Evaluation Does Not

Current files:

- `src/pms/evaluation/adapters/scoring.py`
- `src/pms/evaluation/metrics.py`
- `src/pms/evaluation/spool.py`
- `src/pms/evaluation/feedback.py`
- `src/pms/storage/eval_store.py`

What works:

- `EvalSpool` asynchronously receives decisions/fills and writes `EvalRecord`.
- `Scorer` computes Brier score, P&L, and slippage for a decision/fill pair.
- `MetricsCollector` aggregates Brier, P&L, slippage, fill rate, win rate, and
  calibration sample counts.
- The fill-rate wiring bug was fixed so rejected/unfilled decisions can affect
  metrics when outcomes are known.

Gaps:

- `EvalRecord` is decision-level, not strategy-run-level.
- There is no `BacktestRun`, `StrategyRun`, or `StrategyMetrics` model.
- There is no evaluator entry point that can replay a strategy over a market
  universe and compare it to baselines.
- Calibration is grouped by `model_id` or category, but not by full strategy,
  market class, liquidity bucket, venue, or time window.
- Paper/live mode cannot produce Brier records until outcomes resolve, so there
  needs to be a resolution/backfill path before long-running strategy quality is
  visible.
- Feedback currently emits threshold warnings; it does not propose concrete
  strategy changes or isolate which strategy component failed.

Implication:

Evaluator is useful for smoke-level correctness, but it does not yet answer:
"Which strategy worked, where, why, and under what market conditions?"

### 4. Actuator and Credentials: Important, But Not the Immediate Bottleneck

Current files:

- `src/pms/actuator/adapters/polymarket.py`
- `src/pms/config.py`
- `config.yaml.example`

What works:

- Backtest and paper actuators are usable.
- Live mode is guarded by `live_trading_enabled`.

Gaps:

- `PolymarketActuator` still rejects live execution after the guard.
- Credential storage and validation are not strong enough yet. Local secret files
  should be clearly ignored and documented before live order work proceeds.
- The live-client boundary should be injectable and testable without touching
  real funds.

Implication:

Live execution matters, but implementing it before strategy/backtest abstractions
would increase risk without improving the core discovery loop.

### 5. Documentation and Verification Consistency

Current files:

- `README.md`
- `dashboard/e2e/dashboard.spec.ts`
- `docs/superpowers/plans/2026-04-15-e2e-verification-and-readme.md`

Gaps:

- README still hardcodes test-count baselines in a few places. These numbers drift
  and should move to executable scripts or CI output.
- `dashboard/e2e/dashboard.spec.ts` still contains a stale "Known open question"
  comment even though `/feedback` limits were resolved.
- The historical plan under `docs/superpowers/plans/` still contains a "Known open
  questions" section. If kept as a historical artifact, it should be labelled as
  superseded so future readers do not treat it as current state.
- There is no single developer script for local verification. The desired shape
  is a `scripts/dev.sh` entry point with explicit subcommands for `test`,
  `dashboard`, `e2e`, and live/paper smoke checks.

## Controller Tech Spec Questions To Answer Next

The next spec should answer these before implementation:

1. What is the minimal `Strategy` protocol?
2. What data does a strategy receive: one `MarketSignal`, a market history window,
   an orderbook history window, external priors, or a combined feature bundle?
3. What does a strategy return before execution: opportunity score, abstain reason,
   raw probability, calibrated probability, suggested side, suggested size, and
   rationale?
4. How should multiple strategies be configured and selected?
5. How do we identify and version strategies in `TradeDecision`, `EvalRecord`, and
   dashboard payloads?
6. What is the minimal backtest contract for a strategy: input dataset, portfolio
   assumptions, execution assumptions, metrics output, and baseline comparison?
7. Which existing components remain independent: forecaster, calibrator, sizer,
   router, risk manager?

## Evaluator Tech Spec Questions To Answer Next

The next spec should answer these before implementation:

1. What is the difference between decision metrics and strategy metrics?
2. What is the schema for `BacktestRun`, `StrategyRun`, and `StrategyMetrics`?
3. How do we aggregate by strategy, market, venue, liquidity bucket, time window,
   and opportunity type?
4. How do we compare a candidate strategy against baselines such as hold, market
   price, random abstain, or current controller behavior?
5. How does paper/live evaluation backfill outcomes after markets resolve?
6. What dashboard pages should expose strategy discovery and backtest results?
7. What are the minimum acceptance gates before a strategy can be allowed to send
   orders to the Actuator?

## Reference: Select-Coin Backtesting Framework

Reference paths inspected on 2026-04-16:

- Research backtesting framework:
  `/Users/stometa/dev/quant/select-coin-backtesting/select-coin-pro_v1.8.1`
- Production/live strategy framework:
  `/Users/stometa/dev/quant/select-coin-prod/select-coin-pro`

Note: these select-coin reference frameworks are proprietary and explicitly
marked for personal learning use only. Use them as architecture references. Do
not copy code into this repo.

### Core abstraction order

The research backtesting framework confirms the architecture direction discussed
on 2026-04-16:

1. **Factor first.** Factor modules compute reusable scalar columns from market
   history. The same factor can be reused across many strategies with different
   params.
2. **Filter second.** Pre-selection and post-selection filters narrow the
   market universe before and after ranking.
3. **Strategy third.** `StrategyConfig` combines factor tuples, sort direction,
   params, weights, filters, selected-count rules, holding period, offsets, and
   capital weights.
4. **Backtest config fourth.** `BacktestConfig` holds global account,
   execution, cost, leverage, date-range, and multi-strategy settings.
5. **Execution simulation last.** The backtest produces target allocation ratios
   first. Execution simulation converts ratios into lots/orders using fees,
   minimum order amounts, lot sizes, rebalance mode, and price assumptions.
6. **Evaluation closes the loop.** Evaluation consumes the resulting equity
   curve and produces annual return, max drawdown, return/drawdown ratio,
   win/loss periods, volatility, and calendar-period returns.

This is different from the current PMS controller, where forecasters directly
produce probabilities and the pipeline immediately converts the averaged
probability into a `TradeDecision`.

### Useful patterns to adapt conceptually

- Factor outputs should be cached/queryable as columns keyed by
  `(factor_name, param, market_id, timestamp)`.
- Strategy configuration should be declarative and stable enough to hash/version.
- Strategy selection should be separated from execution. In the reference system,
  strategy output is a target allocation matrix; the simulator decides fills and
  costs.
- Multi-strategy support should aggregate target allocations before simulation.
- Backtest and live should share the same strategy selection path; the main
  divergence is execution and fill modeling.
- Evaluation should be run-level and strategy-level, not only decision-level.
- Parameter search should be a first-class workflow. The reference framework can
  generate many `BacktestConfig` variants, precompute shared factor columns once,
  then evaluate strategy variants. PMS currently has no equivalent.
- Strategy evaluation should support decomposition: combined, long-only/YES-only,
  short-only/NO-only, and per-offset/per-schedule where relevant.
- Dynamic module loading is useful for factor discovery, but PMS should start
  more conservatively with an explicit registry plus typed config. The reference
  framework can fall back to a dummy strategy when no strategy file exists; PMS
  should fail fast on unknown factor/strategy ids because live execution is in
  scope.
- Factor-analysis and parameter-analysis tools are separate from the main
  backtest path. PMS should mirror that separation: the runner executes; offline
  analysis scripts/dashboards compare factor bins, parameter sweeps, and run
  cohorts.

### Production/live framework lessons

The production/live select-coin repo is important for PMS because it shows how
strategy research is carried forward into operational validation.

Key observations:

- `config.py` imports a concrete strategy module from `strategy/`, and
  `backtest_name` is the strategy-combination name. Its comments explicitly map
  one backtest group to one live account.
- A production strategy is a bundle of sub-strategies: each sub-strategy has
  offsets, holding period, spot/swap selection flag, long/short selection
  counts, capital weights, factor lists, pre-filters, and post-filters.
- The repo did not expose a private order-placement client, credential flow, or
  balance/order sync path. The only exchange interaction I found is public
  exchange-rule ingestion for minimum order quantities.
- Live trading is represented through artifacts generated elsewhere:
  `data/实盘结果/<trading_name>/equity.csv` plus daily selection pkl files under
  `data/实盘结果/<trading_name>/选币`.
- `tools/tool3_回测实盘对比.py` aligns live and backtest results, then compares
  equity curves and selection overlap.
- The comparison tool makes two operational concepts explicit that PMS should
  model directly: timestamp alignment (`hour_offset` plus timezone adjustment)
  and symbol normalization (`BTC-USDT` vs `BTCUSDT`).
- Rebalance behavior is also explicit via `RebalanceMode`: always rebalance,
  rebalance by account-equity threshold, or rebalance by position threshold.

Implication for PMS:

- Backtest and paper/live should share the same strategy-selection path.
- Before real-money orders, PMS should be able to persist and compare backtest,
  paper, and live selection snapshots.
- Evaluator should report selection overlap and execution divergence separately.
  A live PnL difference is not enough; we need to know whether the strategy
  selected different markets, selected the same markets but executed differently,
  or saw different input data.
- The Actuator should remain the execution boundary, but Controller/Evaluator
  need first-class artifacts for `StrategyBundle`, `SelectionSnapshot`,
  `PortfolioTarget`, `BacktestLiveComparison`, `TimeAlignmentPolicy`,
  `SymbolNormalizationPolicy`, `RebalancePolicy`, and `VenueTradingRule`.

### Prediction-market mapping

For PMS, a factor should be the highest-level reusable primitive under
Controller. Candidate factor families:

- Market price factors: implied probability, price momentum, spread, volatility,
  jump size, time-to-resolution decay.
- Liquidity/orderbook factors: depth imbalance, best-bid/best-ask spread,
  notional depth, stale book detection, recent trade pressure.
- External-prior factors: Metaculus/public-model delta, fair-value delta,
  news/LLM prior delta, category-level prior.
- Structural-arbitrage factors: mutually exclusive set sum, complement pair
  consistency, subset/superset violation.
- Outcome-history factors: venue/category calibration residuals, market-maker
  bias, recurring event-family error.

For PMS, a strategy should combine factors into ranked opportunities, not merely
wrap one forecaster. Minimal strategy output should likely be:

- opportunity score
- side (`buy_yes`, `buy_no`, or abstain)
- target allocation or desired risk budget
- selected factor values and rationale
- strategy id, config hash, and version
- execution constraints and abstain reasons

The Evaluator should then compare strategy runs over a market universe, not only
score individual `TradeDecision` objects.

### Delta from current PMS implementation

Current PMS backtest mode is a smoke-oriented replay:

1. `HistoricalSensor` emits one `MarketSignal` at a time from JSONL/CSV.
2. `ControllerPipeline` gates the single signal, runs forecasters, averages
   probabilities, sizes with Kelly, and emits a `TradeDecision`.
3. `BacktestActuator` loads fixture orderbooks and simulates whether that one
   decision fills.
4. `EvalSpool` writes one decision-level `EvalRecord` when an outcome is known.

The reference framework is a research-oriented backtest:

1. Load the whole market universe over a date range.
2. Compute and cache all factor columns required by all strategies.
3. For each strategy, apply filters, rank the universe, select candidates, and
   produce target allocation ratios.
4. Aggregate offsets and multiple strategies into a time x market allocation
   matrix.
5. Simulate execution, costs, rebalance rules, and portfolio equity.
6. Evaluate full run quality and produce comparable reports.

This gap changes the next implementation priority:

- Do not make `Forecaster` the top-level strategy abstraction.
- Add `Factor` and `StrategyConfig` concepts under Controller first.
- Add a backtest runner that consumes market snapshots/factor panels, not only
  one signal at a time.
- Extend `TradeDecision`/evaluation provenance only after strategy/run identity
  is specified.
- Keep actuator execution modeling behind the same strategy output contract so
  backtest and live diverge mainly at the fill/order layer.

## Proposed Harness Flow

Do not implement directly from this note. Use this sequence:

1. Requirements discovery focused on Controller/Evaluator.
2. Write a Harness spec under `.harness/<task-id>/spec.md`.
3. Run spec evaluation and revise until approved.
4. Execute checkpoints with TDD:
   - strategy protocol and domain models
   - strategy config/registry
   - backtest runner contract
   - strategy-level evaluation metrics
   - dashboard/API surfaces for strategy understanding
   - documentation and `scripts/dev.sh`
5. Run E2E verification, review loop, full verification, PR, and retro.

## Non-Goals For The Next Spec

- Do not prioritize real-money live order submission first.
- Do not add a broad external dependency stack until the strategy/backtest
  contracts are explicit.
- Do not treat dashboard charts alone as proof of strategy quality.
- Do not hide strategy behavior inside stringly-typed stop conditions.
