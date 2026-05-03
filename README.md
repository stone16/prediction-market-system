# Prediction Market System (pms)

Modular prediction market trading system organised as a concurrent cybernetic
feedback web across Sensor, Controller, Actuator, Evaluator, and feedback edges.
Sensor, Controller, Actuator, and Evaluator run as concurrent asyncio tasks
with bidirectional feedback edges; the runtime is not a phased pipeline.

Target venues: Polymarket (primary). Kalshi is reserved in the venue enum but
has no adapter in v1 — see CP06's stub gate. Implemented run modes are
`backtest`, `paper`, and gated Polymarket `live`. LIVE mode remains fail-closed
unless `live_trading_enabled=true`, required Polymarket credentials validate,
and the first live order is approved by an operator gate.

## Status: Gate 2 CLOSED — Ready for Paper Soak

All core PRs merged as of 2026-05-03. The system is code-complete for
H1 (FLB contrarian) strategy and ready for live-data paper soaking.

| Milestone | Status | PR | Description |
|-----------|--------|-----|-------------|
| Gate 1: Research | ✅ | #16 | Prediction methodology brief (H1+H2 strategy) |
| FLB Feasibility | ✅ | #45 | Data feasibility analysis + contract-level FLB analysis |
| Risk Config | ✅ | #43 | Live-soak risk envelope (drawdown halt, exposure limits) |
| Config Loading | ✅ | #44 | Paper soak config at API startup |
| FLB Data Pipeline | ✅ | #47 | Historical warehouse source for robust FLB measurement |
| LLM Forecaster | ✅ | #46 | Provider-switchable (Anthropic/OpenAI) with per-market cache |
| Strategy Relax | ✅ | #48 | Default strategy required factors relaxed for PAPER |
| **Gate 2: Edge Validation** | ✅ | — | All gates closed, paper soak unblocked |

### What's Needed Before Live Trading

Three things remain between paper soak and real capital:

1. **Polymarket credentials** — 6 fields (private_key, api_key, api_secret,
   api_passphrase, funder_address, signature_type). Set as env vars, never in
   config files.
2. **Confirm risk envelope** — Proposed defaults: $100 bankroll, $5/market
   max, $20 daily max loss. Adjust via `config.live-soak.yaml`.
3. **24-hour paper soak** — Run with live Polymarket data to verify sensors,
   risk engine, order lifecycle, and auto-halt triggers before risking capital.
   See [Orchestration guide](#orchestration-guide) below.

See [docs/operations/live-polymarket-runbook.md](docs/operations/live-polymarket-runbook.md)
for the full PAPER soak, credential setup, first live order, rollback, and
emergency stop runbook. Install the optional live SDK with
`uv sync --extra live` before starting LIVE mode.

## Orchestration Guide

### Architecture: Four Concurrent Layers

```
┌──────────────────────────────────────────────────────┐
│                    Sensor Layer                       │
│  HistoricalSensor → MarketDiscoverySensor → Stream   │
│  (pulls market data from Polymarket / Gamma API)      │
├──────────────────────────────────────────────────────┤
│                  Controller Layer                     │
│  Pipeline: features → forecaster → sizer → planner   │
│  (Beta-Binomial + LLM forecaster + Kelly sizing)      │
├──────────────────────────────────────────────────────┤
│                  Actuator Layer                       │
│  RiskManager → ExecutionPlan → OrderState            │
│  (6 auto-halt triggers, exposure limits, drawdown)    │
├──────────────────────────────────────────────────────┤
│                  Evaluator Layer                      │
│  MetricsCollector → EvalSpool → FeedbackEngine       │
│  (Brier score, hit rate, calibration, FLB edge)       │
└──────────────────────────────────────────────────────┘
        ▲                                              │
        └──────────── Feedback Edges ──────────────────┘
```

### Strategy Components

| Component | What It Does | Config Key |
|-----------|-------------|------------|
| **Beta-Binomial Forecaster** | Bayesian probability model using historical resolution data | `forecaster: beta_binomial` |
| **LLM Forecaster** | Provider-switchable (Anthropic/OpenAI) market prediction with per-market 30s TTL cache | `forecaster: llm`, `llm.provider`, `llm.base_url` |
| **FLB Signal** | Detects Favorite-Longshot Bias in Polymarket pricing | `strategies.h1.enabled` |
| **Kelly Sizer** | Position sizing based on edge magnitude and Kelly fraction | `sizing.kelly_fraction` |
| **RiskManager** | 6 auto-halt triggers (drawdown, consecutive losses, slippage, rate limit, stale orders, credential failure) | `risk.max_drawdown_pct`, `risk.max_position_per_market` |

### Run Modes

| Mode | Purpose | Command |
|------|---------|---------|
| **backtest** | Historical simulation against warehouse data | `uv run pms-backtest --config config.backtest.yaml` |
| **paper** | Live data, simulated trades, no real orders | `uv run pms-api --config config.live-soak.yaml` |
| **live** | Real Polymarket trading (gated) | `uv run pms-api` with `PMS_MODE=live` + credentials |

### Step-by-Step: From Zero to Paper Soak

```bash
# 1. Install dependencies (including live SDK for paper mode)
uv sync --extra live

# 2. Start PostgreSQL (required for market data persistence)
docker compose up -d postgres

# 3. Apply migrations
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_dev
uv run alembic upgrade head

# 4. Copy and customize the paper soak config
cp config.live-soak.yaml config.local.live-soak.yaml
# Edit config.local.live-soak.yaml:
#   - Adjust risk.max_position_per_market (proposed: $5)
#   - Adjust risk.max_total_exposure (proposed: $100)
#   - Adjust risk.max_drawdown_pct (proposed: 20)

# 5. Start paper soak (live data, no real orders)
uv run pms-api --config config.local.live-soak.yaml

# 6. Monitor status
curl http://127.0.0.1:8000/status

# 7. Generate daily paper soak report
uv run python scripts/paper-report.py --date 2026-05-03
```

### Step-by-Step: From Paper Soak to Live

After at least 24 hours of paper soak validating signal quality:

```bash
# 1. Set Polymarket credentials (env vars only, never in config)
export PMS_POLYMARKET_PRIVATE_KEY="<your private key>"
export PMS_POLYMARKET_API_KEY="<your API key>"
export PMS_POLYMARKET_API_SECRET="<your API secret>"
export PMS_POLYMARKET_API_PASSPHRASE="<your API passphrase>"
export PMS_POLYMARKET_FUNDER_ADDRESS="<your wallet address>"
export PMS_POLYMARKET_SIGNATURE_TYPE="<your signature type>"

# 2. Create config.live.yaml
#    mode: live
#    live_trading_enabled: true
#    risk: (copy from your validated paper soak config)

# 3. Create first-order approval file
#    /secure/pms/first-order.json — reviewed and approved by operator

# 4. Start live mode
export PMS_MODE=live
export PMS_LIVE_TRADING_ENABLED=true
uv run pms-api --config config.live.yaml
```

**Critical**: The system is fail-closed. If any credential is missing, risk
limits are violated, or an auto-halt trigger fires, the system stops submitting
orders automatically.

## Agent Strategy Boundary

Agent strategy modules may propose, judge, and explain market actions, but they
cannot submit orders, cannot override risk, and cannot override reconciliation.
Their typed output path is `TradeIntent | BasketIntent` -> `ExecutionPlan` ->
`RiskDecision` -> `OrderState` -> reconciliation -> evaluator.

Execution planning is an executability gate, not execution authority. The
planner checks quotes, depth, book freshness, minimum size, tick size, and
slippage before any planned order can reach the existing risk and actuator
path. RiskManager and ActuatorExecutor remain the only order-submission route.
Predict-Raven is an external reference pattern for plugin/runtime shape and
durable artifacts, not an architecture PMS copies wholesale.

## Layout

```
src/pms/               # Python package
  actuator/            # risk + executor + feedback adapters
  api/                 # FastAPI app + `pms-api` CLI entry
  controller/          # decision pipeline (forecaster, sizer, planner)
  core/                # frozen dataclasses, enums, Protocol interfaces
  evaluation/          # metrics collector + eval spool + feedback engine
  market_selection/    # active-perception selector + subscription controller
  sensor/              # HistoricalSensor + MarketDiscoverySensor + stream
  storage/             # JSONL stores + Postgres market-data persistence
  runner.py            # orchestrator wiring all four layers
  config.py            # PMSSettings (pydantic-settings)
dashboard/             # Next.js console (port 3100)
rust/                  # PyO3 workspace stub (reserved for perf paths)
scripts/               # Utility scripts (paper report, migrations, strategy)
tests/                 # pytest suite (unit + integration)
docs/
  operations/          # Runbooks (live-polymarket-runbook.md)
  research/            # Research briefs, FLB analysis, backtest specs
```

## Quick start — backend + dashboard end-to-end

```bash
# 1. Start PostgreSQL for local development
docker compose up -d postgres

# 2. Install Python deps
uv sync

# 3. Point PMS at your local dev database and apply migrations
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_dev
uv run alembic upgrade head

# Escape hatch: roll back to the pre-migration state for the current DATABASE_URL
uv run alembic downgrade base

# 4. Start the FastAPI backend (port 8000 by default)
uv run pms-api                       # → http://127.0.0.1:8000
# Optional: auto-start the runner at boot
PMS_AUTO_START=1 uv run pms-api
# First-live paper soak: load the tight risk envelope explicitly
uv run pms-api --config config.live-soak.yaml

# 5. In another shell, start the dashboard (port 3100)
cd dashboard
npm install
PMS_API_BASE_URL=http://127.0.0.1:8000 npm run dev
#   → http://127.0.0.1:3100
```

schema.sql is a reference artifact, not the runtime source; apply runtime
schema changes with Alembic.

If `PMS_API_BASE_URL` is unset the dashboard silently falls back to the
bundled mock store (`dashboard/lib/mock-store.ts`) — useful for pure frontend
work, but every page will show fabricated data.

## Runner lifecycle via the API

```bash
curl -X POST http://127.0.0.1:8000/run/start   # begin ingesting signals
curl       http://127.0.0.1:8000/status        # {running: true, ...}
curl -X POST http://127.0.0.1:8000/run/stop    # graceful shutdown
```

The `Overview` dashboard page includes a **Runner Controls**
panel that calls these endpoints directly.

## Development

```bash
uv sync
uv run pytest -q                              # full default suite
uv run mypy src/ tests/ --strict              # strict type check
PMS_RUN_INTEGRATION=1 uv run pytest -m integration   # PostgreSQL + live-network tests
```

Baseline invariants enforced by CI:
- pytest default suite stays green; integration checks are gated on
  `PMS_RUN_INTEGRATION=1`.
- mypy strict must be clean on every committed source file.
- Research sweep and worker spec format: `docs/research/backtest-spec-format.md`

### Isolating dev state

Runtime state now lives in PostgreSQL. Use a per-shell database name so
parallel sessions do not share evaluator or feedback rows:

```bash
export DATABASE_URL=postgres://localhost/pms_dev_$(whoami)
uv run pms-api
```

Legacy `.data/*.jsonl` files are no longer part of the runtime contract.
If you need to preserve old local rows, migrate them once with:

```bash
python scripts/migrate_jsonl_to_pg.py --data-dir .data --database-url "$DATABASE_URL"
```

See `CLAUDE.md` for the active engineering rules promoted from retros.
