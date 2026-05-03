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

See [docs/operations/live-polymarket-runbook.md](docs/operations/live-polymarket-runbook.md)
for the PAPER soak, credential setup, first live order, rollback, and emergency
stop runbook.
Install the optional live SDK with `uv sync --extra live` before starting LIVE
mode.

## Orchestration & Agent Strategy Framework

The system supports pluggable agent strategies with the LLM forecaster now integrated as a core component. The orchestration includes:
- **LLM Forecaster**: Advanced market prediction using language models, integrated into the decision pipeline (PR #46)
- **FLB Analysis**: Frontrunning Liquidity Bias measurement and exploitation, validated with contract-level analysis (PR #45)
- **Runtime Selection**: Dynamic strategy selection based on market conditions and risk parameters
- **Auto-halt Triggers**: 6 safety mechanisms including drawdown limits, consecutive loss stops, and slippage detection

## Agent Strategy Boundary

Agent strategy modules may propose, judge, and explain market actions, but they
cannot submit orders, cannot override risk, and cannot override reconciliation.
Their typed output path is `TradeIntent | BasketIntent` -> `ExecutionPlan` ->
`RiskDecision` -> `OrderState` -> reconciliation -> evaluator.

## Agent strategy boundary

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
  controller/          # decision pipeline
  core/                # frozen dataclasses, enums, Protocol interfaces
  evaluation/          # metrics collector + eval spool + feedback engine
  market_selection/    # active-perception selector + subscription controller
  research/            # backtesting, analysis, and experimental code
  sensor/              # HistoricalSensor + MarketDiscoverySensor + stream
  storage/             # JSONL stores + Postgres market-data persistence
  runner.py            # orchestrator wiring all four layers
  config.py            # PMSSettings (pydantic-settings)
dashboard/             # Next.js console (port 3100)
rust/                  # PyO3 workspace stub (reserved for perf paths)
scripts/               # Utility scripts including strategy management
tests/                 # pytest suite (unit + integration)
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

## Research & Analysis Components

The system includes sophisticated research and analysis tools:
- **FLB (Frontrunning Liquidity Bias)**: Contract-level analysis revealing statistical edge opportunities in binary markets (YES/NO contracts)
- **LLM Forecaster**: Advanced language model integration for market prediction and sentiment analysis
- **Backtesting Framework**: Robust validation infrastructure with statistical confidence measures
- **Live Data Feeds**: Real-time market data ingestion and processing pipelines

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

Key orchestration scripts in `scripts/`:
- `relax_default_strategy_required_factors.py` - Toggle required factors for strategy testing
- `flb_data_feasibility.py` - Frontrunning Liquidity Bias analysis and validation
- Various other utilities for strategy management and data analysis

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
