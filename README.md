# Prediction Market System (pms)

Modular prediction market trading system organised as a concurrent cybernetic
feedback web across Sensor, Controller, Actuator, Evaluator, and feedback edges.

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

## Layout

```
src/pms/               # Python package
  actuator/            # risk + executor + feedback adapters
  api/                 # FastAPI app + `pms-api` CLI entry
  controller/          # decision pipeline
  core/                # frozen dataclasses, enums, Protocol interfaces
  evaluation/          # metrics collector + eval spool + feedback engine
  market_selection/    # active-perception selector + subscription controller
  sensor/              # HistoricalSensor + MarketDiscoverySensor + stream
  storage/             # JSONL stores + Postgres market-data persistence
  runner.py            # orchestrator wiring all four layers
  config.py            # PMSSettings (pydantic-settings)
dashboard/             # Next.js console (port 3100)
rust/                  # PyO3 workspace stub (reserved for perf paths)
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
uv run pytest -q                              # full suite (246 pass, 54 skip)
uv run mypy src/ tests/ --strict              # strict type check
PMS_RUN_INTEGRATION=1 uv run pytest -m integration   # PostgreSQL + live-network tests
```

Baseline invariants enforced by CI:
- pytest 246 passing, 54 skipped (integration gated on `PMS_RUN_INTEGRATION=1`).
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
