# Prediction Market System (pms)

Modular prediction market trading system organised around a **cybernetic loop**:

```
Sensor → Controller → Actuator → Evaluator → Feedback → (Controller)
```

Target venues: Polymarket (primary) and Kalshi. Supports three run modes:
`backtest`, `paper`, and `live` (gated by config).

## Layout

```
src/pms/               # Python package
  actuator/            # risk + executor + feedback adapters
  api/                 # FastAPI app + `pms-api` CLI entry
  controller/          # decision pipeline
  core/                # frozen dataclasses, enums, Protocol interfaces
  evaluation/          # metrics collector + eval spool + feedback engine
  sensor/              # HistoricalSensor + PolymarketRestSensor + stream
  storage/             # EvalStore + FeedbackStore (JSONL persistence)
  runner.py            # orchestrator wiring all four layers
  config.py            # PMSSettings (pydantic-settings)
dashboard/             # Next.js console (port 3100)
rust/                  # PyO3 workspace stub (reserved for perf paths)
tests/                 # pytest suite (unit + integration)
```

## Quick start — backend + dashboard end-to-end

```bash
# 1. Install Python deps
uv sync

# 2. Start the FastAPI backend (port 8000 by default)
uv run pms-api                       # → http://127.0.0.1:8000
# Optional: auto-start the runner at boot
PMS_AUTO_START=1 uv run pms-api

# 3. In another shell, start the dashboard (port 3100)
cd dashboard
npm install
PMS_API_BASE_URL=http://127.0.0.1:8000 npm run dev
#   → http://127.0.0.1:3100
```

If `PMS_API_BASE_URL` is unset the dashboard silently falls back to the
bundled mock store (`dashboard/lib/mock-store.ts`) — useful for pure frontend
work, but every page will show fabricated data.

## Runner lifecycle via the API

```bash
curl -X POST http://127.0.0.1:8000/run/start   # begin ingesting signals
curl       http://127.0.0.1:8000/status        # {running: true, ...}
curl -X POST http://127.0.0.1:8000/run/stop    # graceful shutdown
```

The `Overview` and `Backtest` dashboard pages include a **Runner Controls**
panel that calls these endpoints directly.

## Development

```bash
uv sync
uv run pytest -q                              # full suite (70 pass, 2 skip)
uv run mypy src/ tests/ --strict              # strict type check
PMS_RUN_INTEGRATION=1 uv run pytest -m integration   # live network tests
```

Baseline invariants enforced by CI:
- pytest ≥ 70 passing, 2 skipped (integration gated on `PMS_RUN_INTEGRATION=1`).
- mypy strict must be clean on every committed source file.

### Isolating dev state

The backend writes two JSONL files under `.data/` by default:
`feedback.jsonl` and `eval_records.jsonl`. Both are gitignored, but
`FeedbackStore` reloads `feedback.jsonl` on start, so dev sessions
persist feedback across restarts. To isolate:

```bash
export PMS_DATA_DIR=/tmp/pms-dev    # or any ephemeral path
uv run pms-api                      # writes to $PMS_DATA_DIR/*.jsonl
```

Tests are already isolated — every `FeedbackStore` / `EvalStore` test
instance is constructed with an explicit `tmp_path`, so the shared
`.data/` is untouched regardless of whether `PMS_DATA_DIR` is set.

To reset the repo-default store: `rm -rf .data/` (or
`rm -f .data/feedback.jsonl .data/eval_records.jsonl` to keep other
files you may have placed there).

See `CLAUDE.md` for the active engineering rules promoted from retros.
