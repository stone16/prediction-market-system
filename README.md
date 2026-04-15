# Prediction Market System (pms)

Modular prediction market trading system organised around a **cybernetic loop**:

```
Sensor → Controller → Actuator → Evaluator → Feedback → (Controller)
```

Target venues: Polymarket (primary) and Kalshi. Run modes: `backtest`,
`paper`, `live` (gated by config). This README is the contributor entry
point; active engineering rules live in [`CLAUDE.md`](./CLAUDE.md).

## Architecture

### The cybernetic loop

Every stage is a Protocol with pluggable adapters — swap any one without
touching the others:

- **Sensor** ingests market signals. Adapters: `HistoricalSensor` (JSONL
  fixtures), `PolymarketRestSensor` (REST poll), `PolymarketStreamSensor`
  (websocket). See `src/pms/sensor/adapters/`.
- **Controller** runs forecaster → calibrator → sizer → risk check,
  turning a signal into a `TradeDecision`. See
  `src/pms/controller/pipeline.py`.
- **Actuator** executes decisions against a `backtest`, `paper`, or
  `polymarket` adapter and emits `OrderState` / `FillRecord`. See
  `src/pms/actuator/adapters/`.
- **Evaluator** scores decisions against realised outcomes (Brier),
  emits metrics, and generates feedback on threshold breaches. See
  `src/pms/evaluation/`.
- **Feedback** closes the loop back into the controller as policy-level
  adjustments, persisted across restarts via `FeedbackStore`.

### Module boundaries

The orchestrator (`src/pms/runner.py`) and the controller pipeline
(`src/pms/controller/pipeline.py`) only reference the Protocol types in
`src/pms/core/interfaces.py` — `ISensor`, `IController`, `IActuator`,
`IEvaluator`, `IForecaster`, `ICalibrator`, `ISizer`. No concrete
imports. This is what makes layer swapping work without touching the
orchestrator.

### Data flow: browser → BFF → backend

The browser never talks to the FastAPI port directly. Every dashboard
fetch goes through a Next.js Route Handler under
`dashboard/app/api/pms/*`, which forwards to `PMS_API_BASE_URL`. This
BFF seam is load-bearing for future CORS / auth work — do not bypass it.

## Backend

### Runtime

- **FastAPI** (`src/pms/api/app.py`) — nine endpoints: `/status`,
  `/signals`, `/decisions`, `/metrics`, `/feedback`,
  `/feedback/{id}/resolve`, `/config`, `/run/start`, `/run/stop`.
- **uvicorn** serves the app; `pms-api` is the console-script entry
  declared in `pyproject.toml`.
- **pydantic-settings** (`src/pms/config.py`) loads env vars into
  `PMSSettings`. `PMS_AUTO_START=1` starts the runner at boot;
  `PMS_DATA_DIR` isolates JSONL state.
- **Runner** (`src/pms/runner.py`) wires
  sensor → controller → actuator → evaluator, owns the asyncio loops,
  and exposes start / stop as async methods.

### Storage

Persistent state is JSONL append-only logs under `data_dir()`:
`eval_records.jsonl` (one `EvalRecord` per resolved decision) and
`feedback.jsonl` (feedback events). Path controlled by `PMS_DATA_DIR`
(default `.data/`, gitignored). `FeedbackStore` reloads from disk on
start so feedback survives restarts; `EvalStore` is write-only to the
JSONL with an in-memory list for the current session.

Rationale: offline-debuggable, greppable, no migration story needed for
a single-node dev system.

### Dependencies

Python `>=3.11`. Runtime deps (from `pyproject.toml`):
`fastapi>=0.110.0` (HTTP API), `uvicorn>=0.30` (ASGI server),
`pydantic>=2.12.0` + `pydantic-settings>=2.11.0` (validation + env
config), `httpx>=0.28.1` (Polymarket REST), `websockets>=16.0`
(Polymarket stream), `pyyaml>=6.0.3` (fixtures).

### Test and type gates

```bash
uv sync
uv run pytest -q                             # full suite
uv run mypy src/ tests/ --strict             # strict type check
PMS_RUN_INTEGRATION=1 uv run pytest -m integration   # live network tests
```

Baseline: **72 passing, 2 skipped** (integration gated on
`PMS_RUN_INTEGRATION=1`). mypy strict must be clean on every committed
file. Path resolution is pinned in `pyproject.toml`:
`pythonpath = ["src", "."]` for pytest, `mypy_path = "src"` for mypy —
removing either breaks fresh-clone collection.

## Frontend

### Pages

| Route        | Component                          | Data source(s)                                         |
| ---          | ---                                | ---                                                    |
| `/`          | `dashboard/app/page.tsx`           | `/api/pms/status`, `/api/pms/metrics`, `/api/pms/feedback` (via `DashboardClient`) |
| `/signals`   | `dashboard/app/signals/page.tsx`   | `/api/pms/signals?limit=100` — h1 "Signal Stream"      |
| `/decisions` | `dashboard/app/decisions/page.tsx` | `/api/pms/decisions?limit=100` — h1 "Decision Ledger"  |
| `/metrics`   | `dashboard/app/metrics/page.tsx`   | `/api/pms/metrics` + recharts — h1 "Metric Review"     |
| `/backtest`  | `dashboard/app/backtest/page.tsx`  | `/api/pms/status`, `/api/pms/metrics`, `/api/pms/signals?limit=200`, plus `/api/pms/run/{start,stop}` via `RunControls` |

### Tech stack

- **Next.js `^16.0.0`** with Turbopack (App Router).
- **React `^19.0.0`** server + client components.
- **recharts `^3.0.0`** for line + calibration charts on `/metrics`.
- **Playwright `^1.57.0`** for E2E (`npx playwright test`).
- **TypeScript `^5.9.0`** strict mode.
- **BFF**: `dashboard/app/api/pms/*` Route Handlers proxy to
  `PMS_API_BASE_URL`; unset → falls back to `dashboard/lib/mock-store.ts`
  (fabricated data, useful for pure-frontend iteration).

## Running the system

### Quick start — backend + dashboard end-to-end

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
bundled mock store — every page then shows fabricated data.

### Runner lifecycle via the API

```bash
curl -X POST http://127.0.0.1:8000/run/start   # begin ingesting signals
curl       http://127.0.0.1:8000/status        # {running: true, ...}
curl -X POST http://127.0.0.1:8000/run/stop    # graceful shutdown
```

The `Overview` and `Backtest` dashboard pages include a **Runner
Controls** panel that calls these endpoints directly.

## Verifying correctness

### Static gates

```bash
uv run pytest -q                     # 72 passing, 2 skipped
uv run mypy src/ tests/ --strict     # must be clean
```

Both are load-bearing — never commit without running both.

### Runtime health check

```bash
curl -s http://127.0.0.1:8000/status | python3 -m json.tool
```

What to check in the response:

- `running: true` after `/run/start`.
- `sensors[0].last_signal_at` advances over time in paper / live mode.
- `controller.decisions_total >= actuator.fills_total` (risk gate /
  liquidity rejects widen the gap).
- `evaluator.eval_records_total <= controller.decisions_total`. Only
  decisions with a resolved outcome produce an `EvalRecord` (see
  `EvalSpool._run`); the gap reflects pending markets, not a bug.
- `evaluator.brier_overall ∈ [0, 1]` when eval records exist, else null.

### Integration smoke

```bash
PMS_RUN_INTEGRATION=1 uv run pytest -m integration
```

Runs the paper-mode runner against live Polymarket REST (no credentials).

### Visual smoke capture

```bash
cd dashboard && PMS_API_BASE_URL=http://127.0.0.1:8000 \
  npx playwright test dashboard.spec.ts -g "smoke capture"
```

Screenshots land in `dashboard/e2e/baseline/*.png`, committed for manual
PR diffs. Not a deterministic regression baseline — backend state
(feedback count, decision count, timestamps) varies per session, so any
diff requires manual review against expected state changes.

## Development workflow

### Branch and commit conventions

- Feature branches: `feat/<short>`, `fix/<short>`, `chore/<short>`,
  `docs/<short>`.
- No merges to `main` without passing `uv run pytest -q` AND
  `uv run mypy src/ tests/ --strict`.
- No `Co-Authored-By` lines (see [`CLAUDE.md`](./CLAUDE.md) §Git Rules).

### The retro → rule promotion loop

1. After each non-trivial task, write a retro under
   `.harness/retro/<task>/`.
2. Retros propose rules; `.harness/retro/index.md` tracks each proposal's
   lifecycle (proposed → observed → promoted).
3. Rules observed ≥ 2 times across tasks are promoted to
   [`CLAUDE.md`](./CLAUDE.md) §Active rules and become enforceable.

See `docs/continuation-guide.md` for the Phase 3D rule-adoption process.

### Where to put new work

- **New venue adapter**: `src/pms/{sensor,actuator}/adapters/<venue>.py`
  plus tests under `tests/integration/` marked `@pytest.mark.integration`.
- **New controller component** (forecaster / sizer / calibrator): drop
  into `src/pms/controller/{forecasters,sizers,calibrators}/`. The
  pipeline picks it up via the Protocol interfaces in
  `src/pms/core/interfaces.py` — no orchestrator changes needed.
- **New dashboard page**: `dashboard/app/<route>/page.tsx` plus a
  matching BFF handler under `dashboard/app/api/pms/<route>/route.ts`.

### Isolating dev state

The backend writes `feedback.jsonl` and `eval_records.jsonl` under
`.data/` by default. Both are gitignored, but `FeedbackStore` reloads
`feedback.jsonl` on start, so feedback persists across restarts. To
isolate:

```bash
export PMS_DATA_DIR=/tmp/pms-dev    # or any ephemeral path
uv run pms-api                      # writes to $PMS_DATA_DIR/*.jsonl
```

Tests are already isolated — every `FeedbackStore` / `EvalStore` test
instance takes an explicit `tmp_path`, so the shared `.data/` is
untouched regardless of `PMS_DATA_DIR`.

Reset the repo-default store with `rm -rf .data/`.

### Known open questions

- `/feedback` has no `limit` query parameter, unlike `/signals` and
  `/decisions` (both default 50). A long-running dev session can push
  700+ entries to `/api/pms/feedback`, bloating the home-page render and
  the visual smoke capture. Fix: mirror the `_latest(limit)` pattern
  around the handler in `src/pms/api/app.py`.
- Paper-mode was observed on 2026-04-15 producing decisions but zero
  fills in a 126 s window. Either the paper actuator is not incrementing
  `fills_total`, or the observation window was too short for any paper
  order to fill. See `claudedocs/paper-mode-smoke-2026-04-15.md`.
- `brier_series` and `pnl_series` timestamps collapse into a sub-second
  window in backtest mode because `HistoricalSensor` replays all records
  as fast as it can. Charts on `/metrics` become unreadable. Options:
  subsample / aggregate in the metrics collector, or space backtest
  replay at a configurable rate.
- The calibration curve on `/metrics` degenerates to a single point when
  the forecaster is the placeholder "always 0.5" stub. Expected for a
  backtest with the stub forecaster, but the chart renders as a lone dot
  with no hint that this is a forecaster limitation rather than a UI
  bug. Add an empty-state copy explaining the degeneracy.

See [`CLAUDE.md`](./CLAUDE.md) for the active engineering rules promoted
from retros.
