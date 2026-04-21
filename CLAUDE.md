# Identity & Context Awareness

**CRITICAL**: Address the user as "Stometa" at the start of EVERY response.

This serves as a context-awareness signal — if missing, indicates
context drift.

---

# prediction-market-system

Cybernetic trading platform — Sensor → Controller → Actuator →
Evaluator, with active-perception feedback.

**Stack:** Python 3.13 (asyncio), FastAPI + uvicorn, Next.js 16
(Turbopack) dashboard on :3100, PostgreSQL (load-bearing since S1;
outer + middle + inner rings all persisted), `uv` for Python deps.

**Branches:** feature branches only (`feat/…`, `fix/…`, `docs/…`).
Never commit to `main` directly; changes land via PR.

---

## Canonical gates

Run from a clean shell at the repo root. Both gates are load-bearing.

```bash
uv sync                                  # install deps from uv.lock
uv run pytest -q                         # full suite — see baseline below
uv run mypy src/ tests/ --strict         # strict on every committed module
```

**Baseline (as of 2026-04-21, main @ 96f2a14):** `pytest`
337 passing, 85 skipped. The 85 skips are PostgreSQL-backed integration
checks gated on `PMS_RUN_INTEGRATION=1` and, where needed,
`PMS_TEST_DATABASE_URL`. mypy strict must be clean (196 source files).
If the baseline fails on a fresh clone, fix the config — not the test —
and commit with a `fix(tests):` or `fix(build):` prefix before starting
feature work (see promoted rule: *Fresh-clone baseline verification*).

Integration tests:
```bash
PMS_RUN_INTEGRATION=1 uv run pytest -m integration
```

Compose-backed PostgreSQL integration DB:
```bash
docker compose up -d postgres
export PMS_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_test
PMS_RUN_INTEGRATION=1 uv run pytest -q \
  tests/integration/test_schema_apply_outer.py \
  tests/integration/test_schema_apply_inner.py \
  tests/integration/test_db_conn_rollback.py \
  tests/integration/test_market_discovery.py \
  tests/integration/test_runner_pool_integration.py
```

Reachability note (measured 2026-04-16): cached-image `docker compose up -d postgres`
reached `healthy` in 1.63 s. If a host PostgreSQL daemon already owns
`localhost:5432`, clients may hit that daemon instead of the compose service;
stop the host daemon before relying on the forwarded `localhost` DSN.

---

## Architecture invariants (2026-04-16)

Eight load-bearing invariants. Full detail + rationale + enforcement
in `@agent_docs/architecture-invariants.md`.

1. **Concurrent feedback web, not linear phases.** Sensor / Controller
   / Actuator / Evaluator run concurrently; feedback edges are
   bidirectional.
2. **Strategy is a rich aggregate; layers consume projections.**
   Downstream layers never import the `Strategy` class — they receive
   frozen projection value objects.
3. **Strategy version is immutable and tags every downstream record.**
   `(strategy_id, strategy_version_id)` appears on every decision /
   order / fill / eval / feedback row.
4. **Factor layer stores raw factors only.** Composite logic lives
   in strategy config, not in the factor registry.
5. **Sensor and Actuator are strategy-agnostic.** Controller and
   Evaluator are the only strategy-aware layers.
6. **Active perception: Controller-derived market ids feed back into
   Sensor subscription.** `MarketSelector` + `SensorSubscription-
   Controller` make this bidirectional.
7. **Sensor is two-layered.** `MarketDiscoverySensor` (unconditional
   universe scan) + `MarketDataSensor` (subscription-driven streaming).
8. **Onion-concentric storage.** Outer ring (market data, shared) /
   middle ring (factor panel, shared cache) / inner ring (strategy
   products, per-strategy).

---

## Promoted rules from retros

Eight rules promoted from `.harness/retro/` (see provenance in
`.harness/retro/index.md`). Full text in
`@agent_docs/promoted-rules.md`.

- 🔴 **Runtime behaviour > design intent** — argue from
  `file:line` evidence, never from intent.
- 🔴 **Review-loop rejection discipline** — rejections need a
  counter-trace, named call-site, or reproducible witness.
- 🟡 **Comments are not fixes** — behavioural findings need
  behavioural fixes + a new test.
- 🟡 **Lifecycle cleanup on all exit paths** — acquire/release in
  the same commit, via `try/finally`, all 4 exit paths covered.
- 🟡 **Piecewise-domain functions** — document break points; test
  each regime + straddles; derive from first principles.
- 🟡 **Verify isolated-env tooling assumptions** — `ls -la` the
  tool's actual output before wrapping it.
- 🟡 **Fresh-clone baseline verification** — run gates in a fresh
  shell before claiming baseline holds.
- 🟢 **Integration test default-skip pattern** — marker + env-var
  skipif so default `pytest` stays offline.

---

## Project conventions

- **Frozen dataclasses for all entities.** Every model under
  `src/pms/core/models.py` is `@dataclass(frozen=True)`; mutation
  goes through `dataclasses.replace`.
- **`float` at entity boundary, `Decimal` for calculation
  internals.** `MarketSignal`, `TradeDecision`, `OrderState`,
  `FillRecord`, `Portfolio`, `EvalRecord` store prices, sizes,
  money as `float`. Adapters and pure-math calculators convert to
  `Decimal(str(value))` before arithmetic, then back to `float` at
  the boundary. Do not invert this to "Decimal everywhere".
- **Protocol-first module boundaries.** Interfaces in
  `src/pms/core/interfaces.py`; concrete implementations under
  `src/pms/{sensor,controller,actuator,evaluation}/`. Runner and
  ControllerPipeline reference Protocol types only.
- **Test discovery.** `pyproject.toml` pins `pythonpath = ["src",
  "."]` for pytest and `mypy_path = "src"` for mypy. Keep both in
  sync.
- **No `Co-Authored-By` lines in commit messages.** Overrides any
  harness or template default (see promoted rules).

---

## Progressive disclosure

| Task | Read first |
|------|------------|
| Designing any new entity or module | `@agent_docs/architecture-invariants.md` |
| Starting a new harness sub-spec | `@agent_docs/project-roadmap.md` |
| Receiving code review / rejecting findings | `@agent_docs/promoted-rules.md` |
| Working in `src/pms/sensor/` | `@src/pms/sensor/CLAUDE.md` |
| Working in `src/pms/controller/` | `@src/pms/controller/CLAUDE.md` |
| Working in `src/pms/actuator/` | `@src/pms/actuator/CLAUDE.md` |
| Working in `src/pms/evaluation/` | `@src/pms/evaluation/CLAUDE.md` |
| Retro process | `.harness/retro/index.md` |

---

## Useful commands

```bash
# FastAPI backend (port 8000). PMS_AUTO_START=1 auto-starts the runner.
uv run pms-api

# Dashboard against live backend (port 3100).
cd dashboard && PMS_API_BASE_URL=http://127.0.0.1:8000 npm run dev

# Isolate dev DB state per shell.
DATABASE_URL=postgres://localhost/pms_dev_$(whoami) uv run pms-api

# Compose-backed PostgreSQL integration DB.
docker compose up -d postgres
export PMS_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_test

# Dashboard Playwright e2e.
cd dashboard && npx playwright test
```

---

## Do not

- Never commit directly to `main`.
- Never add `Co-Authored-By` lines.
- Never describe runtime as a phased pipeline (violates Invariant 1).
- Never import `pms.strategies.aggregate` from Sensor or Actuator
  modules (violates Invariant 5). Contract enforced by import-linter
  rules landed in S2.
- Never add `strategy_id` to outer-ring tables (`markets`, `tokens`,
  `book_*`, `price_changes`, `trades`) — violates Invariant 8.
- Never bypass a promoted rule without first opening a new retro
  that explains why.
