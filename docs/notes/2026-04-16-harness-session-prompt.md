# 2026-04-16 Harness Session Kickoff Prompt

Paste the content of the fenced block below into a fresh Claude Code
session running in this repo. The prompt is self-contained — the new
session does not need any prior conversation context.

Scope of the task the new session will run: **Sensor real-data upgrade
+ PostgreSQL storage unification**. All major design decisions are
already resolved; the new session's job is to turn the discovery notes
into a Harness spec, run the spec evaluation loop, then implement.

---

```
# Task: PMS Sensor + PostgreSQL Storage — Run the Harness Flow

You are picking up work on the `prediction-market-system` repo. A complete
discovery session was run on 2026-04-16 with the user. All major design
decisions are recorded in the docs below. Your job is to run the full
Harness flow: requirements → spec → spec evaluation → checkpoint
execution → E2E verification → review loop → full verification → PR → retro.

## Read these files FIRST, in this order

Do not start planning before you have read all of them.

1. `CLAUDE.md` — active engineering rules for this repo (promoted from prior
   retros). Load-bearing gates: `uv run pytest -q` (≥70 passing, 2 skipped)
   and `uv run mypy src/ tests/ --strict`.
2. `docs/notes/2026-04-16-repo-issues-controller-evaluator.md` — full design
   for the Sensor upgrade (Route B / PolymarketStreamSensor) and PostgreSQL
   storage migration. Every major decision is marked RESOLVED with rationale.
   The "Summary: Decisions Captured On 2026-04-16" section at the bottom is
   the canonical decision list.
3. `docs/notes/2026-04-16-evaluator-entity-abstraction.md` — companion note on
   Evaluator/entity direction (Factor, Strategy, Opportunity, BacktestRun).
   **Out of scope for this task**, but read it to avoid making schema choices
   that foreclose that future work.
4. `docs/continuation-guide.md` — the Phase 3D retro → rule promotion process.
5. `.harness/retro/index.md` — index of prior retros and promoted rules.
6. `.harness/pms-v2/` — browse the structure (spec.md, checkpoints/, e2e/,
   full-verify/) as a template for what you will produce.
7. `README.md` — product-level overview; useful for the PR description later.

## What is already decided (do NOT re-litigate)

From the discovery note's summary section. If you find yourself arguing for
a different choice, stop and ask the user — don't silently invert a decision:

- **Sensor**: upgrade `PolymarketStreamSensor` to parse real `book` +
  `price_change` events from `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
  Wire it into `runner._build_sensors()` for paper + live modes. REST
  `GET /book` stays as a fallback, not the primary path.
- **Database**: PostgreSQL in all environments (local dev, CI, production).
  No SQLite. No `IMarketDataStore` Protocol for portability — a single
  concrete `PostgresMarketDataStore` is sufficient.
- **Driver**: `asyncpg`. Single pool owned by Runner, min_size=2, max_size=10.
  One INSERT per event. Bulk `COPY` deferred until measured as a bottleneck.
- **Schema shape**: both `book` snapshots and `price_change` deltas are
  stored. Snapshots expanded into `book_levels` rows (no JSON blobs). Prices
  as `DOUBLE PRECISION`, times as `TIMESTAMPTZ`, enums as `CHECK` constraints.
  Draft DDL is in the discovery note under "Q2: How to store bids/asks arrays".
- **Storage unification**: `feedback.jsonl` and `eval_records.jsonl` both
  migrate to PostgreSQL tables. `FeedbackStore` and `EvalStore` become thin
  SQL wrappers. No JSONL in the runtime contract. Dev state isolation moves
  to per-shell PG databases.
- **Tests**: transaction-rollback fixture against a shared test DB. No
  `pytest-postgresql` dependency. Cross-connection integration tests fall
  back to per-test `TRUNCATE`.

## What is still open — you must answer these in the spec

1. **Q4 — retention policy** for `price_changes` (keep forever, rolling
   window, tiered).
2. **`price_changes` UNIQUE constraint** on `(market_id, ts, price, side)`
   for idempotent replay — yes or no?
3. **`sensor_sessions` lifecycle table** (connected_at / disconnected_at /
   subscribe_reason) — include it or defer?
4. **`MarketSignal.orderbook` backward compatibility** — keep the in-memory
   dict shape populated from latest book state, or change the type?
5. **Task id** for `.harness/<task-id>/`. Existing ids: pms-v1, pms-v2,
   pms-phase2, pms-known-open-questions. Propose a new id (e.g.
   `pms-ingest-v1` or `pms-sensor-pg-v1`) and confirm with the user.
6. **Reconnection strategy** for WebSocket drops — exponential backoff is
   already there, but snapshot re-request on reconnect is not. Spec must
   specify the reconciliation flow when a gap is detected.
7. **Heartbeat**: add 10-second PING to `PolymarketStreamSensor._iterate()`
   per the Polymarket protocol docs.

## Suggested flow

Use the superpowers skills that apply:

1. **`superpowers:writing-plans`** to draft `.harness/<task-id>/spec.md`.
   Include checkpoints roughly in this order (you may refine):
   - CP1: Docker Compose + schema.sql + docs for local PG dev setup.
   - CP2: `asyncpg` pool wired in Runner (no-op writes, pool lifecycle only).
   - CP3: `PostgresMarketDataStore` — write methods for book_snapshots,
     book_levels, price_changes, trades, markets, tokens.
   - CP4: `PostgresFeedbackStore` + `PostgresEvalStore` — one-shot swap of
     the two existing stores, preserving public API.
   - CP5: `PolymarketStreamSensor` — parse `book` and `price_change`,
     maintain local book mirror, add PING heartbeat.
   - CP6: Wire `PolymarketStreamSensor` into `_build_sensors()` for paper
     + live modes; populate `MarketSignal.orderbook` from latest book state
     for backward compat.
   - CP7: Dashboard API + frontend — expose real orderbook depth on /signals
     page (or wherever the user wants).
   - CP8: Docs + retro seeds.
2. **Spec evaluation**: use `sto:harness` or `superpowers:writing-plans`
   self-review. Revise until the user approves.
3. **TDD execution**: `superpowers:test-driven-development` for each
   checkpoint. Tests FIRST, implementation after. The transaction-rollback
   fixture pattern (from discovery note) is mandatory for Store tests.
4. **Verification before completion**: `superpowers:verification-before-completion`
   before claiming any checkpoint done. Run full gate after each CP:
   `uv run pytest -q && uv run mypy src/ tests/ --strict`.
5. **Review loop + full verify + PR + retro** per the standard flow.

## Non-negotiable constraints

- **No `Co-Authored-By`** lines in commit messages (user's global rule).
- **Feature branch only**, never commit to main.
- **Runtime behaviour > design intent** (CLAUDE.md §Active rules). When
  reviewing any finding, cite file:line runtime traces, not intent.
- **Comments are not fixes** — if a finding describes incorrect behaviour,
  fix the code, don't add a comment.
- **Lifecycle cleanup on all exit paths** — any `acquire()` / `connect()` /
  `register()` must have matching cleanup in the same commit, wired via
  `try/finally` or a context manager.
- **Fresh-clone baseline verification**: run `uv run pytest -q` and
  `uv run mypy src/ tests/ --strict` before you touch anything. If the
  baseline is broken, your first commit fixes the baseline with a
  `fix(tests):` or `fix(build):` prefix.
- **float at entity boundary, Decimal for calc internals** (CLAUDE.md) —
  PG stores `DOUBLE PRECISION`; dataclasses read back as `float`.
- **Start the backend + dashboard and actually look at it** before calling
  the task done. Type checking ≠ feature correctness.

## Out of scope (defer to later tasks)

- Evaluator entity abstraction (Factor, Strategy, Opportunity, BacktestRun,
  StrategyRun, EvaluationReport) — tracked in the companion note.
- Kalshi adapter — Polymarket only.
- Controller strategy framework / registry.
- Live trading credential flow.
- Retention-policy automation (cover the decision in the spec, but the
  background-job implementation is deferred unless the user requests it).

## First actions

1. Read the 7 files listed above. Do not skim — all of them.
2. Propose a task id and a checkpoint list.
3. Ask the user to approve the scope before writing spec.md.
4. Once approved, write `.harness/<task-id>/spec.md`, then run spec
   evaluation and wait for user approval before starting CP1.
```

---

## Notes on using this prompt

- Paste exactly the fenced block into the new session. The `#` lines inside
  the block are part of the prompt content, not Markdown headers for the
  surrounding note.
- If the new session proposes a different decision from the "already
  decided" list without citing a reason grounded in the discovery note's
  runtime rationale, reject it. The prompt explicitly tells the session
  not to silently invert decisions.
- If the session asks clarifying questions about the 7 open items, answer
  them as a user normally would. Those are real decisions that belong in
  the spec, not in this prompt.
- If the session starts code generation before the spec is approved, stop
  it. The Harness flow requires spec first.
