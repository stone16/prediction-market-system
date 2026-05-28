# Production Readiness — Path to Live (2026-05)

> **Owner:** Stometa
> **Created:** 2026-05-28
> **Status:** prep doc for the next work block. Bundles the outer prompt,
> a ≤4000-char `/goal` condition, the constraints, and the underlying
> state-of-the-system analysis. The `/goal` body got rejected at
> 7878 chars; section 2 below is the trimmed paste-ready replacement.

---

## 1. Outer prompt — context that does **not** need to fit the 4000-char limit

The end-state target for this repo is **supervised live canary on
Polymarket**: a tiny-notional real-money order with `operator_approval_mode=
every_order`, full venue + DB reconciliation, and post-live evidence
captured. Unsupervised production scaling is explicitly **not** on the
table — that comes later, only after a clean canary plus post-trade
reconciliation proof.

This work block stops well short of any real-money order. Credentials are
**not** a blocker for this block — they're explicitly deferred. The block
ends when:

1. The three code-level blockers identified below are fixed — each with a
   regression test that encodes the business invariant — and have landed
   on `main` via feature-branch PR(s).
2. The canonical gates pass on the post-merge `main` HEAD (not just PR
   head — promoted rule: fresh-clone baseline verification).
3. A real PAPER-soak artifact (`scripts/paper_report.py --require-go`)
   has been produced locally and inspected. **A clean GO outcome is NOT
   required.** The deliverable is the artifact plus a written gap list
   for the follow-up work block.

**Out of scope** (deferred to the next block): Polymarket credentials,
credentialed preflight against the real venue, Fly app split (paper-soak
app vs. live app), 30-day soak duration, operator/compliance rehearsal,
the tiny live canary itself, and post-live reconciliation.

The framing for the analyst that pasted the original review: the repo
already has *real* safety scaffolding (LIVE fail-closed, every-order
operator approval, risk envelope, venue reconciliation, quote guard,
audit artifacts). The production-readiness gap is **launch evidence**,
not safety architecture. This work block targets the small set of code
bugs that block a clean LIVE preflight + a trustworthy paper-soak
report, so that the *next* block can move on to credentials, Fly split,
and the canary order.

---

## 2. Goal condition — copy/paste into `/goal` (≤4000 chars)

Everything inside the fenced block below is one self-contained `/goal`
body, sized to fit the harness's 4000-char limit. Keep this section
literal so it stays paste-ready.

```
Bring prediction-market-system from "close to PAPER-soak ready" to "supervised live canary ready." Scope here is code fixes + local PAPER-soak evidence ONLY. Polymarket credentials, Fly live-app split, 30-day soak duration, operator rehearsal, tiny canary, and post-live reconciliation are OUT OF SCOPE — deferred to a follow-up work block.

Land THREE atomic feature-branch commits, each with a regression test that encodes the business invariant (not just the symptom):

[P0/P1] Fix LIVE preflight SQL join in src/pms/live_preflight.py.
  Current: LEFT JOIN markets ON markets.market_id = book_snapshots.market_id
  Problem: markets PK is condition_id, not market_id. book_snapshots.market_id is an FK pointing at markets(condition_id). PostgreSQL throws UndefinedColumn — blocks `pms-live preflight` and the LIVE startup gate.
  Fix: LEFT JOIN markets ON markets.condition_id = book_snapshots.market_id
  Regression: Postgres INTEGRATION test (NOT a mock) — apply migrated schema (alembic upgrade head), insert fresh two-sided book_snapshots + book_levels rows. Assert preflight PASS when markets.risk_group_id is non-null; assert FAIL when the markets row is missing OR risk_group_id is null.

[P1] Fix /metrics window filtering for quote_records.
  Bug: handler filters `records` by since/until but does NOT apply the same filter to `quote_records`. The full set is forwarded into _metrics_payload, which uses it to compute quote_calibration and quality. Result: windowed dashboards and PAPER-soak evidence are contaminated — the exact data we depend on for paper-soak GO.
  Fix: filter quote_records by the same since/until window.
  Regression: /metrics?since=...&until=... HTTP test with at least two quote_records (one inside, one outside window) — assert outside record does NOT influence calibration/quality.

[P1] Fix FillStore.read_positions() position-netting key.
  Bug: accumulator key includes risk_group_id. BUY/SELL hedging netting happens inside the same accumulator. If an old fill lacks risk_group_id and an exit fill has one (or risk_group_id mutates), the same contract splits into two positions. After restart, closed exposure can be "resurrected" or a live position split — a real-money reconciliation hazard.
  Fix: position identity = (market_id, token_id, venue, strategy_id, strategy_version_id). risk_group_id becomes metadata on the resulting position, NOT part of the netting key.
  Regression: unit test driving BUY → SELL fills across a risk_group_id change — assert a single net position whose metadata reflects the latest risk_group_id. Plus a backwards-compat test replaying a historical fill row under the new netting logic.

After the three commits land on main:
- Re-run gates on main HEAD (NOT just PR head):
    uv sync
    uv run pytest -q
    uv run mypy src/ tests/ --strict
    uv run lint-imports
    (cd dashboard && npm run build)   # mandatory — past prod-build failures slipped past Vitest/lint
    (cd dashboard && npm run test)
- Run Postgres integration suite for fix 1:
    PMS_RUN_INTEGRATION=1 PMS_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_test uv run pytest -m integration -q
- Run a local PAPER soak with the documented subscription seed (cold-start failure mode is known: thin 7-day-horizon universe + missing subscription seed).
- Generate `scripts/paper_report.py --require-go` artifact. A clean GO outcome is NOT required. Deliverable = the artifact + a written gap list for the follow-up block.

Hard rules (project CLAUDE.md):
- Feature branches only; never commit to `main`.
- No Co-Authored-By lines.
- No `--no-verify`; no `--amend` on pushed commits without explicit ask.
- mypy --strict must stay clean on every committed module.

Done = all gates green on post-fix main HEAD, three commits landed via PR(s), PAPER-soak artifact produced and inspected, written gap list for the next block.
```

---

## 3. Constraints / hard rules (project-level — apply to every commit here)

- **Feature branches only** — three fixes = three atomic commits.
  Default landing strategy = one PR with three atomic commits, unless
  the reviewer asks to split.
- **No `Co-Authored-By` lines** in commit messages (project convention,
  overrides any harness/template default).
- **mypy `--strict` clean** on every committed module (196 source files
  baseline as of 2026-04-21).
- **Tests must encode the business invariant**, not just the symptom
  (promoted rule). A test that still passes after the meaningful rule
  changes is shallow.
- The P0 SQL fix **must** have a Postgres integration regression — not
  a unit-level mock — because the bug only manifests against the real
  schema. Use the compose-backed `pms_test` DB pattern from
  `CLAUDE.md`.
- The position-netting refactor must be **backwards-compatible** with
  historical fill rows so paper-soak replay isn't broken.
- **Surgical changes** (promoted rule): touch only what the task
  requires. Don't bundle adjacent cleanup. Match existing style.
- **No premature refactors** (promoted rule): don't refactor anything
  beyond the three specified bugs without opening a separate
  conversation.

---

## 4. State of the system (summary of the upstream analysis)

| Phase | State | Notes |
| --- | --- | --- |
| Backend / PAPER soak | ~ready | Code can sustain continued paper soak. |
| Polymarket credentialed preflight | not done | PR #75 is explicit about this. |
| Tiny live canary order | NO-GO | Hits the P0 SQL blocker on real PostgreSQL. |
| Scaled / continuous production | NO-GO | Missing soak GO, rehearsal, post-live reconciliation. |

Already-correct safety design (do **not** re-litigate):

- `validate_live_mode_ready()`: requires `live_trading_enabled=true`,
  `mode=live`, full Polymarket credentials, valid signature type, valid
  funder address, approved secret source, local-secret-file permissions.
- LIVE rejects GTC (requires IOC/FOK); rejects
  `quote_source: postgres_snapshot`; requires `strict_factor_gates=true`;
  disables agent runtime; initial real-money phase requires
  `operator_approval_mode=every_order`.
- Runner LIVE startup: validate live mode → load + verify credentialed
  preflight artifact → check active strategy fingerprint → unresolved
  submission_unknown → market data freshness → DB portfolio
  reconciliation → venue account reconciliation → only then start
  sensors/controllers.
- Polymarket actuator pre-order: re-validate live mode, preflight
  artifact, strict operator gate, quote guard; per-order approval via
  sidecar provenance.
- `RiskManager`: min order, per-market cap, total exposure, risk-group
  cap, drawdown, max open positions, slippage, free cash, max quantity.
- Auto-halt: credential failure, drawdown, daily loss, 5-in-a-row
  losses, slippage spike, 429 rate limit, 30-min unfilled order.

CI signal: PR head was green at merge time, but the **merge commit
itself** on `main` has no recorded workflow run (the upstream analysis
got `workflow_runs: []` from its connector). Promoted rule:
fresh-clone baseline verification — re-run the full gate on the
current `main` HEAD before assuming the baseline holds.

---

## 5. Required code fixes (detail)

### 5.1 [P0/P1] LIVE preflight SQL join bug

- **File:** `src/pms/live_preflight.py`
- **Symptom:** `pms-live preflight` and the LIVE startup gate throw
  `UndefinedColumn` on a real PostgreSQL database.
- **Root cause:** the risk-group metadata-freshness check uses

  ```sql
  LEFT JOIN markets ON markets.market_id = book_snapshots.market_id
  ```

  but `markets` PK is `condition_id`. `book_snapshots.market_id` is the
  FK pointing at `markets(condition_id)`. There is no
  `markets.market_id` column.

- **Fix:**

  ```sql
  LEFT JOIN markets ON markets.condition_id = book_snapshots.market_id
  ```

- **Regression (must be Postgres integration, not a mock):**
  1. apply the migrated schema (`alembic upgrade head`)
  2. insert fresh two-sided `book_snapshots` + `book_levels` rows
  3. assert preflight **PASS** when `markets.risk_group_id` is non-null
  4. assert preflight **FAIL** when the markets row is missing OR
     `risk_group_id` is null

  Mocks won't catch this — that's exactly why the bug exists in the
  first place. Use the compose pattern from `CLAUDE.md`.

### 5.2 [P1] /metrics window filtering for quote records

- **File:** the `/metrics` handler (confirm location at fix time —
  likely `src/pms/api/`).
- **Symptom:** dashboards rendering windowed views show contaminated
  `quote_calibration` and `quality` figures that include quote records
  outside the requested window. This pollutes the windowed evidence we
  depend on for PAPER-soak GO.
- **Root cause:** the handler filters `records` by `since/until` but
  does **not** apply the same filter to `quote_records`; the full set
  is forwarded into `_metrics_payload`, which uses these
  `quote_records` to compute `quote_calibration` and `quality`.
- **Fix:** filter `quote_records` by the same `since/until` window
  before passing to `_metrics_payload`.
- **Regression:** `/metrics?since=...&until=...` HTTP test with at
  least two `quote_records` — one inside the window and one outside.
  Assert the outside record does **not** influence the resulting
  `quote_calibration` / `quality`.

### 5.3 [P1] FillStore.read_positions() position-netting key

- **File:** `FillStore.read_positions()` (confirm location at fix time).
- **Symptom:** after a restart with mixed-metadata fill history, a
  single contract can show up as two separate positions — "resurrected"
  closed exposure, or a live position split. Real-money reconciliation
  hazard.
- **Root cause:** the accumulator key includes `risk_group_id`.
  BUY/SELL hedging netting happens within the same accumulator. Any
  mutation of `risk_group_id` over time, or a missing value on a
  historical fill row, breaks netting.
- **Fix:** position identity is

  ```
  (market_id, token_id, venue, strategy_id, strategy_version_id)
  ```

  `risk_group_id` becomes **metadata** attached to the resulting
  position (latest-fill wins, or whatever rule the reviewer agrees on
  — flag during review), **not** part of the netting key.
- **Regression:**
  1. Unit test driving a BUY → SELL fill pair across a `risk_group_id`
     change; assert a single net position whose metadata reflects the
     latest `risk_group_id`.
  2. Backwards-compat test replaying a historical fill row with the
     old key shape under the new netting logic — assert no split, no
     resurrection.

---

## 6. Verification (post-fix `main` HEAD)

Gate suite — all must be green on the merged `main` HEAD (not just PR
head):

```bash
uv sync
uv run pytest -q
uv run mypy src/ tests/ --strict
uv run lint-imports

(cd dashboard && npm run build)        # mandatory — past prod-build failures slipped past Vitest/lint
(cd dashboard && npm run test)
```

Postgres integration regression for fix 5.1:

```bash
docker compose up -d postgres
export PMS_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_test
PMS_RUN_INTEGRATION=1 uv run pytest -m integration -q
```

PAPER-soak evidence:

```bash
uv run python scripts/paper_report.py --require-go > paper_soak_2026-05-28.txt
```

Inspect the artifact for: provenance, gates, P&L, drawdown, Brier
improvement, Sharpe, risk events, calibration. Write a short gap list
naming what's missing for a **launch-grade** GO artifact (30-day
duration, real credential preflight, category prior baseline, FLB
calibration CSV, execution model telemetry, paper-vs-backtest
execution diff).

---

## 7. Deferred to the next work block

Listed here so they don't fall on the floor — **not** part of the
current scope.

- **Credentialed preflight against the real venue:**

  ```bash
  PMS_RUN_INTEGRATION=1 \
  PMS_RUN_LIVE_PREFLIGHT=1 \
  PMS_LIVE_PREFLIGHT_CONFIG=config.live.yaml \
  uv run pytest -q tests/integration/test_live_credentialed_preflight.py
  ```

  i.e. `run_live_preflight(..., skip_venue=False)` end-to-end with all
  `final_required_checks` passing: `live_config`,
  `runtime_dependencies`, `operator_approval`, `emergency_audit`,
  `first_order_audit`, `database_connection`, `schema_current`,
  `market_data_freshness`, `submission_unknown`, `live_open_orders`,
  `active_strategies`, `venue_reconciliation`. No `--skip-venue`, no
  DB URL override, must have an active strategy fingerprint.

- **Fly app split.** Current `fly.toml` targets `pms-paper-soak` with
  `config.live-soak.yaml` (paper mode, no credentials). Live deployment
  needs a separate app name, private volume/paths, secret manager, API
  token, Discord alerting, DB migration, health/readiness, and
  `config.live.yaml` filled in (paper-soak report, operator rehearsal
  report, execution model artifact, paper-vs-backtest diff,
  credentialed preflight artifact, every-order approval path,
  category prior file, FLB calibration file).

- **30-day PAPER soak** producing a launch-grade GO artifact (the
  current block only validates the artifact *shape* with a local run).

- **Operator / compliance rehearsal** and signoff.

- **Tiny live canary** with `operator_approval_mode=every_order`,
  post-trade venue + DB reconciliation, first-order audit + emergency
  audit + post-live reconciliation artifacts archived.

---

## 8. Open questions for the next session

- Single PR or three separate PRs for the three fixes? Default = one
  PR with three atomic commits, unless the reviewer asks to split.
- Position-netting `risk_group_id` metadata-merge semantics:
  latest-fill wins? first-fill wins? mark conflicting? Needs a
  one-line decision during code review.
- Patch the known PAPER-soak cold-start failure mode (thin 7-day-
  horizon universe + missing subscription seed) inside this block, or
  document and defer? Recommendation: document and defer unless it
  blocks the local soak run.
- The upstream analysis cites the bugs by description but not file
  path/line for fixes 5.2 and 5.3. At fix-start time, confirm exact
  locations with `rg`/`grep` before editing — don't trust the
  paraphrase.

---

## Provenance

This doc consolidates an external production-readiness review pasted
into the `/goal` command on 2026-05-28; the original was 7878 chars
and got rejected by the 4000-char limit. Section 2 above is the
trimmed paste-ready replacement. The full review identified three
code-level bugs (a P0/P1 SQL join, a P1 windowed-metrics filter, and
a P1 position-netting key) plus a list of launch-evidence gaps that
are explicitly deferred to a follow-up block.
