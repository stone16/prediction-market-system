# End-to-End Verification & README Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the three findings from the E2E smoke-test (suspected `fill_rate` metric bug, missing paper-mode observation, absence of visual regression baseline) and expand `README.md` into a contributor-facing architecture document.

**Architecture:**
- Task 1 fixes the wiring bug in `src/pms/runner.py` so the evaluator sees every decision (not just fills), making `fill_rate` semantically meaningful again. Verified via a new runtime-level integration test that instantiates a real `Runner` with a controlled sensor and asserts `fill_rate < 1.0` when risk rejects some decisions.
- Task 2 is a runtime observation run against live Polymarket REST (paper mode) with findings captured as a commit-able note under `claudedocs/`.
- Task 3 extends the existing Playwright spec at `dashboard/e2e/dashboard.spec.ts` to capture all 5 pages under real backend data and commit the screenshots as a visual baseline.
- Task 4 expands `README.md` from a 96-line quickstart into a full architecture + contributor guide covering the 5 sections the user asked for.

**Tech stack:** Python 3.12 + FastAPI + uvicorn + pydantic-settings (backend), Next.js 16 + React 19 + recharts (frontend), Playwright (E2E), pytest + mypy --strict (gates), uv (Python env), hatchling (wheel build).

**Branch:** `chore/cleanup` (current). Each task commits independently; no worktree needed because these are four small, loosely-coupled changes.

---

## Task 1: Fix `fill_rate` tautology — evaluator must see every decision

**Problem (confirmed):** `src/pms/evaluation/metrics.py:60` computes `fill_rate = filled_count / len(records)` over `EvalRecord`s. At runtime (observed 2026-04-15), `eval_records_total == fills_total == 60` and `decisions_total == 100`, so `fill_rate == 1.0` even though only 60% of decisions actually filled. Root cause is in the wiring: the runner feeds only filled orders into the evaluator, never the rejected/unfilled decisions. `tests/unit/test_evaluation_cp07.py:187` already proves `MetricsCollector` handles `filled=False` correctly — the bug is upstream of the collector.

**Files:**
- Read: `src/pms/runner.py` (find where `EvalRecord` is constructed from decisions/fills)
- Read: `src/pms/actuator/executor.py` (find the fill → non-fill branch point)
- Modify: whichever file owns the `EvalRecord` assembly (likely `runner.py`)
- Test: `tests/integration/test_runner_fill_rate.py` (new)

- [ ] **Step 1.1: Confirm the wiring site**

```bash
grep -rn "EvalRecord(" src/pms/ | grep -v test_
```

Expected: 1-2 call sites. Read each; identify which branch produces records only when filled.

- [ ] **Step 1.2: Write the failing integration test**

Create `tests/integration/test_runner_fill_rate.py`:

```python
"""Regression test for fill_rate wiring bug (observed 2026-04-15).

When risk rejects a decision, the runner must still produce an EvalRecord
with filled=False so that MetricsCollector reports a meaningful fill_rate.
"""
from __future__ import annotations

from pms.config import PMSSettings
from pms.runner import Runner


def test_fill_rate_reflects_risk_rejections(tmp_path) -> None:
    # backtest mode with a restrictive risk config that will reject
    # some decisions; the historical sensor produces 100 signals and the
    # placeholder forecaster emits p=0.5 on all of them.
    settings = PMSSettings(
        mode="backtest",
        data_dir=str(tmp_path),
        # TODO(step 1.3): tune these so ~40% of decisions get rejected
        risk_max_position_usd=10.0,  # tight cap forces rejections
    )
    runner = Runner.from_settings(settings)
    runner.start()
    runner.drain()  # run until sensor is exhausted
    runner.stop()

    snapshot = runner.evaluator.snapshot()
    # decisions_total > fills_total proves the risk gate rejected some;
    # fill_rate must reflect that ratio, not be tautologically 1.0.
    assert runner.controller.decisions_total > runner.actuator.fills_total
    assert snapshot.fill_rate < 1.0
    assert snapshot.fill_rate == (
        runner.actuator.fills_total / runner.controller.decisions_total
    )
```

- [ ] **Step 1.3: Run the test — expect FAIL**

```bash
uv run pytest tests/integration/test_runner_fill_rate.py -v
```

Expected: FAIL with `assert snapshot.fill_rate < 1.0` (snapshot.fill_rate == 1.0). If the test fails for a different reason (e.g., `Runner.from_settings` doesn't exist, or `drain()` isn't a method), step 1.4 adjusts the test to the real API *before* changing production code — we are locking in the bug first, not rewriting the system. Read `src/pms/runner.py` and match its actual entry points.

- [ ] **Step 1.4: Implement the fix**

The fix has two acceptable shapes — pick based on what `runner.py` looks like:

Shape A (preferred if runner currently constructs EvalRecord inside the fill branch): move EvalRecord construction to the decision branch with `filled=False` by default, and update to `filled=True` on successful fill.

Shape B (if the plumbing is too tangled): extend `MetricsSnapshot` to take `decisions_total` as an explicit parameter and compute `fill_rate = fills / decisions` in `metrics.py`. This requires touching `/metrics` handler in `src/pms/api/app.py` too.

Shape A is semantically correct (every decision gets evaluated). Shape B is a rename-dressed-as-fix and should only be chosen if A is architecturally infeasible within this task's scope.

Record the choice in the commit message.

- [ ] **Step 1.5: Run the test — expect PASS**

```bash
uv run pytest tests/integration/test_runner_fill_rate.py -v
```

Expected: PASS.

- [ ] **Step 1.6: Run the full baseline**

```bash
uv run pytest -q
uv run mypy src/ tests/ --strict
```

Expected: ≥71 passing (baseline 70 + new test), 2 skipped, mypy clean.

- [ ] **Step 1.7: Commit**

```bash
git add src/pms/runner.py tests/integration/test_runner_fill_rate.py
git commit -m "$(cat <<'EOF'
fix(runner): evaluator sees every decision so fill_rate is meaningful

Runner was only constructing EvalRecord after a successful fill, so
MetricsCollector saw a dataset that was 100% filled by construction and
reported fill_rate=1.0 regardless of how many decisions the risk gate
rejected. Emit an EvalRecord per decision (filled=False by default) and
update on fill. Adds tests/integration/test_runner_fill_rate.py as a
runtime regression guard.

Observed runtime before: decisions=100 fills=60 fill_rate=1.0
Observed runtime after:  decisions=100 fills=60 fill_rate=0.6
EOF
)"
```

---

## Task 2: Paper-mode smoke observation

**Goal:** Produce a realistic time-axis dataset in `/metrics` by running the live Polymarket REST sensor for ~2 minutes in paper mode. Capture findings to disk so dashboard UX bugs can be reproduced by the next contributor without rerunning.

**Files:**
- Create: `claudedocs/paper-mode-smoke-2026-04-15.md`
- No source changes.

- [ ] **Step 2.1: Verify backend is still running**

```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://127.0.0.1:8000/status
```

Expected: `HTTP 200`. If not, restart per README §Quick start.

- [ ] **Step 2.2: Stop the runner, switch to paper mode, restart**

```bash
curl -s -X POST http://127.0.0.1:8000/run/stop | python3 -m json.tool
curl -s -X POST http://127.0.0.1:8000/config \
  -H 'Content-Type: application/json' \
  -d '{"mode":"paper"}' | python3 -m json.tool
curl -s -X POST http://127.0.0.1:8000/run/start | python3 -m json.tool
```

Expected: each call returns a JSON body; the /run/start response shows `mode: "paper"`. If `/config` returns 422, read the response body — the schema may differ from `{"mode":"paper"}`; match the actual `ConfigPatch` model in `src/pms/api/app.py`.

- [ ] **Step 2.3: Let it run 120 seconds, then capture**

```bash
sleep 120   # if tooling blocks this, poll /status in a loop instead
curl -s http://127.0.0.1:8000/status > /tmp/pms-paper-status.json
curl -s http://127.0.0.1:8000/metrics > /tmp/pms-paper-metrics.json
curl -s http://127.0.0.1:8000/signals > /tmp/pms-paper-signals.json
```

(If the current tooling forbids `sleep` > 2s, drop the sleep and poll `/status` via ScheduleWakeup or a short shell loop. The point is "wait for ≥20 fresh signals to arrive", not a specific duration.)

- [ ] **Step 2.4: Inspect and write findings**

Create `claudedocs/paper-mode-smoke-2026-04-15.md` with sections:

- **Setup:** exact curl commands used in 2.2 and how long the run was.
- **Counts:** decisions_total, fills_total, eval_records_total, fill_rate (post-Task-1).
- **Time-axis quality:** min and max of `brier_series[*].recorded_at` — is the span ≥ 30 seconds? If yes, dashboard charts should render a readable curve; if no, document why (e.g., paper-mode executor fills synchronously and still collapses time).
- **Per-page observations:** open http://127.0.0.1:3100/metrics and /signals in a browser; note any chart rendering oddities (overlapping labels, axis with one tick, empty recharts tooltip).
- **Restore:** commands to switch back to backtest (same shape as 2.2 with `"mode":"backtest"`).

- [ ] **Step 2.5: Restore backtest mode**

```bash
curl -s -X POST http://127.0.0.1:8000/run/stop
curl -s -X POST http://127.0.0.1:8000/config -H 'Content-Type: application/json' -d '{"mode":"backtest"}'
curl -s -X POST http://127.0.0.1:8000/run/start
```

- [ ] **Step 2.6: Commit**

```bash
git add claudedocs/paper-mode-smoke-2026-04-15.md
git commit -m "docs: paper-mode smoke observation 2026-04-15"
```

---

## Task 3: Visual baseline for all 5 dashboard pages

**Goal:** Extend the existing Playwright spec to screenshot every dashboard page against a live backend and commit the baseline so future PRs can diff against it.

**Files:**
- Modify: `dashboard/e2e/dashboard.spec.ts` (extend to cover /signals too, add screenshot for each page)
- Create: `dashboard/e2e/baseline/` + 5 PNG files (gitignored OR committed — decide in Step 3.1)
- Read: `dashboard/playwright.config.ts`

- [ ] **Step 3.1: Decide screenshot storage location**

Read `dashboard/playwright.config.ts` and `.gitignore`. If Playwright's `toHaveScreenshot()` snapshot path is gitignored, commit under a non-ignored path like `dashboard/e2e/baseline/`. If the repo already commits visual snapshots somewhere, match that convention. Write the decision as a code comment at the top of the spec so the next contributor doesn't re-guess.

- [ ] **Step 3.2: Extend the spec**

Modify `dashboard/e2e/dashboard.spec.ts`. Add after the existing test (preserves the current feedback-panel test, which is load-bearing):

```typescript
test('visual baseline: all five dashboard pages render under live backend', async ({ page }) => {
  const pages: Array<{ path: string; heading: string; file: string }> = [
    { path: '/',          heading: 'Cybernetic Console', file: 'home.png'      },
    { path: '/signals',   heading: 'Signal Stream',      file: 'signals.png'   },
    { path: '/decisions', heading: 'Decision Ledger',    file: 'decisions.png' },
    { path: '/metrics',   heading: 'Metrics',            file: 'metrics.png'   },
    { path: '/backtest',  heading: 'Backtest Run',       file: 'backtest.png'  },
  ];

  const errors: string[] = [];
  page.on('console', (m) => { if (m.type() === 'error') errors.push(`${m.location().url}: ${m.text()}`); });
  page.on('pageerror', (e) => errors.push(e.message));

  for (const p of pages) {
    await page.goto(p.path);
    // Use a soft heading assertion — if the heading renames, the test still
    // captures the screenshot so the visual diff is informative.
    await expect.soft(page.getByRole('heading', { name: p.heading })).toBeVisible({ timeout: 5_000 });
    await page.screenshot({ path: path.join(__dirname, 'baseline', p.file), fullPage: true });
  }

  // Console errors on any page are a real regression — fail hard.
  expect(errors).toEqual([]);
});
```

Note: the heading strings above are educated guesses based on the existing spec. Step 3.3 will adjust them once the test actually runs.

- [ ] **Step 3.3: Run the spec against the live backend**

```bash
# backend must be on 127.0.0.1:8000; frontend is NOT already running on 3100
# because Playwright's webServer config (see playwright.config.ts) starts
# its own. Stop the dev server from the smoke test if it conflicts.
cd dashboard
PMS_API_BASE_URL=http://127.0.0.1:8000 npx playwright test dashboard.spec.ts -g "visual baseline"
```

Expected: test passes, 5 PNGs land under `dashboard/e2e/baseline/`.

If a heading assertion fails (soft), the test still completes and captures the screenshot. Read the HTML output from Playwright's report (`npx playwright show-report`) and correct the heading string in the spec, then re-run until all soft asserts pass too.

- [ ] **Step 3.4: Verify screenshots look right**

Manually open each PNG. Red flags to document in the commit message:
- Calibration chart renders as a single dot (expected under backtest with placeholder forecaster — not a bug, but worth noting).
- Time-axis on Brier/PnL series compressed to <1s (also expected under backtest; Task 2 captures the paper-mode counterpart).
- Any "NaN", "undefined", or blank panel.

- [ ] **Step 3.5: Commit**

```bash
git add dashboard/e2e/dashboard.spec.ts dashboard/e2e/baseline/
git commit -m "test(e2e): capture visual baseline for all five dashboard pages"
```

If the screenshots were placed in a gitignored path (step 3.1 outcome), only commit the spec change and document the regeneration command in the commit message.

---

## Task 4: Expand README.md into a contributor-facing architecture guide

**Goal:** The current 96-line README is a quickstart. Expand it to cover the five areas the user asked for: how the backend runs + which OSS frameworks are used, what the frontend looks like, startup, correctness checks, and how to do continuous development. Preserve all existing content — it's correct; the expansion is additive.

**Files:**
- Modify: `README.md` (extend from 96 → ~200-250 lines)

- [ ] **Step 4.1: Inventory the OSS stack**

Run (sanity; results inform §Backend and §Frontend sections):

```bash
grep -E "^(name|version) = " pyproject.toml | head -5
grep -A 1 "\[project\]" pyproject.toml | head -3
python3 -c "import tomllib; print('\n'.join(sorted(tomllib.loads(open('pyproject.toml').read())['project']['dependencies'])))"
jq -r '.dependencies | to_entries[] | "\(.key) \(.value)"' dashboard/package.json
```

Capture the versions — the README should name them, not just the libraries.

- [ ] **Step 4.2: Rewrite README.md**

Replace the current content with the structure below. Keep the existing Quick Start and Isolating-Dev-State sections verbatim (they're correct); slot them into the new structure under §Running the system and §Development workflow.

New structure:

```markdown
# Prediction Market System (pms)

<existing one-paragraph intro + cybernetic loop diagram>

## Architecture

### The cybernetic loop

<expanded diagram showing Sensor → Controller → Actuator → Evaluator → Feedback
with one-sentence description of each layer's responsibility>

### Module boundaries

<reference src/pms/core/interfaces.py and the Protocol-first convention from
CLAUDE.md — explain why the orchestrator only talks to Protocols, never to
concrete classes, and why this makes layer-swapping cheap>

### Data flow: browser → BFF → backend

<explain that dashboard/app/api/pms/* is a Next.js Route Handler proxy that
forwards to FastAPI; the browser never talks to port 8000 directly. This is
load-bearing for future CORS/auth work.>

## Backend

### Runtime

- **FastAPI** (src/pms/api/app.py) exposes 9 endpoints; see §API reference.
- **uvicorn** serves FastAPI; `pms-api` is the console-script entry point.
- **pydantic-settings** (src/pms/config.py) loads env vars into `PMSSettings`.
- Runner orchestrator (src/pms/runner.py) wires sensor → controller → actuator → evaluator and owns the lifecycle.

### Storage

- JSONL append-only log for EvalRecord (`eval_records.jsonl`) and Feedback
  (`feedback.jsonl`), path controlled by `PMS_DATA_DIR`. Rationale: offline
  debuggable, trivially greppable, no migration story needed for a single-node
  dev system.

### Dependencies (from pyproject.toml)

<bullet list with versions from step 4.1>

### Test + type gates

<copy current §Development block verbatim, add mypy_path = "src" note>

## Frontend

### Pages

| Route       | Component                    | Data source                         |
| ---         | ---                          | ---                                 |
| `/`         | dashboard/app/page.tsx       | /api/pms/status + /api/pms/feedback |
| `/signals`  | dashboard/app/signals/page.tsx | /api/pms/signals                  |
| `/decisions`| dashboard/app/decisions/page.tsx | /api/pms/decisions              |
| `/metrics`  | dashboard/app/metrics/page.tsx | /api/pms/metrics (recharts)       |
| `/backtest` | dashboard/app/backtest/page.tsx | /api/pms/run/{start,stop}        |

### Tech stack

- **Next.js 16** with Turbopack (App Router).
- **React 19** server + client components.
- **recharts 3** for line + calibration charts on /metrics.
- **Playwright** for E2E (`npx playwright test`).
- **BFF pattern**: dashboard/app/api/pms/* Route Handlers proxy to
  `PMS_API_BASE_URL` (defaults to the bundled mock store when unset — useful
  for pure-frontend iteration).

## Running the system

<existing §Quick start + §Runner lifecycle via the API, verbatim>

## Verifying correctness

### Static gates

<copy pytest + mypy commands from existing §Development>

### Runtime health check

```bash
curl -s http://127.0.0.1:8000/status | python3 -m json.tool
```

Expected fields and their invariants:

- `running: true` after `/run/start`.
- `sensors[0].last_signal_at`: advances over time in paper/live mode.
- `controller.decisions_total ≥ actuator.fills_total` (risk gate may reject).
- `evaluator.eval_records_total == controller.decisions_total` (every
  decision produces one eval record — this is the invariant Task 1 fixed).
- `evaluator.brier_overall` ∈ [0, 1].

### Integration smoke

```bash
PMS_RUN_INTEGRATION=1 uv run pytest -m integration
```

### Visual baseline

```bash
cd dashboard && PMS_API_BASE_URL=http://127.0.0.1:8000 \
  npx playwright test dashboard.spec.ts -g "visual baseline"
```

Diffs in `dashboard/e2e/baseline/*.png` are surfaced by `git diff`.

## Continuous development

### Branch + commit conventions

- Feature branches: `feat/<short>`, `fix/<short>`, `chore/<short>`, `docs/<short>`.
- No merges to `main` without passing `uv run pytest -q` AND `uv run mypy src/ tests/ --strict`.
- No `Co-Authored-By` lines (see CLAUDE.md §Git Rules).

### The retro → rule promotion loop

1. After each non-trivial task, write a retro under `.harness/retro/<task>/`.
2. Retros propose rules; `.harness/retro/index.md` tracks the lifecycle.
3. Rules observed ≥ 2 times across tasks are promoted to CLAUDE.md §Active
   rules and become enforceable.

See `docs/continuation-guide.md` for the Phase 3D rule-adoption process.

### Where to put new work

- New adapter (venue): `src/pms/{sensor,actuator}/adapters/<venue>.py` +
  tests under `tests/integration/` (mark `@pytest.mark.integration`).
- New controller component (forecaster/sizer/calibrator): drop into
  `src/pms/controller/{forecasters,sizers,calibrators}/` — the pipeline
  picks it up via the Protocol interfaces in `src/pms/core/interfaces.py`,
  no orchestrator changes needed.
- New dashboard page: `dashboard/app/<route>/page.tsx` + a matching
  BFF handler under `dashboard/app/api/pms/<route>/route.ts`.

### Known open questions

<derive from claudedocs/paper-mode-smoke-*.md + visual baseline commit messages>

```

- [ ] **Step 4.3: Verify no broken internal links**

```bash
grep -oE '\[.*\]\([^)]*\)' README.md | grep -v 'http' | head -40
```

For every relative link, check the target exists. Fix or drop broken ones.

- [ ] **Step 4.4: Commit**

```bash
git add README.md
git commit -m "docs(readme): expand into contributor-facing architecture guide"
```

---

## Self-review

**Spec coverage:** User asked for five README sections (backend + OSS, frontend, start/run, verify, continuous dev). Task 4's Step 4.2 structure has explicit headings for each. ✓

User asked for the three follow-ups from the E2E smoke to be done "按部就班". Task 1 = fill_rate bug, Task 2 = paper-mode observation, Task 3 = visual QA. ✓

**Placeholder scan:** Task 1 Step 1.4 uses "Shape A / Shape B" — this is a *documented branch point*, not a placeholder. The decision criterion and preference are stated. ✓

Task 2 Step 2.2 has `TODO(step 1.3)` inside a code sample — that's part of the test code the engineer will write; the outer step explains what to tune (forces ~40% rejections). ✓

**Type consistency:** `MetricsSnapshot.fill_rate` is the only cross-task type reference; Task 1 preserves its `float` type and `src/pms/evaluation/metrics.py:17` confirms. ✓

**Risk note:** Task 1 Step 1.4 Shape B changes `MetricsSnapshot`'s constructor — if chosen, update `tests/unit/test_evaluation_cp07.py:201-219` (three existing `MetricsSnapshot(...)` literals) or the baseline breaks. Shape A avoids this. Prefer Shape A.
