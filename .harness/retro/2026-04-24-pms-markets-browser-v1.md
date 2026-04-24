---
task_id: pms-markets-browser-v1
task_title: "PMS Markets Browser v1 — prices, filters, detail drawer, subscriptions"
date: 2026-04-24
checkpoints_total: 14
checkpoints_passed_first_try: 13
total_eval_iterations: 16
total_commits: 40
reverts: 0
avg_iterations_per_checkpoint: 1.14
---

# Retro — pms-markets-browser-v1

## TL;DR

Markets Browser v1 landed end-to-end: 14 checkpoints, 13 first-pass evaluator
wins, 16 total evaluator iterations (CP01 alone took 3), 40 commits, zero
reverts. The branch delivered real-time price snapshots, filter/search UI,
subscription toggle with optimistic state, detail drawer, and a Playwright
E2E suite across two spec files.

The cross-model peer review (2 rounds) returned 15 findings — 5 major, 5
minor, 5 suggestions — and 8 were fixed (all 5 major, 1 minor: f9, 2
suggestions: f11/f12), including correctness defects (SQL filter logic,
data-write ordering) and UX seams (debounce, URL mutation, YES-token copy).
This is the 7th occurrence of
`cross-checkpoint-integration`: checkpoint-local green status did not surface
these seams.

Full-verify iter-1 produced one hard failure: `cd dashboard && npm run build`
exited non-zero because `useSearchParams()` in the markets page was not
wrapped in `<Suspense>`. This failure was invisible to every other gate
(lint, Vitest, Playwright) because Next.js static prerender only runs during
`next build`. Full-verify iter-2 confirmed the idiomatic Suspense-wrapper fix
and closed PASS. This introduces a new pattern: `npm-build-gate-gap`.

Two proposals are issue-ready: adding `npm run build` to the canonical gate
list, and increasing the coverage threshold from 79 to 85 (user-requested).

---

## Observations

### What Worked Well

1. **13/14 first-pass checkpoints, zero reverts, clean commit graph.** The
   only multi-iteration checkpoint (CP01) was delayed by harness bookkeeping
   (spec-format) and a coverage policy decision already tracked by issue #22,
   not by a code defect. The remaining 13 checkpoints all passed on the first
   evaluator attempt.

2. **Review-loop consensus in 2 rounds.** The cross-model peer review found
   real defects, and all 8 accepted findings were fixed with regression tests
   before the round-2 verification. The 7 deferred items were documented with
   proper `Verification: reason:` blocks explaining why each deferral requires
   external evidence or design decisions.

3. **Full-verify caught what no earlier gate could.** The iter-1 FAIL on
   `npm run build` is proof that the full-verify phase catches a class of
   errors that lint, Vitest, and Playwright collectively cannot. The single-file
   `<Suspense>` fix was mechanical; full-verify routing it through iter-2 was
   the correct harness behavior.

4. **Data-integrity fixes were accompanied by regression tests.** Findings f4
   (SQL filter), f9 (write ordering), and f12 (Pydantic validation) each landed
   with new unit or integration coverage verifying the fixed behavior, not just
   a code change.

5. **Deferred items were evidence-grounded, not dismissed.** All 7 deferrals
   (f6, f7, f8, f10, f13, f14, f15) attached `Verification: reason:` explaining
   the concrete external dependency or design decision required. None were
   authority-only dismissals. This follows the promoted review-rejection
   discipline rule.

---

### Error Patterns

#### 1. [category: cross-checkpoint-integration] (7th occurrence)

The cross-model peer found 5 major + 1 minor correctness issues at branch-level
review that the checkpoint evaluators did not surface:

- **f1** (major): Search input fires a URL/refetch per keystroke. The
  `MarketsPageClient` search flow spans the URL query-parameter state, the
  debounce timing, and the `/markets` endpoint call — three concerns that are
  only visible together at the branch level.
- **f2** (major): Optimistic subscription toggle overwrote the `subscribed`
  field derived from strategy runtime state. The `subscribed` vs
  `subscription_source` distinction requires understanding the full
  subscription projection contract across sensor, controller, and API layers.
- **f4** (major): The `subscribed=only` SQL filter omitted rows from the
  persisted `market_subscriptions` table. The filter logic in
  `market_data_store.py` required understanding both the runtime
  `current_asset_ids` path and the persisted subscription path, which land in
  separate checkpoints.
- **f5** (major): The React unmount cleanup in `MarketsPageClient` rewrote
  `window.location.search`, which corrupted the next route's URL on navigation.
  The cleanup side-effect is only destructive when the drawer-close → navigation
  path is exercised across components.
- **f9** (minor): `poll_once` wrote a market row before token parsing was
  attempted, leaving orphan market rows on parse failure. The
  market-discovery-sensor → storage write ordering only surfaces as a
  data-integrity issue when the full discovery + persistence pipeline is
  assembled.

All five issues passed checkpoint-local evaluator review. This is the same
shape seen in pms-v1, pms-market-data-v1, pms-strategy-aggregate-v1,
pms-factor-panel-v1, pms-research-backtest-v1, and cathedral-v1. The
standing harness-side proposal (end-of-branch integration trace step) remains
the correct fix. Status: **proposed** (7th occurrence, unchanged).

#### 2. [category: npm-build-gate-gap] (NEW, 1st occurrence, HIGH)

Full-verify iter-1 failed with a hard failure on `cd dashboard && npm run
build`. The error:

```
useSearchParams() should be wrapped in a suspense boundary at page "/markets".
Error occurred prerendering page "/markets".
Export encountered an error on /markets/page: /markets, exiting the build.
```

Root cause: `dashboard/lib/useMarketsFilters.ts:191` calls `useSearchParams()`
from `next/navigation`. The consumer component (`MarketsPageClient`) was
rendered directly in `dashboard/app/markets/page.tsx` without a `<Suspense>`
boundary, which Next.js App Router requires for any component that reads
search params during static prerender.

**Why every earlier gate missed this:**

| Gate | Why it did not catch HF1 |
|------|--------------------------|
| `uv run pytest` + mypy | Python-only; no Next.js awareness |
| `cd dashboard && npm run lint` | `tsc --noEmit` type-checks but does not run prerender |
| `cd dashboard && npm run test` | Vitest runs in jsdom; no Next.js prerender |
| `cd dashboard && npx playwright test` | Playwright exercises the dev server (`next dev`), which performs no static prerender at all |
| `npm run build` in review-loop | **Not included in the review-loop verification gate list** |

The review-loop summary (`summary.md §Verification`) lists five gates:
`pytest`, `mypy`, `npm run lint`, `npm run test`, and `npx playwright test`.
`npm run build` is absent. The full-verify discovery file correctly included
it; the review-loop gate list did not.

This is both a **project-level gap** (CLAUDE.md canonical gates section does
not include `cd dashboard && npm run build`) and a **harness skill defect**
(the review-loop verification protocol for fullstack/dashboard tasks should
include `npm run build`).

First occurrence; severity HIGH (deployment-breaking). Promoted to
**proposed** per frequency-or-severity rule.

Fix applied in iter-2: `dashboard/app/markets/page.tsx` wraps
`<MarketsPageClient />` in `<Suspense fallback={<main className="shell"
aria-busy="true" />}>`. Build output confirmed `/markets` as `○ (Static)`.

---

### Rule Conflict Observations

Four rule conflict notes were recorded in retro-input.md. None represent
genuine rule conflicts:

- **CP01 iter-1** — harness bookkeeping (spec-format commit), not a
  product-scope change.
- **CP01 iter-2** — coverage below harness default 85%; accepted as
  pre-existing, tracked by issue #22. No new rule needed; issue #22 tracks
  the resolution.
- **CP01 iter-3** — mechanical fix for iter-2's evaluator critical failure.
- **CP03** — `_parse_outcome_prices` fallback choice made autonomously because
  the spec's §8 already specified the behavior; no rule gap.
- **CP04** — spec text inconsistency (`NOW()` vs `markets.price_updated_at`
  lag gauge). Generator correctly selected the §8 definition because it
  detects the failure case. No rule conflict; spec text was ambiguous.
- **CP06** — Node v25 experimental `localStorage` shim in `vitest.setup.ts`
  was required to make the documented `npm run test` gate pass. Test-env-only
  change; correct behavior.

No CLAUDE.md rule conflicts requiring clarification were observed.

---

## Recommendations

### Proposal 1: Add `npm run build` to canonical dashboard gates

- **Pattern**: npm-build-gate-gap
- **Severity**: high
- **Status**: Proposed
- **Root cause**: The project's canonical gate list in CLAUDE.md documents
  `(cd dashboard && npm run test)` as the dashboard verification step but
  omits `(cd dashboard && npm run build)`. Next.js App Router's static
  prerender step runs only during `next build`; the dev server never
  prerendering means Playwright E2E, Vitest, and lint cannot catch prerender
  errors (missing `<Suspense>` wrappers, dynamic API usage in static routes,
  etc.). The review-loop gate list mirrored this omission. Full-verify caught
  the gap at iter-1 because `discovery.md` independently scans `package.json`
  scripts and includes `npm run build` as a check. The fix is mechanical and
  the rule is clear: any branch touching `dashboard/` must run a production
  build before review-loop sign-off.
- **Drafted rule text**:
  ~~~markdown
  For dashboard work, also run:

  ```bash
  (cd dashboard && npm run build)          # Next.js production build catches
                                           # prerender errors dev server misses
  ```

  `next build` is the only gate that exercises App Router static prerendering.
  A `useSearchParams()` call without a `<Suspense>` boundary, a dynamic API
  import in a static route, or any other prerender-time error is silent in
  lint / Vitest / Playwright but hard-fails the production build. Run this
  gate on every dashboard-touching branch before the review-loop phase.
  ~~~
- **Issue-ready**: true

---

### Proposal 2: Increase coverage threshold from 79% to 85%

- **Pattern**: coverage-below-harness-default
- **Severity**: medium
- **Status**: Proposed
- **Root cause**: The project `.harness/config.json` `coverage_threshold` is
  79, accommodating the pre-existing coverage gap first tracked in issue #22
  (opened during CP01 iter-2 of this task). The harness default for
  backend/fullstack tasks is 85. Current measured backend coverage is 80.01%
  (8203 statements, 1640 missed). The 6-point gap means 1640 production
  statements — across sensor adapters, storage helpers, API routes, and
  evaluation logic — are untested. The user has explicitly requested an issue
  to increase test coverage. The configured 79% threshold should be treated as
  a temporary floor, not a permanent project standard.
- **Drafted rule text**:
  ```
  **Coverage threshold**: The project targets ≥ 85% backend line coverage
  (the Harness default for backend/fullstack tasks). The `.harness/config.json`
  `coverage_threshold` must be updated from 79 to 85 as coverage is brought
  up. Contributors adding new modules are expected to add corresponding tests
  so coverage does not regress below 85. Progress tracked in issue #22 (and
  the follow-up issue created from this retro).
  ```
- **Issue-ready**: true

---

### Skill Defect Flags

#### SD-1 — Review-loop gate list missing `npm run build` for fullstack tasks

- **Task**: pms-markets-browser-v1
- **Skill**: harness review-loop
- **Severity**: medium
- **Defect**: The review-loop verification section for fullstack/dashboard
  tasks does not include `cd dashboard && npm run build`. The full-verify
  discovery step independently identifies this gate, which is why full-verify
  caught HF1 while review-loop missed it. The review-loop protocol should
  mirror the full-verify gate set for fullstack tasks: if `npm run build` is a
  hard-failure gate in full-verify discovery, it should appear in the
  review-loop verification checklist as well.
- **Status**: flagged for review

---

### Existing patterns confirmed

- **`cross-checkpoint-integration`** (7th occurrence) — still proposed
  harness-side (end-of-branch integration trace); no change to status.
- No recurrence of `generated-artifact-drift`, `stale-baseline`, or
  `magnitude-overrun-tests` this task.

---

## Filed Issues

- [#24](https://github.com/stone16/prediction-market-system/issues/24) — `[Harness Retro] Add npm run build to canonical dashboard gates`
- [#25](https://github.com/stone16/prediction-market-system/issues/25) — `[Harness Retro] Increase backend coverage threshold from 79% to 85%`
- [#26](https://github.com/stone16/prediction-market-system/issues/26) — `[Harness Retro] Add production build gate to review-loop verification for dashboard tasks`

---

## Retro Metadata

- 14 checkpoints total; 13 passed first try
- CP01: 3 iterations (iter-1 harness bookkeeping fix; iter-2 coverage policy
  decision; iter-3 mechanical fix for iter-2 critical failure)
- 16 total evaluator iterations (13×1 + 1×3)
- 40 commits on the branch; 0 reverts
- Review-loop: 2 rounds + fresh final pass; 15 findings; 8 fixed, 7 deferred
  with verification blocks; CONSENSUS: Approved
- Full verify:
  - iter-1 FAIL: `npm run build` hard failure (HF1 — `useSearchParams` without
    Suspense on `/markets`); all other 7 checks PASS; coverage 80.01% ≥ 79%
  - iter-2 PASS: single-file fix; `/markets` confirmed `○ (Static)` in build;
    649 passed, 155 skipped; mypy clean (291 files); 23 Vitest files / 60 tests;
    4/4 Playwright specs
- PR: https://github.com/stone16/prediction-market-system/pull/23
- New pattern: `npm-build-gate-gap` (1st occurrence, HIGH, proposed)
- New skill defect: SD-1 review-loop gate list missing `npm run build`
- Recurring pattern confirmed: `cross-checkpoint-integration` (7th occurrence,
  proposed)
