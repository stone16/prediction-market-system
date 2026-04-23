---
task_id: cathedral-v1
task_title: "PMS Dashboard Cathedral v1 — 12-checkpoint narrative reframe and closeout"
date: 2026-04-23
checkpoints_total: 12
checkpoints_passed_first_try: 11
total_eval_iterations: 13
total_commits: 28
reverts: 0
avg_iterations_per_checkpoint: 1.1
---

# Retro — cathedral-v1

## TL;DR

Cathedral v1 landed end to end: 12 checkpoints, 11 first-pass evaluator wins,
13 total evaluator iterations, zero reverts, and a complete post-checkpoint
closeout bundle. The branch finished with durable markets browse, fill-backed
positions/trades, durable accept flow, SSE event log, public share page, Today
hero, and the server-side first-trade metric, plus fresh task-level E2E,
review-loop, and full-verify artifacts.

The closeout pass surfaced one real branch seam and one process recurrence.
The real seam was cross-checkpoint: CP08's dedup-aware actuator handoff added a
`dedup_acquired` kwarg that older executor doubles did not accept, and the bug
only appeared once the CP06/CP07 integration slice was rerun after all
checkpoints were green. The process recurrence was generated-artifact drift:
running Playwright again dirtied the tracked
`dashboard/e2e/evidence/signals-real-depth.png`, which had to be restored
before commit.

No new CLAUDE.md rule is warranted. `cross-checkpoint-integration` recurs
again, strengthening the existing harness-side proposal. `generated-artifact-
drift` now has a second occurrence and moves from observation to monitoring.

## Observations

### What Worked Well

1. **The 12-checkpoint plan held without branch churn.** Cathedral stayed
   linear: 11 of 12 checkpoints passed first try, the only retry was CP06,
   and there were zero reverts across the whole branch.

2. **Closeout caught a real behavioural seam rather than a cosmetic issue.**
   The final integration slice exposed that the runner now passed
   `dedup_acquired=` into actuator executors unconditionally. Production code
   was compatible, but the older executor contract still existed in tests and
   custom doubles. The fix added signature-aware fallback dispatch plus a unit
   regression test.

3. **The browser suite is now deterministic at branch closeout.** The failing
   Cathedral-era Playwright specs were not product regressions; they were
   depending on leftover database state and ambiguous selectors. Scoping the
   nav lookup and routing populated table fixtures made the suite stable again,
   and the full run closed at `18 passed, 1 skipped`.

4. **The dashboard gate now matches the repo workflow instead of pointing to a
   missing command.** Full-verify initially failed because `npm run lint` did
   not exist in `dashboard/package.json`. Adding a runnable `lint` script
   backed by `tsc --noEmit` closed the gap and made the documented command real.

### Error Patterns

1. **[category: cross-checkpoint-integration] (6th occurrence)** The final
   branch-level rerun caught the `dedup_acquired` executor seam only after CP06
   fill persistence and CP08 accept-flow queue ownership were composed on the
   same branch. Checkpoint-local green status was not enough to expose the
   mismatch. This is the same family already seen across prior harness tasks;
   the existing harness-side proposal remains correct.

2. **[category: generated-artifact-drift] (2nd occurrence)** Re-running the
   browser suite dirtied the tracked
   `dashboard/e2e/evidence/signals-real-depth.png` artifact again. The branch
   restored it before commit, but the pattern has now recurred and should move
   from observation to monitoring.

3. A fixed-expiry CP07 integration fixture aged past real UTC during closeout
   and flipped a pending decision to expired. The repair was local and obvious:
   make the fixture clock-relative. Single occurrence; no rule proposal.

### Rule Conflict Observations

None.

## Recommendations

### No new CLAUDE.md proposals this task

The important recurrence is still `cross-checkpoint-integration`, which already
has a standing harness-side proposal. Cathedral strengthens that case but does
not introduce a new project rule.

### Follow-up actions recommended

1. Keep the final cross-checkpoint integration slice mandatory for fullstack
   dashboard branches. Cathedral is another case where the real defect only
   appeared after all checkpoints were green.
2. Continue hardening review/closeout automation to avoid staging tracked local
   artifacts created by Playwright evidence runs.
3. Prefer clock-relative timestamps in integration fixtures when the behaviour
   under test is explicitly time-sensitive.

### Skill Defect Flags

None.

## Retro Metadata

- 12 checkpoints total; 11 passed first try
- 13 total evaluator iterations
- 28 total commits on the branch
- 0 reverts
- Review-loop: 1 round, read-only complete, no findings
- Full verify:
  - `uv sync` successful
  - `uv run pytest -q` -> `600 passed, 138 skipped`
  - `uv run mypy src/ tests/ --strict` -> clean across 277 source files
  - `uv run pytest --cov=src/pms --cov-report=term -q` -> `80%`
  - `cd dashboard && npm run lint` -> pass (`tsc --noEmit`)
  - `cd dashboard && npx vitest run` -> `27 passed`
- `cd dashboard && npm run build` -> pass
- `cd dashboard && npx playwright test` -> `18 passed, 1 skipped`
- Cathedral cross-checkpoint integration slice -> `11 passed`
- PR: #21
- Recurring patterns confirmed:
  - `cross-checkpoint-integration` (6×, proposed)
  - `generated-artifact-drift` (2×, monitoring)
