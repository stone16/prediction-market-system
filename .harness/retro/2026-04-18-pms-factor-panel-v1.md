---
task_id: pms-factor-panel-v1
task_title: "S3 — Middle-ring factor panel + rules/statistical migration + /factors page"
date: 2026-04-18
checkpoints_total: 8
checkpoints_passed_first_try: 7
total_eval_iterations: 9
total_commits: 21
reverts: 0
avg_iterations_per_checkpoint: 1.1
---

# Retro — pms-factor-panel-v1

## TL;DR

S3 landed the middle ring of the onion: `factors` / `factor_values`
schema, raw `FactorDefinition` modules, `FactorService` cadence worker,
and a `/factors` dashboard page. Eight checkpoints, 17 TDD commits,
3 PR review-loop fixes, 1 post-merge review-follow-up commit on PR #12,
zero reverts. Seven of eight checkpoints passed first try; CP05 (the
composition emulator) took two iterations because iter-1 under-scoped
the "rules + statistical + LLM + averaging" acceptance text and also
tripped the L-sized magnitude review trigger.

The full-verify iteration for this task was written **retrospectively**
on 2026-04-18, after PRs #11 and #12 had already merged to `main`. The
branch shipped without a pre-merge full-verify gate. When full-verify
was finally exercised today, it passed — 219 passing / 51 skipped
(fast suite), 270 passing with 93% coverage (integration), dashboard
build clean, `/factors` Playwright e2e green — with one soft warning
for stale `pms_test` schema on the first integration run.

No new CLAUDE.md rule proposals are warranted. Three existing patterns
recurred: `cross-checkpoint-integration` (4th occurrence, still
proposed harness-side), `magnitude-overrun-tests` (2nd occurrence,
monitoring), and `stale-baseline` (3rd occurrence, rule already
active — the recurrence is both the `pms_test` DB schema drift and
the CLAUDE.md baseline still reading `145 passing / 42 skipped`).
One new process observation is flagged for monitoring only:
`skipped-full-verify-pre-merge`.

---

## Observations

### What Worked Well

1. **7/8 checkpoints first-try PASS on a three-layer stack.** CP01
   (schema), CP02–CP04 (raw factor definitions), CP06 (boot migration),
   CP07 (FactorService cadence worker), CP08 (dashboard) each passed
   evaluator review on the first pass. The only retry was CP05.

2. **CP05 retry was a clean scope correction, not a code defect.** The
   evaluator's `REVIEW` verdict on iter-1 (`fc884df`) named two items:
   a magnitude overrun (936 insertions / 19 files vs. the L threshold
   of 900 / 36) and an LLM/averaging parity gap
   (`src/pms/factors/defaults.py:31` missed the averaging branch while
   `src/pms/controller/pipeline.py:64` still averaged calibrated
   probabilities). Iter-2 (`6094121 feat(cp05): model branch averaging
   in composition`) added the branch graph in 284 insertions across 4
   files and closed both items. No production code from iter-1 was
   reverted — iter-2 extended it.

3. **Post-merge review follow-ups caught a real data-quality defect.**
   `828363b fix(cp07): drop spurious zero rows from YesCount/NoCount`
   removed factor-value rows that recorded literal zero counts when
   the raw detector had no true input to observe, preventing the
   `/factors` UI from rendering misleading flatlines. This is
   behaviour-class, not style-class — and it only surfaced once the
   CP07 worker ran against live-shaped data.

4. **Zero reverts across 20 branch commits + 1 post-merge commit.**
   The commit graph is linear; every review item became a forward
   commit, never a revert.

5. **Playwright `factors.spec.ts` is behaviour-backed.** The test
   seeds `factor_values` rows via a Python fixture and asserts the
   `/factors` page renders them with no console errors (per the
   `fix(cp08): stabilize factors chart sizing` fix — the chart
   container sizing needed explicit fallback or the render produced
   console warnings).

### Error Patterns

1. **[category: cross-checkpoint-integration] (4th occurrence)** The
   PR #12 `fix(factors): address review follow-ups` commit touched 24
   files across `src/pms/factors/`, `src/pms/runner.py`,
   `dashboard/app/api/pms/factors/route.ts`, `schema.sql`, and 5
   integration tests. That breadth is the same pattern seen in
   `pms-v1`, `pms-market-data-v1`, and `pms-strategy-aggregate-v1`:
   producer/consumer seams across checkpoints look coherent locally
   but reveal contract mismatches at branch-level review. The 3
   pre-merge `fix(cp07)` / `fix(ci)` / `fix(cp08)` commits are the
   same class. Already `proposed` in the index; recurrence confirms
   the harness-side proposal (end-of-branch integration trace) is
   the right shape. Still not a project CLAUDE.md rule.

2. **[category: magnitude-overrun-tests] (2nd occurrence)** CP05
   iter-1 shipped 936 insertions / 19 files, tripping the L review
   trigger at `>900 / >36`. Same pattern as `pms-v1`; index had this
   in `monitoring`. Recurrence is a second data point for the harness
   template to consider L-size automatic split guidance. Still below
   the 3× proposal threshold.

3. **[category: stale-baseline] (3rd occurrence)** Two separate
   manifestations this task:
   - **Runtime manifestation:** the compose `pms_test` PG instance
     was started before S3 DDL landed. `schema.sql` uses
     `CREATE TABLE IF NOT EXISTS` throughout, so reapplying the file
     did not install newly-added unique indexes. The first integration
     run on 2026-04-18 hit `asyncpg.exceptions.InvalidColumnReferenceError`
     on ON CONFLICT clauses whose target constraints were new in S3.
     Resolved by `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`
     then reapplying `schema.sql`.
   - **Documentation manifestation:** `CLAUDE.md:35` still records
     `145 passing, 42 skipped` as the baseline, as of
     `feat/pms-strategy-aggregate-v1` on 2026-04-17. Current state
     on `main` after S3 merge is `219 passing, 51 skipped`.
   Rule already active (promoted in `phase2-P2`). Recurrence means
   the rule is necessary; no change to rule status needed. Updating
   CLAUDE.md baseline to post-S3 state is a separate docs commit
   that this retro recommends but does not make.

4. **[category: skipped-full-verify-pre-merge] (NEW, observation)**
   `.harness/pms-factor-panel-v1/full-verify/` did not exist before
   this retro iteration was written; PRs #11 and #12 both merged to
   `main` without a formal full-verify gate producing artefacts. The
   stale-`pms_test` warning documented in this task's verification
   report is exactly the class of environmental drift full-verify
   is designed to catch before merge, not after. First occurrence;
   single-task observation, not a rule candidate yet.

5. **[category: empty-harness-phase-artifacts] (NEW, observation)**
   `.harness/pms-factor-panel-v1/spec-review/` and
   `.harness/pms-factor-panel-v1/e2e/` directories are empty.
   `pms-strategy-aggregate-v1` had populated `spec-review/round-1-*`
   and `spec-review/round-2-*` files; `pms-market-data-v1` had full
   `full-verify/iter-1/verification-report.md` + e2e discovery
   artefacts. S3 either skipped those phases or the artefacts were
   not persisted. Observational only; paired with observation 4,
   suggests the harness flow for S3 was compressed. First occurrence;
   monitoring.

### Rule Conflict Observations

None. No active rule contradicted another during this branch. The
`fix(cp07): drop spurious zero rows` commit is an example of the
active **"Comments are not fixes"** rule working correctly: the
defect was fixed in behaviour (removing the zero-row emission path),
not annotated with a code comment.

---

## Recommendations

### No new proposals this task

No new pattern crossed the 3× frequency threshold, and all
recurrences are covered by existing active or proposed patterns. The
two new `-NEW` observations enter monitoring only.

### Follow-up commits recommended (outside this retro)

1. **Baseline refresh in `CLAUDE.md`.** Update the "Canonical gates"
   block from `145 passing, 42 skipped (feat/pms-strategy-aggregate-v1)`
   to `219 passing, 51 skipped (main, post-S3)`. Add a line noting
   that `pms_test` PG databases started before S3 DDL land require
   a `DROP SCHEMA public CASCADE` + reapply because `schema.sql`
   uses `CREATE TABLE IF NOT EXISTS` without DDL diff tracking.

2. **Consider running full-verify before merging future sub-specs.**
   Not a CLAUDE.md rule (too few data points), but a note for the
   next sub-spec kickoff: spec S4 `pms-active-perception-v1` should
   include `full-verify/iter-1` as an explicit pre-merge checkpoint
   rather than a retrospective artefact.

### Active / existing patterns confirmed

- **`cross-checkpoint-integration`** (4×) — still proposed
  harness-side; no change.
- **`magnitude-overrun-tests`** (2×) — still monitoring; one more
  data point before any rule proposal.
- **`stale-baseline`** (3×) — rule active; recurrence confirms
  necessity. Update CLAUDE.md baseline as a docs commit.

### Skill Defect Flags

None from this task. No evaluator or harness protocol defects
observed within the checkpoints that were executed. The two harness
process observations (`skipped-full-verify-pre-merge`,
`empty-harness-phase-artifacts`) are user-process notes, not tool
defects.

---

## Retro Metadata

- 8 checkpoints total; 7 passed first try
- CP05: 2 iterations (iter-1 REVIEW for magnitude overrun + LLM/averaging
  parity gap; iter-2 PASS after adding branch averaging in 284 insertions)
- 9 total evaluator iterations (7×1 + 1×2)
- 21 commits total on the S3 work (17 CP commits incl. CP05 retry +
  3 pre-merge fix commits + 1 post-merge PR #12 follow-up)
- 0 reverts
- Review-loop: not formally separated into rounds in this task;
  findings landed as additional `fix()` commits on the PR before merge
- Test suite: 219 passed, 51 skipped on `main` post-merge
  (HEAD `e226660`); 270 passed with 93% coverage under
  `PMS_RUN_INTEGRATION=1`
- mypy strict: clean, 135 source files
- Playwright `factors.spec.ts`: 1 passed
- Dashboard build: `/factors` route present as dynamic
- New patterns entering monitoring: `skipped-full-verify-pre-merge`,
  `empty-harness-phase-artifacts`
- Recurring patterns confirmed: `cross-checkpoint-integration` (4×,
  proposed), `magnitude-overrun-tests` (2×, monitoring),
  `stale-baseline` (3×, active)

---

## Index diff (for Orchestrator to apply to .harness/retro/index.md)

Increment occurrence counts and last-task for recurring patterns:

| Pattern                          | Old occurrences | New occurrences | Old last task               | New last task           |
|----------------------------------|-----------------|-----------------|-----------------------------|-------------------------|
| cross-checkpoint-integration     | 3               | 4               | pms-strategy-aggregate-v1   | pms-factor-panel-v1     |
| magnitude-overrun-tests          | 1               | 2               | pms-v1                      | pms-factor-panel-v1     |
| stale-baseline                   | 2               | 3               | pms-market-data-v1          | pms-factor-panel-v1     |

New rows to append to the frequency table:

| skipped-full-verify-pre-merge    | 1 | medium | observation | pms-factor-panel-v1 | pms-factor-panel-v1 | Observation |
| empty-harness-phase-artifacts    | 1 | low    | observation | pms-factor-panel-v1 | pms-factor-panel-v1 | Observation |

New monitoring entries to append to the Monitoring section:

- `skipped-full-verify-pre-merge` — feature branch merged to `main`
  without a formal `full-verify/` artefact; environmental drift
  (e.g. stale `pms_test` schema) surfaces post-merge instead of
  pre-merge
- `empty-harness-phase-artifacts` — `spec-review/` and `e2e/`
  directories created but not populated during the task; harness
  flow was compressed relative to S1 / S2
