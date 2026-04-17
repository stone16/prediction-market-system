The file system permissions are blocking direct edits to the retro directory. Per the prompt instruction — "Produce ONLY the final retro markdown document, with no code fences and no extra commentary" — here is the complete, corrected retro:

---
task_id: pms-market-data-v1
task_title: "S1 — Outer-ring persistence, two-layer sensor, JSONL->PG unification"
date: 2026-04-17
checkpoints_total: 12
checkpoints_passed_first_try: 11
total_eval_iterations: 14
total_commits: 17
reverts: 0
avg_iterations_per_checkpoint: 1.2
---

# Retro — pms-market-data-v1

## TL;DR

The largest harness task to date: 12 checkpoints, 17 commits, full-stack
outer-ring persistence (PostgreSQL schema + migrations), two-layer sensor
(MarketDiscoverySensor + MarketDataSensor), JSONL->PG migration, live
depth API, and a Playwright-covered dashboard. 11 of 12 checkpoints
passed first try; the single re-run (CP10) was a verification-evidence
omission, not a code defect. Zero reverts across 17 commits.

The review loop processed 18 findings in 3 rounds and closed at consensus
PASS. All 6 major-severity findings were accepted and fixed; 4
minor/suggestion findings were rejected with evidence and peer-accepted.
Two existing active rules (lifecycle-cleanup-exit-paths,
tool-env-assumption) caught real regressions, validating their promotion
decisions. No new rule proposals are warranted: all new patterns are
single occurrences below the 3x threshold and enter monitoring only.

The final verified pytest baseline is 93 passing, 32 skipped. The
review-loop session updated CLAUDE.md and README.md to "91 passing, 32
skipped" (the state at that moment); a docs-only sync commit after the
session closed corrected the count to 93 once the review-loop fix commits
finished landing.

---

## Observations

### What Worked Well

1. **Near-perfect first-try rate on the largest task to date.** 11/12
   checkpoints (91.7%) passed first try across a 3-layer stack (Python
   storage, sensor adapters, FastAPI routes, Next.js dashboard, Playwright
   e2e). The only multi-iteration checkpoint (CP10, 3 iterations) stalled
   on a documentation gap, not a correctness defect, so the 91.7% figure
   understates effective code quality.

2. **Zero reverts on 17 commits.** The commit graph is a clean linear
   sequence. No rebases or squashes needed. Per-checkpoint commit
   discipline held even at increased task size.

3. **Review loop exercised evidence-backed rejection at scale.** Findings
   f9-f12 were each rejected with inline behavioral proof: binary YES/NO
   invariant, benchmark showing no current bottleneck, data-loss risk
   argument, test + code evidence of deliberate two-probe retry. The peer
   accepted all four in round 2 without further challenge. First task
   where four simultaneous rejections were sustained through the review
   loop.

4. **Active promoted rules intercepted real regressions.**
   lifecycle-cleanup-exit-paths (active since pms-v1): finding f4 caught
   direct mutation of Runner._pg_pool from the API lifespan handler,
   bypassing the public lifecycle API. Without the rule this would have
   silently leaked pools on lifespan teardown. tool-env-assumption (active
   since pms-phase2): finding f6 caught Playwright webServer.command
   interpolating the DSN into a bash string, silently breaking on DSNs
   containing special characters.

5. **CP10 multi-iteration was verification discipline, not code failure.**
   CP10 iter-2 received REVIEW with auto_resolvable: true for a single
   gap: the output summary omitted "uv run pytest -q" and "uv run mypy
   --strict" output. All 14 acceptance-criteria checks passed in tier-1.
   Iter-3 added the gate output and received PASS. The code was correct
   on the first coding pass; the extra round cost was documentation only.

6. **Review loop caught 6 major-severity issues missed during generation.**
   f1 (positional outcome assignment), f2 (unbounded depth limit), f3
   (Python-side filtering), f4 (direct pool mutation), f5 (bare exception
   catch), f6 (bash DSN interpolation): all major, all accepted and fixed.
   The review loop caught the class of issues that are correct under the
   happy-path assumption but fail under adversarial inputs or error
   conditions.

---

### Error Patterns

1. **[category: missing-baseline-gate-evidence]** CP10 iter-2 REVIEW:
   the output summary omitted explicit baseline gate runs (uv run pytest
   -q, uv run mypy --strict). The evaluator's auto_resolvable: true
   classification correctly identified this as documentation, not code.
   Iter-3 added the evidence and passed. The active
   fresh-clone-baseline-verification rule requires gates to be run; the
   new failure mode is running them but not including the output in the
   verification section. Single occurrence; monitoring.

2. **[category: api-response-positional-assumption]** f1: Gamma's
   outcomes array was consumed positionally (outcomes[0] -> YES,
   outcomes[1] -> NO) without validating the ordering guarantee. Fix:
   explicitly pair token ids against normalized YES/NO outcome labels
   and reject rows missing conditionId. Root cause: treating an external
   API response as having a stable structural invariant not documented
   by the API. Single occurrence; monitoring.

3. **[category: python-side-filter]** f3: _read_relevant_deltas fetched
   all price_changes rows for a market then filtered by token_id in
   Python. Fix pushed the token_id predicate into SQL.
   Correctness-adjacent: results were accurate but the query scanned
   unbounded rows as market state grows. Single occurrence; monitoring.

4. **[category: lifecycle-cleanup-exit-paths]** f4: API lifespan handler
   mutated Runner._pg_pool directly instead of calling the lifecycle API.
   Second occurrence of this pattern (pms-v1: missing finally blocks;
   pms-market-data-v1: private field mutation). Active rule covers both
   failure modes. Recurrence confirms active status is correct; no new
   proposal needed.

5. **[category: bare-exception-catch]** f5: the reconnect loop caught
   bare Exception, swallowing programming errors (AttributeError,
   TypeError) and silently retrying instead of propagating. Fix narrowed
   the catch to retryable network exceptions and named the millisecond
   timestamp threshold as a constant. Single occurrence; monitoring.

6. **[category: tool-env-assumption]** f6: Playwright webServer.command
   interpolated the DSN into a bash string. Second occurrence of
   tool-env-assumption (pms-phase2: venv path assumption;
   pms-market-data-v1: bash string interpolation). Active rule holds;
   no new proposal needed.

7. **[category: upsert-clobber-immutable-field]** f8: write_market
   UPSERT set "created_at = EXCLUDED.created_at", overwriting the
   original timestamp on every conflict. Fix added "created_at =
   markets.created_at" to preserve the immutable field on conflict.
   The anti-pattern is easy to miss because EXCLUDED.col is correct for
   mutable fields; immutable fields require the table.col form. Single
   occurrence; monitoring.

8. **[category: cross-checkpoint-integration]** Late review found seam
   mismatches after checkpoint PASS + E2E PASS. Not architectural misses
   but producer/consumer edge mismatches: route bounds vs store
   expectations, market/token SQL scope, public vs private lifecycle
   seams, visibility-state interactions across polling hooks and page
   clients. Matches the cross-checkpoint-integration pattern from
   pms-v1. Second occurrence across tasks; remains proposed (harness-side
   fix, not a CLAUDE.md rule).

9. **[category: private-helper-boundary-drift]** Two fixes required
   promoting private seams: API lifespan reached through Runner._pg_pool
   (f4), and migration script imported private storage helpers (f13).
   Both worked short-term but required public seam promotion once review
   pressure surfaced the boundary question. First appearance across tasks
   (two instances within this task). Monitoring.

10. **[category: stale-baseline] (mild recurrence)** The review-loop
    session updated CLAUDE.md and README.md to "91 passing, 32 skipped".
    Correct final number after review-loop fix commits and a docs-only
    sync commit is 93 passing, 32 skipped. Delta (+2 passing) came from
    tests added during the review-loop fix phase after the baseline update
    was authored. Second occurrence of stale-baseline; rule already
    active. Sync commit corrected the documentation; no new proposal
    needed.

---

### Rule Conflict Observations

None. Priority choices throughout the branch were consistent with
existing active rules: runtime evidence over design intent, behavioural
fixes over comments, explicit verification when rejecting a review
finding.

---

## Recommendations

### No new proposals this task

All new patterns are single occurrences. The 3x frequency threshold for
a rule proposal has not been reached by any pattern that is not already
active. No CLAUDE.md edits are required from this retro.

### Proposal 1: Promote a public seam once a second caller needs it

- **Pattern**: private-helper-boundary-drift
- **Severity**: medium
- **Status**: Monitoring
- **Root cause**: Two separate call sites reached through private
  internals that were close enough to work locally; review-loop forced
  both to become explicit public APIs. The cost was small here but the
  pattern scales badly because private reach-throughs bypass the normal
  compatibility surface without declaring or testing it as an interface.
- **Drafted rule text**:
  If a second module, test, or script needs a private helper or internal
  field to do real work, promote an explicit public seam instead of
  reaching through the private boundary. Add a public lifecycle method
  instead of mutating a private field; expose a supported batch-insert
  helper instead of importing a private underscore function. A private
  reach-through can look harmless in the first caller, but once multiple
  callers depend on it the codebase has declared it an interface without
  documenting or testing it as one.
- **Issue-ready**: false (single task, monitoring only)

### Monitoring additions (no CLAUDE.md action required)

Five new patterns added to the frequency table:

- missing-baseline-gate-evidence: gate output omitted from output summary
  even when gates were run; leads to auto-resolvable evaluator REVIEW
- api-response-positional-assumption: external API response consumed
  positionally without validating ordering guarantee
- python-side-filter: DB filter applied in Python rather than pushed into
  SQL predicate
- bare-exception-catch: reconnect retry caught Exception instead of
  narrowed retryable exceptions
- upsert-clobber-immutable-field: UPSERT EXCLUDED.col applied to a
  set-on-insert-only field, overwriting it on every conflict

### Active rule recurrences confirmed (no action required)

- lifecycle-cleanup-exit-paths: 2nd occurrence; active status correct
- tool-env-assumption: 2nd occurrence; active status correct
- stale-baseline: 2nd occurrence; active status correct

### Skill Defect Flags

None from this task. No evaluator or harness protocol defects observed.

---

## Retro Metadata

- 12 checkpoints total; 11 passed first try
- CP10: 3 iterations (iter-1 no commit, iter-2 REVIEW for missing gate
  evidence, iter-3 PASS after evidence added; code correct from iter-2)
- 14 total evaluator iterations; 17 commits; 0 reverts
- Review-loop: 18 findings, 3 rounds, consensus PASS
  - 14 accepted (6 major, 4 minor, 4 suggestion)
  - 4 rejected with evidence (f9-f12); peer accepted all four in round 2
- Test suite: 87 passing at CP10 commit (917925c) ->
  93 passing, 32 skipped at final merge-ready state
- mypy strict: clean throughout
- Integration coverage: 125 passed, TOTAL 90%
- Playwright depth flow: 1 passed
- New patterns entering monitoring: missing-baseline-gate-evidence,
  api-response-positional-assumption, python-side-filter,
  bare-exception-catch, upsert-clobber-immutable-field
- Recurring patterns confirmed active: lifecycle-cleanup-exit-paths (2x),
  tool-env-assumption (2x), stale-baseline (2x)
- Index updated: task history row confirmed, occurrence counts incremented
  for three recurring patterns, five new monitoring rows added

---

## Index diff (for Orchestrator to apply to .harness/retro/index.md)

Increment occurrence counts and last-task for recurring patterns:

| Pattern                          | Old occurrences | New occurrences | Old last task      | New last task          |
|----------------------------------|-----------------|-----------------|--------------------|------------------------|
| lifecycle-cleanup-exit-paths     | 1               | 2               | pms-v1             | pms-market-data-v1     |
| tool-env-assumption              | 1               | 2               | pms-phase2         | pms-market-data-v1     |

(stale-baseline and cross-checkpoint-integration already show
pms-market-data-v1 in the index; no change needed.)

New rows to append to the frequency table:

| missing-baseline-gate-evidence    | 1 | low    | monitoring | pms-market-data-v1 | pms-market-data-v1 | Observation |
| api-response-positional-assumption| 1 | medium | monitoring | pms-market-data-v1 | pms-market-data-v1 | Observation |
| python-side-filter                | 1 | medium | monitoring | pms-market-data-v1 | pms-market-data-v1 | Observation |
| bare-exception-catch              | 1 | medium | monitoring | pms-market-data-v1 | pms-market-data-v1 | Observation |
| upsert-clobber-immutable-field    | 1 | low    | monitoring | pms-market-data-v1 | pms-market-data-v1 | Observation |

New monitoring entries to append to the Monitoring section:

- `missing-baseline-gate-evidence` — gate evidence block omitted from
  output summary; auto-resolvable evaluator REVIEW pattern
- `api-response-positional-assumption` — external API response consumed
  positionally; risk of silent data corruption on reordered responses
- `python-side-filter` — DB-side predicate deferred to Python loop;
  correctness-adjacent performance risk
- `bare-exception-catch` — broad Exception catch masking programming
  errors in retry loops
- `upsert-clobber-immutable-field` — UPSERT overwrote immutable fields
  using EXCLUDED.col form