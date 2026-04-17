---
task_id: pms-strategy-aggregate-v1
task_title: "S2 — Strategy aggregate + registry + import-linter + default seed"
date: 2026-04-17
checkpoints_total: 7
checkpoints_passed_first_try: 7
total_eval_iterations: 7
total_commits: 17
reverts: 0
avg_iterations_per_checkpoint: 1.0
---

# Retro — pms-strategy-aggregate-v1

## Observations

### What Worked Well

1. **Checkpoint execution stayed clean.** All 7 checkpoints passed on the
   first evaluator pass, with zero reverts across 17 commits. The branch
   moved linearly from red/green checkpoint work through E2E without any
   checkpoint-local rewrites.

2. **Review-loop found the right class of defects.** Claude surfaced 10
   findings in 2 rounds. Eight were accepted and fixed, one suggestion
   (`f6`) was rejected with a concrete runtime trace, and one finding
   (`f10`) was deferred because the user explicitly marked those files as
   out of scope. The peer accepted the `f6` rejection once the Pydantic
   `Z` vs `+00:00` mismatch was demonstrated.

3. **Full-verify failed for one narrow reason and recovered in one
   commit.** The initial full-verify run had no command failures; the
   only hard failure was repo-wide coverage at `81%` versus the harness
   floor of `85%`. A single targeted test commit
   (`7e08d27 test(full-verify): raise strategy aggregate coverage`) took
   `src/pms/storage/strategy_registry.py` to `100%`, covered the
   `pms-api` entrypoint, and moved total coverage to `85%` without
   touching production code.

### Error Patterns

1. **[category: cross-checkpoint-integration]** The review loop found two
   real contract seams after all checkpoints had already passed:
   `schema.sql` seeded a default strategy payload that the registry could
   not decode, and CI still ran a stale hand-curated subset of
   integration files instead of the full `-m integration` suite. The
   code inside each checkpoint was locally coherent, but the producer /
   consumer contract across schema, registry, and CI verification only
   became visible once the branch was reviewed as a whole. This is a
   recurrence of the existing harness-side pattern, not a new
   project-level rule.

2. **[category: review-rejection-hygiene]** `f6` recommended removing
   the `/strategies` `created_at` serializer as redundant. The rejection
   only held because it was backed by a direct runtime reproduction
   showing Pydantic JSON mode emits `Z` while the CP07 route contract and
   tests require `datetime.isoformat()` with `+00:00`. This is the active
   rule working as intended: rejection by witness, not by preference.

3. **[category: project-ide-tooling-drift]** `dashboard/next-env.d.ts`
   drifted to `.next/dev/types/routes.d.ts` during local frontend tool
   runs and had to be restored twice, once during review-loop fixes and
   again after full-verify. The file is Next-managed and not a product
   change; the pattern remains generated-file churn rather than a
   correctness issue.

### Rule Conflict Observations

None.

## Recommendations

### No new proposals this task

No new pattern crossed the promotion threshold. The recurring issues on
this branch were already covered by existing harness learnings.

### Active / existing patterns confirmed

- **`cross-checkpoint-integration`** recurred. Keep it as a harness-side
  proposed improvement: end-of-branch review still catches producer /
  consumer seams that checkpoint-local evaluation cannot see.
- **`review-rejection-hygiene`** recurred and validated the active rule.
  The `f6` rejection succeeded because it included a named runtime trace
  and a concrete contract contradiction.
- **`project-ide-tooling-drift`** recurred. Keep it in monitoring; the
  fix remains to restore generated files before commit rather than to add
  project CLAUDE rules.

### Skill Defect Flags

None from this task.
