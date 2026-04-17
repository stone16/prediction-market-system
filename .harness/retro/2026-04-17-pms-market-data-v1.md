---
task_id: pms-market-data-v1
task_title: "S1 — Outer-ring persistence, two-layer sensor, JSONL→PG unification"
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

This branch shipped 12 checkpoints with 11 first-pass approvals, 17
commits, and zero reverts. The dominant lesson was not checkpoint
failure but edge hardening: the branch was already functionally
complete at E2E time, then the cross-model review loop found a set of
runtime-boundary issues that were individually small but collectively
important. The fixes were real behaviour changes, not comment-level
papering over, and the existing promoted rules were sufficient to
close them without inventing a new project-wide policy.

## Observations

### What Worked Well

1. **Checkpoint isolation held.** Eleven of twelve checkpoints passed
   on the first evaluator round. The one checkpoint that iterated
   stayed bounded and did not force a revert or cross-checkpoint
   rewrite, which is the main signal that the spec slicing was
   appropriate for this branch.

2. **Cross-model review paid for itself.** The Claude review loop
   found 18 items after E2E, but almost all were seam hardening
   rather than design reversals: explicit Gamma outcome validation,
   bounded depth query inputs, token-scoped delta SQL, narrower retry
   exceptions, public lifecycle helpers, SQL escaping in the E2E
   seeding path, and hidden-tab stale-load suppression. This is the
   right shape of late feedback: small runtime defects discovered
   before merge, without reopening the architecture.

3. **Evidence-backed rejection discipline worked.** Four peer
   findings were rejected with concrete traces or benchmarks, and the
   peer accepted all four on the next round. That is the exact
   behaviour the promoted review-loop rule was meant to produce:
   disagreement stayed technical instead of drifting into taste or
   authority.

4. **Full-verify remained a real gate.** The branch did not stop at
   unit tests. Final evidence included default-suite pytest, mypy
   strict, dashboard build, integration coverage at 90%, and the
   PostgreSQL-backed Playwright depth flow. That made the docs-only
   baseline sync at the end straightforward rather than speculative.

### Error Patterns

1. **[category: cross-checkpoint-integration] Late review still found
   seam mismatches after checkpoint PASS + E2E PASS.** The issues were
   not broad architectural misses; they were producer/consumer edge
   mismatches across layers: route bounds vs store expectations,
   market/token SQL scope, public vs private lifecycle seams, and
   visibility-state interactions across polling hooks and page
   clients. This matches the earlier `cross-checkpoint-integration`
   pattern from `pms-v1`: local correctness is not sufficient proof
   that the last layer boundary is hardened.

2. **[category: stale-baseline] Verified counts drifted after
   review-loop changes.** The review loop added tests and moved the
   verified baseline to `93 passed, 32 skipped`, but the committed
   docs still said `91 passing, 32 skipped` until a final sync commit.
   This is a direct recurrence of the already-active stale-baseline
   rule, not a new category.

3. **[category: private-helper-boundary-drift] Temporary success
   hid interface debt.** Two separate fixes promoted this pattern:
   API/integration code reached through `Runner` internals for pool
   management, and the JSONL migration script imported private storage
   helpers. Both worked in the short term, but both had to be replaced
   with explicit public seams once the peer review forced the boundary
   question. The repeated shape suggests a reusable caution, but only
   at monitoring level for now.

### Rule Conflict Observations

None. The branch had to choose priorities several times, but each
choice was consistent with existing active rules: runtime evidence
over design intent, behavioural fixes over comments, and explicit
verification when rejecting a review finding.

## Recommendations

### Proposal 1: Promote a public seam once a second caller needs it

- **Pattern**: private-helper-boundary-drift
- **Severity**: medium
- **Status**: Monitoring
- **Root cause**: This branch solved two immediate problems by
  reaching through private internals that were "close enough" to work
  locally. Review-loop then forced those call sites to either become
  legitimate public APIs or stop existing. The cost was small here,
  but the pattern scales badly because private helper reach-throughs
  bypass the normal compatibility surface.
- **Drafted rule text**:
  ```
  ## Promote The Seam, Not The Reach-Through (🟡 MONITORING)

  If a second module, test, or script needs a private helper or
  internal field to do real work, stop reaching through the private
  boundary and promote an explicit public seam instead.

  Examples:
  - add a public lifecycle method instead of mutating a private field
  - expose a supported batch-insert helper instead of importing a
    private underscore function

  Reason: a private reach-through can look harmless in the first
  caller, but once multiple callers depend on it the codebase has
  already declared it to be an interface without documenting or
  testing it as one.
  ```
- **Issue-ready**: false

### Existing Rule Validation

- `cross-checkpoint-integration` remains a harness-side proposal worth
  keeping active in the retro index. This task is the second concrete
  example that review after E2E still catches real seam defects.
- `stale-baseline` remains active and was validated again by the final
  docs-sync commit.
- `review-rejection-hygiene` remains active and worked exactly as
  intended on the four evidence-backed rejections.

### Skill Defect Flags

None from the task itself. The only operational wobble in this
session was a GitHub connector handshake failure and a slow retro CLI
wrapper, but neither changed the branch outcome or produced a bad
artifact in-repo.

### No New Issue-Ready Proposals

This branch does not justify a new auto-filed issue. The strongest
new observation, `private-helper-boundary-drift`, appeared twice in a
single task but has not yet recurred across tasks. Monitoring is the
right status.

## Retro Metadata

- 12 checkpoints total
- 11 checkpoints passed first try
- 14 total evaluator iterations
- 17 commits, 0 reverts
- Review-loop: 18 findings, 14 accepted fixes, 4 evidence-backed
  rejections accepted by the peer, final consensus `Approved`
- Final verification:
  - `uv run pytest -q` → `93 passed, 32 skipped`
  - `uv run mypy src/ tests/ --strict` → clean
  - integration coverage run → `125 passed`, `TOTAL 90%`
  - Playwright depth flow → `1 passed`
