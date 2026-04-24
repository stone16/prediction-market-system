# Harness Retro Index

Central frequency table for error patterns and pending proposals across all
harness tasks. Updated by the Retro agent at the end of each task.

## How To Read

- **Pattern**: tag from `[category: tag]` observations in individual retros
- **Occurrences**: number of task retros where this pattern was observed
- **Status**:
  - `observation` — seen once, monitoring
  - `monitoring` — seen 2x, watching for escalation
  - `proposed` — seen 3+ times OR high severity, rule drafted
  - `active` — rule is live in CLAUDE.md
  - `retired` — rule retired after the underlying issue was resolved
- **First/Last Task**: first and most-recent task IDs where the pattern was
  observed

## Frequency Table

Status legend: `observation` (1×) → `monitoring` (2×) → `proposed` (3+× or
high severity, rule drafted) → `active` (rule promoted to project
CLAUDE.md, contributors expected to follow) → `retired` (resolved).

| Pattern                              | Occurrences | Severity | Status     | First Task | Last Task   | Proposal |
|--------------------------------------|-------------|----------|------------|------------|-------------|----------|
| review-rejection-hygiene             | 2           | high     | active     | pms-v1     | pms-strategy-aggregate-v1 | Proposal 1 |
| domain-math-piecewise                | 1           | high     | active     | pms-v1     | pms-v1      | Proposal 2 |
| lifecycle-cleanup-exit-paths         | 1           | high     | active     | pms-v1     | pms-v1      | Proposal 3 |
| document-instead-of-fix              | 1           | medium   | active     | pms-v1     | pms-v1      | Proposal 4 |
| cross-checkpoint-integration         | 7           | medium   | proposed   | pms-v1     | pms-markets-browser-v1 | Proposal 5 |
| magnitude-overrun-tests              | 2           | low      | monitoring | pms-v1     | pms-factor-panel-v1 | Proposal 6 |
| rule-conflict-precedence             | 1           | low      | active     | pms-v1     | pms-v1      | Proposal 7 |
| runtime-behaviour-vs-design-intent   | 1           | high     | active     | pms-v1     | pms-v1      | Principle |
| project-ide-tooling-drift            | 2           | low      | monitoring | pms-v1     | pms-strategy-aggregate-v1 | Skill defect |
| tool-env-assumption                  | 1           | medium   | active     | pms-phase2 | pms-phase2  | phase2-P1 |
| stale-baseline                       | 4           | medium   | active     | pms-phase2 | pms-research-backtest-v1 | phase2-P2 |
| pytest-marker-no-auto-deselect       | 1           | low      | active     | pms-phase2 | pms-phase2  | phase2-P3 |
| lockfile-drift-on-optional-dep       | 1           | low      | observation| pms-phase3 | pms-phase3  | phase3-P1 |
| private-helper-boundary-drift        | 1           | medium   | observation| pms-market-data-v1 | pms-market-data-v1 | Observation |
| skipped-full-verify-pre-merge        | 1           | medium   | observation| pms-factor-panel-v1 | pms-factor-panel-v1 | Observation |
| empty-harness-phase-artifacts        | 1           | low      | observation| pms-factor-panel-v1 | pms-factor-panel-v1 | Observation |
| generated-artifact-drift             | 2           | low      | monitoring | pms-research-backtest-v1 | cathedral-v1 | Observation |
| npm-build-gate-gap                   | 1           | high     | proposed   | pms-markets-browser-v1 | pms-markets-browser-v1 | Proposal markets-P1 |
| coverage-below-harness-default       | 1           | medium   | proposed   | pms-markets-browser-v1 | pms-markets-browser-v1 | Proposal markets-P2 |

Active rules are codified in `/CLAUDE.md` at the repo root (Phase 3D).
Cross-checkpoint integration remains `proposed` because the harness-side
fix (an evaluator integration trace step) belongs in the orchestrator
template, not in project CLAUDE.md.

`npm-build-gate-gap` is `proposed` on first occurrence because severity is
HIGH (deployment-breaking build failure). The fix is both a project-level
gate documentation addition and a harness review-loop skill defect (SD-1).

`coverage-below-harness-default` is `proposed` on first occurrence because
the user explicitly requested an issue and severity is medium. The
`.harness/config.json` threshold of 79 is a temporary accommodation for
issue #22; target is 85 (the Harness default).

## Pending Proposals

### Issue-Ready (status=proposed, severity>=medium)

These are ready for the Orchestrator to auto-create GitHub issues.

1. **Proposal 1** — Review-loop rejection discipline (high) — `pms-v1`
2. **Proposal 2** — Piecewise-domain verification for non-linear
   functions (high) — `pms-v1`
3. **Proposal 3** — Lifecycle cleanup on all exit paths (high) —
   `pms-v1`
4. **Proposal 4** — Comments are not fixes (medium) — `pms-v1`
5. **Proposal 5** — Cross-checkpoint integration trace after final
   checkpoint (medium) — `pms-v1`
6. **Principle** — Runtime behaviour > design intent (high) — `pms-v1`
7. **Proposal 7** — Codify commit-message precedence (low, but
   concrete draft ready) — `pms-v1`
8. **phase2-P1** — Verify isolated-env tooling assumptions before
   wrapping (medium) — `pms-phase2`
9. **phase2-P2** — Fresh-clone baseline verification (medium) —
   `pms-phase2`
10. **phase2-P3** — Pytest integration markers must be env-gated
    (low, recommended) — `pms-phase2`
11. **phase3-P1** — Stage lockfile changes with pyproject edits
    (low, observation) — `pms-phase3`
12. **markets-P1** — Add `cd dashboard && npm run build` to canonical gate
    list for all dashboard-touching branches (high) — `pms-markets-browser-v1`
13. **markets-P2** — Increase `.harness/config.json` coverage threshold from
    79 to 85%; user-requested issue for coverage improvement
    (medium) — `pms-markets-browser-v1`

### Monitoring (low severity, watching for recurrence)

- `magnitude-overrun-tests` — harness tuning suggestion, not a
  CLAUDE.md rule
- `project-ide-tooling-drift` — Pyright false positives, harness
  template defect
- `private-helper-boundary-drift` — temporary reach-through to private
  helpers or fields until review pressure forces a public seam
- `skipped-full-verify-pre-merge` — feature branch merged to `main`
  without a formal `full-verify/` artefact; environmental drift
  (e.g. stale `pms_test` schema) surfaces post-merge instead of
  pre-merge
- `empty-harness-phase-artifacts` — `spec-review/` and `e2e/`
  directories created but not populated during the task; harness
  flow was compressed relative to S1 / S2
- `generated-artifact-drift` — automated checkpoint or review-loop
  commits stage generated local artifacts (`.coverage`, framework
  stubs, Playwright evidence) that are not intended product changes

### Proposed (issue-ready, rule text drafted)

- `npm-build-gate-gap` — `npm run build` absent from review-loop gate
  list for fullstack/dashboard tasks; Next.js prerender errors are
  silent in lint/Vitest/Playwright but hard-fail the production build.
  Proposed rule: add `(cd dashboard && npm run build)` to CLAUDE.md
  canonical gates. Also a harness skill defect (SD-1).
- `coverage-below-harness-default` — project `.harness/config.json`
  `coverage_threshold` is 79 vs the harness default 85; measured
  coverage is 80.01%. Proposed rule: raise threshold to 85 and add
  contributor guidance. User explicitly requested an issue.

## Skill Defect Log

| Task   | Skill              | Severity | Defect                                                    | Status           |
|--------|--------------------|----------|-----------------------------------------------------------|------------------|
| pms-v1 | harness bootstrap  | low      | Pyright not configured to use uv-managed venv             | flagged for review |
| pms-v1 | harness evaluator  | low      | Magnitude gate conflates production and test LOC          | flagged for review |
| pms-research-backtest-v1 | harness review-loop | low | Preflight checkpoint commit staged generated artifacts (`.coverage`, `next-env.d.ts`, Playwright evidence PNG) | flagged for review |
| pms-markets-browser-v1 | harness review-loop | medium | SD-1: Review-loop verification gate list for fullstack/dashboard tasks does not include `cd dashboard && npm run build`; Next.js prerender errors are invisible to lint/Vitest/Playwright but surface as hard failures in `next build` | flagged for review |

## Task History

| Task        | Date       | CPs | Passed 1st | Iters | Reverts | Retro |
|-------------|------------|-----|------------|-------|---------|-------|
| pms-v1      | 2026-04-08 | 10  | 9          | 11    | 0       | [2026-04-08-pms-v1.md](./2026-04-08-pms-v1.md) |
| pms-phase2  | 2026-04-08 | 7   | 6          | 9     | 0       | [2026-04-08-pms-phase2.md](./2026-04-08-pms-phase2.md) |
| pms-phase3  | 2026-04-09 | 4   | 4          | 4     | 0       | [2026-04-09-pms-phase3.md](./2026-04-09-pms-phase3.md) |
| pms-market-data-v1 | 2026-04-17 | 12  | 11         | 14    | 0       | [2026-04-17-pms-market-data-v1.md](./2026-04-17-pms-market-data-v1.md) |
| pms-strategy-aggregate-v1 | 2026-04-17 | 7   | 7          | 7     | 0       | [2026-04-17-pms-strategy-aggregate-v1.md](./2026-04-17-pms-strategy-aggregate-v1.md) |
| pms-factor-panel-v1 | 2026-04-18 | 8   | 7          | 9     | 0       | [2026-04-18-pms-factor-panel-v1.md](./2026-04-18-pms-factor-panel-v1.md) |
| pms-research-backtest-v1 | 2026-04-20 | 13  | 11         | 15    | 0       | [2026-04-20-pms-research-backtest-v1.md](./2026-04-20-pms-research-backtest-v1.md) |
| cathedral-v1 | 2026-04-23 | 12  | 11         | 13    | 0       | [2026-04-23-cathedral-v1.md](./2026-04-23-cathedral-v1.md) |
| pms-markets-browser-v1 | 2026-04-24 | 14  | 13         | 16    | 0       | [2026-04-24-pms-markets-browser-v1.md](./2026-04-24-pms-markets-browser-v1.md) |
