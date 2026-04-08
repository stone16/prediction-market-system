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

| Pattern                              | Occurrences | Severity | Status     | First Task | Last Task   | Proposal |
|--------------------------------------|-------------|----------|------------|------------|-------------|----------|
| review-rejection-hygiene             | 1           | high     | proposed   | pms-v1     | pms-v1      | Proposal 1 |
| domain-math-piecewise                | 1           | high     | proposed   | pms-v1     | pms-v1      | Proposal 2 |
| lifecycle-cleanup-exit-paths         | 1           | high     | proposed   | pms-v1     | pms-v1      | Proposal 3 |
| document-instead-of-fix              | 1           | medium   | proposed   | pms-v1     | pms-v1      | Proposal 4 |
| cross-checkpoint-integration         | 1           | medium   | proposed   | pms-v1     | pms-v1      | Proposal 5 |
| magnitude-overrun-tests              | 1           | low      | monitoring | pms-v1     | pms-v1      | Proposal 6 |
| rule-conflict-precedence             | 1           | low      | proposed   | pms-v1     | pms-v1      | Proposal 7 |
| runtime-behaviour-vs-design-intent   | 1           | high     | proposed   | pms-v1     | pms-v1      | Principle |
| project-ide-tooling-drift            | 1           | low      | monitoring | pms-v1     | pms-v1      | Skill defect |
| tool-env-assumption                  | 1           | medium   | proposed   | pms-phase2 | pms-phase2  | phase2-P1 |
| stale-baseline                       | 1           | medium   | proposed   | pms-phase2 | pms-phase2  | phase2-P2 |
| pytest-marker-no-auto-deselect       | 1           | low      | proposed   | pms-phase2 | pms-phase2  | phase2-P3 |

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

### Monitoring (low severity, watching for recurrence)

- `magnitude-overrun-tests` — harness tuning suggestion, not a
  CLAUDE.md rule
- `project-ide-tooling-drift` — Pyright false positives, harness
  template defect

## Skill Defect Log

| Task   | Skill              | Severity | Defect                                                    | Status           |
|--------|--------------------|----------|-----------------------------------------------------------|------------------|
| pms-v1 | harness bootstrap  | low      | Pyright not configured to use uv-managed venv             | flagged for review |
| pms-v1 | harness evaluator  | low      | Magnitude gate conflates production and test LOC          | flagged for review |

## Task History

| Task        | Date       | CPs | Passed 1st | Iters | Reverts | Retro |
|-------------|------------|-----|------------|-------|---------|-------|
| pms-v1      | 2026-04-08 | 10  | 9          | 11    | 0       | [2026-04-08-pms-v1.md](./2026-04-08-pms-v1.md) |
| pms-phase2  | 2026-04-08 | 7   | 6          | 9     | 0       | [2026-04-08-pms-phase2.md](./2026-04-08-pms-phase2.md) |
