---
task_id: pms-known-open-questions
task_title: "Resolve README Known Open Questions"
date: 2026-04-16
checkpoints_total: 5
checkpoints_passed_first_try: 5
total_eval_iterations: 5
total_commits: 5
reverts: 0
avg_iterations_per_checkpoint: 1.0
---

# Retro — pms-known-open-questions

## Observations

### Error Patterns

None in the product implementation. All five checkpoints passed on the
first evaluator round, and the final verification suite stayed green.

### Rule Conflict Observations

No rule conflicts were recorded in checkpoint summaries.

### What Worked Well

1. **README questions became executable acceptance criteria.** Each
   open question mapped to a bounded implementation checkpoint with a
   direct regression test: feedback bounds, paper-mode executable depth,
   signal-time metrics, degenerate calibration copy, and documentation
   cleanup.

2. **Paper-mode behavior was fixed at the data boundary.** Deriving
   synthetic orderbook depth from Gamma liquidity keeps the actuator and
   runner behavior realistic without adding special-case fill logic in
   downstream code.

3. **Timestamp semantics moved into the evaluation boundary.** The spool
   now makes `recorded_at` follow the signal source time, preserving
   backtest chronology while keeping the scorer itself focused on score
   construction.

4. **The UI edge case got browser-level proof.** The calibration-copy
   checkpoint included a Playwright route fixture for the one-probability
   case, so the README claim is backed by an actual rendered-dashboard
   assertion rather than only component reasoning.

5. **Per-checkpoint commits stayed reviewable.** Five commits landed for
   five checkpoints, with no reverts and no cleanup commit required after
   full verification.

## Recommendations

### Upgrade to Rule

None. This task reinforced existing project practice: when documentation
contains unresolved runtime questions, close them with code, tests, and
then documentation updates in that order.

### Upgrade to Principle

None.

### Rule Conflict Resolution

None.

### Skill Defect Flags

None filed for this repo. During execution, the preferred Claude CLI
agent path hit provider-side failures, so Codex sub-agents/manual harness
artifacts were used as a fallback. That is an orchestration environment
issue rather than a project defect.
