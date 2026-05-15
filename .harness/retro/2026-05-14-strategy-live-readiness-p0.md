---
task_id: strategy-live-readiness-p0
task_title: Strategy Live-Readiness P0
date: 2026-05-14
checkpoints_total: 7
checkpoints_passed_first_try: 7
total_eval_iterations: 7
total_commits: 9
reverts: 0
avg_iterations_per_checkpoint: 1.0
---

# Retro - strategy-live-readiness-p0

## TL;DR

Cleanest task execution on this project to date. All 7 checkpoints passed
first try, 9 commits, zero reverts, zero rule conflicts. The branch closed
every remaining P0 gap for paper-soak readiness: calibration wiring,
CLOB staleness fix, Kelly sizing, evidence-derived confidence, two real
forecasters (Rules + Statistical), and a strategy-agnostic exit monitor.
Diff: 38 files, +2953/-106 lines.

The cross-model peer review (Claude CLI, 3 rounds) surfaced 10 findings.
Three were accepted and fixed - including a **critical** boundary
limit-price crash (f1) that would have thrown `ValueError` in production
when `current_price` hit 0.0 or 1.0 - plus a semantic `expected_edge`
correction (f2) and dead config removal with regression guard (f8). The
remaining 7 rejections were all well-documented with `git show`, `rg`, and
test output evidence; the peer accepted every rejection without escalation.

Full-verify PASS on the first iteration: 1258 passed / 169 skipped, mypy
strict clean (425 files), lint-imports 9/0, coverage 82% >= 79%. This
breaks the pattern from the prior two tasks (`pms-markets-browser-v1` and
`pms-agent-strategy-plugins-v1`) where full-verify caught real defects
after review-loop consensus.

No new error patterns introduced. No new CLAUDE.md rule proposals. All 8
active promoted rules honored. PR opened at
https://github.com/stone16/prediction-market-system/pull/73.

---

## Observations

### What Worked Well

1. **Perfect 7/7 first-pass checkpoint execution.** Every checkpoint
   cleared its evaluator on the first attempt. Combined with zero reverts,
   this yields an average iteration count of 1.0 - the theoretical
   minimum. The only other 7/7 first-pass task in project history is
   `pms-strategy-aggregate-v1`.

2. **Review-loop caught a critical production crash path (f1).** The
   exit monitor's `build_exit_decision` passed `exit_signal.current_price`
   directly to `TradeDecision.limit_price`, which rejects values <= 0.0
   or >= 1.0. A market signal with `yes_price = 0.0` or `1.0` would
   have crashed the actuator path. The fix (clamp to [0.001, 0.999]) is
   exactly the boundary treatment the `domain-math-piecewise` promoted
   rule calls for. The checkpoint evaluator did not surface this because
   it tested the exit monitor's threshold logic, not the downstream
   `TradeDecision` constructor domain.

3. **Rejection discipline was precise and well-documented.** 7 of 10
   review-loop findings were rejected, and the peer accepted all 7
   without counter-argument or escalation. Each rejection included
   concrete verification evidence:
   - f3, f5: `git show origin/main:...` proving pre-existing main
     behavior
   - f4: test output proving calibration choice is acceptance-tested
   - f6, f7: source code examination proving the tuple-contract
     assumption was incorrect
   - f9: test output proving required-missing abstention works
   - f10: sensor adapter source proving token-specific signal semantics

   This is the review-rejection-hygiene promoted rule at its best.

4. **Full-verify PASS on first iteration.** This is the first time in 3
   tasks that full-verify did not catch a post-consensus defect. The
   `full-verify-catches-post-consensus` pattern (2 occurrences on prior
   tasks) did **not** recur. Contributing factors: (a) the review-loop
   caught the critical boundary issue before full-verify, (b) no
   dashboard/fullstack surface was touched (eliminating the `npm run build`
   gap), (c) all mypy additions were in focused test files that the
   evaluator gate-checked immediately.

5. **Architecture invariants preserved without tension.** All 8
   architecture invariants were honored by construction:
   - Exit monitor imports only `pms.config`, `pms.core.enums`,
     `pms.core.models` (Invariant 5, verified by lint-imports)
   - Exit decisions carry `strategy_id`/`strategy_version_id` from
     position (Invariant 3)
   - No outer-ring tables gained strategy columns (Invariant 8)
   - Exit signals re-enter the concurrent feedback web through
     `_enqueue_decision`, not a linear pipeline (Invariant 1)
   - Forecasters receive `FactorCompositionStep` projections, not the
     Strategy aggregate (Invariant 2)

6. **Frozen-dataclass + Decimal-at-boundary applied uniformly.** All new
   models (`ExitSignal`, `ExitTriggerConfig`) are `@dataclass(frozen=True)`.
   `KellySizer` arithmetic uses `Decimal(str(value))` internally and
   returns `float`. Exit monitor `_position_pnl_pct` is pure `float`
   comparison. The project convention was followed without inversion.

7. **Coverage trending up.** 82% this task, up from 81%
   (`pms-agent-strategy-plugins-v1`) and 80.01%
   (`pms-markets-browser-v1`). Still below the 85% harness default
   target, but the trajectory is positive.

---

### Cross-model Learning (review-loop)

The review-loop ran 3 rounds with Claude CLI as peer:

| Round | Findings | Accepted | Rejected | Outcome |
|-------|----------|----------|----------|---------|
| 1     | 10       | 3        | 7        | 3 fixes committed |
| 2     | 0        | -        | -        | Peer verified fixes + accepted rejections |
| 3     | 0        | -        | -        | Fresh final: CONSENSUS: Approved |

**Accepted findings analysis:**

| ID | Severity | Issue | Why checkpoint evaluator missed it |
|----|----------|-------|-----------------------------------|
| f1 | critical | `build_exit_decision` crashes at boundary prices 0.0/1.0 | CP07 evaluator tested threshold logic, not downstream `TradeDecision` constructor domain |
| f2 | minor | `expected_edge = pnl_pct / 100.0` makes stop-loss exits look like negative-alpha decisions | Semantic issue; tests checked numeric correctness, not whether the value was meaningfully interpreted downstream |
| f8 | suggestion | Dead top-level `calibration:` in `config.live-soak.yaml` ignored by PMSSettings | Config file not covered by CP01's strategy-level CalibrationSpec tests |

**Rejected findings analysis (all peer-accepted):**

| ID | Severity | Rejection reason | Category |
|----|----------|-----------------|----------|
| f3 | major | Pre-existing `live_allowed` policy in origin/main | out-of-scope |
| f4 | minor | Intentional calibration choice with green acceptance tests | design-choice |
| f5 | major | Pre-existing DDL bootstrap in origin/main | out-of-scope |
| f6 | minor | Tuple-contract misunderstanding; `ForecastResult[2]` is rationale, not model_id | incorrect-premise |
| f7 | minor | Same tuple-contract issue as f6 | incorrect-premise |
| f9 | minor | Optional posterior counts intentionally default to zero; required factors abstain | intentional-behavior |
| f10 | minor | Conflation of trade side with outcome side; signals are token-specific | incorrect-premise |

The 70% rejection rate with 100% peer acceptance is notable. Three of
the seven rejections (f6, f7, f10) were caused by the peer reviewer
making incorrect assumptions about the codebase's domain model - the
distinction between trade side vs. outcome side (f10) and the tuple
contract for different forecaster types (f6, f7). Two (f3, f5) were
correctly scoped out as pre-existing main behavior. These are not process
defects; they reflect the inherent information asymmetry between a
branch-level reviewer and the codebase owner.

---

### Error Patterns

No new error patterns identified this task. This is the first task in
project history where no new pattern tag was introduced.

---

### Rule Conflict Observations

All 7 checkpoint output summaries recorded no rule conflicts. This
continues the clean record from `pms-agent-strategy-plugins-v1`.

---

## Existing Patterns Confirmed

- **`review-rejection-hygiene`** (active) - HONORED. 7 rejections, all
  with `git show` / `rg` / `pytest` verification evidence. Peer accepted
  all without counter-trace. Textbook application.

- **`domain-math-piecewise`** (active) - REINFORCED. f1 (boundary
  limit-price crash at 0.0/1.0) is exactly the domain-edge case this
  rule covers. The rule says "test each regime + straddles"; the fix
  added parametrized boundary tests at both edges. Caught by review-loop,
  not by checkpoint evaluator - the evaluator's gate scope did not
  include downstream constructor domain validation.

- **`comments-are-not-fixes`** (active) - HONORED. All 3 accepted
  findings (f1, f2, f8) landed with code changes AND regression tests.
  f1: parametrized 0.0/1.0 boundary test. f2: `expected_edge=0.0`
  assertion. f8: `"\ncalibration:" not in yaml_text` guard.

- **`lifecycle-cleanup-exit-paths`** (active) - HONORED. Exit monitor
  handles: disabled state -> `None` immediately; zero `locked_usdc` ->
  `pnl_pct` returns 0.0 (division-by-zero guarded); zero `shares_held` ->
  `current_price` returns `None` (division-by-zero guarded); `opened_at`
  is `None` -> `_held_days` returns 0.

- **`runtime-behaviour-vs-design-intent`** (active) - HONORED.
  Evaluator and review-loop argued from `file:line` evidence throughout.
  Rejection verifications used `git show`, `rg`, and actual test output -
  never design-intent-only reasoning.

- **`fresh-clone-baseline-verification`** (active) - HONORED.
  Full-verify ran all canonical gates from a clean state: 1258 passed,
  mypy clean (425 files), lint-imports 9/0.

- **`integration-test-default-skip`** (active) - HONORED. 169 skipped
  tests are integration-marker / env-gated. Default `pytest -q` stays
  offline.

- **`piecewise-domain-functions`** (active) - see `domain-math-piecewise`
  above. Same rule, reinforced by f1.

- **`cross-checkpoint-integration`** (8 occurrences, proposed) - NOT
  recurring this task. All E2E data-flow audits matched. The 3 accepted
  review-loop findings were all within-checkpoint issues (exit_monitor.py
  and config.live-soak.yaml), not cross-CP seams. No increment.

- **`full-verify-catches-post-consensus`** (2 occurrences, proposed) -
  NOT recurring this task. Full-verify PASS on iter-1. This is a positive
  signal that the review-loop is covering more of the gate surface when
  the task scope is backend-only.

- **`generated-artifact-drift`** (3 occurrences, monitoring) - NOT
  recurring this task. No untracked WIP or generated files in the diff.

- **`magnitude-overrun-tests`** (3 occurrences, monitoring) - NOT
  recurring this task. Diff is +2953/-106 across 38 files with 7
  checkpoints; no single checkpoint triggered a magnitude exceedance
  flag.

---

## Recommendations

No new CLAUDE.md rule proposals. No new skill defects. No new pattern
tags introduced. This task validates the existing rule set without
requiring any additions.

The one reinforcement worth noting: the `domain-math-piecewise` promoted
rule continues to earn its weight. f1 (boundary limit-price crash) is the
canonical example of "test each regime + straddles" - the checkpoint
evaluator tested the happy-path threshold arithmetic but not the domain
boundary where the downstream constructor rejects the value. Review-loop
caught it; the rule's test discipline prevented a production crash path.

---

## Skill Defect Flags

None this task.

---

## Filed Issues

None. No new issue-ready items identified.

---

## Retro Metadata

- 7 checkpoints total; 7 passed first try
- 7 total evaluator iterations (7x1)
- 9 commits on the branch; 0 reverts
- Review-loop: 3 rounds; 10 findings (1 critical + 2 major + 6 minor +
  1 suggestion); 3 accepted and fixed, 7 rejected with peer acceptance;
  CONSENSUS: Approved
- Full-verify: iter-1 PASS; 1258 passed / 169 skipped; coverage 82% >=
  79%; mypy strict clean (425 files); lint-imports 9/0
- E2E: iter-1 PASS; 1255 passed / 169 skipped; all 8 success criteria
  verified; all 8 architecture invariants compliant; all cross-CP data
  flows matched
- Branch diff: 38 files, +2953 / -106 lines
- PR: https://github.com/stone16/prediction-market-system/pull/73
- New patterns introduced: none (first task with no new pattern tag)
- Recurring patterns confirmed: 8 active rules honored; no proposed
  pattern incremented
- Rule-conflict notes: 0/7 checkpoints reported any rule conflict
