---
task_id: pms-agent-strategy-plugins-v1
task_title: Agent-aware strategy plugin architecture
date: 2026-04-28
checkpoints_total: 7
checkpoints_passed_first_try: 6
total_eval_iterations: 8
total_commits: 20
reverts: 0
avg_iterations_per_checkpoint: 1.1
---

# Retro — pms-agent-strategy-plugins-v1

## TL;DR

The agent-aware strategy plugin architecture landed end-to-end across seven
checkpoints. Six of seven passed first try; CP03 (single-leg execution
planner) took a second iteration. Twenty commits, zero reverts, no rule
conflicts recorded across any checkpoint. Final E2E gates: 827 passed / 161
skipped, mypy strict clean across 330 files, `lint-imports` 8/0 (including
the new CP06 contract `Strategy plugins: no actuator, controller, or venue
adapter imports`). The default-path diff over `runner.py`, `api.py`,
`main.py`, `controller/`, `sensor/`, `actuator/` is empty — the existing
controller-driven cybernetic feedback web is byte-identical to base, and
the bridge is disabled-by-default with no production wiring.

The cross-model peer review (Codex × claude, 2 rounds) accepted **13/13
findings** — a perfect cross-model agreement signal. Findings spanned
artifact immutability (`UPSERT → DO NOTHING`), baseline migration
isolation, run-level artifact batching, judgement payload semantics for
unsupported baskets, intent_key collision on multi-order single-leg
plans, value-shaped secret detection, FK exception documentation, stale
README pass counts, Python metadata drift, and a dollar-quote-aware
schema statement splitter.

Full-verify iter-1 then caught two **hard failures that survived
review-loop consensus**: (a) two unreachable negative `isinstance`
assertions in `tests/unit/test_ripple_strategy_plugin.py` broke
`mypy --strict`, and (b) the baseline statement splitter handled `'…'`
and `$tag$ … $tag$` regions but not `--` line comments or `/* … */`
block comments, which masked real `;` characters inside SQL comments
and corrupted DDL on real PostgreSQL. Both were fixed in `28645e0`
with a regression test that would have caught F2 in review. Iter-2
closed `PASS_WITH_WARNINGS` (coverage 81%, two soft warnings on
untracked WIP and a pre-existing SOCKS-proxy environmental issue).

No new CLAUDE.md rule proposal is required. `cross-checkpoint-
integration` recurs (8th occurrence) and `generated-artifact-drift`
recurs (3rd occurrence). Two patterns are introduced as observations
worth tracking on first sight: **migration-isolation-test-discipline**
and **artifact-immutability-discipline**. The full-verify-after-consensus
result is also worth flagging: it is the second time on recent branches
that full-verify caught a real defect after review-loop returned
`CONSENSUS: Approved`, suggesting the review-loop verification gate
list still under-covers what full-verify finds (this round it was
mypy-on-tests and SQL comment lexing, not `npm run build`).

---

## Observations

### What Worked Well

1. **Six of seven checkpoints first-pass with zero reverts and no rule
   conflicts.** Only CP03 needed a second iteration. Twenty commits
   across the branch with a linear graph; the harness's per-checkpoint
   atomic-commit discipline held throughout.

2. **Cross-model review-loop achieved consensus on 13/13 findings in 2
   rounds.** Every Codex finding was accepted by claude with a code
   change and a regression test, and the round-2 fresh-final pass
   returned `0` real issues. There were no rejected-then-resolved or
   escalated findings — the highest-quality cross-model signal observed
   on this project so far.

3. **Disabled-by-default discipline is structurally enforced, not just
   asserted.** `agent_strategy_runtime_enabled: bool = False` defaults
   the bridge off; the bridge raises `RuntimeError` *before* any
   registry lookup, artifact write, or planner call when the flag is
   false; production wiring (`runner.py`, `api.py`, `main.py`, startup
   hooks) does **not** import the bridge. Verified by grep and an empty
   `git diff --stat` over the runtime layers. This is a textbook
   application of "lifecycle cleanup on all exit paths" extended to
   "feature gating on all entry paths".

4. **Invariant 1, 5, and 8 preserved by construction.** The branch
   added a new execution-planning seam without touching Sensor,
   Actuator, or the runtime feedback web. The new ripple plugin lives
   under `pms.strategies.ripple` and the new import-linter contract
   forbids actuator/controller/venue-client imports — promoted from a
   spec line into a machine-checked contract.

5. **Frozen-dataclass + Decimal-at-boundary applied uniformly.** Every
   new value object across CP02–CP07 is `@dataclass(frozen=True,
   slots=True)`, including `StrategyContext`, `StrategyJudgement`,
   `TradeIntent`, `BasketIntent`, `PlannedOrder`, `ExecutionPlan`,
   `StrategyJudgementArtifact`, `StrategyExecutionArtifact`,
   `RippleStrategyModule`, `AgentStrategyRuntimeBridge`, and
   `StrategyRunResult`. Money/price arithmetic in `planner._edge_after_cost`
   and `_violates_limit` converts via `Decimal(str(value))` and returns
   `float` at the entity boundary — the project convention applied
   without inversion.

6. **Inner-ring artifact storage preserved Invariant 8.** Alembic
   `0014_strategy_artifacts.py` adds artifact tables only; no
   outer-ring (`markets`, `tokens`, `book_*`, `trades`) or middle-ring
   (`factor_*`) table gained a strategy column. SQL `CHECK
   (strategy_id <> '' AND strategy_version_id <> '')` constraints lock
   in the non-empty rule at the storage layer, complementing the Python
   `_require_strategy_identity` validators at construction time.

7. **README and architecture-invariants document updated as a first-class
   deliverable.** CP01 landed before any code, set the public boundary
   ("agents may propose, judge, and explain, but cannot submit orders,
   override risk, or override reconciliation"), and that boundary
   guided every later CP's acceptance criteria.

---

### Cross-model Learning (review-loop)

The Codex × claude review-loop ran 2 rounds and produced 13 real
findings — every one accepted with a code change and a regression
test. The pattern of findings is informative because it shows what
checkpoint-local evaluators routinely miss when artifact-shape and
audit-trail logic span multiple checkpoints:

| # | Finding (severity) | Why checkpoint-local missed it |
|---|---|---|
| f1 | Artifact `ON CONFLICT DO UPDATE` rewrites durable audit rows (major) | CP05 evaluator validated insert path; "durability under retry" only visible at branch level |
| f2 | Baseline downgrade table list incomplete (major) | CP05 evaluator validated upgrade; downgrade list audit only meaningful after CP05 lands |
| f3 | Integration test cannot prove migration 0014 actually ran (major) | CP05 schema test asserted final shape, not delta; migration-isolation only visible end-to-end |
| f4 | Approved-judgement payload included unsupported basket runtime (major) | CP05 wrote artifacts, CP07 added basket-runtime gate; the payload-semantics conflict only emerges when both land |
| f5 | `_execution_artifact` could crash on empty evidence refs (minor) | CP07 evaluator covered happy path; structural fallback only visible across CP05↔CP07 |
| f6 | `intent_key` collision on multi-order single-leg plans (minor) | CP03 + CP07 each individually correct; collision only visible when bridge composes them |
| f7 | Substring-based secret detector flagged benign judgement text (minor) | CP06 fixture text triggered CP05 detector; cross-CP false positive |
| f8 | Artifact tables lacked FK to `strategy_versions` without doc (minor) | Invariant 3 exception only meaningful once CP05 + CP07 unknown-version path exists |
| f9 | README hard-coded stale baseline test counts (minor) | CP01 wrote counts; later CPs grew the suite; drift only visible at branch close |
| f10 | `markets.venue` CHECK admits reserved `kalshi` (minor) | Pre-existing schema; reviewers noticed during artifact-table audit |
| f11 | Python version metadata drift across `pyproject` / `mypy` / CI (suggestion) | Cross-file metadata; no single CP owns the alignment |
| f12 | Per-artifact transaction not atomic per run (suggestion) | CP05 + CP07 each correct on their own; run-level atomicity only visible across them |
| f13 | Naive `;` split could corrupt dollar-quoted schema blocks (minor, r2) | Round-1 fix used naive split; round-2 hardened to dollar-quote-aware splitter |

**Round-by-round breakdown:**

- Round 1: 12 findings, all accepted.
- Round 2: 1 finding (f13), accepted — the round-1 fix to the baseline
  splitter introduced a naive `split(";")` that the peer caught and
  hardened.
- Fresh-final: 0 real issues.

This is the first occurrence in this project of a 100% cross-model
acceptance rate over a 13-finding review. The signal: when the project
has a strong invariant frame (architecture-invariants.md +
import-linter contracts) and the code respects it, peer review converges
fast and finds real seams rather than style debates.

---

### Full-verify Learning (post-consensus failures)

Despite review-loop consensus on `28645e0`'s predecessor, full-verify
iter-1 caught **two hard failures** that the review-loop verification
checklist did not surface:

1. **F1 — `mypy --strict` on test files**:
   `tests/unit/test_ripple_strategy_plugin.py:106-107` had two
   unreachable negative `isinstance` assertions:
   ```python
   assert not isinstance(intent, TradeDecision)
   assert not isinstance(intent, OrderState)
   ```
   These are structurally disjoint type relationships that mypy can
   already prove, so `mypy --strict` flagged them as unreachable. The
   review-loop did run mypy, but these test-only assertions were added
   after the round-1 + round-2 acceptance churn, and the CP07
   evaluator's gate run did not catch them because the assertions were
   appended in a later cleanup commit.

2. **F2 — Baseline splitter handled dollar-quoted bodies but not SQL
   comments**: The round-2 hardening of `_split_sql_statements` taught
   the splitter to skip `'…'` and `$tag$ … $tag$` regions, but
   `schema.sql` also contains `-- line comments` and `/* block comments
   */` that contain `;` characters inside their text. Real
   PostgreSQL execution split them naively, producing
   `psycopg.errors.SyntaxError: syntax error at or near "replay"`
   across the integration suite. Iter-2's fix extended the splitter
   with two new branches for `--` and `/* */`, plus a regression test
   `test_baseline_statement_splitter_ignores_semicolons_in_sql_comments`
   that feeds both forms through the splitter. Verified clean on real
   PostgreSQL: `159 passed` on the committed-branch-only integration
   re-run.

This is the **second time on recent branches** that full-verify caught
a real defect after review-loop returned `CONSENSUS: Approved`. The
prior occurrence was `npm-build-gate-gap` on `pms-markets-browser-v1`
(SD-1). Different surface (mypy-on-tests + SQL comment lexing here vs.
`next build` prerender there), same shape: review-loop's gate list is
narrower than full-verify's discovery list. The promoted rule
`runtime behaviour > design intent` plus the existing harness skill
defect SD-1 already cover the principle; this round adds a second
concrete witness.

Iter-2 closed at `PASS_WITH_WARNINGS` (coverage 81%, mypy clean,
lint-imports 8/0, both blockers verified fixed).

---

### Warning Handling (full-verify iter-2 soft warnings)

Two soft warnings were correctly classified as non-task issues and
intentionally kept out of the PR scope:

- **W1 — `tests/integration/test_alembic_cp01.py`**: untracked WIP
  test (`git ls-files` returns 0 rows) carried over from a previous
  task. It asserts `alembic heads` is empty, but the head is now
  `0014_strategy_artifacts (head)` after this task's migration. Not a
  product change.
- **W2 — `dashboard/next-env.d.ts`**: generated drift, not a product
  edit.
- Plus a pre-existing `python-socks` env failure on
  `tests/integration/test_market_data_sensor.py::test_market_data_sensor_live_polymarket_writes_first_snapshot_within_five_seconds`,
  unchanged on this branch (`git log --oneline main..HEAD` returns
  nothing for that file). Environmental constraint of the evaluator
  shell, not a regression.

The committed-branch-only re-run confirmed `159 passed, 0 failed` on
real PostgreSQL. This is `generated-artifact-drift` recurring at low
severity (3rd occurrence; remains monitoring) — the iter-2 evaluator
handled it correctly by reporting it as `PASS_WITH_WARNINGS` rather
than letting it block.

---

### Error Patterns

#### 1. [category: cross-checkpoint-integration] (8th occurrence)

The 13-finding review-loop result is the canonical evidence: every one
of those findings is a branch-level seam that checkpoint-local
evaluators cleared. Six of the thirteen (f1, f2, f3, f4, f6, f12)
required composing CP02 ↔ CP05 ↔ CP07 to even surface — for example,
"approved judgement payload includes unsupported basket runtime"
(f4) needs CP05's payload schema, CP07's basket gate, and the bridge's
artifact-write order to all exist before the inconsistency is
visible.

This is the same pattern recorded in pms-v1, pms-market-data-v1,
pms-strategy-aggregate-v1, pms-factor-panel-v1,
pms-research-backtest-v1, cathedral-v1, and pms-markets-browser-v1.
Recurrence count is now 8. Status remains **proposed**; the existing
harness-side proposal (end-of-branch integration trace step) is the
right fix and does not belong in project CLAUDE.md.

#### 2. [category: generated-artifact-drift] (3rd occurrence)

Full-verify iter-2 surfaced two pieces of generated/WIP drift
(`tests/integration/test_alembic_cp01.py` and `dashboard/next-env.d.ts`)
that were correctly excluded from PR scope. Third occurrence on
recent branches. Stays in monitoring; no rule proposal yet.

#### 3. [category: migration-isolation-test-discipline] (NEW, 1st occurrence, MEDIUM)

Finding f3 (`Integration test cannot tell whether migration 0014
actually ran`) is a discipline that emerged from this branch. The CP05
schema test originally asserted the post-`upgrade head` shape, which
is true regardless of whether `0014_strategy_artifacts` ran or whether
the artifact tables were already present in the baseline. The fix —
upgrade to `0013`, assert the artifact tables are absent, then upgrade
to head and assert the new constraints — proves migration-N actually
performed the delta. This is a generalizable discipline for any
schema-changing migration test: the test must straddle the migration
boundary, not just the final state.

First occurrence on this project; severity medium. Status:
**observation** (single occurrence). Not yet rule-worthy, but worth
tagging so a recurrence escalates it.

#### 4. [category: artifact-immutability-discipline] (NEW, 1st occurrence, MEDIUM)

Finding f1 (`Artifact UPSERT silently rewrites durable audit rows`)
captures a sub-class of issues specific to inner-ring artifact tables:
`ON CONFLICT DO UPDATE` is wrong for durable audit rows because a
retry must not silently rewrite the original. `ON CONFLICT DO
NOTHING` plus an assertion that the inserted row is unchanged is the
correct shape. This is closely related to Invariant 3 (every product
row is tagged immutably) but applies to the row payload, not the tag.

First occurrence on this project; severity medium. Status:
**observation**. If a future task lands inner-ring artifact storage
and reaches for `ON CONFLICT DO UPDATE`, this becomes a recurring
pattern worth promoting.

#### 5. [category: full-verify-catches-post-consensus] (2nd occurrence, MEDIUM)

Iter-1 of full-verify caught two hard failures (mypy unreachable
assertions on tests, baseline splitter SQL comments) after the
review-loop returned `CONSENSUS: Approved`. The previous occurrence
was `npm-build-gate-gap` on `pms-markets-browser-v1`. Different
surfaces, same shape: the review-loop verification gate list under-
covers what full-verify discovers.

Second occurrence; severity medium. The existing skill defect SD-1
(`Review-loop gate list missing 'npm run build' for fullstack tasks`)
is one specific manifestation; this round suggests a more general
principle: **the review-loop verification gate list should mirror the
full-verify discovery output for the touched surface**. If
full-verify discovery names a gate, review-loop should run it before
claiming consensus.

Status: **monitoring** (2nd occurrence). Worth proposing a harness-side
expansion of SD-1 rather than a project CLAUDE.md rule.

---

### Rule Conflict Observations

All seven checkpoint output summaries recorded `Rule Conflict Notes:
- None.` This is the cleanest rule-conflict record on the project
since pms-strategy-aggregate-v1. No CLAUDE.md clarification is
warranted.

---

## Recommendations

### Proposal 1: Document migration-isolation test discipline

- **Pattern**: migration-isolation-test-discipline
- **Severity**: medium
- **Status**: Monitoring (1st occurrence)
- **Root cause**: Schema integration tests that only assert
  post-`upgrade head` shape cannot prove that the new migration N
  actually performed the delta — the assertion would also pass if the
  shape was already present in baseline N-1. A test that upgrades to
  N-1, asserts target objects are absent, then upgrades to head and
  asserts the new constraints, proves migration-N's behavior.
  Surfaced as f3 in this branch; not yet recurring, but a clean
  generalizable discipline worth flagging.
- **Drafted rule text** (deferred until 2nd occurrence):
  ```
  **Migration-isolation tests.** Schema integration tests for any new
  Alembic migration must straddle the migration boundary: upgrade to
  the prior revision, assert the new objects are absent, then upgrade
  to head and assert the new constraints. Asserting only the final
  shape cannot prove the migration actually ran.
  ```
- **Issue-ready**: false (single occurrence; observation only)

### Proposal 2: Document artifact-immutability discipline for inner-ring stores

- **Pattern**: artifact-immutability-discipline
- **Severity**: medium
- **Status**: Monitoring (1st occurrence)
- **Root cause**: Durable audit rows in inner-ring artifact tables
  must not be silently rewritten on retry. `ON CONFLICT (artifact_id)
  DO UPDATE` is structurally wrong for audit semantics; `DO NOTHING`
  plus an assertion that the existing row is unchanged is correct.
  Surfaced as f1 in this branch; closely related to Invariant 3 but
  applies to row payload rather than tag.
- **Drafted rule text** (deferred until 2nd occurrence):
  ```
  **Inner-ring artifact rows are immutable on conflict.** Use `ON
  CONFLICT (<pk>) DO NOTHING` for durable audit/artifact tables. A
  retry must not silently rewrite the original row. Tests should
  assert the inserted row is unchanged after a conflicting retry.
  ```
- **Issue-ready**: false (single occurrence; observation only)

### Proposal 3: Expand SD-1 — review-loop gate list should mirror full-verify discovery

- **Pattern**: full-verify-catches-post-consensus
- **Severity**: medium
- **Status**: Proposed (2nd occurrence)
- **Root cause**: This is the second branch in a row where full-verify
  iter-1 caught a real defect after review-loop returned `CONSENSUS:
  Approved`. The first surface was `npm run build` (Next.js prerender
  errors), now it is mypy-on-tests + SQL comment lexing in the
  baseline migration. The general principle: review-loop's
  verification gate list under-covers what full-verify's discovery
  step independently scans. Existing skill defect SD-1 captures the
  Next.js manifestation; the broader fix is for the harness review-loop
  protocol to consume `discovery.md`'s gate set as input rather than
  re-deriving a smaller set.
- **Drafted text** (harness-side, not project CLAUDE.md):
  ```
  Review-loop verification step must run every gate named by the
  current task's full-verify `discovery.md` for the touched surface,
  not a hand-curated subset. If full-verify discovery names a gate
  (e.g. `npm run build`, `mypy --strict` on tests, `lint-imports`,
  `alembic upgrade head` on real PostgreSQL), the review-loop must run
  it before declaring consensus. Skipping a discovery-named gate is a
  protocol violation.
  ```
- **Issue-ready**: true
- **target_repo**: harness

### Proposal 4: Recalibrate `M`-class magnitude budget for paired-payload checkpoints

- **Pattern**: magnitude-overrun-tests
- **Severity**: low
- **Status**: Monitoring (3rd occurrence — was monitoring, stays
  monitoring with no escalation)
- **Root cause**: CP07 added 705 insertions vs. the `M` 3× cap of
  450. The CP07 evaluator audited line-by-line and confirmed every
  block ties to a stated acceptance criterion (paired payload
  serializers, registry resolution, artifact write ordering, queue
  enqueue, and tests for each). This is the third occurrence of a
  legitimate magnitude exceedance on a checkpoint that requires paired
  payload serializers + corresponding tests; the rule "magnitude is a
  signal, not a verdict" has been honored each time.
- **Drafted text** (harness-side, not project CLAUDE.md):
  ```
  The harness `M`-class magnitude budget should be parameterized by
  checkpoint shape. Checkpoints that explicitly require paired payload
  serializers (write + read, encode + decode, persist + load) should
  receive a 4× cap rather than the default 3×. Recalibrate the
  `M`-class default or add a `paired_payload: true` flag in spec
  generation that bumps the budget.
  ```
- **Issue-ready**: false (low severity, no project rule needed; the
  existing "audit line-by-line when exceeded" practice already works)

### Proposal 5: Track `prob_estimate` semantics before enabling the bridge

- **Pattern**: runtime-behaviour-vs-design-intent (existing active
  rule reinforced)
- **Severity**: medium
- **Status**: Active rule already covers this; no new rule needed
- **Root cause**: `runtime_bridge.py:268` sets
  `prob_estimate=intent.expected_price`, but `expected_price` is a
  price level and downstream consumers (`Risk`, evaluator) read
  `prob_estimate` as a probability. The bridge is disabled-by-default
  so nothing downstream reads the agent-routed value today, but this
  must be tightened before `agent_strategy_runtime_enabled=True` is
  set in any environment. The E2E evaluator already noted this at
  medium severity.
- **Drafted rule text**: not needed — the existing
  `runtime-behaviour-vs-design-intent` rule covers the principle. The
  follow-up is a code task on this codebase, not a CLAUDE.md edit.
- **Issue-ready**: true (concrete code task before bridge enablement)
- **target_repo**: host

---

## Skill Defect Flags

No new skill defects this task. The full-verify-after-consensus
phenomenon is captured by **expanding** existing SD-1 (Proposal 3
above) rather than filing a new one.

---

## Existing Patterns Confirmed

- **`cross-checkpoint-integration`** (8th occurrence) — strengthened by
  the 13-finding review-loop result; existing harness-side proposal
  (end-of-branch integration trace step) remains the correct fix.
  Status unchanged: proposed.
- **`generated-artifact-drift`** (3rd occurrence) — recurs at low
  severity via untracked WIP test + `next-env.d.ts`. Stays monitoring.
- **`magnitude-overrun-tests`** (3rd occurrence) — CP07 paired-payload
  exceedance, audited and accepted. Stays monitoring with the
  recalibration suggestion above.
- **`runtime-behaviour-vs-design-intent`** (active) — reinforced by
  full-verify iter-1's two hard failures and by the `prob_estimate`
  E2E review item. The rule earned its keep again.
- **`piecewise-domain functions`** (active) — both rounds of the
  baseline splitter fix (round-2 dollar-quote awareness, then
  full-verify iter-2's `--`/`/* */` extension) demonstrate the rule:
  every regime present in real input must be recognized, not just the
  one that motivated the original fix.
- **`fresh-clone baseline verification`** (active) — full-verify iter-2
  re-ran every canonical gate from a fresh shell, including the
  integration suite against real PostgreSQL. Honored.
- **`integration test default-skip pattern`** (active) — the 161
  skipped suites in the default gate are integration-marker /
  env-gated. Default `pytest -q` stays offline. Honored.
- **`review-loop rejection discipline`** (active) — N/A this task; 0
  rejections out of 13 findings.
- **`comments are not fixes`** (active) — every accepted finding
  landed a code change *and* a regression test. Honored.

---

## Retro Metadata

- 7 checkpoints total; 6 passed first try; CP03 took 2 iterations
- 8 total evaluator iterations (6×1 + 1×2)
- 20 commits on the branch; 0 reverts
- Review-loop: 2 rounds; 13 findings (4 major + 7 minor + 2
  suggestions, plus 1 minor in round 2); **13 accepted, 0 rejected, 0
  escalated**; CONSENSUS: Approved
- Full-verify:
  - iter-1 FAIL: 2 hard failures (mypy unreachable assertions on
    tests; baseline splitter SQL comment handling)
  - iter-2 PASS_WITH_WARNINGS @ `28645e0`: 836 passed / 161 skipped,
    coverage 81% ≥ 79%, mypy strict clean (330 files), lint-imports
    8/0, integration committed-branch-only `159 passed, 0 failed`;
    two soft warnings (untracked WIP + pre-existing SOCKS env)
- E2E iter-1 PASS @ `912be30`: 827 passed / 161 skipped, mypy strict
  clean, lint-imports 8/0, default-path diff empty over runtime layers
- Branch diff: 35 files, +3915 / −3
- 3 review items from E2E (informational, non-blocking):
  `prob_estimate` semantics (medium), `StrategyRunResult` ownership
  back-edge (low), CP07 magnitude exceedance carry-forward (low)
- New patterns introduced as observations:
  - `migration-isolation-test-discipline` (medium)
  - `artifact-immutability-discipline` (medium)
- New pattern at 2nd occurrence (proposed):
  - `full-verify-catches-post-consensus` (medium) — expansion of SD-1
- Recurring patterns confirmed:
  - `cross-checkpoint-integration` (8th occurrence, proposed)
  - `generated-artifact-drift` (3rd occurrence, monitoring)
  - `magnitude-overrun-tests` (3rd occurrence, monitoring)
- Rule-conflict notes: 0/7 checkpoints reported any rule conflict
