# Promoted Retro Rules

**Status:** active. These are rules promoted from post-task
retrospectives under `.harness/retro/` after being observed ≥2 times
(see `.harness/retro/index.md` for promotion provenance).

**Complementary to:** `agent_docs/architecture-invariants.md`. Retros
address past mistakes; invariants define the positive architecture.
Both are load-bearing.

---

## 🔴 CRITICAL — Runtime behaviour > design intent

*Promoted from `pms-v1` retro (Principle).*

When evaluating whether code is correct — whether reviewing your own,
defending against a peer, or accepting a finding — **always argue
from runtime behaviour, never from design intent**. Runtime claims
are falsifiable, reproducible, and terminate the discussion. Design-
intent claims are unfalsifiable, depend on context the reader may
not share, and are almost always wrong under scrutiny.

If you catch yourself writing "the intent is…", rewrite the sentence
to start with "when called with…".

---

## 🔴 CRITICAL — Review-loop rejection discipline

*Promoted from `pms-v1` retro Proposal 1.*

When rejecting a peer reviewer's finding during a review loop, the
rejection MUST include:

1. **A concrete counter-trace.** Name the file, line range, and
   specific runtime behaviour that contradicts the finding.
   "Documented in NOTE" and "handled elsewhere" are NOT valid
   rejections.
2. **A named call-site.** If claiming a behaviour is "handled
   elsewhere", link to the exact `file:line` where the handling
   occurs and show that the caller actually hits it.
3. **A reproducible witness.** If claiming an edge case cannot
   occur, either cite a test that proves it or write one in the
   rejection.

Rejections failing this bar must be re-classified as "accept with
minimal fix" before the round closes. "This is out of scope" is
only valid when the scope boundary is explicit in the task spec —
not when it is implicit in the current checkpoint.

---

## 🟡 IMPORTANT — Comments are not fixes

*Promoted from `pms-v1` retro Proposal 4.*

When a review finding describes incorrect runtime behaviour (wrong
values, missing calls, silent truncation), the fix MUST change
runtime behaviour. Adding a docstring, a `NOTE` comment, a `TODO`,
or an "informational" caveat is NOT a valid fix for a behavioural
finding.

A behavioural finding is closed only when:
- The code produces different output for the original failing
  input, AND
- A new test locks in the corrected behaviour.

Documentation-only resolutions are reserved for findings that are
themselves about documentation (missing docstrings, unclear
parameter contracts, stale comments).

---

## 🟡 IMPORTANT — Lifecycle cleanup on all exit paths

*Promoted from `pms-v1` retro Proposal 3.*

When introducing stateful dedup, tokens, locks, or any resource
acquired mid-function that must be released:

1. **Acquire and release must be wired in the SAME commit.** A PR
   that adds `acquire()` without the corresponding `release()` is
   incomplete, regardless of test status.
2. **Release MUST be in a `try/finally`** (or equivalent context
   manager) scoped to the widest block that owns the state — not
   distributed across individual exit branches.
3. **Every early-return path must be checked against the cleanup
   contract:** reject, skip, exception, success — all four paths
   must release.
4. **Reviewer should grep for every `acquire` / `add` / `register`
   call in the diff** and verify each has a matching cleanup wired
   in a control structure that covers all exit paths.

---

## 🟡 IMPORTANT — Piecewise-domain functions

*Promoted from `pms-v1` retro Proposal 2.*

When implementing a function whose output has piecewise semantics
over its input domain (inventory regimes, fill tiers, leverage
bands, signed vs unsigned inputs, covered vs uncovered trades):

1. **Identify and name every break point** in the domain before
   writing code. Document them in a docstring
   `# Break points: ...`.
2. **Write at least one test input per regime** AND at least one
   test input that straddles each break point (e.g. `size =
   inventory`, `size = inventory + 1`).
3. **Derive each regime from first principles**, not by
   sign-flipping or adapting another regime's formula. Copy-
   adapting piecewise code is a known source of symmetry bugs.
4. **When fixing a bug in a piecewise function, re-derive all
   regimes.** A fix that only corrects one regime is a ticking
   bomb for the other.

---

## 🟡 IMPORTANT — Verify isolated-env tooling assumptions

*Promoted from `pms-phase2` retro Proposal 1.*

When writing a wrapper around a third-party CLI tool that produces
files or directories on disk (`uv venv`, `npm init`, `cargo init`,
`python -m venv`), enumerate what the tool actually creates before
writing wrapper code that depends on the output:

1. Run the tool against a temp dir manually and `ls -la` the
   result.
2. Note which "obvious" files are missing (pip in a uv venv,
   `package.json` entries in an `npm init`, etc.).
3. Write the wrapper against the **actual** layout, not the
   **assumed** layout.

The failure mode is silent until the first real invocation; unit
tests with mocked subprocess calls will pass either way.

---

## 🟡 IMPORTANT — Fresh-clone baseline verification

*Promoted from `pms-phase2` retro Proposal 2.*

When picking up a project on a new machine, ALWAYS run the
documented baseline verification commands from a fresh clone in a
fresh shell before assuming the documented baseline holds. Dev-
machine state (IDE plugins, stale venv, `sys.path` injections) can
hide config bugs that bite the next contributor.

If the baseline fails on a fresh clone, **fix the config (not the
test)** and commit the fix as the first commit on the feature
branch with a `fix(tests):` or `fix(build):` prefix. Do not start
feature work against a broken baseline.

---

## 🟢 RECOMMENDED — Integration test default-skip pattern

*Promoted from `pms-phase2` retro Proposal 3.*

When adding a `@pytest.mark.integration` (or `@pytest.mark.slow`)
test that requires network / real subprocess / external state,
combine the marker with an env-var skipif so the default `pytest`
invocation does not run it:

```python
@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("PMS_RUN_INTEGRATION") != "1",
    reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
)
```

The marker keeps `pytest -m integration` working as expected; the
skipif keeps the default invocation fast and offline.

---

## Commit-message precedence (promoted from `pms-v1` retro Proposal 7)

Commit-message attribution follows this precedence (highest wins):

1. User's global git rules (`~/.claude/CLAUDE.md` "Git Rules")
2. Generator agent system prompt instructions
3. Harness system prompt defaults
4. Anthropic/upstream template defaults

Specifically: the user's global rule "Never add `Co-Authored-By`
lines" overrides any harness or template that adds them. This is
settled — do not re-derive at every commit.

---

## Promotion process

A rule is promoted from retro to this document when:

- It has been observed in ≥ 2 task retros, OR
- It is marked high-severity on first observation, OR
- The user explicitly promotes it during retro review.

Each promotion updates `.harness/retro/index.md` (lifecycle column
moves to `active`) and appends a row to this document with
provenance.

A promoted rule is retired when the underlying issue is resolved
(e.g. by a tooling change that prevents the class of bug). Retired
rules move to the bottom of this document with a dated retirement
note.
