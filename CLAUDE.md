# CLAUDE.md — prediction-market-system

Project-level instructions for Claude (and any future contributor) working
in this repo. This file consolidates the rules promoted from the
`.harness/retro/` proposals after pms-v1 and pms-phase2 — see Phase 3D in
`docs/continuation-guide.md` for the rule-adoption process.

When picking up a task here, read this file first, then `docs/continuation-guide.md`,
then the relevant retro under `.harness/retro/`.

## Canonical verification commands

Always run from a clean shell at the repo root:

```bash
uv sync                                  # install deps from uv.lock
uv run pytest -q                         # full test suite
uv run mypy python/ tests/ scripts/ --strict   # strict type check on every committed module
```

The pytest baseline must be **≥323 passing, 2 skipped** as of 2026-04-09.
Skipped = 1 baseline + 1 integration test gated on `PMS_RUN_INTEGRATION=1`.
mypy strict must be clean on **every** source file. Both gates are
load-bearing — never commit without running both.

`pytest -m integration` requires `PMS_RUN_INTEGRATION=1` and runs the real
py-clob-client end-to-end against the public Polymarket CLOB endpoint
(no credentials needed).

## Active rules (promoted from retros)

These rules are **active**, meaning they have been observed enough times
across tasks to deserve enforcement rather than just documentation. The
provenance is tracked in `.harness/retro/index.md`.

### 🔴 CRITICAL — Runtime behaviour > design intent

(Promoted from pms-v1 retro Principle.)

When evaluating whether code is correct — whether reviewing your own,
defending against a peer, or accepting a finding — **always argue from
runtime behaviour, never from design intent**. Runtime claims are
falsifiable, reproducible, and terminate the discussion. Design-intent
claims are unfalsifiable, depend on context the reader may not share,
and are almost always wrong under scrutiny.

If you catch yourself writing "the intent is...", rewrite the sentence
to start with "when called with...".

### 🔴 CRITICAL — Review-loop rejection discipline

(Promoted from pms-v1 retro Proposal 1.)

When rejecting a peer reviewer's finding during a review loop, the
rejection MUST include:

1. A concrete counter-trace: name the file, line range, and specific
   runtime behaviour that contradicts the finding. "Documented in NOTE"
   and "handled elsewhere" are NOT valid rejections.
2. A named call-site: if claiming a behaviour is "handled elsewhere",
   link to the exact file:line where the handling occurs and show that
   the caller actually hits it.
3. A reproducible witness: if claiming an edge case cannot occur,
   either cite a test that proves it or write one in the rejection.

Rejections failing this bar must be re-classified as "accept with
minimal fix" before the round closes. "This is out of scope" is only
valid when the scope boundary is explicit in the task spec — not when
it is implicit in the current checkpoint.

### 🟡 IMPORTANT — Comments are not fixes

(Promoted from pms-v1 retro Proposal 4.)

When a review finding describes incorrect runtime behaviour (wrong
values, missing calls, silent truncation), the fix MUST change runtime
behaviour. Adding a docstring, a NOTE comment, a TODO, or an
"informational" caveat is NOT a valid fix for a behavioural finding.

A behavioural finding is closed only when:
- The code produces different output for the original failing input, AND
- A new test locks in the corrected behaviour.

Documentation-only resolutions are reserved for findings that are
themselves about documentation (missing docstrings, unclear parameter
contracts, stale comments).

### 🟡 IMPORTANT — Lifecycle cleanup on all exit paths

(Promoted from pms-v1 retro Proposal 3.)

When introducing stateful dedup, tokens, locks, or any resource
acquired mid-function that must be released:

1. Acquire and release must be wired in the SAME commit. A PR that
   adds `acquire()` without the corresponding `release()` is incomplete,
   regardless of test status.
2. Release MUST be in a `try/finally` (or equivalent context manager)
   scoped to the widest block that owns the state — not distributed
   across individual exit branches.
3. Every early-return path must be checked against the cleanup contract:
   reject, skip, exception, success — all four paths must release.
4. Reviewer should grep for every `acquire` / `add` / `register` call in
   the diff and verify each has a matching cleanup wired in a control
   structure that covers all exit paths.

### 🟡 IMPORTANT — Piecewise-domain functions

(Promoted from pms-v1 retro Proposal 2.)

When implementing a function whose output has piecewise semantics over
its input domain (inventory regimes, fill tiers, leverage bands, signed
vs unsigned inputs, covered vs uncovered trades):

1. Identify and name every break point in the domain before writing
   code. Document them in a docstring `# Break points: ...`.
2. Write at least one test input per regime AND at least one test input
   that straddles each break point (e.g. `size = inventory`,
   `size = inventory + 1`).
3. Derive each regime from first principles, not by sign-flipping or
   adapting another regime's formula. Copy-adapting piecewise code is a
   known source of symmetry bugs.
4. When fixing a bug in a piecewise function, re-derive all regimes. A
   fix that only corrects one regime is a ticking bomb for the other.

### 🟡 IMPORTANT — Verify isolated-env tooling assumptions

(Promoted from pms-phase2 retro Proposal 1.)

When writing a wrapper around a third-party CLI tool that produces
files or directories on disk (`uv venv`, `npm init`, `cargo init`,
`python -m venv`), enumerate what the tool actually creates before
writing wrapper code that depends on the output:

1. Run the tool against a temp dir manually and `ls -la` the result.
2. Note which "obvious" files are missing (pip in a uv venv, package.json
   entries in an npm init, etc.).
3. Write the wrapper against the actual layout, not the assumed layout.

The failure mode is silent until the first real invocation; unit tests
with mocked subprocess calls will pass either way.

### 🟡 IMPORTANT — Fresh-clone baseline verification

(Promoted from pms-phase2 retro Proposal 2.)

When picking up a project on a new machine, ALWAYS run the documented
baseline verification commands from a fresh clone in a fresh shell
before assuming the documented baseline holds. Dev-machine state (IDE
plugins, stale venv, sys.path injections) can hide config bugs that
bite the next contributor.

If the baseline fails on a fresh clone, fix the config (not the test)
and commit the fix as the first commit on the feature branch with a
`fix(tests):` or `fix(build):` prefix. Do not start feature work
against a broken baseline.

### 🟢 RECOMMENDED — Integration test default-skip pattern

(Promoted from pms-phase2 retro Proposal 3.)

When adding a `@pytest.mark.integration` (or `@pytest.mark.slow`) test
that requires network / real subprocess / external state, combine the
marker with an env-var skipif so the default `pytest` invocation does
not run it:

```python
@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("PMS_RUN_INTEGRATION") != "1",
    reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
)
```

The marker keeps `pytest -m integration` working as expected; the
skipif keeps the default invocation fast and offline.

## Project-specific conventions

### Frozen dataclasses for all data models

Every type under `pms.models.*` is `@dataclass(frozen=True)`. Mutation
goes through `dataclasses.replace`. New models added to `pms.models`
must follow the same convention — the runtime invariants in `risk.py`,
`executor.py`, and the strategy modules all assume immutability.

### Decimal for all financial math

Never use `float` for prices, sizes, P&L, exposure, or any monetary
quantity. Use `Decimal(str(value))` when converting from JSON to avoid
binary float precision loss. The existing tests for `risk.py`,
`executor.py`, and `strategy/arbitrage.py` exercise the Decimal path
explicitly; new tests should match.

### Protocol-first module boundaries

Every module slot in `python/pms/protocols/` defines a Protocol. Concrete
implementations live under `python/pms/{connectors,strategy,execution,...}/`.
The pipeline orchestrator (`python/pms/orchestrator/pipeline.py`) only
ever holds references to the Protocol type, never to the concrete class.
This is what makes module swapping work without touching the pipeline.

### Test discovery requires `pythonpath = ["python", "."]`

(Locked in by `fix(tests)` commit `27c7f89`.)

`tests/test_pipeline.py` imports `from tests._registry_target import ...`
which only resolves when the project root is on `sys.path`. The
`pyproject.toml` `[tool.pytest.ini_options]` section MUST keep `.` on
`pythonpath` — removing it breaks pytest collection on a fresh clone.

### Subprocess runner install command parser is whitelist-only

(Locked in by `feat(phase2)` commit `b18d850`.)

`python/pms/tool_harness/subprocess_runner.py` accepts only:
- `pip install <pkg>` (rewritten to `uv pip install --python <venv>`)
- `uv pip install <pkg>` (rewritten to `uv pip install --python <venv>`)
- `npm install <pkg>`
- `cargo add <pkg>`

Any other shape (`uv sync`, `make install`, multi-step pipes, shell
redirects) raises `UnsupportedCandidateError`. Do not loosen this
parser to accept more shapes — adding shell-execution to the runner
re-introduces the failure mode the whitelist was designed to prevent.

### No `Co-Authored-By` lines in commit messages

(Promoted from pms-v1 retro Proposal 7 — user's global preference.)

Commit message attribution follows this precedence (highest wins):

1. User's global git rules (`~/.claude/CLAUDE.md` "Git Rules")
2. Generator agent system prompt instructions
3. Harness system prompt defaults
4. Anthropic/upstream template defaults

Specifically: the user's global rule "Never add `Co-Authored-By` lines"
overrides any harness or template that adds them. Do not re-derive this
at every commit; it is settled.

## Where things live

```
prediction-market-system/
├── python/pms/                    # main package
│   ├── models/                    # frozen dataclasses
│   ├── protocols/                 # 9 Protocol interfaces
│   ├── connectors/                # polymarket.py, kalshi.py
│   ├── tool_harness/              # benchmark/candidate runner + subprocess
│   ├── orchestrator/              # pipeline + module registry
│   ├── strategy/                  # arbitrage, correlation, base
│   ├── execution/                 # risk, executor, guardrails
│   ├── evaluation/                # metrics, feedback
│   └── embeddings/                # engine, sentence_transformer fallback
├── rust/                          # Cargo workspace (scaffolded only)
├── benchmarks/                    # 10 module benchmark YAMLs
├── candidates/                    # 18 real-tool candidate YAMLs + probes/
├── scripts/                       # one-off CLI helpers (export to auto-research)
├── tests/                         # 323 tests, fixtures/ directory
├── docs/                          # continuation guide + workflow docs
├── .harness/pms-v1/spec.md        # original spec
└── .harness/retro/                # task retros + index.md
```

## Useful commands

```bash
# Full single-module evaluation against the real candidate dirs.
uv run pms-harness evaluate --module data_connector \
    --output-dir /tmp/p2_eval

# Cross-module run with eval_results.yaml aggregate.
uv run pms-harness evaluate-all --output-dir /tmp/p2_run

# Convert eval_results.yaml → human_feedback.md for auto-research handoff.
uv run python scripts/export_to_auto_research.py \
    --eval-results /tmp/p2_run/eval_results.yaml

# Run the integration smoke test against real py-clob-client.
PMS_RUN_INTEGRATION=1 uv run pytest \
    tests/test_subprocess_runner.py::test_py_clob_client_probe_end_to_end -q
```
