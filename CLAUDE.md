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
uv run mypy src/ tests/ --strict         # strict type check on every committed module
```

The pytest baseline must be **≥70 passing, 2 skipped** as of 2026-04-15 (pms-v2).
Skipped = 2 integration tests gated on `PMS_RUN_INTEGRATION=1`.
mypy strict must be clean on **every** source file. Both gates are
load-bearing — never commit without running both.

`pytest -m integration` requires `PMS_RUN_INTEGRATION=1` and runs the
paper-mode runner against the live Polymarket REST API (no credentials needed).

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

Every entity under `pms.core.models` is `@dataclass(frozen=True)`. Mutation
goes through `dataclasses.replace`. New models must follow the same
convention — the runtime invariants in `actuator/risk.py`,
`actuator/executor.py`, and every module under `controller/` assume
immutability.

### `float` at entity boundary, `Decimal` for calculation internals

The v2 schema-spec pins entity financial fields to Python `float`
(matching the source venue payloads and the OSS reference
implementations). See the module docstring on `src/pms/core/models.py`
for the rationale.

Rules:
- `MarketSignal`, `TradeDecision`, `OrderState`, `FillRecord`,
  `Portfolio`, `EvalRecord` store prices, sizes, and money as `float`.
- Adapters and pure-math calculators (e.g. `controller/sizers/kelly.py`,
  Kalshi cent-fixed-point reconciliation) MUST convert to `Decimal` via
  `Decimal(str(value))` before arithmetic, then convert back to `float`
  when writing into an entity. The `str(...)` step avoids binary float
  precision loss.
- Do not invert this rule to "Decimal everywhere" — that pattern was
  pms-v1 and was reversed deliberately when v2 locked the schema-spec.

### Protocol-first module boundaries

Protocol interfaces live in `src/pms/core/interfaces.py`. Concrete
implementations live under `src/pms/{sensor,controller,actuator,evaluation}/`
(with further `adapters/` subpackages per layer). The orchestrator
(`src/pms/runner.py`) and the controller pipeline
(`src/pms/controller/pipeline.py`) only ever reference the Protocol
types (`ISensor`, `IController`, `IActuator`, `IEvaluator`,
`IForecaster`, `ICalibrator`, `ISizer`), never concrete classes. This
is what makes module swapping work without touching the orchestrator.

### Test discovery requires `pythonpath = ["src", "."]`

The package lives under `src/pms/` (hatchling target in
`pyproject.toml`). `[tool.pytest.ini_options]` MUST keep both `src`
and `.` on `pythonpath` so `from pms.*` imports resolve against the
in-tree source and so `tests/` fixtures can be imported as modules.
Removing either entry breaks pytest collection on a fresh clone.

Strict mypy resolves via `mypy_path = "src"` in the same `pyproject.toml`
— keep those two paths in sync.

### Data directory is a single entry point

Persistent JSONL state (`feedback.jsonl`, `eval_records.jsonl`) goes
through `pms.config.data_dir()`. Default is `.data/` (gitignored);
override for dev or tests with the `PMS_DATA_DIR` env var. Tests in
`tests/` always pass an explicit `tmp_path` to `FeedbackStore` and
`EvalStore`, so nothing in the test suite touches the shared `.data/`
even without the env var set.

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
├── src/pms/                       # main package (hatchling wheel target)
│   ├── core/                      # frozen dataclasses, enums, Protocol interfaces
│   ├── sensor/                    # stream + watchdog + adapters/{historical,polymarket_rest,polymarket_stream}
│   ├── controller/                # pipeline + router + forecasters/ + calibrators/ + sizers/
│   ├── actuator/                  # risk + executor + feedback + adapters/{backtest,paper,polymarket}
│   ├── evaluation/                # metrics + eval spool + feedback + adapters/scoring
│   ├── storage/                   # EvalStore + FeedbackStore (JSONL persistence)
│   ├── api/                       # FastAPI app + `pms-api` CLI entry
│   ├── runner.py                  # orchestrator wiring sensor → controller → actuator → evaluation
│   └── config.py                  # PMSSettings (pydantic-settings) + data_dir()
├── dashboard/                     # Next.js console on port 3100 + Playwright e2e
├── rust/                          # PyO3 workspace stub (scaffolded — pms-v1 canonical refs
│                                  #   in READMEs/crate docstrings are historical)
├── tests/                         # pytest suite (unit + integration), with fixtures/
├── docs/                          # research notes + historical continuation guide
├── .data/                         # gitignored JSONL state (override via PMS_DATA_DIR)
├── .harness/{pms-v1,pms-v2}/      # spec + checkpoint artifacts per task
└── .harness/retro/                # task retros + index.md (promoted rules live here)
```

Note: `benchmarks/`, `candidates/`, `scripts/`, and the `tool_harness/` module
from pms-v1 no longer exist in v2. The v2 architecture replaces that
benchmark-driven tool-evaluation pipeline with the cybernetic loop (sensor →
controller → actuator → evaluation). Historical references to those pms-v1
paths survive in `.harness/retro/` and the rust crate docstrings — do not
treat them as a map of current code.

## Useful commands

```bash
# Run the FastAPI backend (port 8000). PMS_AUTO_START=1 auto-starts the runner.
uv run pms-api

# Run the dashboard against the live backend.
cd dashboard
PMS_API_BASE_URL=http://127.0.0.1:8000 npm run dev   # → http://127.0.0.1:3100

# Full test + type baseline (matches the canonical gates at the top of this file).
uv run pytest -q
uv run mypy src/ tests/ --strict

# Integration tests against the live Polymarket REST API (no credentials needed).
PMS_RUN_INTEGRATION=1 uv run pytest -m integration

# Isolate dev JSONL state from the committed repo.
export PMS_DATA_DIR=/tmp/pms-dev
uv run pms-api
# When done, rm -rf /tmp/pms-dev (or unset PMS_DATA_DIR and delete .data/).

# Dashboard Playwright e2e (requires backend OR will fall back to mock-store).
cd dashboard && npx playwright test
```
