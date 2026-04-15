# Phase 1 Cleanup Report — `chore/cleanup`

Branch cut from `main @ f2d7067` (merge of PR #3 end-to-end wiring).
Target: zero runtime behaviour change. Everything below is doc, config,
or dead-code removal.

## Baseline before / after

| Gate | Before | After |
|------|--------|-------|
| `uv run pytest -q` | 68 passed, 2 skipped | **70 passed, 2 skipped** (+2 new config tests) |
| `uv run mypy src/ tests/ --strict` | Success, 55 files | Success, 55 files |
| `cd dashboard && npx tsc --noEmit` | clean | clean |

## What changed and why

### 1. `PMS_DATA_DIR` env var + `pms.config.data_dir()` single entry

**Why**: `FeedbackStore` and `EvalStore` each hardcoded `Path(".data/...")`
as their default path, and `FeedbackStore.__post_init__` reloads
`feedback.jsonl` on start. Dev sessions therefore carry feedback rows
across restarts, and two separate stores each had their own literal of the
directory name — drift-prone.

**Change**:
- `src/pms/config.py`: added `data_dir()` and `DEFAULT_DATA_DIR = Path(".data")`.
  `data_dir()` honours `PMS_DATA_DIR` env var, falling back to `.data`.
- `src/pms/storage/feedback_store.py`: `default_factory` now calls
  `data_dir() / "feedback.jsonl"`.
- `src/pms/storage/eval_store.py`: same for `eval_records.jsonl`.
- `tests/unit/test_core_foundation.py`: two regression tests that lock the
  env-var override and the store wiring.

**Tests already isolate**: every `FeedbackStore(...)` / `EvalStore(...)` in
`tests/` passes an explicit `tmp_path`, so the shared `.data/` is
untouched regardless of `PMS_DATA_DIR`.

**Rollback**: revert the three files above (`config.py`, both storage
modules) and the test additions. Default path semantics are unchanged —
stores without `PMS_DATA_DIR` write to `.data/`, same as before.

### 2. Removed unused dependency `numpy`

**Why**: Grep across `src/` and `tests/` showed zero `import numpy` / `from
numpy` usages. Declared in `pyproject.toml` for no runtime consumer.

**Change**:
- `pyproject.toml`: dropped `numpy>=2.4.4` from `dependencies`.
- `uv.lock`: regenerated via `uv lock`.

**Rollback**: `uv add numpy>=2.4.4` (or restore the pyproject line and
`uv lock`).

### 3. Deleted `dashboard/conftest.py`

**Why**: Misleadingly named file (pytest `conftest.py` in a JS directory
is a code smell). Defined `seed_feedback()` but **zero** importers in
`src/`, `tests/`, `dashboard/`, or anywhere else. The Playwright spec at
`dashboard/e2e/dashboard.spec.ts` has its own inline `seedFeedback()` and
does not depend on this file.

**Rollback**: `git checkout f2d7067 -- dashboard/conftest.py`.

### 4. CLAUDE.md rewrite (the pms-v1 doc drift)

**Why**: CLAUDE.md still described pms-v1 layout (`python/pms/`,
`models/`, `protocols/`, `tool_harness/`, `orchestrator/`, `strategy/`,
`execution/`, `embeddings/`). The v2 layout under `src/pms/` replaces all
of these with a cybernetic-loop layering (`core/`, `sensor/`,
`controller/`, `actuator/`, `evaluation/`, `storage/`, `api/`). Multiple
active engineering rules referenced code paths that no longer exist, and
one rule (`Decimal for all financial math`) was **actively opposite** to
the v2 schema-spec invariant (v2 entities use `float` at the boundary;
`Decimal` is reserved for calculation internals, per
`src/pms/core/models.py` module docstring).

Future Claude sessions start from CLAUDE.md, so stale guidance there is
load-bearing and must match reality.

**Change**:
- Baseline bumped `≥62 passing` → `≥70 passing`.
- "Frozen dataclasses" section: `pms.models.*` → `pms.core.models`;
  referenced modules updated to `actuator/risk.py`, `actuator/executor.py`,
  `controller/`.
- "Decimal for all financial math" **reversed** into **`float` at entity
  boundary, `Decimal` for calculation internals**, with rationale pointing
  at the `src/pms/core/models.py` docstring and `controller/sizers/kelly.py`
  as the canonical Decimal-internal example.
- "Protocol-first module boundaries" updated: `python/pms/protocols/` →
  `src/pms/core/interfaces.py`; concrete impls under
  `src/pms/{sensor,controller,actuator,evaluation}/`; pipeline owner is
  `src/pms/runner.py` + `src/pms/controller/pipeline.py`; full Protocol
  list (`ISensor`, `IController`, `IActuator`, `IEvaluator`, `IForecaster`,
  `ICalibrator`, `ISizer`) enumerated.
- "Test discovery requires `pythonpath`" corrected: `["python", "."]` →
  `["src", "."]`, matching actual `pyproject.toml`.
- "Subprocess runner install command parser is whitelist-only" rule
  **removed**. The rule was attached to
  `python/pms/tool_harness/subprocess_runner.py` which does not exist in
  v2. The general "whitelist shell inputs" principle is still good
  engineering hygiene, but keeping a rule with no v2 anchor misleads
  readers. If v2 ever adds a subprocess wrapper, re-derive at that point.
- **New rule added**: "Data directory is a single entry point" (points at
  `pms.config.data_dir()` and the `PMS_DATA_DIR` override).
- "Where things live" tree replaced end-to-end with the actual v2 layout,
  including explicit note that pms-v1 directories (`benchmarks/`,
  `candidates/`, `scripts/`, `tool_harness/`) no longer exist and any
  references in retros or rust crate docstrings are historical.
- "Useful commands" replaced with v2 equivalents: `pms-api` (FastAPI
  server), dashboard dev command, integration test invocation,
  `PMS_DATA_DIR` isolation recipe, Playwright e2e command. The
  `pms-harness` CLI invocations were removed because no such binary
  exists.

**Rollback**: `git checkout f2d7067 -- CLAUDE.md`. No code depends on
CLAUDE.md content, so a revert is side-effect free.

### 5. README.md baseline bump + dev-state isolation recipe

**Why**: README said `(66 pass, 2 skip)` — stale. Also missing guidance
on how to isolate dev JSONL state, which matters now that
`PMS_DATA_DIR` exists.

**Change**: updated baseline numbers to `70 pass, 2 skip`; added a
"Isolating dev state" subsection under `Development` covering
`PMS_DATA_DIR` and the reset recipe.

### 6. Docs: deprecation banners on pms-v1 historical docs

**Why**: `docs/continuation-guide.md` describes pms-v1 phases P2-01
through P2-05 and references `tool_harness/`, `pms-harness evaluate`,
`benchmarks/`, `candidates/` — none of which exist in v2. A new
contributor reading it will waste effort trying to run commands that no
longer work. Same for `docs/feeding-results-to-auto-research.md`
(depends on `pms.tool_harness.aggregate` + `pms-harness evaluate-all`).

**Change**: added a `⚠ Deprecated — pms-v1 historical` banner at the top
of each file, pointing readers at CLAUDE.md + README.md + the retro
index for current state. Kept the full historical body intact as a
paper trail of the v1 → v2 migration. **No files deleted.**

**Rollback**: revert the header hunks in each file.

### 7. docs/research/README.md — single-line fix

Changed `python/pms/models/` → `src/pms/core/models.py` in the one
sentence referencing the schema-spec's consumer. Other content is
research prose and doesn't depend on code layout.

### 8. .gitignore comment refresh

The `reports/` gitignore entry had a comment saying "generated by
`pms-harness evaluate`". That CLI does not exist. Rewrote the comment to
describe why the ignore still earns its keep (stale v1 working copies)
without naming a fictional command.

## Explicitly NOT touched

- **`rust/README.md`, `rust/Cargo.toml`, `rust/crates/{datafeed,executor,embeddings}/src/lib.rs`**:
  these reference pms-v1 canonical paths (`python/pms/connectors/polymarket.py`,
  `python/pms/execution/executor.py`, `python/pms/embeddings/engine.py`,
  `python/pms/_accel/__init__.py`). The rust crates are **scaffolded only** —
  zero real Rust code, zero PyO3 wiring, zero impact on the Python
  runtime. Updating the comments without updating the crates would be
  busywork; the right time is when the rust path is actually
  implemented and a v2 canonical target is chosen. Flagged as a Phase 3
  follow-up.
- **`.harness/retro/*`, `.harness/pms-v{1,2}/*`**: frozen historical
  records. Leaving intact is a project convention — retros are the
  provenance trail for every CLAUDE.md rule.
- **`.review-loop/latest/`**: gitignored, local-only. Not committed, no
  cleanup action needed.
- **`uv` dev tooling deps** (`mypy`, `pytest`, `pytest-asyncio`,
  `pytest-cov`, `types-pyyaml`): all used, all kept.

## Follow-ups for Phase 2 (not this PR)

- `R1` runner state-accumulation fix (`Runner.start(reset=True)` + API
  query param).
- `R2` first-class `TradeDecision.forecaster` field replacing the
  `stop_conditions: model_id:...` prefix hack.
- `R3` remove `dashboard/lib/mock-store.ts` fs writes, stay in-memory.
- Rust crate docstring refresh once v2 canonical targets exist for
  datafeed / executor / embeddings.
- Full rewrite (not banner) of `docs/continuation-guide.md` when the v2
  phase story stabilizes.

## Verification

```bash
uv run pytest -q                        # 70 passed, 2 skipped
uv run mypy src/ tests/ --strict        # Success: 55 files
cd dashboard && npx tsc --noEmit        # (empty output, clean)
```

All three gates green against the chore/cleanup branch.
