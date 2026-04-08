# Continuation Guide — Prediction Market System

**Purpose**: Let any agent (Claude Code, Codex, etc.) on any machine pick up where the previous session left off and continue building the prediction market system step-by-step.

**Last updated**: 2026-04-08
**Current phase**: **v1 complete, ready for Phase 2 (real tool evaluation)**

---

## Section 1: Current State Snapshot

### What's done (pms-v1 — merged to `main`, pushed to GitHub)

- **All 10 planned checkpoints (CP01-CP10)** passed individually
- **E2E integration** verified with documented fixes
- **5-round cross-model review loop** with Codex reached consensus (14 findings fixed, 1 rejection accepted)
- **Full-verify**: 228 tests passing, 92% coverage, mypy strict clean on all 55 files
- **Retrospective**: `.harness/retro/2026-04-08-pms-v1.md` with 7 proposed CLAUDE.md rules

### Repository layout (already on `main`)

```
prediction-market-system/
├── python/pms/                    # Main package (single pms package, sub-packages)
│   ├── models/                    # Frozen dataclasses (Market, Order, CorrelationPair, etc.)
│   ├── protocols/                 # 9 Protocol interfaces (Connector, Strategy, Executor, ...)
│   ├── connectors/                # polymarket.py, kalshi.py (httpx + MockTransport)
│   ├── tool_harness/              # runner, schema, loader, reports, cli, mock_candidate
│   ├── orchestrator/              # pipeline, config, registry
│   ├── strategy/                  # arbitrage.py, correlation.py, base.py
│   ├── execution/                 # risk.py, executor.py, guardrails.py
│   ├── evaluation/                # metrics.py (in-memory), feedback.py (rule-based)
│   └── embeddings/                # engine.py, sentence_transformer.py (lazy)
├── rust/                          # Cargo.toml scaffolded, crates empty
├── benchmarks/                    # data_connector.yaml (sample)
├── candidates/                    # mock_connector.yaml (only one for now)
├── tests/                         # 228 tests, fixtures/ directory
├── .harness/pms-v1/spec.md        # The approved spec (read this first)
├── .harness/retro/                # Retrospective + proposed rules
└── docs/continuation-guide.md     # THIS FILE
```

### Related repo: auto-research

The tool catalog (Phase 1 research output) lives in a sibling repo:
- **URL**: https://github.com/stone16/auto-research
- **Key snapshot**: `examples/pms-tool-eval-2026-04-08/`
  - `tool-catalog.yaml.md` — 173-line catalog of 10 modules with first-choice + backup tools
  - `benchmark.json` — 10-item benchmark
  - `topic.md` — research goals + quality dimensions
  - `judge-feedback-trace.md` — per-iteration improvement history
  - `README.md` — run metadata

Clone it alongside this repo on the new machine (same parent directory):
```
dev/
├── prediction-market-system/      # this repo
└── auto-research/                  # https://github.com/stone16/auto-research
```

---

## Section 2: Environment Setup on New Machine

**Prerequisites**: `git`, `uv` (Python package manager), `gh` CLI (optional but helpful).

### Step 1: Clone both repos

```bash
mkdir -p ~/dev && cd ~/dev
git clone https://github.com/stone16/prediction-market-system.git
git clone https://github.com/stone16/auto-research.git
cd prediction-market-system
```

### Step 2: Install dependencies

```bash
uv sync
```

This creates `.venv/` and installs everything from `pyproject.toml` + `uv.lock`.

### Step 3: Verify the baseline works

```bash
uv run pytest -q                        # Expect: 228 passed, 1 skipped
uv run mypy python/ tests/ --strict     # Expect: Success, 55 source files
uv run pms-harness --help               # Expect: argparse usage printed
```

If any of these fail, stop and diagnose before proceeding. The baseline must be green before starting new work.

### Step 4: Verify the auto-research catalog is readable

```bash
ls ../auto-research/examples/pms-tool-eval-2026-04-08/
# Expect: README.md, benchmark.json, iteration-results.tsv, judge-feedback-trace.md, tool-catalog.yaml.md, topic.md
```

---

## Section 3: Next Phase — Phase 2 "Real Tool Evaluation"

**Goal**: Convert the auto-research catalog into real `candidates/*.yaml` files and run the harness against actual open-source prediction market tools. Validate which ones actually work.

This is the first time pms-v1's harness meets reality instead of the mock candidate.

### Why this phase (from the previous session's recommendation)

The whole design (survival gate + functional tests + report generation) is wasted if we only ever run the mock candidate. Running against real tools:
1. Surfaces which of the 10+ researched repos actually install, connect, and return data
2. Provides measured data for Phase 3 (pipeline wiring against the winners)
3. Exercises subprocess execution, real network calls, and real failure modes
4. Feeds results back to auto-research as `eval_results.yaml` to trigger supplementary research for gaps

### Phase 2 Checkpoints (step-by-step)

Use the Harness orchestration skill (`sto:harness`) to execute these. Each checkpoint should be a fresh Generator + Evaluator cycle.

---

#### **Checkpoint P2-01: Candidate YAML extraction from catalog**

**Scope**: Write a script or manually produce `candidates/*.yaml` files for the top candidates per module from the auto-research catalog.

**Acceptance criteria**:
- [ ] `candidates/` directory contains at least 10 real-tool YAML files (not counting mock_connector.yaml)
- [ ] Each file conforms to the candidate schema in `python/pms/tool_harness/schema.py` (Candidate dataclass)
- [ ] First-choice tools from each of the 10 modules in the catalog are represented
- [ ] Each candidate file has `name`, `repo`, `language`, `install`, `platforms`, `module`, `notes`, and optional `config` fields populated from the catalog
- [ ] Cross-module tools (pmxt, nautilus_trader, Polymarket/agents) are listed once per module they cover
- [ ] `uv run pms-harness evaluate --module data_connector` loads without YAML validation errors
- [ ] New tests in `tests/test_candidates.py` verify each real candidate YAML parses to a valid Candidate dataclass

**Files to read first**:
- `python/pms/tool_harness/schema.py` — Candidate dataclass definition
- `candidates/mock_connector.yaml` — existing example
- `../auto-research/examples/pms-tool-eval-2026-04-08/tool-catalog.yaml.md` — source catalog

**Suggested output files** (one per candidate, grouped by priority):

Priority candidates (highest ROI — try these first):
- `candidates/pmxt.yaml` — covers 4 modules (data_connector, data_normalizer, order_executor, arbitrage_calculator)
- `candidates/py-clob-client.yaml` — data_connector + order_executor (Polymarket official Python SDK)
- `candidates/kalshi-python-sync.yaml` — data_connector + order_executor (Kalshi official, private repo)
- `candidates/kalshi-python-async.yaml` — same but async variant
- `candidates/real-time-data-client.yaml` — realtime_feed (Polymarket official WebSocket)
- `candidates/nautilus-trader.yaml` — backtesting_engine

Secondary candidates:
- `candidates/poly-maker.yaml` — risk_manager reference
- `candidates/prediction-market-arbitrage-bot.yaml` — correlation_detector + arbitrage_calculator
- `candidates/polybot.yaml` — analytics_dashboard (ent0n29/polybot)
- `candidates/prediction-market-analysis.yaml` — correlation_detector backup

**Effort**: M (mostly data entry, some validation code)

---

#### **Checkpoint P2-02: Subprocess-based test runner for real candidates**

**Scope**: Currently `HarnessRunner` takes a `test_fn` callable. For real candidates, we need a subprocess-based runner that:
1. Creates an isolated Python virtual environment per candidate
2. Runs the install command inside it (with timeout from the benchmark's survival_gate)
3. Executes a canonical "fetch one market" probe script for each candidate
4. Returns structured results (success/failure, elapsed_ms, any error output)

**Acceptance criteria**:
- [ ] New module `python/pms/tool_harness/subprocess_runner.py` implements `make_subprocess_test_fn(candidate)`
- [ ] For Python candidates: creates a temp venv via `uv venv` + installs via the candidate's `install` command
- [ ] For non-Python candidates (TypeScript, Rust): use `npm install` / `cargo add` respectively in temp dirs
- [ ] Timeout enforcement via `asyncio.wait_for` wrapping subprocess calls
- [ ] Captures stdout/stderr for failed runs, includes them in SurvivalItemResult.error
- [ ] A dedicated "probe script" per candidate (stored in `candidates/probes/<candidate>.py`) that imports the tool and fetches one market — the test_fn runs this script inside the candidate's venv and checks exit code
- [ ] Integration test runs the probe for `py-clob-client` end-to-end (requires network — mark `@pytest.mark.integration`)
- [ ] `uv run pms-harness evaluate --module data_connector` now actually runs real tools

**Files to read first**:
- `python/pms/tool_harness/runner.py` — current HarnessRunner
- `python/pms/tool_harness/mock_candidate.py` — how the mock test_fn is shaped
- Python docs: `subprocess`, `asyncio.create_subprocess_exec`

**Critical rules**:
- Must not affect pms-v1's own `.venv` — each candidate gets its own isolated venv
- Must clean up temp venvs after the run (use `tempfile.TemporaryDirectory`)
- Must handle network failures gracefully (retry once, then mark as survival failure)
- Must not leak API keys or account data into test output

**Effort**: L (subprocess orchestration is non-trivial)

---

#### **Checkpoint P2-03: Probe scripts for priority candidates**

**Scope**: Write minimal "hello world" probe scripts for each priority candidate from P2-01. Each probe is a single Python/TS/Rust file that:
1. Imports the candidate's main interface
2. Attempts to fetch one active market
3. Prints JSON result to stdout
4. Exits 0 on success, non-0 on any failure

**Acceptance criteria**:
- [ ] `candidates/probes/py_clob_client_probe.py` — fetches one Polymarket market
- [ ] `candidates/probes/pmxt_probe.ts` — fetches one market from each supported platform
- [ ] `candidates/probes/kalshi_python_sync_probe.py` — fetches one Kalshi market (may require credentials — handle missing creds gracefully)
- [ ] `candidates/probes/real_time_data_client_probe.py` — connects and receives one message, then disconnects
- [ ] Each probe exits 0 on success with a one-line JSON summary
- [ ] Each probe exits 2 on missing credentials (distinct from 1 = network failure)
- [ ] A README in `candidates/probes/README.md` documents the exit code contract and how to add new probes

**Effort**: M (each probe is small, but there are 6-8)

---

#### **Checkpoint P2-04: Benchmark definitions for remaining modules**

**Scope**: Currently only `benchmarks/data_connector.yaml` exists. Phase 2 needs benchmarks for the other 9 modules so `pms-harness evaluate` can run across all of them.

**Acceptance criteria**:
- [ ] `benchmarks/realtime_feed.yaml`
- [ ] `benchmarks/order_executor.yaml`
- [ ] `benchmarks/arbitrage_calculator.yaml`
- [ ] `benchmarks/backtesting_engine.yaml`
- [ ] `benchmarks/correlation_detector.yaml`
- [ ] `benchmarks/embedding_engine.yaml`
- [ ] `benchmarks/data_normalizer.yaml`
- [ ] `benchmarks/analytics_dashboard.yaml`
- [ ] `benchmarks/risk_manager.yaml`
- [ ] Each benchmark has: 3 survival_gate items (install, connect, fetch_one_or_equivalent), 4 functional_test categories (data_coverage, performance, integrability, code_quality), weights summing to 1.0
- [ ] Use `benchmarks/data_connector.yaml` as the template
- [ ] `uv run pytest tests/test_harness.py` still passes (no schema regressions)
- [ ] New test `test_all_module_benchmarks_load` in `tests/test_harness.py` asserts each benchmark file parses

**Effort**: M (mostly YAML authoring informed by the catalog's quality dimensions)

---

#### **Checkpoint P2-05: Full evaluation run + report aggregation**

**Scope**: Run `pms-harness evaluate --module <each>` for all 10 modules, collect the reports, and produce a single cross-module summary that can be fed back to auto-research.

**Acceptance criteria**:
- [ ] New CLI subcommand: `uv run pms-harness evaluate-all --output-dir reports/phase2-run-<date>`
- [ ] Runs all modules in sequence (or parallel if safe)
- [ ] Produces `reports/phase2-run-<date>/<module>-scores.json` and `<module>-report.md` per module
- [ ] Produces a top-level `reports/phase2-run-<date>/eval_results.yaml` in the format auto-research expects (see spec.md's "feedback/eval_results.yaml format" section in the original design brief)
- [ ] `eval_results.yaml` contains: evaluated count, survived count, top_candidate, top_score, gaps list, and `request_more_candidates` + `search_hints` flags per module
- [ ] New test `test_evaluate_all_produces_aggregate_report` in `tests/test_harness_reports.py`
- [ ] Gaps in the report explicitly flag modules where no candidate passed the survival gate

**Files to read first**:
- Original spec brief for the eval_results.yaml format: see the auto-research discussion in `.harness/retro/2026-04-08-pms-v1.md` or the brainstorming conversation history

**Effort**: M

---

#### **Checkpoint P2-06: Feedback to auto-research (optional, manual trigger)**

**Scope**: Copy the `eval_results.yaml` from P2-05 into auto-research's `runs/pms-tool-eval/human_feedback.md` as a structured update, then kick off a new auto-research loop round to fill identified gaps.

**Acceptance criteria**:
- [ ] Documentation in `docs/feeding-results-to-auto-research.md` describing the manual workflow
- [ ] A helper script `scripts/export_to_auto_research.py` that converts `eval_results.yaml` → human_feedback.md format
- [ ] A short note in the retro explaining which gaps were handed back and what supplementary research was requested

This checkpoint is **optional** for Phase 2 closure — Phase 2 can ship with just P2-01 through P2-05.

**Effort**: S

---

## Section 4: Alternative Next Phases

If Phase 2 "Real Tool Evaluation" isn't the right call when you resume, these are the other options that were weighed:

### Phase 3A: Live Pipeline Wiring
Connect the real Polymarket + Kalshi connectors to live APIs. Requires API keys, introduces real money exposure. **Recommend waiting until Phase 2 tells you which tools actually work.**

### Phase 3B: Positions Ledger
Close the E2E Issue #3 gap — executor-maintained in-memory positions ledger derived from OrderResults. Small, self-contained (~50 LOC in execution/executor.py + tests).

### Phase 3C: LLM-based Correlation Refinement
Add an optional LLM layer on top of the rule-based CorrelationDetector. The spec already allows this as optional. Moderate effort (~150 LOC). Would require choosing a provider (Anthropic SDK default).

### Phase 3D: Retrospective Rule Adoption
Go through `.harness/retro/2026-04-08-pms-v1.md`'s 7 drafted rules and promote the highest-value ones into the user's global CLAUDE.md. No code, just governance.

### Phase 3E: Rust Performance Path
Start implementing the three Rust crates (datafeed, executor, embeddings). Each module must ship with a Python fallback. High effort (requires Rust + PyO3 setup).

---

## Section 5: How to Continue on the New Machine

When you start the next session on the other laptop, use this prompt template:

```
I'm continuing work on the prediction-market-system project. Read
docs/continuation-guide.md for the current state and next phase.

Current state: pms-v1 is complete and on main. I want to start Phase 2
"Real Tool Evaluation" following Section 3 of the continuation guide.
Begin with checkpoint P2-01 (candidate YAML extraction from catalog).

Use the sto:harness skill to orchestrate Phase 2 as a new task.
Task ID: pms-phase2. Before starting:
1. Verify the baseline is green (pytest + mypy)
2. Read the auto-research tool catalog at
   ../auto-research/examples/pms-tool-eval-2026-04-08/tool-catalog.yaml.md
3. Read the spec patterns in .harness/pms-v1/spec.md
4. Read the retro at .harness/retro/2026-04-08-pms-v1.md for lessons learned
```

The agent should then:
1. Set up git identity / worktree if needed
2. Create a new feature branch: `feature/pms-phase2`
3. Initialize a new harness task via `$ENGINE init --task-id pms-phase2`
4. Write a new spec at `.harness/pms-phase2/spec.md` using the P2-01 through P2-05 checkpoints above as the foundation
5. Run spec-evaluator, fix any issues, then begin execution

---

## Section 6: Hard Rules for Continuing Agents

Learned from the pms-v1 retrospective — these are non-negotiable:

1. **Ignore Pyright IDE warnings about `pms.*` imports** — canonical check is `uv run mypy python/ tests/ --strict`. Pyright doesn't use the uv venv and produces false positives on every file.

2. **Review-loop rejections must argue from runtime behavior, not design intent** — "it's documented as a limitation" is not a valid rejection; if the bug is real, fix it or document it as a tracked issue. Codex insisted on 3 of my round-1 rejections and was correct all 3 times.

3. **Comments are not fixes** — adding a NOTE comment explaining a bug does not resolve the bug. If the behavior is wrong, the code must change.

4. **Piecewise domain functions need piecewise math** — the sell-exposure bug in risk.py was fixed 4 times across 4 review rounds because each "fix" only handled one portion of the piecewise function. When an exposure function is non-linear, write the math out on paper before coding.

5. **Lifecycle cleanup must fire on ALL exit paths** — `try/finally` not `try/except`. `clear_opportunity` was missed on the exception path even after explicit wiring.

6. **Cross-checkpoint integration is invisible to per-checkpoint evaluators** — CorrelationDetector was "passing" CP10 but wasn't wired into TradingPipeline. E2E caught it; the checkpoint evaluator couldn't. Every new Protocol must trace producer → consumer during E2E.

7. **Never commit with `Co-Authored-By` lines** (user's global preference — also in `~/.claude/CLAUDE.md`).

8. **Frozen dataclasses for all data models** — immutability is a core invariant. Use `dataclasses.replace` for updates.

9. **Use Decimal for financial math, never float** — `Decimal(str(value))` pattern to avoid binary float precision loss when converting from JSON.

10. **Run `uv run pytest` and `uv run mypy python/ tests/ --strict` before claiming any work is done** — `verification-before-completion` is enforced in the Harness Generator skill.

---

## Section 7: Reference Links

### This repo
- **Spec**: `.harness/pms-v1/spec.md`
- **Retro**: `.harness/retro/2026-04-08-pms-v1.md`
- **Retro index**: `.harness/retro/index.md`
- **Review-loop summary**: `.review-loop/latest/summary.md` (local only, not in git)

### Auto-research repo (sibling)
- **Catalog snapshot**: `../auto-research/examples/pms-tool-eval-2026-04-08/`
- **Original research**: `../auto-research/claudedocs/` (local only, gitignored in auto-research)
- **Live run state**: `../auto-research/runs/pms-tool-eval/` (local only, gitignored)

### GitHub
- **pms-v1**: https://github.com/stone16/prediction-market-system
- **auto-research**: https://github.com/stone16/auto-research

### Harness tooling
- **Engine script**: `~/.claude/plugins/cache/stometa-private-marketplace/sto/*/skills/harness/scripts/harness-engine.sh`
- **Review-loop scripts**: `~/.claude/plugins/cache/stometa-private-marketplace/sto/*/skills/review-loop/scripts/`

---

## Section 8: Known Issues / Technical Debt (documented, not blockers)

From `.harness/pms-v1/spec.md` Out of Scope and from the retro:

1. **Rust implementations** — crates are empty, no code
2. **Live trading** — no real money execution (fixtures only)
3. **Live positions tracking** — RiskManager works in isolation but isn't wired to executor state
4. **WebSocket streaming** — `stream_prices()` and `get_historical_prices()` raise NotImplementedError
5. **Synergy testing** — deferred from CP03
6. **Web UI / Dashboard** — none
7. **Kalshi RSA signing** — stubbed
8. **Kalshi pagination** — capped at MAX_PAGES=20 for safety
9. **ML-based feedback / LLM correlation** — rule-based only
10. **Durable persistence** — MetricsCollector is in-memory; StorageProtocol reserved for future

None of these block Phase 2. They're documented v1 scope limits.

---

**End of continuation guide.** When in doubt, read the spec, read the retro, and run the baseline verification commands before starting new work.
