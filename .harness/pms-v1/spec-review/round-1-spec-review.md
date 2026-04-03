---
task_id: pms-v1
spec_version: 1
round: 1
---

# Spec Review: Round 1

## Verdict: `revise`

Three critical concerns and several warnings prevent approval. The spec is well-structured and shows strong architectural thinking, but needs targeted fixes before a Generator can execute reliably.

---

## Scope Assessment

### Minimum Viable Scope Analysis

The spec covers three distinct subsystems (Tool Harness, Trading Pipeline, Correlation Engine) across 10 checkpoints. This is ambitious for a v1 but defensible given the Protocol-based decoupling -- each subsystem is independently testable.

**Deferrable items identified:**

1. **Synergy tests (CP03)** -- Running top-N candidates in combination is a Phase 2 concern. The harness delivers value with single-candidate evaluation alone. Suggest deferring `SynergyRunner` to a future checkpoint.
2. **`stream_prices()` WebSocket support (CP04/CP05)** -- WebSocket streaming adds significant complexity to connectors. REST polling covers v1 needs. WebSocket can be a separate checkpoint.
3. **`get_historical_prices()` (CP04)** -- Not referenced by any downstream consumer in the spec (no strategy or evaluation module calls it). Defer unless a consumer is identified.
4. **LLM-based relation type classification (CP10)** -- The spec mentions "LLM-based" classification but the embedding similarity approach alone achieves the stated 80% precision goal. LLM refinement adds a provider dependency and cost. Consider making it optional/deferred.

### Complexity Score

| Metric | Count | Assessment |
|--------|-------|------------|
| New packages | 9 Python + 1 Rust stub | High -- but each is small and focused |
| New abstractions (Protocols) | 8 | Moderate -- Protocols are thin interfaces |
| Data models | 16+ frozen dataclasses | High count, but individually simple |
| External dependencies | py-clob-client, sentence-transformers, faiss, pyyaml, anthropic SDK | Moderate risk surface |
| Files touched per checkpoint | 3-6 | Acceptable per checkpoint |
| Total estimated files | ~40-50 | Large for a v1 but well-partitioned |

### What Already Exists in Codebase

**Nothing.** This is a greenfield repo with only an empty initial commit.

**Adjacent codebase: auto-research.** Critical finding: auto-research uses **JSON** for benchmarks (`benchmark.json`), not YAML. Its `BenchmarkItem` model has fields `id`, `question`, `rubric`, `must_include`, `required_sources`. The spec states the tool harness "consumes candidates.yaml and benchmarks/*.yaml from auto-research exports" but auto-research does not export YAML -- it exports JSON. This is a data format mismatch that must be resolved (see Concern #1).

---

## Checkpoint Review

### CP01: Project scaffolding, core data models, and Protocol interfaces

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | TOO_LARGE | 16+ dataclasses + 8 Protocols + monorepo scaffolding in one checkpoint. Split suggestion: CP01a (scaffolding + models), CP01b (protocols). Alternatively acceptable as-is if the Generator treats models and protocols as a single coherent unit -- but 6 acceptance criteria is at the upper bound. |
| Acceptance Criteria | TESTABLE | Each criterion is concrete and verifiable. `frozen=True` mutation test is well-specified. |
| Dependencies | CORRECT | None required. |
| TDD Readiness | YES | Tests can be written before implementation -- instantiation tests, immutability tests, import tests. |

**Effort Estimate:** M (borderline L due to model count)

**Failure Mode:** Model fields defined in CP01 turn out to be insufficient for CP04/CP05 connector normalization. For example, Polymarket's CLOB model may require fields not anticipated in the `Market` dataclass. Mitigation: the `raw: dict` field is a good escape hatch, but consider documenting which fields are required vs. optional in the Protocol contract.

---

### CP02: Tool Harness -- benchmark loading and runner with survival gate

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Focused scope: schema loading + runner + survival gate. |
| Acceptance Criteria | TESTABLE | Clear pass/fail criteria. "Invalid YAML raises clear validation errors" could be more specific -- which validation errors? |
| Dependencies | CORRECT | Depends on CP01 models. |
| TDD Readiness | YES | Mock candidate enables test-first approach. |

**Effort Estimate:** M

**Failure Mode:** The benchmark YAML schema is designed without consulting actual auto-research output format, leading to an impedance mismatch when real candidates are loaded. The auto-research project uses JSON with a specific `BenchmarkItem` schema (`id`, `question`, `rubric`, `must_include`, `required_sources`). The tool harness benchmark schema must either match this or include explicit transformation logic.

---

### CP03: Tool Harness -- report generation, synergy tests, and CLI

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | TOO_LARGE | Three distinct concerns: report generation, synergy testing, CLI. Synergy testing is conceptually separate and deferrable. Split suggestion: CP03a (reports + CLI), CP03-future (synergy tests). |
| Acceptance Criteria | TESTABLE | CLI end-to-end criterion is concrete. `SynergyRunner` criteria are testable but premature. |
| Dependencies | CORRECT | Depends on CP02. |
| TDD Readiness | YES | Report output formats are well-defined (JSON schema + markdown template). |

**Effort Estimate:** M (L if synergy kept)

**Failure Mode:** CLI entry point (`uv run pms-harness evaluate`) fails because pyproject.toml `[project.scripts]` is defined in CP01 but the actual module it points to does not exist until CP03. The scaffolding in CP01 must either include a stub CLI entry or the scripts entry should be added in CP03.

---

### CP04: Polymarket connector

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Single platform, single Protocol implementation. |
| Acceptance Criteria | TESTABLE | Good use of recorded fixtures. `raw` field preservation is a smart test. |
| Dependencies | CORRECT | Depends on CP01. |
| TDD Readiness | YES | Fixtures enable test-first. |

**Effort Estimate:** M

**Failure Mode:** `py-clob-client` library is poorly maintained or has breaking API changes. The Polymarket CLOB client has had periods of instability. Mitigation: pin exact version in dependencies, and ensure the connector wraps the client such that a replacement is a single-file change.

---

### CP05: Kalshi connector

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Mirrors CP04 structure. |
| Acceptance Criteria | TESTABLE | Same quality as CP04. Missing: `get_historical_prices()` is listed in CP04 but not CP05 -- inconsistency. |
| Dependencies | CORRECT | Depends on CP01. |
| TDD Readiness | YES | Same fixture pattern as CP04. |

**Effort Estimate:** M

**Failure Mode:** Kalshi API requires authentication for all endpoints (including market listing), unlike Polymarket which has some public endpoints. If recorded fixtures are captured from an authenticated session, they may contain sensitive data (tokens in headers, account-specific responses). Mitigation: fixture sanitization step in test setup.

---

### CP06: Pipeline orchestrator and config system

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Orchestrator + config + registry is a coherent unit. |
| Acceptance Criteria | TESTABLE | Full cycle with mocks is well-defined. `ModuleRegistry` resolution is testable. |
| Dependencies | MISSING | Listed as "CP01 (protocols)" but the `run_cycle()` loop implies knowledge of Strategy, Risk, Executor contracts that are defined in CP07-CP09. While mock implementations can be written against Protocols alone, the orchestrator's cycle logic implicitly depends on understanding the data flow between modules. This is a soft dependency -- the Protocols from CP01 are sufficient to compile, but the orchestrator may need revision after CP07-CP09 reveal the actual calling conventions. |
| TDD Readiness | YES | Mock implementations enable full cycle testing. |

**Effort Estimate:** M

**Failure Mode:** The orchestrator hard-codes a linear pipeline (sense -> strategy -> risk -> execute -> evaluate -> feedback) but real trading requires conditional branching (e.g., risk rejection should skip execution, strategy may produce zero orders). If the pipeline does not handle empty/rejected states at each stage, it will fail on the first non-trivial scenario. Acceptance criteria should explicitly test: "When RiskManager rejects all orders, pipeline completes without calling Executor.submit_order()."

---

### CP07: Strategy framework with arbitrage calculator

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Focused on one strategy implementation. |
| Acceptance Criteria | TESTABLE | Spread detection and subset violation detection are concrete. Feedback adjustment is testable. |
| Dependencies | MISSING | Listed as "CP01 (protocols, models)" but `on_correlation_found()` implies correlation data from CP10. The strategy needs `CorrelationPair` objects as input, which come from the correlation detector. This creates a circular dependency concern: CP07 needs CP10 output format, CP10 needs CP01 models. In practice CP01 models break the cycle, but the spec should make this explicit. |
| TDD Readiness | YES | Price data and correlation pairs can be constructed in tests. |

**Effort Estimate:** M

**Failure Mode:** Arbitrage strategy generates orders for both sides of a cross-platform spread, but the orders are not atomic -- one side fills and the other does not, leaving an unhedged position. The strategy must track paired-order state. This is not addressed in acceptance criteria.

---

### CP08: Risk manager and order executor

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Two related components, logically coupled. |
| Acceptance Criteria | TESTABLE | Guardrails bounds testing and retry logic are concrete. |
| Dependencies | CORRECT | CP01 + CP04/CP05 for executor routing. |
| TDD Readiness | YES | Mock connectors enable executor testing. |

**Effort Estimate:** M

**Failure Mode:** Executor retry logic on transient failures can cause duplicate order submission if the original order was received but the response was lost (network timeout). Without idempotency keys or order-status checking before retry, this is a real money-losing bug. Acceptance criteria should include: "Executor checks order status before retrying on timeout."

---

### CP09: Evaluation layer -- metrics collector and feedback engine

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Metrics + feedback is a coherent unit. |
| Acceptance Criteria | TESTABLE | Fuzz test for guardrails is excellent. P&L calculation over time window is concrete. |
| Dependencies | CORRECT | Depends on CP01. |
| TDD Readiness | YES | Metrics can be recorded and verified with synthetic data. |

**Effort Estimate:** M

**Failure Mode:** `MetricsCollector.record_order()` and `record_price_snapshot()` "persist data" but the persistence mechanism is unspecified. In-memory storage loses all data on process restart. If this is intentional for v1, state it explicitly. If persistence is needed, specify the storage backend (SQLite? JSON files?).

---

### CP10: Embedding engine and correlation detector

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Embeddings + correlation detection is a coherent unit, though it is the largest checkpoint. |
| Acceptance Criteria | TESTABLE | The 80% precision on 20 hand-labeled pairs is concrete and measurable. Subset relationship detection test is specific. |
| Dependencies | CORRECT | Depends on CP01. |
| TDD Readiness | PARTIAL | The hand-labeled test set (`correlation_test_set.json`) must be created as part of test setup, which is fine. However, the LLM-based classification step is non-deterministic -- tests will be flaky unless the LLM call is mocked or a deterministic fallback is used for CI. |

**Effort Estimate:** L

**Failure Mode:** sentence-transformers + faiss adds ~2GB of model downloads and significant memory usage. CI environments may timeout or OOM. Mitigation: use a smaller model (all-MiniLM-L6-v2 is already small, but faiss index construction can still be memory-heavy for large market sets). Specify a maximum test corpus size.

---

## Concerns

### Concern 1: auto-research integration format mismatch
- **Severity:** critical
- **Details:** The spec states the tool harness "consumes candidates.yaml and benchmarks/*.yaml from auto-research exports." However, auto-research uses JSON exclusively (benchmark.json with a specific `BenchmarkItem` schema: `id`, `question`, `rubric`, `must_include`, `required_sources`). There are no YAML exports in auto-research. The spec also does not define the expected YAML schema for benchmarks or candidates, which means the Generator will invent one that may not match.
- **Suggested fix:** Either (a) change the harness to consume JSON matching auto-research's existing schema, or (b) define the exact YAML schema in the spec and note that a YAML export will be added to auto-research later, or (c) support both formats. Option (a) is simplest. At minimum, document the expected schema fields explicitly in the spec.

### Concern 2: No persistence model specified for MetricsCollector
- **Severity:** critical
- **Details:** CP09 says `record_order()` and `record_price_snapshot()` "persist data" and `get_pnl()` queries over a "time window." This implies durable storage, but no storage backend is specified. The Generator will have to guess: in-memory dict? SQLite? JSON file? This ambiguity affects CP06 (orchestrator) and CP09 (evaluation) and could require rework if the wrong choice is made.
- **Suggested fix:** Specify the storage backend explicitly. Recommendation for v1: in-memory with optional SQLite persistence behind a `StorageProtocol`. Mark persistence as in-memory-only for v1 and note SQLite as a future enhancement.

### Concern 3: Missing acceptance criteria for pipeline edge cases in CP06
- **Severity:** critical
- **Details:** The pipeline orchestrator's `run_cycle()` acceptance criteria only test the happy path (all modules produce output). Real pipeline execution must handle: (a) strategy produces zero orders, (b) risk manager rejects all orders, (c) connector raises an exception during market fetch, (d) executor timeout. Without these, the Generator will build a linear pipeline that crashes on any non-trivial scenario.
- **Suggested fix:** Add acceptance criteria to CP06: "When strategy returns empty order list, pipeline completes without calling risk/executor." "When risk manager rejects all orders, pipeline completes without calling executor." "When connector raises ConnectionError, pipeline logs error and completes cycle without crashing."

### Concern 4: Connector `stream_prices()` WebSocket complexity
- **Severity:** warning
- **Details:** Both CP04 and CP05 include `stream_prices()` yielding `PriceUpdate` objects with "recorded WebSocket messages." WebSocket handling adds async complexity (asyncio event loops, connection lifecycle, reconnection logic) that is disproportionate to v1 needs. No downstream consumer in the spec actually calls `stream_prices()` -- the pipeline orchestrator uses `get_active_markets()`.
- **Suggested fix:** Defer `stream_prices()` to a future checkpoint. For v1, the ConnectorProtocol can define it but connectors can raise `NotImplementedError`. Alternatively, implement it as a polling wrapper over REST endpoints.

### Concern 5: CP01 model count may be under-specified
- **Severity:** warning
- **Details:** CP01 lists 16 dataclasses but does not specify their fields. The Generator will design fields based on general knowledge, but connectors (CP04/CP05) need specific fields to normalize platform data. If Market, OrderBook, etc. are designed without consulting the actual Polymarket/Kalshi API response shapes, rework will be needed.
- **Suggested fix:** Add a brief field listing for the 4-5 most critical models (Market, Order, OrderBook, PriceUpdate, Position) in the spec, informed by the target platform APIs. At minimum: `Market(id, platform, question, outcomes, volume, end_date, url, raw)`, `Order(id, platform, market_id, side, price, size, order_type)`.

### Concern 6: CP03 CLI entry point timing
- **Severity:** warning
- **Details:** CP01 scaffolds pyproject.toml. CP03 adds CLI entry point (`uv run pms-harness evaluate`). The `[project.scripts]` section in pyproject.toml must reference the CLI module, but the module does not exist until CP03. Either CP01 must not declare the script entry, or it must include a stub.
- **Suggested fix:** Add the `[project.scripts]` entry in CP03 when the CLI module is created, not in CP01.

### Concern 7: Executor idempotency not addressed
- **Severity:** warning
- **Details:** CP08's executor retry logic has no idempotency mechanism. On a network timeout, retrying `submit_order()` could duplicate the order on the exchange. This is a real-money safety issue even with recorded fixtures -- the design should account for it from the start.
- **Suggested fix:** Add to CP08 acceptance criteria: "Executor assigns a client-side order ID and checks order status before retry." Or if this is out of scope for v1 (no live trading), explicitly document this as a known gap.

### Concern 8: CP10 test determinism with LLM calls
- **Severity:** warning
- **Details:** CP10's `CorrelationDetector.detect()` uses "LLM-based relation type classification." LLM responses are non-deterministic, making the 80% precision test potentially flaky in CI. sentence-transformers embeddings are deterministic, but the LLM classification layer is not.
- **Suggested fix:** Mock the LLM call in CI tests and test it separately with a recorded response fixture. Or make the LLM refinement step optional with a rule-based fallback for CI.

### Concern 9: Inconsistent `get_historical_prices()` between CP04 and CP05
- **Severity:** info
- **Details:** CP04 lists `get_historical_prices()` as an acceptance criterion. CP05 does not mention it. If this is a `ConnectorProtocol` method, both connectors must implement it.
- **Suggested fix:** Either add `get_historical_prices()` to CP05's criteria, or remove it from CP04 if no consumer needs it. Check which downstream modules require historical data.

### Concern 10: Phase labeling is confusing (A, C, B ordering)
- **Severity:** info
- **Details:** The Goal section labels phases as A, C, B (Tool Harness, Trading Pipeline, Correlation Engine). The checkpoint ordering is A (CP02-03), then interleaved C (CP04-09) and B (CP10). This non-sequential labeling is confusing for the Generator.
- **Suggested fix:** Relabel as Phase 1/2/3 in execution order, or remove phase labels entirely since checkpoints already define the execution sequence.

### Concern 11: uv workspace structure unclear
- **Severity:** info
- **Details:** The directory layout shows 9 Python packages under `python/`. It is unclear if these are separate uv workspace members (each with their own pyproject.toml) or sub-packages of a single package. The uv workspace model requires each member to have its own pyproject.toml. The spec should clarify whether this is a single package with sub-packages or a true workspace with 9 members.
- **Suggested fix:** Specify explicitly: "Each `pms_*` directory is a sub-package of a single `pms` package (single pyproject.toml)" or "Each `pms_*` directory is a workspace member (own pyproject.toml)." The former is simpler for v1.

---

## Effort Estimates Summary

| Checkpoint | Spec Estimate | Reviewer Estimate | Delta |
|------------|--------------|-------------------|-------|
| CP01 | M | M (borderline L) | Agree with caveat |
| CP02 | M | M | Agree |
| CP03 | M | M (L if synergy kept) | Conditional |
| CP04 | M | M | Agree |
| CP05 | M | M | Agree |
| CP06 | M | M | Agree |
| CP07 | M | M | Agree |
| CP08 | M | M | Agree |
| CP09 | M | M | Agree |
| CP10 | L | L | Agree |

**Total estimated effort:** 9M + 1L. This is a substantial v1, roughly 10-12 Generator sessions if each checkpoint maps to one session.

---

## Failure Modes Summary

| Checkpoint | Production Failure Scenario |
|------------|---------------------------|
| CP01 | Model fields insufficient for platform normalization, requiring rework across all consumers |
| CP02 | Benchmark schema mismatch with auto-research JSON output causes silent data loss or load failures |
| CP03 | CLI entry point undefined in pyproject.toml causes `uv run pms-harness` to fail with "command not found" |
| CP04 | py-clob-client breaking change or deprecation blocks Polymarket integration |
| CP05 | Kalshi API auth tokens leak into recorded test fixtures committed to git |
| CP06 | Pipeline crashes when any module returns empty/error result instead of gracefully degrading |
| CP07 | Non-atomic cross-platform arbitrage orders leave unhedged position on partial fill |
| CP08 | Executor retry logic submits duplicate orders on network timeout without idempotency check |
| CP09 | In-memory metrics storage loses all historical data on process restart, breaking P&L calculations |
| CP10 | sentence-transformers model download (400MB+) times out CI, or LLM classification flakiness fails the 80% precision gate |

---

## Recommendations for Revision

1. **Resolve auto-research format mismatch** (Critical) -- Decide JSON vs. YAML and document the exact schema.
2. **Specify storage backend for MetricsCollector** (Critical) -- Even "in-memory only for v1" is an acceptable answer, but it must be stated.
3. **Add error-path acceptance criteria to CP06** (Critical) -- Empty orders, rejected orders, connector failures.
4. **Defer `stream_prices()` and synergy tests** (Warning) -- Reduce scope to accelerate v1 delivery.
5. **Add key model field listings** (Warning) -- At least Market, Order, OrderBook, PriceUpdate.
6. **Clarify package structure** (Info) -- Single package with sub-packages vs. workspace members.
7. **Fix phase labeling** (Info) -- Sequential numbering or remove labels.
