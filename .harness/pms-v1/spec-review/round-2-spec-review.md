---
task_id: pms-v1
spec_version: 2
round: 2
---

# Spec Review: Round 2

## Verdict: `approve`

All three critical concerns from round 1 have been adequately addressed. The spec is now executable by a Generator with unambiguous acceptance criteria for every checkpoint. Two minor warnings remain but do not block execution.

---

## Resolution of Round 1 Criticals

### Critical #1: auto-research format mismatch -- RESOLVED

The spec now defines explicit YAML schemas for both benchmarks and candidates (lines 83-117). The schemas are self-contained with typed fields, and the spec explicitly states "auto-research will produce compatible exports in a future branch" (line 79). This cleanly decouples the harness from auto-research. The Generator can implement schema validation against these definitions without ambiguity.

One note: the benchmark schema defines `metric` as a string union (`"count" | "ms" | "boolean" | "percentage" | "ratio" | "days" | "enum [values]"`). The `"enum [values]"` variant embeds a list inside a string, which is unusual. The Generator should parse this, but it is a minor implementation detail, not a spec-level concern.

### Critical #2: No persistence model -- RESOLVED

CP09 acceptance criteria now explicitly state "in-memory storage (dict-based)" and "explicitly documented as non-persistent for v1" (line 297). `StorageProtocol` is listed in CP01's Protocol interfaces (line 166). The spec's Out of Scope section reinforces this: "MetricsCollector is in-memory only. SQLite/file persistence deferred" (line 334). Clean resolution.

### Critical #3: Missing pipeline edge cases -- RESOLVED

CP06 now includes three explicit error-path acceptance criteria (lines 252-254):
- Empty order list from strategy: pipeline completes without calling risk/executor
- All orders rejected by risk manager: pipeline completes without calling executor
- Connector `ConnectionError`: pipeline logs error and completes without crashing

These are testable and unambiguous. The happy path + 3 error paths give CP06 comprehensive coverage.

---

## Scope Assessment

### Minimum Viable Scope Analysis

The revised spec has tightened scope through four deferrals:
1. SynergyRunner removed from CP03
2. `stream_prices()` deferred (raises `NotImplementedError`)
3. `get_historical_prices()` deferred (raises `NotImplementedError`)
4. LLM classification made optional in CP10

These are correct scope reductions. No further deferral opportunities are apparent -- the remaining 10 checkpoints each deliver a necessary piece of the system.

### Complexity Score

| Metric | Count | Assessment |
|--------|-------|------------|
| New sub-packages | 9 Python + 1 Rust stub | Acceptable -- single pyproject.toml |
| Protocols | 9 (added StorageProtocol) | Acceptable -- thin interfaces |
| Data models | 14 frozen dataclasses | Explicitly field-specified now |
| External dependencies | py-clob-client, sentence-transformers, numpy, pyyaml | Moderate -- faiss removed from explicit mentions |
| Checkpoints | 10 | 9M + 1L, ~10-12 Generator sessions |

### What Already Exists in Codebase

Empty repo (`.git` + `.harness/` only). Greenfield. No existing code to extend or conflict with.

---

## Checkpoint Review

### CP01: Project scaffolding, core data models, and Protocol interfaces

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Planner rejected the split, justifying that models and protocols are tightly coupled. Accepted -- the explicit field definitions (lines 122-159) make scope clear enough for a single session. |
| Acceptance Criteria | TESTABLE | 6 criteria, all concrete. `frozen=True` mutation test, mypy strict, import tests. |
| Dependencies | CORRECT | None. |
| TDD Readiness | YES | All criteria can be test-first. |

**Effort Estimate:** M

**Failure Mode:** A Protocol method signature (e.g., `ConnectorProtocol.get_active_markets()` return type) is designed without consulting the actual calling pattern in CP06's orchestrator, requiring a signature change later. Low risk since Protocols are structural typing, but worth noting.

---

### CP02: Tool Harness -- benchmark loading and runner with survival gate

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Focused scope. |
| Acceptance Criteria | TESTABLE | "Invalid YAML raises `BenchmarkValidationError` with field path and expected type" (line 191) is a strong improvement over round 1's vague "clear validation errors." |
| Dependencies | CORRECT | CP01. |
| TDD Readiness | YES | Mock candidate enables test-first. |

**Effort Estimate:** M

**Failure Mode:** YAML schema validation is implemented as ad-hoc Python checks rather than using a schema validation library (e.g., pydantic, jsonschema). If ad-hoc, edge cases in the schema (nested `functional_tests` categories, `metric` type parsing) may be under-tested. The Generator should consider using pydantic for schema validation, but this is an implementation choice, not a spec concern.

---

### CP03: Tool Harness -- report generation and CLI

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Synergy tests removed. Reports + CLI is a coherent unit. |
| Acceptance Criteria | TESTABLE | CLI exit code test, JSON schema validation, markdown structure requirements all concrete. `[project.scripts]` entry explicitly in this checkpoint (line 205). |
| Dependencies | CORRECT | CP02. |
| TDD Readiness | YES | Report format is well-defined. |

**Effort Estimate:** M

**Failure Mode:** `scores.json` schema is not defined in the spec -- only "validates against JSON schema" is stated (line 202). The Generator must invent this schema. Low risk since the data flows from `ModuleReport` and the structure is implied, but the spec could be more explicit. Not blocking.

---

### CP04: Polymarket connector

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Single platform, clear scope. |
| Acceptance Criteria | TESTABLE | `NotImplementedError` for `stream_prices()` is explicit (line 219). Fixture sanitization criterion added (line 222). `raw` field preservation tested. |
| Dependencies | CORRECT | CP01. |
| TDD Readiness | YES | Fixture-driven testing. |

**Effort Estimate:** M

**Failure Mode:** py-clob-client version instability. The connector wraps this library, so a pin + isolation layer mitigates this. The fixture-based testing also insulates from upstream API changes.

---

### CP05: Kalshi connector

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Mirrors CP04 structure. |
| Acceptance Criteria | TESTABLE | Now consistent with CP04 -- both defer `stream_prices()`, both require sanitized fixtures. |
| Dependencies | CORRECT | CP01. |
| TDD Readiness | YES | Same fixture pattern. |

**Effort Estimate:** M

**Failure Mode:** Kalshi API requires authentication for all endpoints. Recorded fixtures must be captured from an authenticated session, then sanitized. If sanitization misses auth tokens in nested response headers or pagination cursors, secrets could leak to git. The acceptance criterion "no auth tokens, no account data" (line 239) addresses this.

---

### CP06: Pipeline orchestrator and config system

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Orchestrator + config + registry + error paths = coherent unit. |
| Acceptance Criteria | TESTABLE | 8 criteria including 3 error paths. Each error path has a specific input condition and expected outcome. "covers happy path + 3 error paths" (line 257) is explicit. |
| Dependencies | CORRECT | CP01 (protocols). The soft dependency on CP07-CP09 calling conventions is resolved by Protocol interfaces in CP01. |
| TDD Readiness | YES | Mock implementations against Protocols enable complete testing. |

**Effort Estimate:** M

**Failure Mode:** `ModuleRegistry` resolves class names from YAML config to instances. If the registry uses `importlib` dynamic imports, typos in config class paths will produce confusing errors at runtime rather than at config-load time. Acceptance criteria should ensure config validation catches invalid class paths early, but this is an implementation detail the Generator can handle.

---

### CP07: Strategy framework with arbitrage calculator

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Single strategy implementation. |
| Acceptance Criteria | TESTABLE | Paired order tracking added (line 270-271), addressing round 1's non-atomic execution concern. `correlation_id` for cross-platform orders is a clean solution. Feedback adjustment tested. |
| Dependencies | CORRECT | CP01. The circular dependency with CP10 is resolved -- `CorrelationPair` is defined in CP01 models (line 148), not in CP10. The spec explicitly notes this (line 267). |
| TDD Readiness | YES | Synthetic price data and correlation pairs constructable in tests. |

**Effort Estimate:** M

**Failure Mode:** `on_correlation_found()` generates orders for subset pricing violations, but the strategy does not know the current position or open orders. Without this context, it could generate redundant orders for the same opportunity. Risk manager in CP08 provides some protection via exposure limits, but the strategy itself has no de-duplication. Acceptable for v1 given rule-based feedback will adjust thresholds.

---

### CP08: Risk manager and order executor

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Two coupled components. |
| Acceptance Criteria | TESTABLE | Idempotency addressed: "assigns a client-side `order_id` and checks order status before retrying on timeout" (line 285). Guardrails bounds testing, retry with exponential backoff. |
| Dependencies | CORRECT | CP01 + CP04/CP05. |
| TDD Readiness | YES | Mock connectors for executor, synthetic orders for risk manager. |

**Effort Estimate:** M

**Failure Mode:** Exponential backoff without jitter causes thundering herd if multiple orders timeout simultaneously. Implementation detail, not a spec concern.

---

### CP09: Evaluation layer -- metrics collector and feedback engine

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Metrics + feedback, coherent unit. |
| Acceptance Criteria | TESTABLE | In-memory storage explicit. Fuzz test with 100 random inputs (line 302) is excellent -- concrete and measurable. P&L over time window, per-strategy win rate. |
| Dependencies | CORRECT | CP01. |
| TDD Readiness | YES | Synthetic data for metrics, property-based testing for guardrails. |

**Effort Estimate:** M

**Failure Mode:** In-memory dict grows unbounded during a long-running session. No eviction policy specified. For v1, this is acceptable since sessions are short (test or manual), but worth noting for the Generator to add a comment or simple LRU bound.

---

### CP10: Embedding engine and correlation detector

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Granularity | OK | Largest checkpoint but coherent. |
| Acceptance Criteria | TESTABLE | 9 criteria, which is at the upper bound. However, several are closely related (the three "given test markets..." criteria test the same method with different inputs). The 80% precision gate on 20 hand-labeled pairs is concrete. Tests are deterministic -- "no LLM calls; sentence-transformers is deterministic with fixed model" (line 318). |
| Dependencies | CORRECT | CP01. |
| TDD Readiness | YES | Hand-labeled test set created as fixture. Deterministic model inference. |

**Effort Estimate:** L

**Failure Mode:** sentence-transformers model download (~80MB for all-MiniLM-L6-v2) in CI. If the model is not cached, every CI run downloads it. The Generator should either bundle the model or configure a CI cache. Not a spec concern but worth noting.

---

## Concerns

### Concern 1: `scores.json` schema undefined

- **Severity:** warning
- **Details:** CP03 states `scores.json` "validates against JSON schema" (line 202) but no schema is defined in the spec. The Generator must invent the schema based on `ModuleReport` structure. This is likely fine since the report structure is implied by CP02's `SurvivalResult` and scored functional test results, but it is the only output format in the spec that lacks an explicit schema definition while claiming to "validate against" one.
- **Suggested fix:** Either add a brief `scores.json` schema to the spec (even 5-10 lines showing the expected structure), or change the criterion to "writes valid JSON with per-candidate scores, survival status, and weighted overall score" without referencing a separate JSON schema. The latter is simpler and avoids the Generator needing to also create a JSON Schema definition file.

### Concern 2: CP10 acceptance criteria count is at 9

- **Severity:** info
- **Details:** CP10 has 9 acceptance criteria. The round 1 guideline was "if a checkpoint has 5+ acceptance criteria, it is probably too large." However, examining the criteria, several are variants of the same test (three "given test markets..." checks, two threshold behaviors). Functionally, this is testing one method (`detect()`) with different inputs. The criteria are individually small and closely related, so 9 is acceptable here as long as the Generator treats them as a single test suite rather than 9 independent test files.
- **Suggested fix:** No action required. The effort estimate of L already accounts for this being the largest checkpoint.

---

## Effort Estimates Summary

| Checkpoint | Spec Estimate | Reviewer Estimate | Delta |
|------------|--------------|-------------------|-------|
| CP01 | M | M | Agree |
| CP02 | M | M | Agree |
| CP03 | M | M | Agree |
| CP04 | M | M | Agree |
| CP05 | M | M | Agree |
| CP06 | M | M | Agree |
| CP07 | M | M | Agree |
| CP08 | M | M | Agree |
| CP09 | M | M | Agree |
| CP10 | L | L | Agree |

**Total estimated effort:** 9M + 1L. Approximately 10-12 Generator sessions.

---

## Failure Modes Summary

| Checkpoint | Production Failure Scenario |
|------------|---------------------------|
| CP01 | Protocol method signatures require revision when CP04-CP09 reveal actual calling patterns (low risk due to structural typing) |
| CP02 | Ad-hoc YAML validation misses edge cases in nested schema structure (metric type parsing, weight sum validation) |
| CP03 | `scores.json` schema invented by Generator does not match what downstream consumers expect (no consumer exists yet, so low risk) |
| CP04 | py-clob-client version drift breaks Polymarket normalization (mitigated by version pinning + fixture tests) |
| CP05 | Kalshi fixture sanitization misses auth tokens in nested response structures |
| CP06 | Dynamic module import via `ModuleRegistry` produces confusing errors on config typos |
| CP07 | Strategy generates redundant orders for same opportunity without position/open-order awareness |
| CP08 | Exponential backoff without jitter causes correlated retries on simultaneous timeouts |
| CP09 | In-memory metrics dict grows unbounded during long sessions |
| CP10 | sentence-transformers model download causes CI timeout on first uncached run |

---

## Summary

The v2 spec is well-revised and ready for execution. All three critical concerns from round 1 have been cleanly resolved:

1. **YAML schemas** are now explicitly defined in the spec with typed fields.
2. **In-memory storage** is explicitly specified for v1 with `StorageProtocol` as a future extension point.
3. **Error paths** are covered with 3 specific acceptance criteria in CP06.

The scope reductions (synergy tests, WebSocket streaming, historical prices, LLM classification) are appropriate and reduce delivery risk without sacrificing v1 value.

The one remaining warning (undefined `scores.json` schema) is non-blocking -- the Generator can derive the structure from the `ModuleReport` type. The Planner may optionally address it, but it does not require another revision round.

Approved for Generator execution.
