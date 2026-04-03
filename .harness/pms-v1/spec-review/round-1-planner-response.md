---
task_id: pms-v1
round: 1
---

## Accepted Changes

### Critical Fixes

1. **Concern #1 (auto-research format mismatch)**: Accepted. The tool harness will define its own YAML schema explicitly in the spec. auto-research will produce an `exports/` directory with these formats in a future branch. The harness owns its schema; auto-research adapts to it. Added explicit field listings for benchmark and candidate YAML schemas.

2. **Concern #2 (MetricsCollector persistence)**: Accepted. Specified as **in-memory only for v1**. Added `StorageProtocol` as a future extension point. CP09 acceptance criteria updated to test in-memory storage explicitly.

3. **Concern #3 (pipeline edge cases)**: Accepted. Added 3 error-path acceptance criteria to CP06: empty orders, rejected orders, connector failure.

### Warning Fixes

4. **Concern #4 (stream_prices WebSocket)**: Accepted. Deferred `stream_prices()` and `get_historical_prices()` from CP04/CP05. ConnectorProtocol retains the methods but v1 connectors raise `NotImplementedError`. Removed from acceptance criteria.

5. **Concern #5 (model fields under-specified)**: Accepted. Added explicit field listings for Market, Order, OrderBook, PriceUpdate, Position in CP01.

6. **Concern #6 (CLI entry point timing)**: Accepted. `[project.scripts]` entry moved to CP03.

7. **Concern #7 (executor idempotency)**: Accepted. Added client-side order ID + status-check-before-retry to CP08 acceptance criteria.

8. **Concern #8 (CP10 LLM test determinism)**: Accepted. LLM classification mocked in CI. Added rule-based fallback for CI testing. LLM integration tested separately with recorded fixtures.

9. **Concern #9 (historical prices inconsistency)**: Accepted. Removed `get_historical_prices()` from both CP04 and CP05. Deferred.

10. **Concern #10 (phase labeling)**: Accepted. Relabeled to Phase 1/2/3 in execution order.

11. **Concern #11 (package structure)**: Accepted. Clarified: single `pms` package with sub-packages (single pyproject.toml).

### Scope Reductions

- **Synergy tests deferred**: Removed `SynergyRunner` from CP03. CP03 now covers report generation + CLI only.
- **stream_prices deferred**: Removed from CP04/CP05 acceptance criteria.
- **get_historical_prices deferred**: Removed from CP04/CP05 acceptance criteria.
- **LLM refinement made optional**: CP10 correlation detector uses rule-based classification by default, LLM refinement is an optional enhancement.

## Rejected Changes

- **Split CP01**: Keeping as single checkpoint. Models and protocols are tightly coupled (protocols reference models). Splitting adds coordination overhead without meaningful benefit. The field listings make the scope clearer for the Generator.

- **Split CP03**: Accepted removing synergy tests but keeping reports + CLI as one checkpoint. These are tightly coupled (CLI invokes runner, runner produces reports).

## Spec Updated To

Version 2.
