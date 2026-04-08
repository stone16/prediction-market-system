---
task_id: pms-v1
title: "Prediction Market System — Foundation, Tool Harness, and Trading Pipeline"
version: 2
status: approved
branch: feature/pms-v1
created: 2026-04-03T12:00:00Z
updated: 2026-04-03T13:00:00Z
---

## Goal

Build a modular prediction market trading system with three phases (in execution order):

1. **Phase 1 — Tool Evaluation Harness** (CP01-CP03): A framework to systematically evaluate open-source prediction market tools by actually running them against defined benchmarks. Consumes candidate lists and benchmark definitions, executes survival gates and functional tests, and outputs scored reports.

2. **Phase 2 — Trading Pipeline Skeleton** (CP04-CP09): A complete sense → strategy → risk → execute → evaluate → feedback pipeline with pluggable modules via Python Protocols. Each module slot is defined by a Protocol interface, with working implementations for Polymarket and Kalshi.

3. **Phase 3 — Correlation Engine** (CP10): An embedding-based cross-market correlation detector that finds logically related markets (subset, superset, overlapping, contradictory), enabling arbitrage opportunity identification.

The system uses Python as the primary language. Rust is reserved for future performance-critical paths and is not implemented in v1.

Target platforms: **Polymarket** and **Kalshi**.

## Success Criteria

- [ ] `uv run pytest` passes with ≥85% coverage across all modules
- [ ] `uv run mypy python/ --strict` passes with zero errors
- [ ] Tool harness can load a benchmark YAML, run survival gate against a mock candidate, and produce scores.json + report.md
- [ ] Polymarket connector can fetch active markets and return normalized `Market` objects (using recorded fixtures)
- [ ] Kalshi connector can fetch active markets and return normalized `Market` objects (using recorded fixtures)
- [ ] Pipeline orchestrator completes one full cycle (sense → strategy → risk → execute → evaluate → feedback) with mock modules, including error paths (empty orders, rejected orders, connector failure)
- [ ] Correlation detector finds related markets from a test set of market descriptions with >80% precision on hand-labeled pairs (using rule-based classification; LLM refinement is optional)
- [ ] Feedback engine generates `EvaluationFeedback` that adjusts strategy parameters within guardrail bounds

## Technical Approach

### Architecture

Monorepo with a single Python package (`pms`) containing sub-packages. Rust workspace scaffolded but empty for v1.

```
prediction-market-system/
├── rust/                          # Rust workspace (empty for v1)
│   └── Cargo.toml
├── python/
│   └── pms/                       # Single package
│       ├── __init__.py
│       ├── models/                # Core data models (frozen dataclasses)
│       ├── protocols/             # Protocol definitions (interfaces)
│       ├── connectors/            # Platform adapters (Polymarket, Kalshi)
│       ├── strategy/              # Strategy modules (arbitrage, correlation)
│       ├── execution/             # Order executor + risk manager
│       ├── evaluation/            # Metrics collector + feedback engine
│       ├── orchestrator/          # Pipeline orchestrator + config
│       ├── embeddings/            # Embedding engine (Python fallback)
│       └── tool_harness/          # Tool evaluation harness
├── benchmarks/                    # Benchmark YAML definitions
├── candidates/                    # Candidate tool YAML configs
├── tests/                         # Tests + fixtures
├── pyproject.toml                 # Single package config
└── CLAUDE.md
```

### Key Design Decisions

1. **Protocol-based pluggability**: All module interfaces are Python `Protocol` classes (structural typing). Third-party tools need only a thin adapter to conform — no base class inheritance required.

2. **Frozen dataclasses**: All data models use `@dataclass(frozen=True)` for immutability. No shared mutable state between layers.

3. **Python-first, Rust-later**: All modules implemented in pure Python for v1. Rust crates scaffolded but empty. Future Rust modules will have Python fallbacks with factory auto-selection.

4. **Survival gate pattern**: Tool evaluation uses a cheap 3-step gate (install → connect → fetch one record) before expensive functional testing.

5. **Rule-based feedback**: v1 uses explicit rules with guardrail bounds. No ML/RL.

6. **In-memory storage for v1**: MetricsCollector uses in-memory storage. No durable persistence. `StorageProtocol` defined for future SQLite/file backends.

7. **auto-research integration**: The tool harness defines its own YAML schemas (below). auto-research will produce compatible exports in a future branch.

### Benchmark YAML Schema

```yaml
# benchmarks/<module_name>.yaml
module: string                     # Module identifier
version: integer                   # Schema version

survival_gate:                     # Must all pass to proceed
  - id: string                     # Unique test ID
    test: string                   # Human-readable description
    timeout_seconds: integer       # Max execution time

functional_tests:                  # Scored evaluation
  <category_name>:                 # e.g. "data_coverage", "performance"
    weight: float                  # 0.0-1.0, all weights sum to 1.0
    tests:
      - id: string
        test: string               # Description
        metric: string             # "count" | "ms" | "boolean" | "percentage" | "ratio" | "days" | "enum [values]"
        baseline: number | null    # Expected minimum (null = informational only)
        lower_is_better: boolean   # Default: false
```

### Candidate YAML Schema

```yaml
# candidates/<candidate_name>.yaml
name: string                       # Candidate display name
repo: string                       # GitHub URL
language: string                   # "python" | "typescript" | "rust"
install: string                    # Installation command
platforms: list[string]            # ["polymarket", "kalshi", ...]
module: string                     # Which module this is a candidate for
notes: string                      # Freeform notes
config:                            # Candidate-specific config (passed to test runner)
  <key>: <value>
```

### Core Model Field Definitions

```python
Market(platform, market_id, title, description, outcomes: list[Outcome],
       volume: Decimal, end_date: datetime | None, category: str,
       url: str, status: str, raw: dict)

Outcome(outcome_id: str, title: str, price: Decimal)

Order(order_id: str, platform: str, market_id: str, outcome_id: str,
      side: Literal["buy","sell"], price: Decimal, size: Decimal,
      order_type: Literal["limit","market"])

OrderBook(platform: str, market_id: str, bids: list[PriceLevel],
          asks: list[PriceLevel], timestamp: datetime)

PriceLevel(price: Decimal, size: Decimal)

PriceUpdate(platform: str, market_id: str, outcome_id: str,
            bid: Decimal, ask: Decimal, last: Decimal, timestamp: datetime)

OrderResult(order_id: str, status: Literal["filled","partial","rejected","error"],
            filled_size: Decimal, filled_price: Decimal, message: str, raw: dict)

Position(platform: str, market_id: str, outcome_id: str,
         size: Decimal, avg_entry_price: Decimal, unrealized_pnl: Decimal)

CorrelationPair(market_a: Market, market_b: Market, similarity_score: float,
                relation_type: Literal["subset","superset","overlapping","contradictory","independent"],
                relation_detail: str, arbitrage_opportunity: Decimal | None)

RiskDecision(approved: bool, reason: str, adjusted_size: Decimal | None)

EvaluationFeedback(timestamp: datetime, period: timedelta,
                   strategy_adjustments: dict[str, StrategyFeedback],
                   risk_adjustments: RiskFeedback,
                   connector_adjustments: dict[str, ConnectorFeedback])

StrategyFeedback(pnl: float, win_rate: float, avg_slippage: float, suggestion: str)
RiskFeedback(max_drawdown_hit: bool, current_exposure: Decimal, suggestion: str)
ConnectorFeedback(data_staleness_ms: float, api_error_rate: float, suggestion: str)
```

## Checkpoints

### Checkpoint 01: Project scaffolding, core data models, and Protocol interfaces

- **Scope**: Set up the project structure (pyproject.toml with single `pms` package, directory layout, Rust Cargo.toml stub, pytest/mypy config), implement all core frozen dataclasses per field definitions above, and define all Protocol interfaces (ConnectorProtocol, EmbeddingEngineProtocol, CorrelationDetectorProtocol, StrategyProtocol, ExecutorProtocol, RiskManagerProtocol, MetricsCollectorProtocol, FeedbackEngineProtocol, StorageProtocol).
- **Depends on**: none
- **Type**: infrastructure
- **Acceptance criteria**:
  - [ ] `uv sync` succeeds from repo root
  - [ ] All dataclasses in `pms.models` are importable and instantiable with valid data
  - [ ] All Protocol classes in `pms.protocols` are importable
  - [ ] `frozen=True` enforced — mutating a model field raises `FrozenInstanceError`
  - [ ] `uv run mypy python/ --strict` passes with zero errors
  - [ ] `uv run pytest tests/test_models.py` passes — covers instantiation, immutability, field types, and `raw` field preservation
- **Files of interest**: `pyproject.toml`, `python/pms/models/`, `python/pms/protocols/`
- **Effort estimate**: M

### Checkpoint 02: Tool Harness — benchmark loading and runner with survival gate

- **Scope**: Implement the tool evaluation harness: YAML benchmark schema loading/validation per the schema defined above, YAML candidate config loading, `HarnessRunner` with `run_survival_gate()` and `run_functional_tests()` methods. Include a mock candidate implementation for testing.
- **Depends on**: CP01 (models)
- **Type**: backend
- **Acceptance criteria**:
  - [ ] `benchmarks/data_connector.yaml` exists with valid survival_gate and functional_tests sections matching the defined schema
  - [ ] `candidates/mock_connector.yaml` exists as a test candidate matching the candidate schema
  - [ ] `HarnessRunner.run_survival_gate()` returns a `SurvivalResult` with pass/fail per gate item
  - [ ] `HarnessRunner.run_functional_tests()` returns scored results per test category with weighted overall score
  - [ ] Mock candidate passes survival gate and produces non-zero scores
  - [ ] Invalid YAML raises `BenchmarkValidationError` with field path and expected type
  - [ ] `uv run pytest tests/test_harness.py` passes
- **Files of interest**: `python/pms/tool_harness/runner.py`, `python/pms/tool_harness/schema.py`, `benchmarks/data_connector.yaml`
- **Effort estimate**: M

### Checkpoint 03: Tool Harness — report generation and CLI

- **Scope**: Implement report generation (scores.json + report.md from evaluation results) and CLI entry point (`uv run pms-harness evaluate --module data_connector`). Add `[project.scripts]` entry to pyproject.toml.
- **Depends on**: CP02 (harness runner)
- **Type**: backend
- **Acceptance criteria**:
  - [ ] `HarnessRunner.evaluate_module()` produces a `ModuleReport` with ranked candidates
  - [ ] `ReportGenerator` writes valid `scores.json` (machine-readable, validates against JSON schema) and `report.md` (human-readable)
  - [ ] `report.md` includes per-candidate breakdown: survival status, functional scores per category, weighted overall score, rank
  - [ ] CLI command `uv run pms-harness evaluate --module data_connector` runs end-to-end with mock candidate and exits 0
  - [ ] `[project.scripts]` entry added to pyproject.toml pointing to CLI module
  - [ ] `uv run pytest tests/test_harness_reports.py` passes
- **Files of interest**: `python/pms/tool_harness/reports.py`, `python/pms/tool_harness/cli.py`, `pyproject.toml`
- **Effort estimate**: M

### Checkpoint 04: Polymarket connector

- **Scope**: Implement `ConnectorProtocol` for Polymarket using py-clob-client and Gamma API. Normalize Polymarket-specific data into standard models. Include recorded API response fixtures for testing. `stream_prices()` and `get_historical_prices()` raise `NotImplementedError` for v1.
- **Depends on**: CP01 (models and protocols)
- **Type**: backend
- **Acceptance criteria**:
  - [ ] `PolymarketConnector` implements all `ConnectorProtocol` methods
  - [ ] `get_active_markets()` returns `list[Market]` with all fields populated per model definition
  - [ ] `get_orderbook()` returns `OrderBook` with bid/ask `PriceLevel` lists
  - [ ] `stream_prices()` raises `NotImplementedError` with message indicating v1 limitation
  - [ ] `raw` field in returned `Market` objects preserves original Polymarket Gamma API response dict
  - [ ] Tests use recorded fixtures in `tests/fixtures/polymarket/` (no live API calls)
  - [ ] Fixtures are sanitized — no API keys, tokens, or account-specific data
  - [ ] `uv run pytest tests/test_polymarket.py` passes
- **Files of interest**: `python/pms/connectors/polymarket.py`, `tests/fixtures/polymarket/`
- **Effort estimate**: M

### Checkpoint 05: Kalshi connector

- **Scope**: Implement `ConnectorProtocol` for Kalshi using Kalshi REST API. Normalize Kalshi-specific data into standard models. Include sanitized recorded API response fixtures. `stream_prices()` and `get_historical_prices()` raise `NotImplementedError` for v1.
- **Depends on**: CP01 (models and protocols)
- **Type**: backend
- **Acceptance criteria**:
  - [ ] `KalshiConnector` implements all `ConnectorProtocol` methods
  - [ ] `get_active_markets()` returns `list[Market]` with all fields populated per model definition
  - [ ] `get_orderbook()` returns `OrderBook` with bid/ask `PriceLevel` lists
  - [ ] `stream_prices()` raises `NotImplementedError`
  - [ ] `raw` field preserves original Kalshi API response dict
  - [ ] Tests use sanitized recorded fixtures in `tests/fixtures/kalshi/` (no auth tokens, no account data)
  - [ ] `uv run pytest tests/test_kalshi.py` passes
- **Files of interest**: `python/pms/connectors/kalshi.py`, `tests/fixtures/kalshi/`
- **Effort estimate**: M

### Checkpoint 06: Pipeline orchestrator and config system

- **Scope**: Implement `TradingPipeline` that wires all Protocol implementations together and runs the main sense → strategy → risk → execute → evaluate → feedback loop. Implement module registry and YAML config loading. Pipeline must handle error paths gracefully.
- **Depends on**: CP01 (protocols)
- **Type**: backend
- **Acceptance criteria**:
  - [ ] `TradingPipeline.__init__()` accepts all Protocol implementations via constructor injection
  - [ ] `TradingPipeline.run_cycle()` executes one complete happy-path loop with mock modules
  - [ ] When strategy returns empty order list, pipeline completes without calling risk manager or executor
  - [ ] When risk manager rejects all orders, pipeline completes without calling executor
  - [ ] When connector raises `ConnectionError`, pipeline logs error and completes cycle without crashing
  - [ ] Config file (`config.yaml`) specifies which module implementations to use
  - [ ] `ModuleRegistry` resolves implementation class names from config to instances
  - [ ] `uv run pytest tests/test_pipeline.py` passes — covers happy path + 3 error paths
- **Files of interest**: `python/pms/orchestrator/pipeline.py`, `python/pms/orchestrator/config.py`, `python/pms/orchestrator/registry.py`
- **Effort estimate**: M

### Checkpoint 07: Strategy framework with arbitrage calculator

- **Scope**: Implement `StrategyProtocol` base pattern plus `ArbitrageStrategy` — detects cross-market price discrepancies and same-market logical inconsistencies (subset pricing violations).
- **Depends on**: CP01 (protocols, models — CorrelationPair defined in CP01 breaks circular dependency with CP10)
- **Type**: backend
- **Acceptance criteria**:
  - [ ] `ArbitrageStrategy.on_price_update()` detects price spread > configurable threshold across platforms and returns `Order` list
  - [ ] `ArbitrageStrategy.on_correlation_found()` detects subset pricing violations (P(A⊂B) > P(B)) and returns orders
  - [ ] `ArbitrageStrategy.on_feedback()` adjusts internal thresholds based on `EvaluationFeedback`
  - [ ] Strategy respects configurable `min_spread`, `max_position_size` parameters
  - [ ] Cross-platform orders include paired order tracking (both sides reference same `correlation_id`) to support future atomic execution
  - [ ] `uv run pytest tests/test_strategy.py` passes — covers cross-market arb, subset violation, feedback adjustment, and paired order tracking
- **Files of interest**: `python/pms/strategy/arbitrage.py`, `python/pms/strategy/base.py`
- **Effort estimate**: M

### Checkpoint 08: Risk manager and order executor

- **Scope**: Implement `RiskManagerProtocol` with guardrail-bounded checks and `ExecutorProtocol` that routes orders to the correct platform connector with idempotent retry logic.
- **Depends on**: CP01 (protocols), CP04/CP05 (connectors for executor routing)
- **Type**: backend
- **Acceptance criteria**:
  - [ ] `RiskManager.check_order()` returns `RiskDecision` (approve/reject with reason)
  - [ ] Orders exceeding `max_position_per_market` or `max_total_exposure` are rejected
  - [ ] `RiskManager.update_limits()` adjusts limits within `GUARDRAILS` bounds — never exceeds floor/ceiling
  - [ ] `Executor.submit_order()` routes to correct connector based on `order.platform`
  - [ ] Executor assigns a client-side `order_id` and checks order status before retrying on timeout
  - [ ] `Executor` retries on transient failures up to `max_retries` with exponential backoff
  - [ ] `uv run pytest tests/test_risk.py tests/test_executor.py` passes
- **Files of interest**: `python/pms/execution/risk.py`, `python/pms/execution/executor.py`, `python/pms/execution/guardrails.py`
- **Effort estimate**: M

### Checkpoint 09: Evaluation layer — metrics collector and feedback engine

- **Scope**: Implement `MetricsCollectorProtocol` (in-memory storage, records orders/results/snapshots, computes P&L/slippage/latency) and `FeedbackEngineProtocol` (rule-based, generates `EvaluationFeedback` bounded by guardrails).
- **Depends on**: CP01 (protocols, models)
- **Type**: backend
- **Acceptance criteria**:
  - [ ] `MetricsCollector` uses in-memory storage (dict-based); explicitly documented as non-persistent for v1
  - [ ] `MetricsCollector.record_order()` and `record_price_snapshot()` store data in memory
  - [ ] `MetricsCollector.get_pnl()` returns correct P&L over a time window from in-memory data
  - [ ] `MetricsCollector.get_performance_metrics()` returns per-strategy win rate, avg slippage, fill latency
  - [ ] `FeedbackEngine.generate_feedback()` produces `EvaluationFeedback` with appropriate suggestions (low win_rate → raise_min_spread, high slippage → reduce_aggression)
  - [ ] All feedback adjustments are bounded by `GUARDRAILS` — fuzz test with 100 random inputs confirms no adjustment exceeds bounds
  - [ ] `uv run pytest tests/test_evaluation.py` passes
- **Files of interest**: `python/pms/evaluation/metrics.py`, `python/pms/evaluation/feedback.py`
- **Effort estimate**: M

### Checkpoint 10: Embedding engine and correlation detector

- **Scope**: Implement `EmbeddingEngineProtocol` (Python implementation using sentence-transformers for vectorization and numpy for cosine similarity) and `CorrelationDetectorProtocol` (embedding clustering to find candidate pairs, then **rule-based** relation type classification). LLM refinement is optional and not required for acceptance.
- **Depends on**: CP01 (protocols, models)
- **Type**: backend
- **Acceptance criteria**:
  - [ ] `EmbeddingEngine.embed_markets()` produces float32 vectors for a list of Market objects using sentence-transformers (`all-MiniLM-L6-v2`)
  - [ ] `EmbeddingEngine.find_similar_pairs()` returns pairs above a cosine similarity threshold
  - [ ] `CorrelationDetector.detect()` returns `list[CorrelationPair]` with `similarity_score`, `relation_type`, and `relation_detail`
  - [ ] Rule-based classifier identifies subset relationships using keyword overlap + embedding similarity (no LLM required)
  - [ ] Given test markets "Team A beats Team B by 20 points" and "Team A beats Team B", detector identifies the subset relationship
  - [ ] Given test markets on unrelated topics, detector returns no pairs above threshold
  - [ ] Precision >80% on a hand-labeled test set of 20 market pairs (10 related, 10 unrelated) in `tests/fixtures/correlation_test_set.json`
  - [ ] Tests are deterministic (no LLM calls; sentence-transformers is deterministic with fixed model)
  - [ ] `uv run pytest tests/test_correlation.py` passes
- **Files of interest**: `python/pms/embeddings/engine.py`, `python/pms/strategy/correlation.py`, `tests/fixtures/correlation_test_set.json`
- **Effort estimate**: L

## Out of Scope

- **Rust implementations**: v1 is pure Python. Rust Cargo.toml scaffolded but empty.
- **Live trading**: No real money execution. Connectors use recorded fixtures.
- **Live positions tracking**: v1 does not maintain a positions ledger at the pipeline level. `OrderExecutor.get_positions()` returns an empty list unless a positions source is manually registered via `register_positions_source()`. `RiskManager` correctly enforces exposure caps when positions are provided (verified by unit tests), but the pipeline does not yet wire executor results back into a positions store. A post-v1 checkpoint will add either (a) an in-memory positions ledger derived from OrderResults, (b) a `PositionsStore` Protocol wired via config, or (c) live connector `get_positions()` implementations. This gap is consistent with v1's "no live trading" scope: recorded-fixture tests supply synthetic positions directly.
- **WebSocket streaming**: `stream_prices()` and `get_historical_prices()` deferred (raise `NotImplementedError`).
- **Synergy testing**: `SynergyRunner` for cross-module candidate evaluation deferred to future spec.
- **Web UI / Dashboard**: No frontend. CLI and Python API only.
- **Platforms beyond Polymarket + Kalshi**: No Manifold, Metaculus, DeFi, or sportsbook.
- **ML-based feedback / LLM-based correlation**: v1 uses rule-based logic only. LLM refinement is optional.
- **Durable persistence**: MetricsCollector is in-memory only. SQLite/file persistence deferred.
- **Deployment / infrastructure**: No Docker, CI/CD, or cloud. Local development only.
- **auto-research framework modifications**: auto-research consumed as-is.

## Open Questions

1. **API key management**: Environment variables with python-dotenv fallback. (Resolved — default accepted.)
2. **Embedding model**: `all-MiniLM-L6-v2` for speed. (Resolved — default accepted.)
3. **LLM provider for optional correlation refinement**: Claude via Anthropic SDK, if enabled. (Deferred — not required for v1 acceptance.)
