---
title: PMS Project Decomposition Design
task_id: pms-project-decomposition
status: draft
created: 2026-04-16
updated: 2026-04-16
scope: project-level (spans 6 harness sub-specs)
branch: docs/pms-project-decomposition
---

# PMS Project Decomposition Design

## 0. Purpose and relationship to other documents

**What this document is.** The project-level total spec for the
six-sub-spec decomposition that will implement
`agent_docs/architecture-invariants.md`. It defines the scope,
boundaries, and kickoff contract for each sub-spec (S1–S6), and it
provides the boundary-integrity mechanisms (Boundary Matrix, Intake /
Leave-behind, cross-spec gates) that keep the six harness runs from
overlapping or leaving gaps.

**What this document is not.**

- It is not a harness-executable spec. Per-checkpoint acceptance
  criteria, files-of-interest, and effort estimates live in each
  `.harness/pms-<id>-v1/spec.md` when that harness run starts.
- It is not an architecture document. Architecture invariants live
  in `agent_docs/architecture-invariants.md`; this document
  *consumes* those invariants — it does not redefine them.
- It is not a retrospective. Promoted rules from retros live in
  `agent_docs/promoted-rules.md`.

**How to use this document.**

- For designing any new entity or module: read §3 (Boundary Matrix)
  first, then the sub-spec that owns the entity.
- Before starting a new harness run: read §4 (Execution order) and
  the Kickoff Prompt at the end of the relevant sub-spec.
- After finishing a harness run: verify that sub-spec's Leave-behind
  is satisfied, update §12 (Maintenance), and proceed to the next
  gate.

**Source material.**

- `agent_docs/architecture-invariants.md` — the 8 non-negotiable
  architectural invariants. This document's sub-spec acceptance
  criteria reference invariants by number.
- `agent_docs/project-roadmap.md` — the 6-spec DAG skeleton and the
  between-spec gate policy. This document expands that skeleton.
- `agent_docs/promoted-rules.md` — rules promoted from retros.
  Complementary to the invariants: invariants define the positive
  architecture, retros capture past mistakes.
- `docs/notes/2026-04-16-repo-issues-controller-evaluator.md` — the
  schema and `asyncpg` decisions that feed S1 + S2 scope.
- `docs/notes/2026-04-16-evaluator-entity-abstraction.md` — the
  entity catalogue that feeds S2 – S6 scope.
- `src/pms/{sensor,controller,actuator,evaluation}/CLAUDE.md` —
  per-layer enforcement of the invariants most relevant to each
  layer.

---

## 1. Project end state

A **research-grade prediction-market strategy platform** where
multiple strategies run concurrently against live, paper, and
backtest modes under a single runtime, with per-strategy dispatch,
comparable metrics, and active perception driving Sensor
subscription.

### 1.1 Observable capabilities at the finish line

1. **Multi-strategy concurrency.** Several strategies run
   concurrently through a per-strategy `ControllerPipeline`
   (Invariant 1 — concurrent feedback web, not phased runtime).
   Each produces `TradeDecision` rows tagged `(strategy_id,
   strategy_version_id)` (Invariants 2, 3).
2. **Live Polymarket orderbook persistence.** Real `book`
   snapshots and `price_change` deltas from the CLOB WebSocket land
   in `book_snapshots`, `book_levels`, `price_changes`, `trades`
   (Invariants 7, 8). Simulated depth is retired.
3. **`/strategies` dashboard page.** Lists every registered
   strategy with per-strategy Brier, P&L, fill rate, slippage,
   drawdown, and calibration sample count, each grouped by
   `(strategy_id, strategy_version_id)` (Invariant 3).
4. **`/signals` dashboard page.** Renders real orderbook depth
   from the outer-ring tables — the dashboard no longer depends on
   fabricated bid/ask levels.
5. **`/factors` dashboard page.** Shows factor values evolving
   over time per `(factor_id, param, market_id)` (Invariant 4).
6. **`/backtest` dashboard page.** Compares N strategies over a
   configurable market universe and date range, producing a ranked
   comparison report (S6 deliverable).
7. **Strategy onboarding without code rewrite.** A new strategy is
   a new row in `strategies` + a module under
   `src/pms/strategies/<id>/` + a `StrategyConfig` blob; no
   changes required in `src/pms/sensor/` or `src/pms/actuator/`
   (Invariant 5 — strategy-agnostic boundary).
8. **Shared selection path across backtest / paper / live.** All
   three modes consume the same `Factor → StrategySelection →
   Opportunity → PortfolioTarget` chain. Divergence happens only
   inside `ExecutionModel` (S6 owns the abstraction).
9. **Active perception wired end-to-end.**
   `Strategy.select_markets` output drives `MarketSelector`, which
   pushes subscription updates through
   `SensorSubscriptionController` into `MarketDataSensor`. No
   sensor module imports from `pms.strategies.*` (Invariants 5, 6,
   7).
10. **Onion-concentric storage populated.** Outer ring (market
    data, strategy-agnostic), middle ring (factor panel,
    strategy-agnostic cache), and inner ring (strategy products)
    all persist in PostgreSQL with ring-ownership enforced by
    schema plus import-linter rules (Invariant 8).

### 1.2 What the finished system does not do

- **Real-money live execution stays gated** behind
  `live_trading_enabled=false`. The Polymarket adapter exists, is
  integration-tested, and is guarded by
  `LiveTradingDisabledError`; flipping the gate is a human
  decision outside the scope of this decomposition.
- **No automated feedback loop reconfigures strategies.**
  `Feedback` rows from Actuator / Evaluator are surfaced through
  `/feedback` for human resolution; automated strategy adjustment
  is explicitly out of scope (retained from `.harness/pms-v2/`
  non-goals).
- **Kalshi is not implemented.** Venue-agnostic interfaces
  (`ISensor`, `IActuator`) remain in place; a Kalshi adapter pair
  is a follow-on effort after S6.
- **No ORM and no migration framework.** Raw SQL via `asyncpg`
  throughout, with a single `schema.sql` applied at Runner
  startup. Alembic / Sqitch are reconsidered only if schema drift
  makes the single-file approach painful.

### 1.3 How end state differs from the 2026-04-16 baseline

Today (as of the `main` tip on 2026-04-16, commit `b4734fb`):

- The REST sensor's `_gamma_market_to_signal` emits
  `orderbook={"bids": [], "asks": []}` — real orderbook depth is
  absent, not even fabricated
  (`src/pms/sensor/adapters/polymarket_rest.py:90`, inside the
  helper that starts at line 78).
- The stream adapter's top-level `_message_dict_to_signal` keeps
  only messages carrying both `price` and `market_id`, which
  silently drops `book` and `price_change` events
  (`src/pms/sensor/adapters/polymarket_stream.py:71-77`).
  `Runner._build_sensors` never wires the stream sensor in for
  non-backtest modes either
  (`src/pms/runner.py:177-185` — only `PolymarketRestSensor` is
  returned).
- `ControllerPipeline` runs one global pipeline; `TradeDecision`
  has no `strategy_id` / `strategy_version_id` fields.
- `FeedbackStore` and `EvalStore` persist to JSONL under `.data/`;
  there is no PostgreSQL in the runtime path.
- `Factor`, `Strategy`, `MarketSelector`, `BacktestSpec`,
  `StrategyRun` — none of these entities exist.
- The dashboard exposes `/signals`, `/decisions`, `/metrics`, and
  `/backtest` pages plus a feedback panel on the main overview page
  (fed by API routes under `dashboard/app/api/pms/feedback/`). None
  render per-strategy comparison.

End state closes every item above.

---

## 2. Dependency DAG

### 2.1 Graph

```mermaid
graph TD
    S1["S1 — pms-market-data-v1<br/><i>Outer ring + 2 sensors + PG</i>"]
    S2["S2 — pms-strategy-aggregate-v1<br/><i>Inner-ring aggregate + registry</i>"]
    S3["S3 — pms-factor-panel-v1<br/><i>Middle ring + FactorService</i>"]
    S4["S4 — pms-active-perception-v1<br/><i>MarketSelector + SubscriptionController</i>"]
    S5["S5 — pms-controller-per-strategy-v1<br/><i>Per-strategy pipeline + NOT NULL tags</i>"]
    S6["S6 — pms-research-backtest-v1<br/><i>BacktestSpec + ExecutionModel + sweep</i>"]

    S1 --> S2
    S2 --> S3
    S2 --> S4
    S3 --> S5
    S4 --> S5
    S5 --> S6
```

### 2.2 Node summary

| ID | Harness directory                          | Invariants primarily closed | Headline deliverable |
|----|--------------------------------------------|-----------------------------|----------------------|
| S1 | `.harness/pms-market-data-v1/`             | 7, 8                        | Real Polymarket orderbook persisted in PG; `/signals` renders real depth; JSONL stores retired |
| S2 | `.harness/pms-strategy-aggregate-v1/`      | 2, 3, 5, 8                  | `Strategy` aggregate + projections; `strategies` / `strategy_versions` tables; import-linter rules; `"default"` strategy seeded |
| S3 | `.harness/pms-factor-panel-v1/`            | 4, 8                        | `factors` + `factor_values` tables; existing rules-detector logic migrated to raw factor definitions |
| S4 | `.harness/pms-active-perception-v1/`       | 6, 7                        | `MarketSelector` + `SensorSubscriptionController` + `Strategy.select_markets` hook wired into Runner |
| S5 | `.harness/pms-controller-per-strategy-v1/` | 2, 3, 5                     | Per-strategy `ControllerPipeline`; per-strategy Evaluator aggregation; `(strategy_id, strategy_version_id)` upgraded to `NOT NULL`; `/strategies` page |
| S6 | `.harness/pms-research-backtest-v1/`       | (uses all; closes none new) | `BacktestSpec` + `ExecutionModel`; market-universe replay; parameter sweep; `/backtest` comparison |

### 2.3 Edge semantics

An edge `S_a → S_b` in §2.1 means **at least one concept owned by
`S_a` is in `S_b`'s Intake subsection.** The concrete Intake /
Leave-behind lines live inside each sub-spec (§§6.6 – 6.7, 7.6 – 7.7,
…); the edges above are the summary projection of those contracts.

Invariant 1 (concurrent feedback web, *not* linear phases) is
deliberately **not** a DAG edge. It governs runtime behaviour, not
authoring order. Every sub-spec's acceptance criteria enforce it
locally — no sub-spec is allowed to introduce a synchronous barrier
between layers. §4 (Execution order) addresses authoring order;
Invariant 1 addresses runtime topology. The two are orthogonal and
must not be conflated.

### 2.4 Branch and swap points

Only one pair of sub-specs has a discretionary ordering: **S3 and
S4** both depend only on S2, and neither is on the other's Intake
chain. §4 (Execution order) explains why the canonical sequence puts
S3 before S4 and the conditions under which the swap is acceptable.

---

## 3. Boundary Matrix

The Boundary Matrix is the single source of truth for **who owns
what** across the six sub-specs. Every load-bearing concept —
component, table, entity, enforcement hook, dashboard page —
appears exactly once and has exactly one **Owner**. Any sub-spec
that needs to reference the concept appears only as a **Consumer**;
it may not claim ownership.

### 3.1 How to use this matrix

- **When authoring a sub-spec's *Scope in / out*** (§§6.2, 7.2, …):
  include only concepts whose Owner is this sub-spec. If a concept
  you need is owned elsewhere, list it under *Dependencies* or
  *Intake*, never under *Scope in*.
- **When reviewing a sub-spec PR:** grep for every concept the PR
  introduces; verify the PR's sub-spec is this matrix's Owner. A
  concept introduced by a non-owner is a boundary violation and
  must be reassigned before merge.
- **When adding a new concept not in the matrix:** open a PR to
  this document first, pick exactly one Owner, list Consumers, note
  the invariant(s) touched. The concept is not ready for
  implementation until the matrix is updated.

### 3.2 Matrix

Column semantics:

- **Concept** — the load-bearing unit of ownership (module, class,
  table, DDL change, enforcement hook, dashboard page, named
  policy object).
- **Owner** — exactly one sub-spec ID.
- **Consumers** — sub-specs that reference / read / invoke the
  concept. Not owners.
- **Invariant** — comma-separated invariant numbers from
  `agent_docs/architecture-invariants.md` that the concept touches.
  A dash means "no invariant directly; scaffolding."
- **Notes** — one-line clarification where the ownership choice is
  non-obvious.

#### 3.2.1 Outer ring (S1 owns)

| Concept | Owner | Consumers | Invariant | Notes |
|---|---|---|---|---|
| `markets` table (DDL + writes) | S1 | S2, S3, S4, S5, S6 | 7, 8 | — |
| `tokens` table (DDL + writes) | S1 | S2, S3, S4, S5, S6 | 7, 8 | — |
| `book_snapshots` table | S1 | S3, S6 | 7, 8 | — |
| `book_levels` table | S1 | S3, S6 | 7, 8 | Per-level rows, no JSON blobs (§Q2 of `docs/notes/2026-04-16-repo-issues-controller-evaluator.md`). |
| `price_changes` table | S1 | S3, S6 | 7, 8 | `size=0` means level removed; Polymarket semantics. |
| `trades` table | S1 | S3, S6 | 7, 8 | — |
| `PostgresMarketDataStore` (typed methods over outer ring) | S1 | S3, S5, S6 | 8 | Single concrete class; no Protocol abstraction today (§Q5 of discovery note). |
| `asyncpg.Pool` lifecycle (Runner-owned) | S1 | all | — | `min_size=2`, `max_size=10` per §Q6 of discovery note. |
| `schema.sql` file (startup-applied) | S1 | S2, S3, S5, S6 extend it | — | No migration framework yet. |
| `MarketDiscoverySensor` class | S1 | S4 | 7 | Unconditional universe scan; writes `markets` / `tokens`. |
| `MarketDataSensor` class | S1 | S4 | 6, 7 | Subscription-driven; consumes push from `SensorSubscriptionController` (S4). |
| `SensorWatchdog` wiring to stream sensor | S1 | — | 7 | Watchdog class exists today; S1 wires it. |
| WebSocket heartbeat + reconnect reconciliation (snapshot re-request) | S1 | — | 7 | Closes open question Q4 of the discovery note. |
| JSONL → PG migration (`FeedbackStore` + `EvalStore` rewritten over SQL) | S1 | S5 (reads) | 8 | Retires `.data/*.jsonl` as runtime contract. |
| Transaction-rollback test fixture (`db_conn`) | S1 | all test-side | — | Per §Test strategy of discovery note. |
| `compose.yml` for local PG (dev) | S1 | all | — | `postgres:16` image; CI matches tag. |
| `/signals` dashboard page (real orderbook depth) | S1 | — | 7 | Replaces today's empty orderbook with live book / delta rendering. |
| `orders` + `fills` inner-ring product tables (DDL, empty shape) | S1 | S2, S5 | 3, 8 | Per `agent_docs/architecture-invariants.md` §Invariant 8 enumeration of inner-ring product tables. Land as empty tables with `(strategy_id, strategy_version_id)` `NULLABLE` columns (column reservation handled by the next row). Today's runtime emits `OrderState` / `FillRecord` in-memory only; S1 grants persistence so S5's `NOT NULL` migration has all 4 product tables to upgrade uniformly (`feedback`, `eval_records`, `orders`, `fills`). |
| Inner-ring `(strategy_id, strategy_version_id)` columns reserved `NULLABLE` on product tables | S1 | S2, S5 | 3, 8 | Columns land here on all 4 product tables (`feedback`, `eval_records`, `orders`, `fills`); S2 seeds `"default"`; S5 upgrades to `NOT NULL`. |

#### 3.2.2 Inner ring — aggregate + registry (S2 owns)

| Concept | Owner | Consumers | Invariant | Notes |
|---|---|---|---|---|
| `Strategy` aggregate (`src/pms/strategies/aggregate.py`) | S2 | S4 (via projections), S5 (aggregate reader), S6 (aggregate reader) | 2 | Controller + Evaluator are the only aggregate readers. |
| Projection types (`StrategyConfig`, `RiskParams`, `EvalSpec`, `ForecasterSpec`, `MarketSelectionSpec`) | S2 | S4, S5, S6 | 2, 5 | All `@dataclass(frozen=True)`. |
| `strategies` table | S2 | S5, S6 | 3, 8 | One row per strategy id. |
| `strategy_versions` table (immutable, hash-keyed) | S2 | S3, S4, S5, S6 | 3, 8 | Config hash = deterministic over full config; re-config produces a new row. |
| `strategy_factors` link table | S2 | S3, S5 | 2, 4, 8 | Empty shape in S2; S3 populates as factor definitions land. |
| `PostgresStrategyRegistry` | S2 | S4, S5 | — | CRUD over `strategies` + `strategy_versions`. |
| Import-linter rules (`pms.sensor`, `pms.actuator` cannot import `pms.strategies.*` or `pms.controller.*`; `pms.sensor` cannot import `pms.market_selection`) | S2 | all (enforced in CI) | 5, 6 | Codified in `pyproject.toml` or `ruff.toml`; covers Invariants 5 and 6 import directions. |
| `"default"` strategy + version row seed | S2 | pre-S5 runtime writes | 3 | Lets legacy runtime continue writing product rows tagged to `"default"` until S5 upgrades columns to `NOT NULL`. |
| `/strategies` page — registry listing view | S2 | — | — | Minimal listing of registered strategies. Comparative metrics land in S5. |

#### 3.2.3 Middle ring — factor panel (S3 owns)

| Concept | Owner | Consumers | Invariant | Notes |
|---|---|---|---|---|
| `src/pms/factors/definitions/` module tree (one file per raw factor) | S3 | S5, S6 | 4 | Raw factors only; composite logic lives in `StrategyConfig.factor_composition`. |
| `factors` table (one row per factor definition) | S3 | S5, S6 | 4, 8 | No `factor_type` column distinguishing raw / composite. |
| `factor_values` table (`factor_id, market_id, ts, value`) | S3 | S5, S6 | 4, 8 | No `strategy_id` column (Invariant 8). |
| `FactorService` (compute + persist) | S3 | S5, S6 | 4 | Reads outer ring, writes middle ring. |
| Migration of existing rules-detector heuristics into raw `FactorDefinition`s | S3 | S5 (factors feed forecasters) | 4 | Today's `RulesForecaster` / `StatisticalForecaster` split: raw detection → S3 factors, composition → S5 strategy config. |
| `StrategyConfig.factor_composition` field (per-strategy composition logic, JSONB) | S3 | S5 | 2, 4 | Composition is strategy-scoped — lives on the projection, not in `factors`. |
| `/factors` dashboard page | S3 | — | 4 | Shows factor values per `(factor_id, param, market_id)`. |

#### 3.2.4 Active perception (S4 owns)

| Concept | Owner | Consumers | Invariant | Notes |
|---|---|---|---|---|
| `MarketSelector` (`src/pms/market_selection/selector.py`) | S4 | S5 | 6 | Reads universe, applies each strategy's `select_markets(universe)`, returns merged market-id list. |
| `SensorSubscriptionController` | S4 | — | 6, 7 | Pushes subscription updates to `MarketDataSensor`; Sensor never pulls. |
| `Strategy.select_markets(universe)` method (declaration + body + per-strategy tests) | S4 | S5 (per-strategy dispatch) | 2, 6 | Entire method surface is S4-owned: the aggregate class lives on S2's `Strategy` type, but this method lands with the active-perception machinery (`MarketSelector` + subscription controller) to keep the method and its first consumer in the same commit. |
| Runner wiring: boot order (DiscoverySensor → Selector → SubscriptionController → DataSensor) + incremental resubscribe on strategy-config change | S4 | — | 6 | Cold-start handling per §Invariant 6 of `agent_docs/architecture-invariants.md`. |

#### 3.2.5 Per-strategy Controller + Evaluator (S5 owns)

| Concept | Owner | Consumers | Invariant | Notes |
|---|---|---|---|---|
| Per-strategy `ControllerPipeline` dispatch | S5 | — | 2, 5 | Each strategy gets its own forecaster stack / calibrator / sizer; aggregate reader. |
| Per-strategy `Evaluator` aggregation (`GROUP BY strategy_id, strategy_version_id`) | S5 | S6 (reuses shape in backtest evaluator) | 3, 5 | Retires the global `MetricsCollector.snapshot()` shape. |
| `(strategy_id, strategy_version_id)` `NOT NULL` DDL upgrade on all inner-ring product tables | S5 | — | 3 | Pre-S5 columns are `NULLABLE` with `"default"` tagging; upgrade is schema-change-only, no new table. |
| `TradeDecision` / `OrderState` / `FillRecord` / `EvalRecord` strategy-field population end-to-end | S5 | — | 3 | S1 reserves columns, S2 seeds `"default"`, S5 populates real values from per-strategy dispatch. |
| `Opportunity` entity (Controller output pre-execution) | S5 | S6 | 2 | Carries selected factor values + expected edge + rationale; replaces stringly-typed `stop_conditions` routing / model_id mix. |
| `/strategies` comparative-metrics view (Brier / P&L / fill rate / slippage per strategy) | S5 | — | 3 | Upgrades the S2 listing page. |
| `/metrics` per-strategy breakdown | S5 | — | 3 | Current global `/metrics` page extends to per-strategy rollup. |

#### 3.2.6 Research backtest framework (S6 owns)

| Concept | Owner | Consumers | Invariant | Notes |
|---|---|---|---|---|
| `BacktestSpec` (strategy version + dataset + execution model + risk policy + date range + config hash) | S6 | — | 3 | Stable hash for reproducibility across sweep runs. |
| `ExecutionModel` (fill / fee / slippage / latency / staleness policy) | S6 | — | — | The only place backtest / paper / live legitimately diverge. |
| `BacktestDataset` (source, version, coverage, data-quality gaps) | S6 | — | 8 | References outer + middle ring tables by ring, not by strategy id. |
| `BacktestRun` (materialized run with artifact paths) | S6 | — | 3 | One `BacktestRun`, many `StrategyRun`s when multi-strategy. |
| `StrategyRun` (materialized per-strategy run record for backtest runs) | S6 | — | 3 | Backtest-only entity. Live / paper per-strategy tracking happens through the inner-ring product tables (`fills`, `eval_records`, …) grouped by `(strategy_id, strategy_version_id)` — no separate live `strategy_runs` table is needed, and introducing one would create a reverse dependency S5 → S6 that the DAG (§2) forbids. |
| Market-universe replay engine (multi-day, multi-market outer-ring reader) | S6 | — | 8 | Drives `FactorService` (S3) to precompute panels for the replay window. |
| Parameter sweep (generate N `BacktestSpec`s, compare results with shared factor-panel cache) | S6 | — | — | — |
| `BacktestLiveComparison` (equity divergence + selection overlap + backtest-only / live-only opportunities) | S6 | — | — | — |
| `TimeAlignmentPolicy` + `SymbolNormalizationPolicy` | S6 | — | — | Aligns live and backtest timestamps / identifiers before comparison. |
| `SelectionSimilarityMetric` (denominator explicit: backtest set / live set / union) | S6 | — | — | — |
| `EvaluationReport` (run metadata + metrics + attribution + benchmarks) | S6 | — | — | — |
| `PortfolioTarget` (time-indexed target exposure per strategy) | S6 | — | — | Research abstraction; live runtime continues to produce `TradeDecision` directly. |
| `/backtest` ranked N-strategy comparison view | S6 | — | — | Upgrades the existing `/backtest` page. |

### 3.3 Completeness checks

Two mechanical checks make overlap and gap detectable without
reading the sub-specs themselves.

**Overlap check.** Every value in the **Concept** column of §3.2
must be unique across *all* sub-matrices. Any duplicate is an
overlap by definition. Reviewers should grep the `#### 3.2.\d+`
blocks for concept-name duplication before approving any sub-spec
PR.

**Gap check.** Every one of the 8 invariants in
`agent_docs/architecture-invariants.md` must appear in the
**Invariant** column of at least one row below. A missing invariant
is a gap — either the decomposition is incomplete, or the invariant
is silently dropped. Current coverage:

| Invariant | Carried by rows in… |
|---|---|
| 1 (concurrent feedback web) | *not a row* — enforced per sub-spec's *Acceptance criteria* (see §4 and each sub-spec). Invariant 1 is about runtime topology, not ownership. |
| 2 (aggregate + projections) | S2 (aggregate, projections, `strategy_factors`), S3 (`factor_composition` field), S4 (`select_markets` method), S5 (per-strategy `ControllerPipeline`, `Opportunity`) |
| 3 (immutable version tagging) | S1 (reserved `NULLABLE` columns on product tables), S2 (`strategies` + `strategy_versions` + `"default"` seed), S5 (`NOT NULL` upgrade + field population + per-strategy aggregation + comparative view + per-strategy metrics), S6 (`BacktestSpec` + `BacktestRun` + `StrategyRun`) |
| 4 (raw factors only) | S2 (`strategy_factors` link table), S3 (definitions module, `factors`, `factor_values`, `FactorService`, rules-detector migration, `factor_composition`, `/factors` page) |
| 5 (strategy-aware boundary) | S2 (projections, import-linter rules), S5 (per-strategy `ControllerPipeline` + per-strategy `Evaluator` aggregation) |
| 6 (active perception) | S1 (`MarketDataSensor` as subscription sink), S2 (import-linter rules covering `pms.sensor` → `pms.market_selection`), S4 (`MarketSelector` + `SensorSubscriptionController` + `select_markets` method + Runner wiring) |
| 7 (two-layer sensor) | S1 (`MarketDiscoverySensor` + `MarketDataSensor` + watchdog wiring + outer-ring DDL + `/signals` page), S4 (`SensorSubscriptionController` as the subscription push channel) |
| 8 (onion-concentric storage) | S1 (outer-ring DDL + column reservations + JSONL→PG), S2 (inner-ring aggregate tables + `strategy_factors`), S3 (middle-ring tables), S6 (`BacktestDataset` references rings by ring, not by strategy id) |

Every invariant has at least one owning row (Invariant 1 excepted
for the reason noted).

### 3.4 Entities deliberately out of scope for this decomposition

Entities from
`docs/notes/2026-04-16-evaluator-entity-abstraction.md` that have
not been assigned an owner in §3.2 are **intentionally deferred**:

- `StrategyBundle` (multi-strategy group mapping to one live
  account): intentionally deferred. Today each strategy runs
  independently with shared risk budget at the Runner level. Revisit
  after S5, before considering Kalshi or multi-account expansion.
- `MarketUniverse` as a first-class entity: the universe is the
  implicit set of rows in `markets` today. Elevating it becomes an
  entity only if S6 discovers it must be persisted separately from
  the outer-ring tables (e.g., for dated universe snapshots).
- `FactorAttribution` as a first-class Evaluator artifact: S6
  introduces a `EvaluationReport` that *can* carry attribution
  commentary, but a dedicated attribution entity with its own table
  is deferred until the research workflow proves the need.
- Finer-grained backtest entities (`SelectionSnapshot`,
  `PriceLevel`, `MarketUpdate` as persisted event types): the
  outer-ring tables (`book_snapshots`, `book_levels`,
  `price_changes`) already encode equivalent state. Deferred unless
  S6 finds the extraction worth the schema work.

Any future proposal to add one of these to an owning sub-spec must
amend §3.2 **in the same PR** that changes the sub-spec scope.

---

## 4. Execution order and between-spec gates

§2 describes the dependency DAG; this section describes the
**authoring order** derived from that DAG and the mechanical gates a
human runs before moving from sub-spec N to sub-spec N+1. Authoring
order is **orthogonal** to runtime topology (Invariant 1 — concurrent
feedback web, not phased pipeline): sub-specs are authored one at a
time, but the artefacts they produce continue to run concurrently
once they land (reaffirmed here to close the conflation warning from
§2.3).

### 4.1 Canonical sequence

**S1 → S2 → S3 → S4 → S5 → S6, sequentially, one harness run at a
time.** This is the canonical topological order of the §2.1 DAG with
the S3 ↔ S4 tie broken as §2.4 notes (rationale in §4.2 below).
Parallel authoring across two sub-specs is not supported by this
decomposition: the gates in §4.3 assume sub-spec N is complete before
sub-spec N+1 begins.

### 4.2 Execution rationale per edge

The four directed edges in the §2.1 DAG have four distinct arguments.
Each expands a bullet from `agent_docs/project-roadmap.md` §"Why this
exact order".

**S1 → S2.** S1's schema must reserve `(strategy_id,
strategy_version_id)` columns on every inner-ring product table,
`NULLABLE` in S1, upgraded to `NOT NULL` in S5 (§3.2.1 last row;
Invariants 3, 8). If S2 does not follow S1 directly, S1 lands with
unused columns carrying no writer — `schema.sql` declares strategy
tagging that no code actually populates. That combination is scope
drift against Invariant 8: the inner ring exists as a schema shape
without the aggregate that owns it (Invariant 2). Keeping S2 second
means the aggregate, registry, and `"default"` seed land while the
column-reservation rationale is still fresh, and the pre-S5 runtime
writes legitimately tag `"default"` instead of writing `NULL`.

**S2 → S3 and S2 → S4.** Both S3 and S4 depend only on S2; the DAG
does not force one before the other. The canonical order puts **S3
before S4** for one reason: additive vs behavioural side effects. S3
adds a strategy-agnostic middle-ring cache (§3.2.3) — writing to new
tables, reading from outer-ring tables that S1 already owns. Nothing
about the existing Sensor / Controller / Actuator runtime changes.
S4, by contrast, changes the live Sensor subscription mechanism: the
`MarketDataSensor` stops being configured from a static list and
starts being driven by `SensorSubscriptionController` push (§3.2.4;
Invariants 6, 7). That is a runtime side effect with a larger blast
radius — a regression here can mis-subscribe every strategy at once.
Scheduling S3 first means the observable factor stream exists before
the subscription mechanism changes, so S4-induced regressions are
easier to isolate (the factor stream is known-good reference data).
§2.4 + §4.4 describe when this ordering can be swapped.

**S3 → S5 and S4 → S5.** S5's headline deliverable is per-strategy
`ControllerPipeline` dispatch (§3.2.5; Invariants 2, 3, 5) — each
strategy gets its own forecaster stack, calibrator, sizer, and
Evaluator aggregation. That dispatch needs two inputs simultaneously:
(1) **factor values** from the S3 middle ring (strategies read
factors to produce decisions), and (2) **per-strategy subscriptions**
from S4 (strategies only need to reason about markets their
`select_markets` actually returned — a universal subscription would
waste compute on irrelevant markets and blur per-strategy
accountability). Landing S5 before either of S3 or S4 forces either
mocking factor values or hardcoding subscriptions; both are
known-bad precedents. Keeping S5 after both means the per-strategy
dispatch connects real factor reads to real subscription output in
the same commit.

**S5 → S6.** S6 introduces research-grade backtest infrastructure:
`BacktestSpec`, `ExecutionModel`, parameter sweep, `BacktestLive-
Comparison` (§3.2.6). Its value proposition is comparing strategies,
which requires the full per-strategy runtime (S5) to exist as
**reference behaviour** — backtest results are only interpretable
relative to what the live runtime would have done. Running S6 before
S5 means the backtest compares a global controller against itself,
producing results that cannot be cross-checked against live. S6 also
carries the heaviest scope of the six (market-universe replay,
parameter-sweep infrastructure, shared factor-panel cache), so it
benefits from landing on the most stable foundation — every invariant
already enforced, every dashboard page already shipped.

### 4.3 Between-spec gates

Before opening the harness directory for sub-spec N+1, a human
verifier must confirm every item below against sub-spec N's completed
work. **Every item is verifiable** — no "looks good" gates.

1. **Retro written and indexed.** `.harness/retro/<sub-spec-id>.md`
   exists and is appended to `.harness/retro/index.md` per the retro
   process documented at `.harness/retro/index.md`. A sub-spec
   without a retro cannot clear this gate even if every other item
   passes.
2. **Architecture invariants spot-checked.** Grep sub-spec N's
   introduced concepts (from the PR diff) against §3.2's Owner
   column; every concept must appear in the matrix with sub-spec N
   as Owner. Concepts introduced by a non-owner are boundary
   violations (per §3.1's "When reviewing a sub-spec PR" guidance)
   and must be reassigned before merge. Separately, grep any new
   DDL for `strategy_id` **or** `strategy_version_id` columns on
   outer-ring or middle-ring tables — zero matches required for
   both identifiers (Invariant 8 Enforcement explicitly names both,
   `agent_docs/architecture-invariants.md` §Invariant 8).
3. **`CLAUDE.md` updated with any rule promoted from the retro.**
   If the retro produced a promoted rule per the criteria in
   `agent_docs/promoted-rules.md` §"Promotion process" (observed
   ≥2 times, or high-severity on first observation, or user-
   promoted), the rule is appended to `agent_docs/promoted-rules.md`
   with provenance and mirrored to `CLAUDE.md` §"Promoted rules
   from retros". No mirroring = gate fails.
4. **Boundary integrity check — Leave-behind union matches Intake.**
   Sub-spec N+1's Intake is diffed against the **union of all
   predecessor Leave-behinds** — every sub-spec that has an
   incoming edge to N+1 in the §2.1 DAG, not just the immediately
   prior one in the §4.1 sequence. The five predecessor sets,
   enumerated against §2.1, are: **S2's Intake against S1's
   Leave-behind** (S1 → S2 is S2's only predecessor edge);
   **S3's Intake against S2's Leave-behind** (single edge);
   **S4's Intake against S2's Leave-behind** (same shape);
   **S5's Intake against S3's Leave-behind ∪ S4's Leave-behind**
   (both edges land into S5 per §2.1); **S6's Intake against S5's
   Leave-behind**. Once each sub-spec's Leave-behind subsection
   exists (lands with Commits 4–9 of this document authoring
   effort), diff the union line-by-line. Any concept in sub-spec
   N+1's Intake that is not produced by some predecessor's
   Leave-behind is a boundary gap. **STOP and reconcile** — either
   amend a predecessor to produce the missing concept, or amend
   sub-spec N+1 to drop the dependency. Do not proceed with a
   partial contract. This is the mechanism that prevents drift
   once sub-specs begin landing in `.harness/`; it is the single
   most important gate item.
5. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   and `uv run mypy src/ tests/ --strict` both pass on a fresh
   shell (see §5.1, §5.2; 🟡 Fresh-clone baseline verification).
6. **Human decision gate.** Record the decision `proceed`,
   `pause`, or `reorder` with a timestamp. `reorder` is supported
   **only** for the S3 ↔ S4 pair per §2.4 and §4.4 — reordering
   any other pair breaks a real DAG edge and requires a new retro.

### 4.4 Swap points and reordering

Per §2.4, only **S3 ↔ S4** has a discretionary ordering. The two
conditions under which swapping becomes attractive:

- **S4 material is blocked and S3 is unblocked.** If active
  perception discovery work reveals a gap in the `MarketSelector`
  contract that requires a separate retro (e.g., multi-strategy
  subscription conflict resolution turns out to need its own
  design), then S3's additive work can proceed while S4's design
  unblocks. This keeps the project moving without forcing the team
  to pause.
- **The subscription push path is the higher-risk work and we
  want it out of the way first.** If diagnostic evidence (a bug
  reproduction, a `file:line` failure trace in today's sensor code
  per 🔴 Runtime behaviour > design intent) shows the
  subscription mechanism is already misbehaving in production-
  shape ways, S4 ahead of S3 front-loads the risk. S3's factor
  stream lands against a known-stable subscription path instead
  of a moving one.

Either swap must be recorded in the §4.3 gate-6 decision row
(`reorder`) with the triggering condition cited. Swapping **any
other pair** (S1 ↔ S2, S2 ↔ S3, S4 ↔ S5, S5 ↔ S6) breaks the
rationale in §4.2 and is not supported by this decomposition; it
would require amending §4.2 and opening a new retro documenting
the architectural reason the original rationale no longer holds.

---

## 5. Cross-spec acceptance gates

Every sub-spec PR — regardless of which sub-spec, which checkpoint,
or which harness run — passes the same baseline before merge. §4's
between-spec gates are about the **transition** from N to N+1;
§5's cross-spec gates are about **every PR inside every sub-spec**.
The two layers stack; passing §5 does not skip §4.

### 5.1 Test baseline

`uv run pytest -q` passes with the `CLAUDE.md`-stated baseline:
**≥ 70 tests passing, 2 skipped**, where the 2 skipped are the
`@pytest.mark.integration` tests gated on the
`PMS_RUN_INTEGRATION=1` env var (🟢 Integration test default-skip
pattern, promoted from `pms-phase2` retro Proposal 3). Integration
tests are run on demand:

```bash
PMS_RUN_INTEGRATION=1 uv run pytest -m integration
```

If the baseline fails on a **fresh clone in a fresh shell**, per
🟡 Fresh-clone baseline verification (promoted from `pms-phase2`
retro Proposal 2), the fix is **the first commit on the feature
branch**, with prefix `fix(tests):` or `fix(build):`, and feature
work only begins after that commit lands. Dev-machine state (IDE
plugins, stale venv, `sys.path` injections) can hide config bugs
that bite the next contributor — do not start feature work against
a broken baseline. The rule is load-bearing: running the gates on a
stale venv and declaring the baseline holds is precisely what the
retro was promoted to prevent.

### 5.2 Strict typing

`uv run mypy src/ tests/ --strict` is clean. Every committed module
— `src/pms/**`, `tests/**`, dashboard Python ingress if any — is
strict-typed. No `# type: ignore` may be introduced without a
comment naming the specific type-system limitation (no generic "mypy
is wrong" justifications). Newly-introduced ignores surface in code
review; reviewers reject the ignore unless the accompanying comment
identifies the limitation (stub gap, variance limitation,
third-party lib without py.typed).

### 5.3 Invariant conformance

Every sub-spec PR must demonstrate conformance with every one of
the 8 invariants in `agent_docs/architecture-invariants.md`. The 8
split into three buckets, mirroring what each invariant's
**Enforcement** block actually says. Of the four non-behavioural
invariants, exactly one (Invariant 5) is enforced by a machine
check alone; the other three (Invariants 3, 6, 8) each name a
second, review-gate enforcement half in their Enforcement block,
and the PR evidence must surface both halves.

- **Mechanically checkable (machine check is sufficient PR
  evidence)** — a machine check (import-linter run, delimited-DDL
  grep) is sufficient because the Enforcement block names *no*
  additional review gate. Invariant 5.
- **Mixed (machine check + review gate)** — the Enforcement block
  names both a mechanical rule and a code-review / reviewer-
  rejection gate. PR evidence must cover both. Invariants 3, 6, 8.
- **Behavioural** — no mechanical check faithfully captures the
  invariant; acceptance criteria + code review are the enforcement.
  Invariants 1, 2, 4, 7.

Be honest about which bucket each invariant lives in — inventing a
grep recipe for a behavioural invariant gives a false-green signal;
a partial grep for a mechanically-checkable invariant misses real
violations; and treating a Mixed invariant as purely mechanical
false-greens PRs that pass the machine check while failing the
review gate (e.g., a PR that satisfies the Invariant 6 import-
linter rule but hardcodes a static subscription list, or a PR that
satisfies the Invariant 8 grep but adds a new table without
declaring its ring).

**Mechanically checkable invariants.** The primary evidence is a
machine check (import-linter run). A grep complement is listed
where it usefully smoke-checks the same property, but the linter
result itself is the PR evidence — not a partial grep recipe. Only
invariants whose **Enforcement** block names *no* additional review
gate live in this bucket.

- **Invariant 5** — import-linter rule (codified in S2's
  `pyproject.toml` or `ruff.toml`): `pms.sensor` and `pms.actuator`
  cannot import from `pms.strategies.aggregate` **or** from
  `pms.controller.*`. The linter rule runs as part of the lint pass;
  the linter report is the PR evidence. Any violation fails CI. A
  grep smoke check exists but must cover both banned targets and
  both `from … import …` and plain `import …` syntaxes (e.g.
  `rg -n '^(from pms\.strategies\.aggregate|from pms\.controller|
  import pms\.strategies\.aggregate|import pms\.controller)'
  src/pms/sensor src/pms/actuator` — zero matches). The grep is not
  a substitute for the linter; it is a cheap preflight.

**Mixed invariants (machine check + review gate).** Each invariant
below has **two** enforcement halves named in its
`agent_docs/architecture-invariants.md` **Enforcement** block: a
mechanical rule and a code-review gate. PR evidence must cover
**both** halves. Passing only the machine check is insufficient and
produces a false-green signal.

- **Invariant 3** — two independent enforcement mechanisms per
  `agent_docs/architecture-invariants.md` §Invariant 3
  **Enforcement**: (a) **schema (mechanical)**: `strategy_version_id`
  is `NOT NULL` on every inner-ring product table after S5
  completes, plus a `CHECK` constraint forbidding known sentinel
  values (empty string). The DDL block must declare both columns
  AND `NOT NULL` AND the `CHECK` once the S5 upgrade lands. A grep
  recipe that only verifies column presence (ignoring `NOT NULL` /
  `CHECK`) is insufficient; the PR evidence is the schema file
  itself plus a post-migration `\d+` or `information_schema` query
  showing the constraint is active. (b) **query-time (review)**:
  no SQL aggregation query over `eval_records` or `fills` may omit
  `GROUP BY strategy_version_id` without an explicit comment
  justifying the cross-version aggregation. A grep can find
  `GROUP BY` clauses but cannot judge whether an omission was
  intentional — reviewer sign-off is the PR evidence.
- **Invariant 6** — two enforcement mechanisms per
  `agent_docs/architecture-invariants.md` §Invariant 6
  **Enforcement**: (a) **import boundary (mechanical)**: import-
  linter rule — `pms.sensor` cannot import from
  `pms.market_selection`. Same mechanism as Invariant 5; linter
  report is the machine-side evidence. Grep smoke check must cover
  both syntaxes
  (`rg -n '^(from pms\.market_selection|import pms\.market_selection)'
  src/pms/sensor` — zero matches). (b) **design review
  (review)**: the Enforcement block requires the spec-evaluation
  reviewer to reject any design that makes Sensor aware of
  strategies to avoid implementing the selector. The linter catches
  a direct import violation but not a semantic one (e.g., Sensor
  hardcoding a static asset-id list that *is* strategy-selected but
  lives in a config file). PR evidence must include a reviewer note
  confirming the Sensor design remains strategy-agnostic —
  specifically that every subscription update arrives through
  `SensorSubscriptionController`, not through a sensor-owned config
  or constant.
- **Invariant 8** — two enforcement mechanisms per
  `agent_docs/architecture-invariants.md` §Invariant 8
  **Enforcement**: (a) **schema (mechanical)**: zero `strategy_id`
  **and** zero `strategy_version_id` columns on outer-ring or
  middle-ring tables. Both identifiers are named explicitly in the
  Enforcement block as the grep-checkable rule. Grep: within the
  outer-ring and middle-ring DDL blocks of `schema.sql` (delimited
  by the block comments introduced in S1 / S3), both identifiers
  must produce zero matches. The DDL block comments are what make
  this mechanical — without them, a naive `rg strategy_id schema.sql`
  would hit legitimate inner-ring declarations. (b) **ring-
  declaration review (review)**: the Enforcement block requires
  that every new-table proposal declare its ring explicitly and
  justify the ring choice. The grep confirms no forbidden columns
  on an existing ring; it does not confirm that a newly-introduced
  table has been placed in the correct ring. PR evidence must
  include the ring declaration in the PR description or migration
  comment and a reviewer sign-off on the ring choice.

**Behavioural invariants** (no grep; enforced by acceptance
criteria and code review).

- **Invariant 1** — concurrent feedback web, not phased runtime.
  There is no grep for "does the runtime actually run layers
  concurrently" — this is enforced by each sub-spec's acceptance
  criteria rejecting synchronous barriers between layers, and by
  code review rejecting any new `asyncio.gather` that blocks one
  layer on another (per `agent_docs/architecture-invariants.md`
  §Invariant 1 **Enforcement**).
- **Invariant 2** — `Strategy` as rich aggregate. The import-linter
  rule (Invariant 5) catches the most common violation, but
  semantic violations like "projection class with mutable
  containers" or "downstream entity field duplicating strategy
  state" are caught by code review reading the diff, not by grep.
- **Invariant 4** — raw factors only. No mechanical check
  distinguishes a "raw" factor from a thinly-disguised composite;
  code review reads the factor definition and rejects any factor
  that encodes strategy-specific weighting (per §3.2.3 and
  `agent_docs/architecture-invariants.md` §Invariant 4
  **Enforcement**).
- **Invariant 7** — two-layer sensor (discovery + data), with
  single-responsibility per class and the discipline that future
  venue adapters land as a pair. `agent_docs/architecture-
  invariants.md` §Invariant 7 **Enforcement** names two review
  gates: (a) S1 acceptance criteria (two separate classes, each
  with a single responsibility); (b) code review of any future
  venue adapter requiring a discovery/data pair. Neither is
  mechanically detectable by a class-name grep — a hybrid adapter
  can satisfy "two class names exist in different files" while
  still owning both responsibilities. A class-name / file-
  separation grep is a useful **smoke check** (zero-match on a
  single-file definition of both classes) but the PR evidence is
  the acceptance-criterion reference and the code-review record.

A sub-spec PR's invariant-conformance section lists (a) which
invariants apply to its scope, (b) for each mechanically checkable
one, the machine-check evidence (import-linter report with expected
zero-match), (c) for each Mixed invariant that applies (3, 6, 8),
**both** the mechanical evidence (schema constraint, linter report,
or delimited-DDL grep) **and** the review-gate evidence (reviewer
sign-off note: aggregation-query review for 3, strategy-agnostic
Sensor design confirmation for 6, ring-declaration justification for
8), (d) for each behavioural one, the acceptance-criterion language
in the sub-spec that enforces it.

### 5.4 Boundary matrix audit

Any load-bearing concept introduced by the sub-spec PR (module,
class, table, DDL change, enforcement hook, dashboard page, named
policy object) must already appear in §3.2 Boundary Matrix with this
sub-spec as its Owner **or** must be added to §3.2 **in the same
PR** — per the §3.1 guidance "When adding a new concept not in the
matrix". A PR that introduces a concept absent from the matrix fails
this gate; the reviewer either approves the matrix update in the
same PR or rejects the concept addition as scope drift.

Reviewer workflow: grep the PR diff for every new class name, table
name, and DDL identifier; for each hit, confirm the identifier
appears in §3.2 with this sub-spec as Owner. If the identifier is
absent, the §3.2 update must land in the same PR.

### 5.5 Retro-promotion workflow

Every sub-spec ends with a retro under `.harness/retro/<sub-spec-
id>.md` (see §4.3 gate 1). If the retro produces a rule that meets
the promotion criteria in `agent_docs/promoted-rules.md`
§"Promotion process" — observed in ≥ 2 task retros, **or**
high-severity on first observation, **or** user-explicit — the rule
is:

1. Appended to `agent_docs/promoted-rules.md` with provenance
   (`Promoted from <sub-spec-id> retro Proposal N`).
2. Mirrored into `CLAUDE.md` §"Promoted rules from retros" with
   severity-emoji (🔴 / 🟡 / 🟢) and a one-line summary.
3. Recorded in `.harness/retro/index.md` with lifecycle column
   moved to `active`.

All three updates land in **one PR** (the retro-promotion PR). A
retro whose rule meets the promotion criteria but whose promotion
PR is not yet merged blocks the §4.3 gate 3 check for the next
sub-spec.

### 5.6 Commit-message discipline

Conventional-commit prefixes are required:

| Prefix | Use for |
|---|---|
| `feat(<scope>):` | New feature / capability |
| `fix(<scope>):` | Bug fix |
| `docs(<scope>):` | Documentation-only change |
| `test(<scope>):` | Test additions / refactoring without behaviour change |
| `refactor(<scope>):` | Code refactor without behaviour change |
| `chore(<scope>):` | Tooling, config, dependency bumps |

Scope is the affected module, sub-spec id, or `build` / `tests` as
appropriate (e.g., `fix(tests): pin pytest-asyncio>=0.23`,
`feat(sensor): two-layer discovery + data split`).

**No `Co-Authored-By` lines.** This is stated twice in the project
rules — once in `CLAUDE.md` §"Do not" (`Never add Co-Authored-By
lines`) and once in `agent_docs/promoted-rules.md` §"Commit-
message precedence" (*Promoted from `pms-v1` retro Proposal 7*).
They are the same rule, promoted. The user's global git rule wins
against any harness / template / upstream default that would add
the line — this is settled, do not re-derive at every commit.
Review-loop commits (`review-loop: changes from round N`) inherit
the same rule.

---

## 6. Sub-spec S1 — pms-market-data-v1

S1 is the **entry point** of the DAG (§2.1): it has no predecessors,
and the five downstream sub-specs all depend — directly or
transitively — on its outer-ring schema, the `asyncpg.Pool` the
Runner owns, and the two-sensor split that Invariant 7 requires.
This section expands the skeleton in `agent_docs/project-roadmap.md`
§S1 into the project-level contract that the harness run under
`.harness/pms-market-data-v1/` will consume.

### 6.1 Goal

S1 replaces today's fabricated orderbook depth with **live
Polymarket CLOB data persisted in PostgreSQL and rendered on
`/signals`** — closing observable-capability items #2 and #4 of §1.1
(real book snapshots + `price_change` deltas land in
`book_snapshots` / `book_levels` / `price_changes` / `trades`;
`/signals` renders real depth, not fabricated bid/ask). In doing so,
S1 also lands as the **schema-foundation sub-spec**: `schema.sql`
declares the outer-ring tables plus the inner-ring product-table
shells that existing runtime code already depends on — today that
includes all 4 inner-ring product tables: `feedback` and
`eval_records` from the JSONL→PG migration (§3.2.1 row 14), plus
`orders` and `fills` as empty shells (§3.2.1 row 18, added per
`agent_docs/architecture-invariants.md` §Invariant 8 enumeration).
All 4 carry `(strategy_id, strategy_version_id)` columns reserved
`NULLABLE` so S2 can seed `"default"` and S5 can upgrade to
`NOT NULL` without a second full-table migration (§3.2.1 row 19,
Invariants 3 + 8). The middle
ring (`factors` / `factor_values`) is owned by S3 (§3.2.3) and does
not appear in S1's `schema.sql` — Invariant 8 (onion-concentric
ring ownership) keeps the middle-ring DDL outside the outer-ring
sub-spec.

### 6.2 Scope (in / out)

**Scope in.** Exactly the 19 concepts owned by S1 in §3.2.1, in the
same order, each with a one-line scope descriptor. Reviewers
verifying boundary integrity grep §3.2.1 against this list; the
lists must match concept-for-concept.

1. **`markets` table (DDL + writes).** Outer-ring table keyed on
   Polymarket `condition_id`; populated by `MarketDiscoverySensor`
   polling the Gamma `/markets` endpoint (Invariants 7, 8).
2. **`tokens` table (DDL + writes).** Per-outcome `YES` / `NO`
   token ids referencing `markets.condition_id`; populated
   alongside `markets` (Invariants 7, 8).
3. **`book_snapshots` table.** One row per `book` event (subscribe /
   reconnect / periodic checkpoint); stores metadata (`ts`, `hash`,
   `source`) (Invariants 7, 8).
4. **`book_levels` table.** Per-level rows referencing
   `book_snapshots.id` with `ON DELETE CASCADE`; no JSON blobs, per
   discovery-note Q2 resolution (Invariants 7, 8).
5. **`price_changes` table.** One row per `price_change` event with
   `size` as the NEW total at the level (Polymarket delta semantics,
   `size = 0` means level removed), plus `best_bid` / `best_ask` for
   quick-scan queries (Invariants 7, 8).
6. **`trades` table.** One row per `last_trade_price` event; minimal
   columns (`market_id`, `token_id`, `ts`, `price`) (Invariants 7, 8).
7. **`PostgresMarketDataStore` (typed methods over outer ring).**
   Single concrete class under `src/pms/storage/` with typed methods
   (`write_market`, `write_token`, `write_book_snapshot`,
   `write_book_level`, `write_price_change`, `write_trade`, plus
   read helpers the dashboard and evaluator need). No
   `IMarketDataStore` Protocol — discovery-note Q5 resolved
   (Invariant 8).
8. **`asyncpg.Pool` lifecycle (Runner-owned).** Single pool created
   in `Runner.start()`, closed in `Runner.stop()`, `min_size=2` /
   `max_size=10`, shared across sensor / controller / actuator /
   evaluator / API tasks (discovery-note Q6).
9. **`schema.sql` file (startup-applied).** One file declaring
   outer-ring tables plus inner-ring product-table shells with
   reserved nullable strategy columns; applied on Runner boot if
   target tables do not exist; no migration framework.
10. **`MarketDiscoverySensor` class.** Low-frequency, strategy-
    agnostic, unconditional; polls Gamma `/markets` on a coarse
    cadence; writes `markets` + `tokens` (Invariant 7).
11. **`MarketDataSensor` class.** High-frequency, subscription-
    driven; connects to the CLOB WebSocket market channel; parses
    `book` + `price_change` + `last_trade_price` events with a
    stateful per-asset orderbook mirror; writes
    `book_snapshots` / `book_levels` / `price_changes` / `trades`
    (Invariants 6, 7).
12. **`SensorWatchdog` wiring to stream sensor.** Existing
    `src/pms/sensor/watchdog.py` class wired to `MarketDataSensor`
    via the same stale-detection hook used elsewhere in the Sensor
    layer (Invariant 7).
13. **WebSocket heartbeat + reconnect reconciliation.** 10-second
    PING / PONG loop on the CLOB connection; on reconnect, re-issue
    the subscribe message and treat the first arriving `book` event
    as the canonical snapshot (`source='reconnect'` on the row);
    resolves discovery-note §1a Q4 (Invariant 7).
14. **JSONL → PG migration (`FeedbackStore` + `EvalStore` rewritten
    over SQL).** Both stores become thin SQL wrappers over the
    shared pool; `.data/*.jsonl` retires from the runtime contract;
    per-shell DB isolation replaces `PMS_DATA_DIR` (discovery-note
    "Storage unification" decision, Invariant 8).
15. **Transaction-rollback test fixture (`db_conn`).** Session-
    scoped schema load + per-test transaction that rolls back on
    teardown; no `pytest-postgresql` dependency. Cross-connection
    integration tests fall back to `TRUNCATE` in an `autouse`
    fixture (discovery-note "Test strategy" decision).
16. **`compose.yml` for local PG (dev).** Committed `postgres:16`
    service definition; CI uses the same image tag via GitHub
    Actions `services.postgres.image`.
17. **`/signals` dashboard page (real orderbook depth).** Replaces
    today's empty-orderbook rendering
    (`src/pms/sensor/adapters/polymarket_rest.py:90` —
    `orderbook={"bids": [], "asks": []}`) with a live view
    reconstructed from `book_snapshots` + `book_levels` + recent
    `price_changes` (Invariant 7).

18. **`orders` + `fills` inner-ring product tables (DDL, empty
    shape).** Per `agent_docs/architecture-invariants.md` §Invariant
    8 enumeration of inner-ring product tables (`eval_records`,
    `feedback`, `orders`, `fills`). Land as empty tables with
    `(strategy_id, strategy_version_id)` `NULLABLE` columns
    (column reservation handled by item 19); today's runtime emits
    `OrderState` / `FillRecord` in-memory only, so S1 grants
    persistence so S5's `NOT NULL` migration has all 4 product
    tables to upgrade uniformly (Invariants 3, 8).
19. **Inner-ring `(strategy_id, strategy_version_id)` columns
    reserved `NULLABLE` on product tables.** All 4 inner-ring
    product tables (`feedback` + `eval_records` from the JSONL→PG
    migration in §3.2.1 row 14, plus `orders` + `fills` from item
    18 above) carry both columns `NULLABLE`; S2 seeds `"default"`,
    S5 upgrades to `NOT NULL` (Invariants 3, 8).

**Scope out.** The following look S1-adjacent but are owned by
other sub-specs. A PR landing any of these under `.harness/pms-
market-data-v1/` is scope drift.

- `Strategy` aggregate + projection types (`StrategyConfig`,
  `RiskParams`, `EvalSpec`, `ForecasterSpec`, `MarketSelectionSpec`)
  → **S2** (§3.2.2; Invariants 2 + 5 — Strategy as rich aggregate,
  strategy-aware boundary). S1 does not import `pms.strategies.*`
  from any sensor or storage module.
- `strategies` / `strategy_versions` aggregate tables and the
  `PostgresStrategyRegistry` → **S2** (§3.2.2; Invariants 3 + 8 —
  immutable version tagging, inner-ring ownership).
- `strategy_factors` link table → **S2** (§3.2.2; Invariants 2 + 4
  + 8 — strategy-side composition over raw factors, inner-ring
  ownership); empty shape only in S2.
- `factors` / `factor_values` tables and `FactorService` → **S3**
  (§3.2.3; Invariants 4 + 8 — raw factors only, middle-ring
  ownership).
- `/factors` dashboard page → **S3** (§3.2.3; Invariant 4).
- `MarketSelector` + `SensorSubscriptionController` +
  `Strategy.select_markets(universe)` method + boot-order wiring
  (DiscoverySensor → Selector → SubscriptionController → DataSensor)
  → **S4** (§3.2.4; Invariants 6 + 7 — active perception,
  subscription sink on data sensor). S1's `MarketDataSensor` ships
  as a subscription *sink* but gets its asset-id list from
  configuration (or a stub loader) until S4 lands the push channel.
- Per-strategy `ControllerPipeline` dispatch, per-strategy Evaluator
  aggregation, the `(strategy_id, strategy_version_id)` `NOT NULL`
  schema upgrade, `Opportunity` entity, `/strategies` comparative
  view, `/metrics` per-strategy breakdown → **S5** (§3.2.5;
  Invariants 2 + 3 + 5 — projections, immutable version tagging,
  strategy-aware boundary for Controller + Evaluator).
- `BacktestSpec` / `ExecutionModel` / `BacktestDataset` /
  `BacktestRun` / `StrategyRun` / parameter sweep /
  `BacktestLiveComparison` / `/backtest` ranked N-strategy view →
  **S6** (§3.2.6; uses Invariants 3 + 8 — version-tagged
  reproducibility and ring-respecting reads).

### 6.3 Acceptance criteria (system-level)

Nine observable-capability bullets. Each is verifiable at the
system level — not a checklist item for an individual CP.

1. **`schema.sql` applies cleanly.** Running the file against a
   fresh PostgreSQL 16 database via `psql -f schema.sql` completes
   with zero errors and produces 6 outer-ring tables (`markets`,
   `tokens`, `book_snapshots`, `book_levels`, `price_changes`,
   `trades`) plus all 4 inner-ring product-table shells
   (`feedback`, `eval_records` from the JSONL→PG migration, plus
   `orders` and `fills` as empty shapes per `agent_docs/
   architecture-invariants.md` §Invariant 8 enumeration). All 4
   product tables have `(strategy_id, strategy_version_id)`
   columns `NULLABLE` (Invariants 3, 7, 8).
2. **A `book` event arrives within 5 seconds of subscription.**
   From a cold start against live Polymarket CLOB, the first
   `book_snapshots` row with `source='subscribe'` is written within
   5 seconds of `MarketDataSensor.subscribe(asset_ids)` being
   called (Invariant 7).
3. **Delta semantics are lossless.** Applying every `price_changes`
   row for a given `(market_id, token_id)` in insertion order on
   top of the most recent `book_snapshots` + `book_levels` rows
   reconstructs an orderbook identical (modulo float tolerance) to
   what the WebSocket last pushed — including zero-size level
   removal per Polymarket semantics (Invariants 7, 8).
4. **`/signals` renders real depth, not fabricated bid/ask.** The
   dashboard `/signals` page displays actual bid/ask levels drawn
   from the outer-ring tables; a Playwright assertion confirms at
   least two distinct depth levels are present on an active market
   (closing `1a Orderbook Data: Simulated, Not Real` of
   `docs/notes/2026-04-16-repo-issues-controller-evaluator.md`).
5. **JSONL runtime contract is retired.** `Runner.start()` +
   `FeedbackStore` + `EvalStore` never read from nor write to
   `.data/*.jsonl`; the runtime contract is the PostgreSQL pool.
   A startup-time assertion fails loudly if the legacy directory
   path is referenced anywhere in the runtime path (Invariant 8).
6. **Discovery and data cadences run independently.** The
   full-universe `markets` / `tokens` refresh cadence (coarse,
   driven by `MarketDiscoverySensor`) and the streaming
   book / delta / trade ingest rate (high-frequency, driven by
   `MarketDataSensor`) are observable as two separate write
   streams; one can stall or restart without blocking the other
   (Invariant 7 — two-layer sensor with independent writers).
7. **Heartbeat + reconnect reconciliation work end-to-end.** A
   forced WebSocket disconnect mid-run is followed by an automatic
   reconnect, a re-subscribe message, and a `book_snapshots` row
   with `source='reconnect'`; the stateful parser resumes without
   double-counting or dropping deltas (Invariant 7).
8. **No market-data row is tagged with a strategy.** Rows written
   to outer-ring tables (`markets`, `tokens`, `book_snapshots`,
   `book_levels`, `price_changes`, `trades`) are visible to every
   strategy and every backtest without per-strategy duplication;
   the same `book` event is written once, not once per strategy
   (Invariant 8 — outer-ring data is strategy-agnostic and
   shared). The mechanical schema grep that witnesses this
   invariant lives in §5.3 Mixed-invariant evidence, not in AC.
9. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   passes with ≥ 70 tests + 2 skipped (integration gated on
   `PMS_RUN_INTEGRATION=1`) and `uv run mypy src/ tests/ --strict`
   is clean, on a fresh shell per 🟡 Fresh-clone baseline
   verification (§5.1, §5.2).

### 6.4 Non-goals

Explicit deferrals. An S1 harness spec that lands any of these
items is scope drift.

- **No ORM, no SQLAlchemy, no SQL query builder.** Raw SQL strings
  via `asyncpg` return frozen dataclasses (discovery-note
  "Persistence Decision" §Non-goals).
- **No migration framework.** No Alembic, no Sqitch, no in-house
  migration runner. Schema iteration during S1–S5 goes through
  edits to `schema.sql`; fresh-dev-DB and fresh-CI-DB reloads
  re-apply the full file. Alembic or Sqitch is reconsidered only
  if the single-file approach becomes painful.
- **No `MarketSignal.orderbook` in-memory model refactor.** The
  in-memory dict shape (`{"bids": [...], "asks": [...]}`) stays as
  today's backward-compatible presentation layer; the new outer-
  ring tables are the authoritative durable record. Revisit only
  if downstream consumers (S3 factors, S5 controller) need a
  different shape — the discovery-note Q2 open sub-question.
- **No backtest replay engine changes.** `HistoricalSensor` keeps
  its current JSONL/CSV replay behaviour; outer-ring reads against
  persisted Polymarket history land in S6.
- **No per-strategy subscription logic.** `MarketDataSensor`
  accepts a flat `asset_ids: list[str]` at start-up. The push
  channel from `SensorSubscriptionController` is S4-owned; S1
  stubs the loader so S4 can replace it without touching sensor
  internals (Invariants 5, 6).
- **Discovery-note open questions NOT resolved in S1:**
  (a) **retention policy** (§Schema Design Q4) — fully deferred
  past S1. S1's implicit contract is "keep everything until disk
  pressure forces action"; the real decision happens during S3 or
  S6 when factor-panel / backtest-replay query shapes make the
  trade-offs concrete. Not on CP-DQ.
  (b) **`price_changes` UNIQUE constraint on `(market_id, ts,
  price, side)` vs. allow duplicates** (§Q2 sub-question) —
  deferred to harness-spec review (see §6.8 CP-DQ below).
  (c) **`sensor_sessions` lifecycle table** (§Q2 sub-question) —
  deferred to the same CP-DQ; if the review chooses to add the
  table, it becomes an S1 concept and §3.2.1 must be amended in
  the same PR. If it stays deferred, Evaluator-side "no event vs.
  sensor down" discrimination lands as a separate future retro-
  triggered change, not inside S1.

### 6.5 Dependencies

- **Upstream sub-specs.** None. S1 is the entry point of the
  §2.1 DAG; no other sub-spec appears as a predecessor edge.
- **Upstream invariants.** Primary: **7** (two-layer sensor) and
  **8** (onion-concentric storage). Partial touches:
  **3** (reserved `NULLABLE` strategy-version columns on inner-ring
  product tables so S5's `NOT NULL` upgrade is a schema-change-only
  migration) and **6** (`MarketDataSensor` ships as the
  subscription sink, shaped so S4's `SensorSubscriptionController`
  push channel drops in without changing Sensor internals —
  Invariants 5 + 6 enforced by the S2 import-linter rule once S2
  lands).

### 6.6 Intake

Minimum set that must exist before a harness run opens under
`.harness/pms-market-data-v1/`. S1 being the DAG entry point,
this list is the shortest in the document.

1. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   passes with ≥ 70 tests + 2 skipped; `uv run mypy src/ tests/
   --strict` is clean (§5.1, §5.2; 🟡 Fresh-clone baseline
   verification).
2. **Polymarket CLOB WebSocket reachable from the dev
   environment.** `wss://ws-subscriptions-clob.polymarket.com/ws/
   market` accepts a connection and responds to a subscribe
   message without authentication — the market channel is public
   per the discovery note §1a "Available Polymarket APIs".
3. **`uv.lock` is current.** `uv sync` leaves no unlocked
   dependency edits in the working tree; `asyncpg` and any
   websocket client updates are captured in the lockfile.
4. **No conflicting migration or schema files in the tree.** No
   `alembic/`, `migrations/`, or `schema/versions/` directories
   exist — S1 is the first sub-spec to introduce durable schema,
   and partial pre-work would indicate scope overlap with a
   parallel effort that must first reconcile.

### 6.7 Leave-behind

Enumerated artefacts produced by S1, keyed back to §3.2.1 row
numbers so the §4.3 gate-4 boundary-integrity check (§4.3 gate 4)
can diff S2's Intake (once §7.6 lands) against this list.

1. **`schema.sql`** (§3.2.1 row 9) applies cleanly against a fresh
   PG 16 DB and produces:
   - Outer ring: `markets`, `tokens`, `book_snapshots`,
     `book_levels`, `price_changes`, `trades` (§3.2.1 rows 1–6;
     Invariants 7, 8).
   - Inner-ring product-table shells: all 4 tables `feedback`,
     `eval_records`, `orders`, `fills` (matching
     `agent_docs/architecture-invariants.md` §Invariant 8
     enumeration), each with `(strategy_id, strategy_version_id)`
     `NULLABLE` (§3.2.1 rows 14, 18, 19 respectively for
     migration-sourced tables, fresh `orders`/`fills` shells, and
     column reservation; Invariants 3, 8). `orders` and `fills`
     are empty-shape tables; today's runtime emits `OrderState` /
     `FillRecord` in-memory only, so S1 grants persistence so
     S5's `NOT NULL` migration has all 4 tables to upgrade
     uniformly.
   - Middle ring: **no** `factors` / `factor_values` DDL lands in
     S1's `schema.sql` — S3 owns both tables (§3.2.3, Invariants
     4 + 8 — middle-ring ownership).
2. **`PostgresMarketDataStore`** (§3.2.1 row 7) at
   `src/pms/storage/market_data_store.py` (harness spec may choose
   final path) with typed methods matching discovery-note Q6 shape
   (one INSERT per event, `write_price_change` / `write_book_level`
   signatures batch-friendly for future COPY upgrade).
3. **`asyncpg.Pool` owned by `Runner`** (§3.2.1 row 8) created in
   `Runner.start()`, closed in `Runner.stop()`,
   `min_size=2` / `max_size=10`, shared across every runtime task.
4. **`MarketDiscoverySensor` + `MarketDataSensor` as separate
   classes** (§3.2.1 rows 10–11) with `Runner._build_sensors()`
   instantiating both for non-backtest modes.
5. **`SensorWatchdog` wired to `MarketDataSensor`** (§3.2.1 row 12).
6. **WebSocket heartbeat + reconnect reconciliation** (§3.2.1
   row 13) active on `MarketDataSensor`; reconnect produces a
   `book_snapshots` row with `source='reconnect'`.
7. **`FeedbackStore` + `EvalStore` rewritten as SQL wrappers**
   (§3.2.1 row 14); `.data/*.jsonl` retired from the runtime
   contract.
8. **Transaction-rollback `db_conn` fixture** (§3.2.1 row 15)
   usable by every subsequent sub-spec's storage-layer tests.
9. **`compose.yml` for local PG + matching CI service
   definition** (§3.2.1 row 16).
10. **`/signals` dashboard page rendering real depth** (§3.2.1
    row 17) from outer-ring tables, verified by Playwright e2e.
11. **Inner-ring product tables reserve `(strategy_id,
    strategy_version_id)` `NULLABLE` columns** (§3.2.1 row 19) on
    all 4 product tables (`feedback`, `eval_records`, `orders`,
    `fills`) so S2 can seed `"default"` and S5 can upgrade to
    `NOT NULL` schema-change-only.

S2's Intake (authored in Commit 5 of this document authoring
effort) reads from this list; diffing the two is the §4.3 gate-4
mechanism.

### 6.8 Checkpoint skeleton

Flat one-line-each list. **Not harness acceptance criteria** — the
per-CP acceptance criteria, files-of-interest, effort estimates,
and test expectations land in `.harness/pms-market-data-v1/spec.md`
when the kickoff prompt (§6.11) triggers that authoring session.

- **CP1 — PG pool + `schema.sql` bootstrap.**
- **CP2 — Test infra (`db_conn` fixture + `compose.yml` + CI
  service).**
- **CP3 — `PostgresMarketDataStore` typed methods.**
- **CP4 — `MarketDiscoverySensor` split.**
- **CP5 — `MarketDataSensor` split + stateful WebSocket parser.**
- **CP6 — Heartbeat + reconnect reconciliation +
  `SensorWatchdog` wiring.**
- **CP7 — JSONL → PG storage unification (`FeedbackStore` +
  `EvalStore`).**
- **CP8 — `/signals` dashboard page upgrade.**
- **CP9 — Inner-ring `(strategy_id, strategy_version_id)` column
  reservations + `"default"` placeholder tolerance.**
- **CP-DQ — Decide deferred §Q2 sub-questions (spec-review
  checkpoint, no code).**

### 6.9 Effort estimate

**L** (largest sub-spec; 10 CPs per §6.8 — CP1–CP9 + CP-DQ;
schema + two sensors + storage layer + CI infra). S1 is the
foundation for every downstream sub-spec and carries the broadest
surface area: a new persistence backend (PostgreSQL + `asyncpg`),
a sensor split that rewrites the conflated `PolymarketRestSensor`,
a WebSocket parser with stateful orderbook reconstruction +
reconnect handling, a JSONL-to-SQL migration for two existing
stores, committed local-dev + CI infra, and a dashboard page
rewrite — each individually medium, together the largest S1–S6
scope.

### 6.10 Risk register

| Risk | Likelihood | Impact | Mitigation | Trigger / early-warning |
|---|---|---|---|---|
| Polymarket WebSocket contract drift (`book` / `price_change` event shape changes upstream) | M | H | Parser logs any unrecognised event shape at WARN with the full raw payload (via existing Python logging); schema-validation test fixtures replay known-good events; upgrade window tracked against Polymarket SDK changelogs | CI integration test fails on a known-good fixture replay; live sensor error-rate counter spikes; unrecognised-event WARN log entries appear |
| `asyncpg.Pool` starvation under burst event rates (price_change bursts during high-volatility periods) | M | M | Start at `max_size=10` per discovery-note Q6; add a bounded in-memory buffer with "batch flush" escape hatch if observed; metrics on `pool.acquire()` latency published to `/metrics` | `pool.acquire()` p99 latency > 100 ms under normal paper-mode load |
| Local PG provisioning friction for contributors without Docker | M | L | `compose.yml` is the primary path; `brew install postgresql@16` documented as fallback; README links directly; CI uses the same image tag so "works in CI" is reproducible locally | New contributor opens a `help wanted` issue citing DB setup; CI green but local `pytest` red |
| Dual-write race between `MarketDiscoverySensor` and `MarketDataSensor` on the same `market` row | L | M | `markets` upsert uses `INSERT ... ON CONFLICT (condition_id) DO UPDATE`; discovery sensor runs on a coarse cadence (seconds), data sensor never writes `markets` (only `book_snapshots` / `book_levels` / `price_changes` / `trades`) — separation is ownership-based, not timing-based (Invariant 7) | Deadlock / lock-wait timeout entries in PG logs; duplicate-key errors in Runner logs |
| JSONL → PG migration data loss for dev environments with existing `.data/*.jsonl` | L | L | `.data/*.jsonl` is gitignored and dev-only per discovery-note §"Storage unification"; a one-off read-only migration script is provided but contributors can also drop-and-restart; migration is bounded (two files, flat schema) | Contributor reports `.data/feedback.jsonl` they wanted to preserve; migration script produces a row-count mismatch |
| `schema.sql`-only approach making iteration painful without a migration framework | M | M | Schema iteration during S1–S5 lands as edits to `schema.sql` with fresh-DB reloads in dev + CI; re-evaluate at S5 completion — if the pattern becomes painful, open a retro and add Alembic or Sqitch before S6 (discovery-note "Persistence Decision" §Non-goals) | Schema-iteration PRs during S2–S4 trigger more than three "had to drop my dev DB" follow-ups across contributors |

### 6.11 Kickoff Prompt

The block below is **copy-paste-ready** for a fresh Claude session
whose job is to author `.harness/pms-market-data-v1/spec.md` — the
harness-executable spec with per-CP acceptance criteria,
files-of-interest, effort, and intra-spec dependencies. The future
session has **no memory** of this document; the prompt is
self-contained.

```
SCOPE:

You are starting harness task `pms-market-data-v1` in the
prediction-market-system repository at
/Users/stometa/dev/prediction-market-system. Your job this
session is to author the harness-executable spec file at
/Users/stometa/dev/prediction-market-system/.harness/pms-market-data-v1/spec.md
(the directory may not yet exist; you will create it).

REQUIRED READING (ordered — read in this order before touching
anything, all paths absolute):

1. /Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md
   — specifically §6 (this sub-spec's total-spec contract).
2. /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
   — focus on Invariants 7 (two-layer sensor) and 8 (onion-
   concentric storage). Partial touches on 3 and 6.
3. /Users/stometa/dev/prediction-market-system/agent_docs/promoted-rules.md
   — especially Runtime behaviour > design intent (🔴),
   Fresh-clone baseline verification (🟡), Integration test
   default-skip pattern (🟢), and Lifecycle cleanup on all exit
   paths (🟡 — relevant for pool + WebSocket lifecycle).
4. /Users/stometa/dev/prediction-market-system/docs/notes/2026-04-16-repo-issues-controller-evaluator.md
   — the primary source document. Load-bearing sections:
   "Persistence Decision: PostgreSQL, All Environments",
   "Schema Design: Open Questions" Q1 / Q2 / Q4 / Q5 / Q6,
   "Summary: Decisions Captured On 2026-04-16".
5. /Users/stometa/dev/prediction-market-system/src/pms/sensor/CLAUDE.md
   — per-layer invariant enforcement for Sensor.
6. /Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
   — structural reference for harness-grade spec shape (CP shape,
   acceptance-criteria shape, files-of-interest shape).

CURRENT STATE SNAPSHOT:

S1 is the entry point of the project decomposition DAG. No prior
sub-specs have landed. The project-level spec
(/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md)
is the authoritative boundary contract: §3.2.1 enumerates the 19
concepts S1 owns, §6 is S1's total-spec subsection.

PREFLIGHT (boundary check — run before any authoring; every
shell command below assumes cwd is
/Users/stometa/dev/prediction-market-system):

- §6.6 Intake items must all be satisfied. If any fails, HALT and
  tell the user:
  * `uv run pytest -q` passes with ≥ 70 tests + 2 skipped
    (PMS_RUN_INTEGRATION=1 gate), and
    `uv run mypy /Users/stometa/dev/prediction-market-system/src/ /Users/stometa/dev/prediction-market-system/tests/ --strict`
    is clean, on a fresh shell.
  * The Polymarket CLOB WebSocket at
    `wss://ws-subscriptions-clob.polymarket.com/ws/market` accepts
    a connection without authentication.
  * `uv sync` leaves no unlocked dependency edits in the working
    tree.
  * No `alembic/`, `migrations/`, or `schema/versions/` directory
    exists under /Users/stometa/dev/prediction-market-system/.

TASK:

Create
/Users/stometa/dev/prediction-market-system/.harness/pms-market-data-v1/spec.md
by expanding §6.8 (Checkpoint skeleton) into harness-grade CPs
with:

- Per-CP acceptance criteria (observable, falsifiable, not
  implementation notes).
- Per-CP files-of-interest (absolute paths under
  /Users/stometa/dev/prediction-market-system/).
- Per-CP effort estimate (S / M / L).
- Intra-spec CP dependencies (which CPs block which).

Use
/Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
as the structural reference for shape. Draft only — stop and wait
for spec-evaluation approval before running any checkpoint.

CONSTRAINTS:

- New feature branch `feat/pms-market-data-v1` off `main`. Never
  commit to `main` directly.
- Respect §3 Boundary Matrix of the project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md).
  Never claim a concept owned by another sub-spec (S2–S6). The 18
  concepts enumerated in §6.2 Scope-in are the complete authorised
  set for S1; anything else is scope drift.
- Every Strategy / Factor / Sensor / ring-ownership claim must
  cite an invariant number from
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md.
- No `Co-Authored-By` lines in any commit
  (/Users/stometa/dev/prediction-market-system/CLAUDE.md §"Do
  not"; promoted rule "Commit-message precedence").
- Conventional-commit prefixes required (§5.6 of the project
  spec): `feat(<scope>):`, `fix(<scope>):`, `docs(<scope>):`,
  `test(<scope>):`, `refactor(<scope>):`, `chore(<scope>):`.
- Follow the promoted rule 🟡 Lifecycle cleanup on all exit paths
  for the asyncpg pool and the WebSocket client: acquire + release
  in the same commit, `try/finally` on all four exit paths.

HALT CONDITIONS:

- Any invariant in
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
  cannot be satisfied by the design you are authoring. Do NOT
  silently amend the invariant. Open a retro under
  /Users/stometa/dev/prediction-market-system/.harness/retro/ and
  return to the user.
- Any attempt to add a `strategy_id` or `strategy_version_id`
  column to an outer-ring table (`markets`, `tokens`,
  `book_snapshots`, `book_levels`, `price_changes`, `trades`).
  This is an Invariant 8 violation — STOP immediately.
- The 19 concepts you author CPs for do not match §3.2.1 of the
  project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md),
  concept-for-concept. Reconcile first.

FIRST ACTION:

Run:

    cd /Users/stometa/dev/prediction-market-system \
      && git status && git branch --show-current \
      && git log --oneline -5

Then read the 6 files in the REQUIRED READING block, in order,
before drafting any content. After reading, report your
understanding of §6.8's 10 checkpoints (CP1–CP9 + CP-DQ) and wait
for go-ahead before drafting
/Users/stometa/dev/prediction-market-system/.harness/pms-market-data-v1/spec.md.
```

---

## 7. Sub-spec S2 — pms-strategy-aggregate-v1

S2 is the **second node** of the DAG (§2.1). Its single predecessor
edge is S1 (S1 → S2); its outgoing edges are S2 → S3 and S2 → S4
(§2.1, §2.4). S2 is the sub-spec that turns S1's reserved
`NULLABLE` strategy columns into a populated inner-ring aggregate,
puts the import-linter rules that Invariants 2 + 5 + 6 require into
CI, and seeds a `"default"` strategy so legacy runtime writes stop
producing implicit `NULL` tags. This section expands the skeleton in
`agent_docs/project-roadmap.md` §S2 into the project-level contract
that the harness run under `.harness/pms-strategy-aggregate-v1/`
will consume.

### 7.1 Goal

S2 delivers the **inner-ring aggregate and registry** that makes
**strategy onboarding without code rewrite** possible — closing
observable-capability item #7 of §1.1 (new strategy = new row in
`strategies` + a module under `src/pms/strategies/<id>/` + a
`StrategyConfig` blob, with no changes required in `src/pms/sensor/`
or `src/pms/actuator/`; Invariant 5 — strategy-agnostic boundary).
Concretely, S2 lands a rich `Strategy` aggregate (Invariant 2) with
frozen projection types (`StrategyConfig`, `RiskParams`, `EvalSpec`,
`ForecasterSpec`, `MarketSelectionSpec`), the
`strategies` + `strategy_versions` + `strategy_factors` DDL
(Invariants 3, 8), a `PostgresStrategyRegistry` CRUD class,
import-linter rules in CI that codify Invariants 2 + 5 + 6 import
directions, a `"default"` strategy + version row seed so legacy
runtime writes tag `"default"` instead of `NULL` during the pre-S5
window (Invariant 3 pattern: NULLABLE → seed → NOT NULL), and a
minimal `/strategies` listing page. **No per-strategy dispatch
lands in S2** — per-strategy `ControllerPipeline` dispatch,
per-strategy Evaluator aggregation, and the `(strategy_id,
strategy_version_id)` `NOT NULL` upgrade are S5-owned (§3.2.5).

### 7.2 Scope (in / out)

**Scope in.** Exactly the 9 concepts owned by S2 in §3.2.2, in the
same order, each with a one-line scope descriptor. Reviewers
verifying boundary integrity grep §3.2.2 against this list; the
lists must match concept-for-concept.

1. **`Strategy` aggregate** at
   `src/pms/strategies/aggregate.py`. The DDD-style rich aggregate
   that owns factor specs, risk params, eval spec, market selection
   rules, forecaster composition, router gating, and versioning
   (Invariant 2). Consumed by S4 (via projections), S5 (aggregate
   reader), and S6 (aggregate reader).
2. **Projection types** (`StrategyConfig`, `RiskParams`,
   `EvalSpec`, `ForecasterSpec`, `MarketSelectionSpec`) at
   `src/pms/strategies/projections.py`. All `@dataclass(frozen=
   True)`; downstream layers receive these, never the aggregate
   itself (Invariants 2, 5). Consumed by S4, S5, S6.
3. **`strategies` table.** One row per strategy id; the inner-ring
   identity table (Invariants 3, 8). Consumed by S5, S6.
4. **`strategy_versions` table** (immutable, hash-keyed). Config
   hash = deterministic hash over the full `StrategyConfig` +
   `RiskParams` + `EvalSpec` + `ForecasterSpec` +
   `MarketSelectionSpec` projection set; re-configuring produces a
   new row, never an in-place edit (Invariants 3, 8). Consumed by
   S3, S4, S5, S6.
5. **`strategy_factors` link table.** Empty shape in S2 —
   columns `(strategy_id, strategy_version_id, factor_id, param,
   weight, direction)` declared but no rows populated until S3
   lands factor definitions. The `strategy_version_id` column is
   required so old strategy versions retain their factor wiring
   after a strategy re-config creates a new version row
   (Invariant 3 immutability). Consumed by S3, S5 (Invariants 2,
   4, 8).
6. **`PostgresStrategyRegistry`.** CRUD class over `strategies` +
   `strategy_versions` using the S1-owned `asyncpg.Pool`; typed
   methods return frozen dataclasses (matches S1's raw-SQL
   convention). Consumed by S4, S5.
7. **Import-linter rules.** Codified in `pyproject.toml` or
   `ruff.toml` per the wording of §3.2.2 row 7: `pms.sensor` and
   `pms.actuator` cannot import `pms.strategies.*` or
   `pms.controller.*`; `pms.sensor` cannot import
   `pms.market_selection`. The authoritative rule target is
   `pms.strategies.aggregate` (per
   `agent_docs/architecture-invariants.md` §§Invariants 2 + 5
   Enforcement); the `pms.strategies.projections` submodule is
   explicitly **allowed** for downstream layers (`RiskParams` is
   the canonical Actuator-boundary import). The linter config
   implements the narrower rule (aggregate forbidden, projections
   allowed) so legitimate projection imports continue to work
   (Invariants 5, 6). Enforced in CI; consumed by all sub-specs
   transitively (every PR runs the linter).
8. **`"default"` strategy + version row seed.** A single seed row
   in `strategies` and a corresponding deterministic-hash row in
   `strategy_versions` so legacy runtime writes (which predate
   per-strategy dispatch) tag `(strategy_id='default',
   strategy_version_id='default-v1')` on every inner-ring product
   row (Invariant 3, NULLABLE→seed pattern). Consumed by pre-S5
   runtime writes.
9. **`/strategies` dashboard page — registry listing view.** A
   minimal Next.js page that reads `strategies` +
   `strategy_versions` through the FastAPI layer and lists each
   registered strategy (id, active version id, created_at).
   Comparative Brier / P&L / fill-rate metrics **do not** land
   here; those are an S5 upgrade to this page (§3.2.5).

**Scope out.** The following look S2-adjacent but are owned by
other sub-specs. A PR landing any of these under
`.harness/pms-strategy-aggregate-v1/` is scope drift.

- **`MarketDiscoverySensor` / `MarketDataSensor` / outer-ring DDL /
  `PostgresMarketDataStore` / `asyncpg.Pool` lifecycle /
  `schema.sql` bootstrap / JSONL → PG migration / `/signals`
  dashboard page → S1** (§3.2.1). S2 extends `schema.sql` to add
  the aggregate tables but does not modify the outer-ring DDL S1
  already applied.
- **`factors` table / `factor_values` table / `FactorService` /
  `src/pms/factors/definitions/` module tree / rules-detector
  migration into raw factors / `StrategyConfig.factor_composition`
  body wiring / `/factors` dashboard page → S3** (§3.2.3). S2
  declares the `strategy_factors` link table as an empty shape
  only; populating it is S3 work.
- **`MarketSelector` / `SensorSubscriptionController` /
  `Strategy.select_markets(universe)` method (declaration + body +
  per-strategy tests) / Runner wiring for the boot order → S4**
  (§3.2.4). §3.2.4 row 3 states the *entire method surface* is
  S4-owned — S2 does **not** ship a `select_markets` signature
  declaration, signature stub, or docstring on the aggregate;
  the method lands wholesale with S4's active-perception
  machinery to keep the method and its first consumer in the
  same commit.
- **Per-strategy `ControllerPipeline` dispatch / per-strategy
  Evaluator aggregation / `(strategy_id, strategy_version_id)`
  `NOT NULL` DDL upgrade on inner-ring product tables /
  `TradeDecision` + `OrderState` + `FillRecord` + `EvalRecord`
  strategy-field population / `Opportunity` entity / `/strategies`
  comparative-metrics view / `/metrics` per-strategy breakdown →
  S5** (§3.2.5). S2 keeps product-table columns `NULLABLE` with
  the `"default"` seed tagging legacy writes; upgrading to
  `NOT NULL` is S5 schema-change-only work.
- **`BacktestSpec` / `ExecutionModel` / `BacktestDataset` /
  `BacktestRun` / `StrategyRun` / market-universe replay engine /
  parameter sweep / `BacktestLiveComparison` /
  `TimeAlignmentPolicy` / `SymbolNormalizationPolicy` /
  `SelectionSimilarityMetric` / `EvaluationReport` /
  `PortfolioTarget` / `/backtest` ranked view → S6** (§3.2.6).
  Strategy-parameter-sweep tooling is an S6 concern; S2 produces
  the aggregate + version hash that S6 will reference, not the
  sweep infrastructure.

### 7.3 Acceptance criteria (system-level)

Eight observable-capability bullets. Each is verifiable at the
system level — not a checklist item for an individual CP.

1. **Aggregate + projection types exist with correct shape.**
   `src/pms/strategies/aggregate.py` defines a `Strategy` class
   that owns `StrategyConfig`, `RiskParams`, `EvalSpec`,
   `ForecasterSpec`, `MarketSelectionSpec`;
   `src/pms/strategies/projections.py` declares all five as
   `@dataclass(frozen=True)` with no setters and no mutable
   containers (Invariant 2). Mypy strict is clean on both modules
   (§5.2).
2. **`strategies` + `strategy_versions` + `strategy_factors` DDL
   lands in `schema.sql` and applies cleanly.** Running `psql -f
   schema.sql` against a fresh PG 16 DB now produces the three
   aggregate tables alongside S1's outer-ring tables; zero errors
   (Invariants 3, 8). All three tables are inner-ring;
   `strategy_factors` is a link table that will carry
   `strategy_id` + `strategy_version_id` + `factor_id` + per-
   strategy weighting columns (populated by S3), which is correct
   for its inner-ring classification. The Invariant 8 check
   (§5.3 mechanical half) is that **no outer-ring or middle-ring
   table gains a `strategy_id` or `strategy_version_id` column** —
   verified by the delimited-DDL grep on the outer-ring and
   middle-ring blocks of `schema.sql`; zero matches required.
3. **Version-hash function is deterministic across processes.**
   Given identical projection inputs, the hash function in
   `src/pms/strategies/versioning.py` (or wherever the harness
   spec places it) produces identical `strategy_version_id`
   output across Python 3.13 sub-interpreters, across machines,
   and across repeated module reloads. A test locks this in
   against at least three synthetic `StrategyConfig` inputs
   (Invariant 3 immutability rationale).
4. **`PostgresStrategyRegistry` CRUD round-trips.** Inserting a
   `Strategy` aggregate via the registry, then reading it back
   through the registry's `get_by_id` + `list_versions` methods,
   produces a byte-identical projection tuple; all tests run
   against the S1-owned `db_conn` transaction-rollback fixture
   (Invariant 2).
5. **Every legacy runtime write carries `(strategy_id='default',
   strategy_version_id='default-v1')`.** A system-level
   integration test drives a signal → decision → eval_record flow
   through the pre-S5 Runner; every resulting row in any of the 4
   inner-ring product tables S1 created (`feedback`, `eval_records`,
   `orders`, `fills` per §3.2.1 rows 14 and 18) carries the
   `"default"` tag (Invariant 3 NULLABLE→seed pattern enforced; no
   implicit `NULL` strategy-tag rows). The pre-S5 Runner may
   produce zero rows in `orders` / `fills` (today's runtime emits
   `OrderState` / `FillRecord` in-memory only); any rows that do
   land must satisfy the `"default"`-tag invariant.
6. **Import-linter rules run in CI and block violations.** A PR
   that introduces `from pms.strategies.aggregate import Strategy`
   inside `src/pms/sensor/` or `src/pms/actuator/` fails CI at the
   linter stage; a PR that introduces `from pms.market_selection`
   inside `src/pms/sensor/` fails likewise. Both rule targets are
   codified; the linter report is the PR evidence (Invariants 5,
   6 — Invariant 5 is mechanically checkable per §5.3; Invariant 6
   is Mixed per §5.3 and also requires reviewer sign-off on
   Sensor strategy-agnosticism).
7. **`/strategies` page renders the seeded `"default"` row.** A
   Playwright assertion loads `/strategies`, confirms at least one
   row is present with id `"default"` and version `"default-v1"`,
   and confirms the page fetches via the FastAPI route rather than
   a hardcoded fixture.
8. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   passes with the §5.1 baseline (≥ 70 tests + 2 skipped
   integration tests gated on `PMS_RUN_INTEGRATION=1`) and
   `uv run mypy src/ tests/ --strict` is clean, on a fresh shell
   per 🟡 Fresh-clone baseline verification (§5.1, §5.2).

### 7.4 Non-goals

Explicit deferrals. An S2 harness spec that lands any of these
items is scope drift.

- **No per-strategy `ControllerPipeline` dispatch.** The existing
  single-global `ControllerPipeline` keeps running through S2;
  dispatching a pipeline per registered strategy is S5 (§3.2.5,
  Invariants 2, 5).
- **No `Strategy.select_markets(universe)` method at all** — no
  declaration, no signature stub, no docstring. Per §3.2.4 row 3
  of the main spec the entire method surface (declaration + body
  + per-strategy tests) is S4-owned and lands wholesale with the
  active-perception machinery in the same commit (§3.2.4,
  Invariant 6).
- **No factor definitions.** `src/pms/factors/definitions/`, the
  `factors` and `factor_values` tables, and `FactorService` are
  S3 (§3.2.3, Invariant 4). The `strategy_factors` link table
  lands as an empty shape only — its rows populate in S3.
- **No `NOT NULL` upgrade on inner-ring product tables.** All 4
  S1-owned inner-ring product tables (`feedback`, `eval_records`,
  `orders`, `fills` per §3.2.1 rows 14 and 18) keep `(strategy_id,
  strategy_version_id)` `NULLABLE` through S2–S4; the schema-
  change-only upgrade is S5 (§3.2.5, Invariant 3). Any future
  product-table addition introduced via §3.2 amendment would
  similarly stay `NULLABLE` until the next S5-style upgrade.
- **No parameter-sweep tooling or `BacktestSpec` / `BacktestRun`
  integration.** Strategy-parameter sweeps, backtest run
  materialisation, and N-strategy comparison reports are S6
  (§3.2.6).
- **No `StrategyBundle` / multi-strategy account grouping.**
  §3.4 defers `StrategyBundle` to post-S5; S2 treats each
  strategy as independent.

### 7.5 Dependencies

- **Upstream sub-specs.** **S1 only.** S2's single incoming DAG
  edge is S1 → S2 (§2.1); S2's Intake (§7.6) reads S1's
  Leave-behind (§6.7) concept-for-concept.
- **Upstream invariants.** Primary: **2** (Strategy as rich
  aggregate; projections), **3** (immutable version tagging),
  **5** (strategy-agnostic Sensor + Actuator boundary),
  **8** (inner-ring ownership of aggregate + product tables).
  Partial touches:
  - **4** — S2 lands the `strategy_factors` link table as an empty
    shape so S3 has the foreign-key shape to populate when factor
    definitions arrive; the raw-factors-only rule (Invariant 4
    Enforcement) is S3-owned.
  - **6** — S2 closes only the **import-boundary half** of
    Invariant 6 (the import-linter rule `pms.sensor` cannot import
    `pms.market_selection`, codified per §3.2.2 row 7 with
    Invariant column "5, 6"). The **behavioural half** of
    Invariant 6 (`MarketSelector` + `SensorSubscriptionController`
    + active-perception loop wiring) is S4-owned (§3.2.4,
    §5.3 Mixed-invariant review gate).

### 7.6 Intake

Minimum set that must exist before a harness run opens under
`.harness/pms-strategy-aggregate-v1/`. S1's Leave-behind (§6.7) is
the source of truth; the items below reference §3.2.1 rows by
number so the §4.3 gate-4 boundary-integrity check can diff this
list against S1's Leave-behind mechanically.

1. **Outer-ring DDL applied via `schema.sql`** (§3.2.1 rows 1–6 +
   row 9). `markets`, `tokens`, `book_snapshots`, `book_levels`,
   `price_changes`, `trades` all exist in a fresh PG 16 DB; S2's
   new `strategies` / `strategy_versions` / `strategy_factors`
   tables land as additions to the same `schema.sql` file, not a
   replacement (Invariant 8 — outer ring untouched by S2).
2. **`PostgresMarketDataStore` available** (§3.2.1 row 7). The
   typed-methods storage layer over the outer ring is in place;
   S2's `PostgresStrategyRegistry` follows the same shape
   (concrete class, no Protocol abstraction, returns frozen
   dataclasses).
3. **`asyncpg.Pool` lifecycle in Runner** (§3.2.1 row 8). The
   Runner-owned pool with `min_size=2` / `max_size=10` is
   created in `Runner.start()` and closed in `Runner.stop()`;
   S2's registry acquires connections from the same pool.
4. **Inner-ring product tables with `(strategy_id,
   strategy_version_id)` reserved `NULLABLE`** (§3.2.1 row 19).
   All 4 inner-ring product tables S1 creates — `feedback` and
   `eval_records` from the JSONL→PG migration (§3.2.1 row 14),
   plus `orders` and `fills` as empty shells (§3.2.1 row 18,
   added per `agent_docs/architecture-invariants.md` §Invariant 8
   enumeration) — carry both columns `NULLABLE`; S2 seeds
   `"default"` to populate the columns on every existing row
   without requiring a schema change. S2 does not extend the
   schema for `orders` / `fills` — S1 has already landed the
   shells.
5. **`/signals` page rendering real depth from outer-ring
   tables** (§3.2.1 row 17). The Playwright e2e confirming real
   depth is green on main; S2's `/strategies` page is additive
   and must not regress this.
6. **Transaction-rollback `db_conn` fixture** (§3.2.1 row 15).
   Every S2 storage-layer test uses this fixture; no test-side
   schema work is required.

The §5.1 / §5.2 **canonical gates green on a fresh clone** is a
cross-spec gate (per §4.3 gate 5 and §§5.1, 5.2; 🟡 Fresh-clone
baseline verification) that every sub-spec's harness run must
satisfy before opening — it is **not** an S1 Leave-behind concept
and therefore not an Intake line item. The §4.3 gate-4 diff
mechanism only compares S1's §6.7 Leave-behind rows against this
§7.6 Intake list; the baseline gates are verified separately under
§4.3 gate 5 and enumerated again in the §7.11 Kickoff Prompt
preflight.

### 7.7 Leave-behind

Enumerated artefacts produced by S2, keyed back to §3.2.2 row
numbers so the §4.3 gate-4 boundary-integrity check can diff S3's
and S4's Intake subsections (once §§8.6, 9.6 land) against this
list.

1. **`Strategy` aggregate class** (§3.2.2 row 1) at
   `src/pms/strategies/aggregate.py` — DDD-style aggregate with no
   setters, no mutable containers, and no direct persistence hooks
   (persistence goes through the registry).
2. **Five frozen projection dataclasses** (§3.2.2 row 2) at
   `src/pms/strategies/projections.py`: `StrategyConfig`,
   `RiskParams`, `EvalSpec`, `ForecasterSpec`,
   `MarketSelectionSpec`. All `@dataclass(frozen=True)`, mypy
   strict clean.
3. **`strategies` table** (§3.2.2 row 3) populated with at least
   the `"default"` seed row.
4. **`strategy_versions` table** (§3.2.2 row 4) populated with
   at least the `"default-v1"` seed row; the deterministic-hash
   function used to compute `strategy_version_id` is exported and
   reusable from tests.
5. **`strategy_factors` link table** (§3.2.2 row 5) declared as
   an empty shape with columns `(strategy_id,
   strategy_version_id, factor_id, param, weight, direction)`; S3
   populates rows as factor definitions land.
6. **`PostgresStrategyRegistry`** (§3.2.2 row 6) at
   `src/pms/storage/strategy_registry.py` (harness spec may pick
   the final path) with typed CRUD methods
   (`create_strategy`, `create_version`, `get_by_id`,
   `list_strategies`, `list_versions`) acquiring from the S1
   pool.
7. **Import-linter rules in CI** (§3.2.2 row 7) codified in
   `pyproject.toml` or `ruff.toml`. The narrowed rule set,
   resolving the §3.2.2 row 7 wildcard against the
   `agent_docs/architecture-invariants.md` §§Invariants 2 + 5
   Enforcement text:
   - `pms.sensor` cannot import from `pms.strategies.aggregate`
     or `pms.controller.*` (Invariant 5);
   - `pms.actuator` cannot import from `pms.strategies.aggregate`
     or `pms.controller.*` (Invariant 5);
   - `pms.sensor` cannot import from `pms.market_selection`
     (Invariant 6 import-boundary half).
   - `pms.strategies.projections` is **explicitly allowed** for
     both `pms.sensor` and `pms.actuator` (Invariant 2 / 5
     Enforcement: "they may import from
     `pms.strategies.projections`").
   A PR violating any of the four bans above fails CI at the
   linter stage.
8. **`"default"` strategy + version seed** (§3.2.2 row 8) applied
   on Runner boot alongside `schema.sql` (either as a trailing
   `INSERT ... ON CONFLICT DO NOTHING` in the same file or as a
   one-shot boot hook — harness spec decides); every legacy
   runtime write from S2 onward carries `"default"` tags.
9. **`/strategies` listing page** (§3.2.2 row 9) under
   `dashboard/app/strategies/` with a matching FastAPI route under
   `src/pms/api/strategies.py` (or the current API convention);
   Playwright e2e asserts the `"default"` row renders.

S3's Intake (authored in Commit 6 of this document authoring
effort) and S4's Intake (Commit 7) both read from this list;
diffing the two is the §4.3 gate-4 mechanism.

### 7.8 Checkpoint skeleton

Flat one-line-each list. **Not harness acceptance criteria** — the
per-CP acceptance criteria, files-of-interest, and effort
estimates land in `.harness/pms-strategy-aggregate-v1/spec.md`
when the kickoff prompt (§7.11) triggers that authoring session.

- **CP1 — Strategy aggregate + projection types.** Frozen
  dataclasses for the five projections under
  `src/pms/strategies/projections.py`; `Strategy` aggregate class
  under `src/pms/strategies/aggregate.py` with no setters, no
  mutable containers; mypy strict clean (Invariant 2).
- **CP2 — `strategies` + `strategy_versions` tables DDL +
  version-hash function.** Both tables added to `schema.sql`;
  deterministic hash function exported from
  `src/pms/strategies/versioning.py`; hash-stability tests lock
  in byte-identical output across repeated calls (Invariants 3,
  8).
- **CP3 — `strategy_factors` link table (empty shape).** DDL
  added to `schema.sql` with columns `(strategy_id,
  strategy_version_id, factor_id, param, weight, direction)`;
  no rows populated; foreign-key columns declared so S3 can
  insert definitions without a schema change (Invariants 2, 4, 8).
- **CP4 — `PostgresStrategyRegistry` CRUD.** Concrete class with
  typed methods acquiring from the S1 pool; unit-tested against
  the `db_conn` fixture; frozen-dataclass returns match the
  projection types (Invariant 2).
- **CP5 — Import-linter rules in CI.** Rules codified in
  `pyproject.toml` or `ruff.toml`; CI runs the linter as part of
  the lint pass; at least one test-fixture PR (or a unit-level
  equivalent) demonstrates the rule blocks a known violation
  (Invariants 5, 6).
- **CP6 — `"default"` strategy + version seed + legacy runtime
  tagging.** Seed applied on Runner boot (trailing
  `INSERT ... ON CONFLICT DO NOTHING` or equivalent boot hook);
  legacy Runner write paths (`FeedbackStore`, `EvalStore`, and
  any inner-ring writers S1 shipped) tag `"default"` on every
  row; integration test confirms no `NULL` tag slips through
  (Invariant 3 NULLABLE→seed pattern).
- **CP7 — `/strategies` listing page.** Next.js page under
  `dashboard/app/strategies/` + FastAPI route reading the
  registry; Playwright e2e asserts the `"default"` row renders;
  no per-strategy metrics yet (that upgrade is S5).

### 7.9 Effort estimate

**M** (5–8 CPs; new aggregate + registry + import-linter rules).
Seven CPs per §7.8. S2's scope is narrower than S1's L (10 CPs
including schema foundation, two sensors, and a dashboard
rewrite): S2 adds a bounded surface area — one Python module for
the aggregate, one for projections, one for versioning, one for
the registry, three aggregate-ring DDL blocks appended to the
existing `schema.sql`, a lint-rule block in `pyproject.toml`, a
seed row, and a minimal listing page. No new persistence backend,
no new streaming adapter, no WebSocket state machine. The
import-linter rule set is the one piece with cross-cutting
impact, but it is additive (rules fail closed — no existing
import is allowed to violate them, so the lint pass stays green
or an existing violation is fixed in the same PR).

### 7.10 Risk register

| Risk | Likelihood | Impact | Mitigation | Trigger / early-warning |
|---|---|---|---|---|
| Strategy config-hash instability across Python versions / runs (e.g. `dict` iteration order, `hash()` randomization, `dataclasses.asdict` field ordering changes) | M | H | Hash over a **canonical serialisation** — `json.dumps(..., sort_keys=True, separators=(",", ":"))` of a fully-sorted projection tuple — not over `repr()` or `hash()`; hash-stability test exercises at least three synthetic configs across repeated process launches; document the canonicalisation choice in `src/pms/strategies/versioning.py` docstring (Invariant 3 rationale) | Hash-stability test fails between two CI runs of the same commit; historical `strategy_versions` rows re-hash to different ids after a Python minor-version bump |
| Import-linter false positives blocking legitimate imports (e.g. typing-only imports under `TYPE_CHECKING`, test-fixture imports that legitimately touch the aggregate) | M | M | Configure `TYPE_CHECKING`-guarded imports as an explicit exemption in the linter config; scope the rule to `src/pms/sensor/` and `src/pms/actuator/` source trees only (not `tests/pms/sensor/`); document the exemption rationale in the linter config with a comment referencing Invariants 5, 6 | Contributor opens an issue citing a rejected PR where the import is type-only; CI false-red on a PR that an independent code review confirms does not actually violate the invariant |
| `"default"` strategy seed causing latent Invariant 3 violations when production strategies arrive in S5 (e.g. `UPDATE` queries that silently change `strategy_id` from `"default"` to a real id, or aggregate queries that double-count `"default"` rows as a real strategy) | L | H | S5's `NOT NULL` upgrade includes a one-off migration that retags legacy `"default"` rows to the appropriate real strategy id **or** archives them to a `legacy_default_records` table; §5.3 Invariant 3 review-gate enforcement (no aggregation query over `eval_records` / `fills` may omit `GROUP BY strategy_version_id` without an explicit justifying comment) catches query-level violations during S5 code review | S5 code review surfaces an aggregate query that silently groups `"default"` with a real strategy; a dashboard metric shows anomalous sample counts traceable to a mixed `"default"` + real-strategy group |
| Projection-type serialization gap if a dashboard or API client deserializes `StrategyConfig` JSONB (e.g. `strategy_versions.config_json` as a client-visible blob) and silently drops frozen-dataclass invariants (field order, typed enums) | L | M | Projection types declare a `from_json(cls, data: dict) -> Self` class method that validates field presence and type (e.g. via `pydantic.TypeAdapter` or a hand-rolled validator); the FastAPI route reading `strategy_versions` goes through this class method, not a naive `dict(row)` dump; mypy strict on the API layer catches type drift at boundary | API integration test surfaces a `StrategyConfig` round-trip mismatch; a dashboard field renders as `null` when the backend row has a value |
| Aggregate-to-projection conversion overhead if called per-signal in the hot controller path (pre-emptive perf concern for S5 — but the shape of the conversion is S2-owned) | L | M | Projections are designed to be **cached on the aggregate** — `Strategy.config` / `Strategy.risk_params` / `Strategy.eval_spec` / `Strategy.forecaster_spec` / `Strategy.market_selection_spec` are computed-once properties that return cached frozen dataclasses; the aggregate class exposes a `snapshot()` method returning all five projections in one call so S5's per-signal controller loop reuses the tuple; document the caching contract in the aggregate-class docstring | S5 profiling during per-strategy dispatch rollout shows projection-conversion time dominating the per-signal latency budget |

### 7.11 Kickoff Prompt

The block below is **copy-paste-ready** for a fresh Claude session
whose job is to author `.harness/pms-strategy-aggregate-v1/spec.md`
— the harness-executable spec with per-CP acceptance criteria,
files-of-interest, effort, and intra-spec dependencies. The future
session has **no memory** of this document; the prompt is
self-contained.

```
You are starting harness task `pms-strategy-aggregate-v1` in the
prediction-market-system repo.

REPO ROOT: /Users/stometa/dev/prediction-market-system
OUTPUT FILE: /Users/stometa/dev/prediction-market-system/.harness/pms-strategy-aggregate-v1/spec.md

REQUIRED READING (ordered, absolute paths — read in this order
before touching anything):

1. /Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md
   — specifically §7 (this sub-spec's total-spec contract). Also
   §3.2.2 Boundary Matrix rows (your complete Scope-in set),
   §3.2.1 (S1's Leave-behind, which is your Intake), §4.2 (S1 →
   S2 execution rationale), and §5 (cross-spec acceptance gates).
   If the absolute path above does not yet exist on `main` (i.e.
   the project-decomposition spec has not yet merged), read the
   branch-stable copy via:
       git show docs/pms-project-decomposition:docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md
   and HALT before writing any spec content — the project-level
   spec must be merged before S2's harness run opens (it is S1's
   §6.11 reading prerequisite as well).
2. /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
   — focus on Invariants 2 (Strategy as rich aggregate;
   projections), 3 (immutable version tagging), 5 (strategy-
   agnostic Sensor + Actuator boundary), and 8 (onion-concentric
   storage; inner ring). Partial touch on Invariant 4 via the
   empty-shape `strategy_factors` link table.
3. /Users/stometa/dev/prediction-market-system/agent_docs/promoted-rules.md
   — especially Runtime behaviour > design intent (🔴),
   Review-loop rejection discipline (🔴), Fresh-clone baseline
   verification (🟡), and Commit-message precedence (no
   Co-Authored-By).
4. /Users/stometa/dev/prediction-market-system/docs/notes/2026-04-16-evaluator-entity-abstraction.md
   — the primary source document for S2's entity design. Load-
   bearing sections: "Strategy Entities" block (StrategyDefinition,
   StrategyConfig, StrategyVersion, StrategySelection, Opportunity,
   PortfolioTarget), "Proposed Abstraction Direction" §§1–3
   (separate stages from entities, Factor as first Controller
   primitive, Strategy as first user-facing primitive).
5. /Users/stometa/dev/prediction-market-system/src/pms/controller/CLAUDE.md
   — per-layer invariant enforcement for Controller; S2's
   `Strategy` aggregate becomes what Controller reads post-S5.
6. /Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
   — structural reference for harness-grade spec shape (CP shape,
   acceptance-criteria shape, files-of-interest shape).

CURRENT STATE SNAPSHOT:

S1 is complete; its Leave-behind (§6.7 of the project-level spec)
is your Intake (§7.6). The outer-ring DDL is applied,
`PostgresMarketDataStore` exists, the `asyncpg.Pool` is Runner-
owned, all 4 inner-ring product tables (`feedback`, `eval_records`,
`orders`, `fills`) carry `(strategy_id, strategy_version_id)`
columns reserved `NULLABLE`, `/signals` renders real orderbook
depth, and the canonical gates are green on a fresh clone. Inner-
ring product-table columns are still `NULLABLE` — S5 will upgrade
them to `NOT NULL`; your job is the aggregate, registry, linter,
and seed work that makes the `NULLABLE` columns carry `"default"`
instead of `NULL`.

PREFLIGHT (boundary check — run before any authoring):

- §7.6 Intake items must all be satisfied. If any fails, HALT and
  tell the user:
  * Outer-ring DDL is applied in a fresh PG 16 DB via `schema.sql`
    (S1 §3.2.1 rows 1–6 + row 9).
  * `PostgresMarketDataStore` exists at the path S1 shipped it to
    (S1 §3.2.1 row 7).
  * `Runner.start()` creates the `asyncpg.Pool`; `Runner.stop()`
    closes it (S1 §3.2.1 row 8; `min_size=2` / `max_size=10`).
  * All 4 inner-ring product tables (`feedback`, `eval_records`,
    `orders`, `fills`) carry `(strategy_id, strategy_version_id)`
    `NULLABLE` columns (S1 §3.2.1 rows 18 + 19).
  * `/signals` renders real depth end-to-end; Playwright e2e green
    on main.
  * `db_conn` transaction-rollback fixture is importable from
    tests (S1 §3.2.1 row 15).
  * `uv run pytest -q` passes with ≥ 70 tests + 2 skipped
    (PMS_RUN_INTEGRATION=1 gate), and
    `uv run mypy src/ tests/ --strict` is clean, on a fresh shell
    (§5.1, §5.2; 🟡 Fresh-clone baseline verification).

TASK:

Create `.harness/pms-strategy-aggregate-v1/spec.md` by expanding
§7.8 (Checkpoint skeleton) into harness-grade CPs with:

- Per-CP acceptance criteria (observable, falsifiable, not
  implementation notes).
- Per-CP files-of-interest (absolute paths under
  /Users/stometa/dev/prediction-market-system/).
- Per-CP effort estimate (S / M / L).
- Intra-spec CP dependencies (which CPs block which).

Use /Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
as the structural reference for shape. Draft only — stop and wait
for spec-evaluation approval before running any checkpoint.

CONSTRAINTS:

- New feature branch `feat/pms-strategy-aggregate-v1` off `main`.
  Never commit to `main` directly.
- Respect §3 Boundary Matrix of the project-level spec. Never
  claim a concept owned by another sub-spec (S1, S3, S4, S5, S6).
  The 9 concepts enumerated in §7.2 Scope-in are the complete
  authorised set for S2; anything else is scope drift.
- Every Strategy / Factor / Sensor / ring-ownership claim must
  cite an invariant number from
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md.
- No `Co-Authored-By` lines in any commit
  (/Users/stometa/dev/prediction-market-system/CLAUDE.md §"Do
  not"; promoted rule "Commit-message precedence" in
  /Users/stometa/dev/prediction-market-system/agent_docs/promoted-rules.md).
- Conventional-commit prefixes required (§5.6 of the project
  spec): `feat(<scope>):`, `fix(<scope>):`, `docs(<scope>):`,
  `test(<scope>):`, `refactor(<scope>):`, `chore(<scope>):`.
- Projection types are `@dataclass(frozen=True)` — no setters, no
  mutable containers (Invariant 2 anti-patterns).

HALT CONDITIONS:

- Any invariant in
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
  cannot be satisfied by the design you are authoring. Do NOT
  silently amend the invariant. Open a retro under
  /Users/stometa/dev/prediction-market-system/.harness/retro/ and
  return to the user.
- Any attempt to violate the S2 import-boundary rules named in
  §7.7 item 7:
  - `pms.sensor` or `pms.actuator` importing from
    `pms.strategies.aggregate` (Invariant 5 violation);
  - `pms.sensor` or `pms.actuator` importing from
    `pms.controller.*` (Invariant 5 violation);
  - `pms.sensor` importing from `pms.market_selection`
    (Invariant 6 import-boundary violation).
  Note: imports of `pms.strategies.projections` are explicitly
  **allowed** for both `pms.sensor` and `pms.actuator` per
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
  §§Invariants 2 + 5 Enforcement. STOP immediately on any of the
  three bans above.
- Any attempt to add a `strategy_id` or `strategy_version_id`
  column to an outer-ring or middle-ring table (`markets`,
  `tokens`, `book_snapshots`, `book_levels`, `price_changes`,
  `trades`, `factors`, `factor_values`). STOP immediately; this
  is an Invariant 8 violation.
- The 9 concepts you author CPs for do not match §3.2.2 of the
  project-level spec, concept-for-concept. Reconcile first.

FIRST ACTION:

Run:

    git status && git branch --show-current && git log --oneline -5

Then read the 6 files in the REQUIRED READING block, in order,
before drafting any content. After reading, report your
understanding of §7.8's 7 checkpoints (CP1–CP7) and wait for
go-ahead before drafting
`.harness/pms-strategy-aggregate-v1/spec.md`.
```

---

## 8. Sub-spec S3 — pms-factor-panel-v1

S3 is the **middle-ring sub-spec** of the DAG (§2.1): it depends on
S2 (for the `strategy_factors` link table and the `StrategyConfig`
projection shape) and feeds S5 + S6 with persisted factor values.
Per the canonical execution order (§4.1, §4.2), S3 lands **before**
S4 because it is an additive middle-ring capability (new tables, new
service, no Sensor behaviour change) while S4 is a runtime side
effect with larger blast radius. This section expands the skeleton
in `agent_docs/project-roadmap.md` §S3 into the project-level
contract that the harness run under `.harness/pms-factor-panel-v1/`
will consume.

### 8.1 Goal

S3 populates the **middle ring of the onion** (Invariant 8) with
raw `FactorDefinition` modules under `src/pms/factors/definitions/`,
`factors` + `factor_values` tables keyed per §3.2.3 row 3 —
`(factor_id, market_id, ts, value)` — with no `strategy_id` column
per Invariant 8, a `FactorService` that reads the outer ring and
persists computed factor values on a cadence, a `/factors` dashboard
page that renders factor evolution over time (Invariant 4
visibility), and the migration of today's `RulesForecaster` +
`StatisticalForecaster` detection heuristics
(`src/pms/controller/forecasters/rules.py`,
`src/pms/controller/forecasters/statistical.py`) into raw factor
definitions — with composition logic moving to
`StrategyConfig.factor_composition` (a projection field owned here
but *consumed* by S5 per §3.2.3 row 6) rather than into the `factors`
registry (Invariant 4). The middle ring is the *strategy-agnostic
cache* of derived signals; per Invariant 8 it is shared across every
strategy and every backtest, paying for itself the first time two
strategies consume `orderbook_imbalance`.

### 8.2 Scope (in / out)

**Scope in.** Exactly the **7 concepts** owned by S3 in §3.2.3, in
the same order, each with a one-line scope descriptor. Reviewers
verifying boundary integrity grep §3.2.3 against this list; the
lists must match concept-for-concept (§3.1 ownership rule).

1. **`src/pms/factors/definitions/` module tree (one file per raw
   factor).** A `FactorDefinition` abstract base class plus one
   module per raw factor (e.g. `orderbook_imbalance.py`,
   `fair_value_spread.py`, `subset_pricing_violation.py`,
   `metaculus_prior.py`, `yes_count.py`, `no_count.py`). Raw factors
   only — **composite logic is forbidden here** and lives in
   `StrategyConfig.factor_composition` (Invariant 4).
2. **`factors` table (one row per factor definition).** DDL landing
   in an S3-authored section of `schema.sql`; one row per registered
   `FactorDefinition` with `factor_id`, human-readable name,
   description, input schema hash, default parameters, output type,
   direction semantics, and owner. **No `factor_type` column
   distinguishing raw / composite** — the distinction is enforced by
   convention + code review, not schema (§8.4 non-goal,
   Invariant 4).
3. **`factor_values` table
   (`factor_id, market_id, ts, value`).** DDL landing in the same
   S3 block, matching §3.2.3 row 3 verbatim. **No `strategy_id`
   column and no `strategy_version_id` column** (Invariant 8 — the
   middle ring is strategy-agnostic; §5.3 Mixed-invariant grep
   verifies both identifiers produce zero matches inside the
   middle-ring DDL block comments). The `param` field noted on the
   `FactorValue` entity in `docs/notes/2026-04-16-evaluator-entity-
   abstraction.md` (§Factor Entities) is a candidate index column
   the harness spec resolves in CP1; if CP1's DDL review adopts
   `param`, §3.2.3 is amended in the same PR per §3.1 "When adding
   a new concept not in the matrix".
4. **`FactorService` (compute + persist).** Class under
   `src/pms/factors/service.py` (harness spec picks the final path)
   that reads outer-ring tables via the S1
   `PostgresMarketDataStore`, invokes each registered
   `FactorDefinition.compute(...)`, and writes `factor_values` rows
   on a configured cadence (Invariants 4, 8). `FactorService` is
   **strategy-agnostic**: it never imports `pms.strategies.*`; it
   iterates the market universe under active subscription (§8.6
   Intake) and computes every registered factor for every market
   (Invariant 5 partial — Factor layer stays strategy-agnostic).
5. **Migration of existing rules-detector heuristics into raw
   `FactorDefinition`s.** Today's `RulesForecaster`
   (`src/pms/controller/forecasters/rules.py`) and
   `StatisticalForecaster`
   (`src/pms/controller/forecasters/statistical.py`) carry
   detection + composition in one class. S3 splits the two: the
   **detection half** (arithmetic over observables such as
   `fair_value − yes_price`, `subset_price − superset_price`, and
   the raw `metaculus_prob` / `yes_count` / `no_count` inputs)
   becomes raw `FactorDefinition` modules; the **composition half**
   (picking between spreads, blending prior strength with observed
   counts, averaging probabilities) moves to
   `StrategyConfig.factor_composition` and is consumed by the S5
   per-strategy `ControllerPipeline`. After migration, the two
   forecaster files contain **no detection logic** — only a thin
   reference to the composition layer (Invariant 4).
6. **`StrategyConfig.factor_composition` field (per-strategy
   composition logic, JSONB).** A typed projection field added to
   the S2 `StrategyConfig` projection (`@dataclass(frozen=True)`),
   holding strategy-specific factor-composition rules as
   well-typed nested Python structures serialised to JSONB. For the
   S2-seeded `"default"` strategy, the field is populated with a
   composition equivalent to today's forecaster behaviour so the
   post-migration runtime produces the same decisions. Composition
   is strategy-scoped, so it **lives on the projection, not in
   `factors`** (Invariants 2, 4). The field is *owned* by S3 (§3.2.3
   row 6) but *consumed* by S5 — S5 reads it to drive per-strategy
   Controller dispatch.
7. **`/factors` dashboard page.** Next.js page under `dashboard/app/`
   that renders a multi-line chart of factor value over time for a
   user-selected `(factor_id, market_id)` pair (extending to
   `(factor_id, param, market_id)` if CP1 adopts the optional
   `param` column, see scope-in row 3), reading from `factor_values`
   via a new `dashboard/app/api/pms/factors/` route. Provides
   Invariant 4 visibility ("raw factor values are the load-bearing
   unit; show them directly").

**Scope out.** The following look S3-adjacent but are owned by other
sub-specs. A PR landing any of these under
`.harness/pms-factor-panel-v1/` is scope drift (§3.1 "When reviewing
a sub-spec PR").

- `Strategy` aggregate, projection types, `strategies` /
  `strategy_versions` tables, `strategy_factors` link table DDL,
  `PostgresStrategyRegistry`, import-linter rules, `"default"`
  strategy seed → **S2** (§3.2.2). S3 *extends* the S2
  `StrategyConfig` projection with one new field
  (`factor_composition`, row 6 above) and *populates* the empty
  `strategy_factors` table as raw `FactorDefinition` modules land
  (the table's DDL ships with S2 in empty shape; S3 inserts one
  row per `(strategy_id, factor_id)` pairing per §3.2.2 row 5
  Notes: "Empty shape in S2; S3 populates as factor definitions
  land"). Ownership of the table stays with S2; S3 is the
  authorised consumer-side writer (§3.1).
- `MarketSelector`, `SensorSubscriptionController`,
  `Strategy.select_markets(universe)` method, Runner boot-order
  wiring → **S4** (§3.2.4). S3's `FactorService` reads from outer-
  ring tables populated by the S1 `MarketDataSensor` against
  whatever subscription is active; it does not drive the
  subscription.
- Per-strategy `ControllerPipeline` dispatch, per-strategy
  `Evaluator` aggregation, `(strategy_id, strategy_version_id)`
  `NOT NULL` upgrade, end-to-end strategy-field population on
  `TradeDecision` / `OrderState` / `FillRecord` / `EvalRecord`,
  `Opportunity` entity, `/strategies` comparative view, `/metrics`
  per-strategy breakdown → **S5** (§3.2.5). S5 is the **consumer**
  of the middle-ring factor values S3 persists — S3 ends at "factor
  values land in the table on cadence"; turning those values into
  per-strategy decisions is S5's job.
- `FactorAttribution` as a persisted Evaluator artifact with its
  own DDL → **deferred** per §3.4 "Entities deliberately out of
  scope". S6's `EvaluationReport` may carry attribution commentary,
  but a dedicated attribution table is not landed in S3.
- `BacktestSpec` / `ExecutionModel` / `BacktestDataset` /
  `BacktestRun` / `StrategyRun` / parameter sweep / market-universe
  replay engine / `/backtest` ranked view → **S6** (§3.2.6). S3's
  `FactorService` computes factor values against the live
  subscription; S6 owns the replay engine that drives precomputed
  factor panels over a historical window.

### 8.3 Acceptance criteria (system-level)

Seven observable-capability bullets. Each is verifiable at the
system level — not a checklist item for an individual CP.

1. **Factor DDL applies cleanly inside the middle-ring block of
   `schema.sql`.** Running `schema.sql` against a fresh PostgreSQL
   16 database produces `factors` and `factor_values` tables inside
   a delimited middle-ring block (the same delimited-comment
   convention §5.3 Invariant-8 evidence depends on). The
   `factor_values` table is keyed per §3.2.3 row 3 verbatim —
   `(factor_id, market_id, ts, value)` — with neither `strategy_id`
   nor `strategy_version_id` as a column (Invariants 4, 8). A
   harness-spec CP1 review may add a `param` index column per the
   `FactorValue` entity definition in
   `docs/notes/2026-04-16-evaluator-entity-abstraction.md`; if it
   does, §3.2.3 is amended in the same PR (§3.1).
2. **`FactorService` writes values on cadence for every subscribed
   market when factor inputs are present.** For every raw factor
   registered in `src/pms/factors/definitions/`, `FactorService`
   writes a `factor_values` row per market within the configured
   cadence window **whenever the factor's required inputs are
   present** (e.g. `fair_value` for `fair_value_spread` per
   `src/pms/controller/forecasters/rules.py:25`; subset/superset
   for `subset_pricing_violation` per `rules.py:40`;
   `metaculus_prob` for `metaculus_prior` per `statistical.py:20`).
   When required inputs are absent, the corresponding row is
   skipped (sparse panel) — `factor_values` does not encode a
   missing-value sentinel. The cadence is observable as a steady
   insert rate on the table modulo input availability (Invariant
   4). If S5 or the harness-spec CP1 review requires dense panels
   (e.g. via NULL-value rows or a separate "computed-but-absent"
   marker), the change is scoped through §3.1 and lands as an
   amendment to AC2, §8.2 row 3, AC3, §8.7 Leave-behind row 3, and
   the §8.11 kickoff prompt in the same PR.
3. **`/factors` page renders a multi-line chart of factor value over
   time for a selected market.** A Playwright e2e test selects a
   `(factor_id, market_id)` pair (or `(factor_id, param, market_id)`
   if CP1 adopts the optional `param` column) that `FactorService`
   has populated and asserts that the chart renders at least two
   distinct timestamps with monotonically-increasing `ts` values
   (Invariant 4 visibility).
4. **Middle-ring tables carry no strategy tag.** A delimited-DDL
   grep inside the middle-ring block of `schema.sql` returns **zero
   matches** for both `strategy_id` and `strategy_version_id`
   (Invariant 8 mechanical evidence per §5.3). Reviewer sign-off on
   ring declaration is the second half of §5.3 Invariant-8 Mixed
   evidence.
5. **Every existing rules-detector heuristic is expressed as a raw
   `FactorDefinition`.** A grep against the post-migration
   `src/pms/controller/forecasters/rules.py` and
   `src/pms/controller/forecasters/statistical.py` confirms **no
   detection arithmetic remains** — the files contain only a thin
   reference to the composition layer reading from
   `StrategyConfig.factor_composition`. Every former heuristic is
   findable as a `FactorDefinition` module under
   `src/pms/factors/definitions/`. A **regression matrix** (not a
   single canonical input) locks in that the post-migration
   forecaster output equals the pre-migration output modulo float
   tolerance across **every piecewise regime** of today's
   forecasters, per the promoted rule 🟡 Piecewise-domain functions:
   - `RulesForecaster._price_spread`
     (`src/pms/controller/forecasters/rules.py:24-37`): edge
     **below**, **at**, and **above** `min_edge`, plus the absent
     `fair_value` path.
   - `RulesForecaster._subset_violation`
     (`src/pms/controller/forecasters/rules.py:39-56`): violation
     **below**, **at**, and **above** `min_edge`, plus the absent
     subset/superset path.
   - `RulesForecaster.predict` precedence
     (`src/pms/controller/forecasters/rules.py:14-18`): both rules
     present (locks `_price_spread` precedence), only spread
     present, only subset present, neither present (locks
     `predict()` returning `None`).
   - `RulesForecaster.forecast` `None`-fallback
     (`src/pms/controller/forecasters/rules.py:20-22`): when
     `predict()` returns `None`, `forecast()` returns
     `signal.yes_price`; when `predict()` returns a non-None tuple,
     `forecast()` returns its first element. Both branches
     exercised — these are two distinct break points across
     `predict()` and `forecast()` and the regression matrix locks
     in **both** boundaries to preserve runtime equivalence.
   - `StatisticalForecaster.predict`
     (`src/pms/controller/forecasters/statistical.py:19-43`):
     `metaculus_prob` **absent** vs **present**; `yes_count` /
     `no_count` at **zero** vs **non-zero** under the seeded
     `prior_strength`. Every break-point straddle is exercised
     (Comments are not fixes, 🟡 — migration MUST preserve runtime
     behaviour equivalently, not silently drop a rule).
6. **`StrategyConfig.factor_composition` field exists on the
   projection and is populated for the `"default"` strategy.** The
   S2 `StrategyConfig` `@dataclass(frozen=True)` carries a
   well-typed `factor_composition` field; the `"default"` strategy
   row in `strategies` / `strategy_versions` is re-hashed with the
   composition blob populated to reproduce today's forecaster
   behaviour. No other strategy carries a populated composition
   blob in S3 (Invariants 2, 4).
7. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   passes with ≥ 70 tests + 2 skipped (integration gated on
   `PMS_RUN_INTEGRATION=1`) and `uv run mypy src/ tests/ --strict`
   is clean, on a fresh shell (§5.1, §5.2; 🟡 Fresh-clone baseline
   verification).

### 8.4 Non-goals

Explicit deferrals. An S3 harness spec that lands any of these items
is scope drift — most of them are **direct Invariant 4 violations**
and must be rejected at spec-evaluation time.

- **No `factor_type ENUM('raw','composite')` column on `factors`.**
  This is the canonical Invariant 4 anti-pattern named in
  `agent_docs/architecture-invariants.md` §Invariant 4
  **Anti-patterns**. Raw-only is enforced by convention + code
  review; the schema must not encode a distinction that should not
  exist.
- **No composite-factor DSL persisted in the database.** A string
  expression like `"a * b + c"` stored as a column on `factors` is
  the second canonical Invariant 4 anti-pattern. Composition
  happens in strategy code reading `StrategyConfig.factor_composition`
  as typed Python — not by evaluating a database string at runtime.
- **No `composite_factors` table.** A separate table for composite
  factors *is* a `factor_type` column re-expressed as table
  cardinality; same Invariant 4 violation.
- **No per-strategy weighting in `factor_values`.** A
  `factor_values` row must never encode a weight derived from a
  specific strategy's config. Per-strategy weighting belongs in
  `StrategyConfig.factor_composition` and is applied *at read time*
  by S5's per-strategy `ControllerPipeline`, not *at write time* by
  `FactorService` (Invariant 4 Anti-patterns; Invariant 8
  Anti-patterns).
- **No `FactorAttribution` Evaluator artifact with its own DDL.**
  Deferred per §3.4; S6's `EvaluationReport` may carry attribution
  commentary once the research workflow proves the need.
- **No dynamic factor loading from a runtime config file.** S3
  starts with an explicit Python registry — each raw factor is a
  module, import-registered at `FactorService` construction time.
  Runtime-config-driven factor registration is a follow-up once
  factor churn makes module-add friction visible.
- **No factor-cache staleness framework.** `FactorService` writes
  on a configured cadence; downstream consumers (S5) read the most
  recent row per `(factor_id, market_id)` (or per
  `(factor_id, param, market_id)` if the optional `param` column
  lands at CP1). A dedicated staleness / invalidation framework is
  deferred — if S5 or S6
  discovers stale reads are a problem, open a retro and scope the
  framework as a follow-on sub-spec.

### 8.5 Dependencies

- **Upstream sub-specs.** **S2** (`pms-strategy-aggregate-v1`). S3
  needs the S2 `strategy_factors` link table (empty shape per
  §3.2.2 row 5 — S3 inserts rows as `FactorDefinition` modules
  land) and the S2 `StrategyConfig` projection (to add the
  `factor_composition` field per §3.2.3 row 6). S3 does **not**
  depend on S4 — per §2.4 and §4.2 the S3/S4 ordering is
  discretionary; canonical order puts S3 first because it is
  additive.
- **Upstream invariants.** Primary: **4** (raw factors only) and
  **8** (onion-concentric storage — middle ring). Partial touches:
  **2** (`StrategyConfig.factor_composition` lives on the S2
  projection, keeping composition strategy-scoped on the aggregate
  side of the boundary); **5** (`FactorService` stays
  strategy-agnostic — Controller consumes factor values, Factor
  layer never reads `pms.strategies.*`). Invariants 1, 3, 6, 7 are
  unrelated to S3's scope.

### 8.6 Intake

Minimum set that must exist before a harness run opens under
`.harness/pms-factor-panel-v1/`. Derived from **§2.1 predecessor
edges**: S3's only predecessor is S2 (the S3 edge carries the
`strategy_factors` link table + `StrategyConfig` projection shape),
and S2 transitively depends on S1's outer ring that `FactorService`
reads.

1. **§3.2.1 outer ring is populated and queryable.** S1's
   `PostgresMarketDataStore` exposes typed read methods over
   `markets` / `tokens` / `book_snapshots` / `book_levels` /
   `price_changes` / `trades`; `FactorService` reads these tables
   to compute raw factor values (Boundary Matrix §3.2.1 rows 1–7).
   Without populated outer-ring tables, `FactorService` has nothing
   to compute from.
2. **§3.2.2 inner-ring aggregate tables exist.** S2's
   `strategies`, `strategy_versions`, and `strategy_factors`
   tables have their DDL applied; `strategies` and
   `strategy_versions` are populated (the `"default"` strategy row
   is seeded); `strategy_factors` ships in **empty shape** per
   §3.2.2 row 5 Notes — S3 is the authorised consumer-side writer
   that populates it as `FactorDefinition` modules land.
   `PostgresStrategyRegistry` is usable. S3's CP5 (populate
   `StrategyConfig.factor_composition` for `"default"`) writes a
   new `strategy_versions` row re-hashing the `"default"` config
   with the composition blob — this requires the registry to be
   operational (Boundary Matrix §3.2.2 rows 1–8).
3. **S2 `StrategyConfig` projection is a frozen dataclass.** The
   projection type is `@dataclass(frozen=True)` per project
   convention; S3 adds a field via `dataclasses.replace`-style
   extension, not mutation.
4. **Import-linter rules are active in CI.** S2 owns the
   import-linter ruleset (§3.2.2 row 8); the existing rules forbid
   `pms.sensor` / `pms.actuator` from importing `pms.strategies.*`
   or `pms.controller.*`. S3 enforces the analogous
   `pms.factors.*` cannot-import-`pms.strategies.aggregate`
   constraint as **review/grep guidance during S3 PRs** (Invariant
   5 partial). Promoting this to a codified import-linter rule
   requires amending §3.2.2 row 8 in the same PR per §3.1 ("When
   adding a new concept not in the matrix") and is therefore
   scoped through S2 / harness-spec review, not unilaterally added
   under S3.
5. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   passes with ≥ 70 tests + 2 skipped; `uv run mypy src/ tests/
   --strict` is clean (§5.1, §5.2; 🟡 Fresh-clone baseline
   verification).

### 8.7 Leave-behind

Enumerated artefacts produced by S3, keyed back to §3.2.3 row
numbers (1–7) so the §4.3 gate-4 boundary-integrity check can diff
S5's Intake (authored when §10 lands) against this list.

1. **`src/pms/factors/definitions/` module tree populated**
   (§3.2.3 row 1): `FactorDefinition` ABC plus at least the raw
   factors migrated from today's `RulesForecaster` and
   `StatisticalForecaster` (at minimum: `fair_value_spread`,
   `subset_pricing_violation`, `metaculus_prior`, `yes_count`,
   `no_count`) plus at least one greenfield raw factor for
   end-to-end validation (e.g. `orderbook_imbalance`). Each module
   is strategy-agnostic; none import `pms.strategies.*`.
2. **`factors` table populated in the middle-ring block of
   `schema.sql`** (§3.2.3 row 2) with one row per registered
   `FactorDefinition`. No `factor_type` column (Invariant 4).
3. **`factor_values` table populated in the middle-ring block of
   `schema.sql`** (§3.2.3 row 3) keyed `(factor_id, market_id, ts,
   value)` per §3.2.3 row 3 verbatim, with no `strategy_id` /
   `strategy_version_id` columns (Invariant 8 mechanical check). A
   `param` index column may land via the CP1 harness-spec review;
   if it does, §3.2.3 is amended in the same PR (§3.1).
4. **`FactorService` running on a configured cadence** (§3.2.3
   row 4) as an independent `asyncio.Task` under the Runner,
   reading outer-ring tables through the S1 store and writing
   `factor_values` rows. Strategy-agnostic: no import from
   `pms.strategies.*`.
5. **Post-migration forecaster files contain no detection logic**
   (§3.2.3 row 5): `src/pms/controller/forecasters/rules.py` and
   `src/pms/controller/forecasters/statistical.py` carry only a
   thin reference to the composition layer. Every former detection
   heuristic is now a `FactorDefinition` module under
   `src/pms/factors/definitions/`, and a regression **matrix**
   (covering every piecewise regime enumerated in AC5: spread
   below/at/above `min_edge`, both rules present to lock
   precedence, metaculus absent vs present, zero vs non-zero
   counts) asserts the post-migration forecaster output equals
   the pre-migration output across all regimes modulo float
   tolerance (🟡 Comments are not fixes; 🟡 Piecewise-domain
   functions).
6. **`StrategyConfig.factor_composition` field lands on the S2
   projection** (§3.2.3 row 6), typed (not stringly-typed), with
   the `"default"` strategy's `strategy_versions` row re-hashed to
   carry a populated composition blob reproducing today's
   forecaster behaviour. Every other strategy continues to carry
   an empty composition in S3.
7. **`/factors` dashboard page renders real factor evolution**
   (§3.2.3 row 7) from the `factor_values` table via a new
   `dashboard/app/api/pms/factors/` route, verified by a Playwright
   e2e test (AC 3).

In addition to the §3.2.3-row-keyed leave-behinds above, S3 leaves
the **S2-owned `strategy_factors` link table populated** (per
§3.2.2 row 5 Notes — "Empty shape in S2; S3 populates as factor
definitions land"): one row per `(strategy_id, factor_id)` pairing
for every `FactorDefinition` the `"default"` strategy uses. The
table's DDL ownership stays with S2; S3 is the authorised
consumer-side writer.

S5's Intake (authored in Commit 8 of this document authoring effort)
reads from this list; diffing the two is the §4.3 gate-4 mechanism.

### 8.8 Checkpoint skeleton

Flat one-line-each list. **Not harness acceptance criteria** — the
per-CP acceptance criteria, files-of-interest, effort estimates, and
test expectations land in `.harness/pms-factor-panel-v1/spec.md`
when the kickoff prompt (§8.11) triggers that authoring session.

- **CP1 — `factors` + `factor_values` DDL + middle-ring block
  comments in `schema.sql`.**
- **CP2 — `FactorDefinition` ABC + first raw factor
  (`orderbook_imbalance`) end-to-end (definition → `FactorService`
  compute → `factor_values` persisted).**
- **CP3 — Migrate `RulesForecaster` detection rules
  (`fair_value_spread`, `subset_pricing_violation`) into
  `FactorDefinition` modules.**
- **CP4 — Migrate `StatisticalForecaster` raw inputs
  (`metaculus_prior`, `yes_count`, `no_count`) into
  `FactorDefinition` modules; derive composition into
  `StrategyConfig.factor_composition`.**
- **CP5 — `StrategyConfig.factor_composition` field on the S2
  projection + `"default"` strategy re-hash carrying
  post-migration composition blob + S2-owned `strategy_factors`
  link table populated for `"default"` (per §3.2.2 row 5 Notes,
  consumer-side population) + forecaster-behaviour regression
  **matrix** covering every piecewise regime named in AC5 (🟡
  Piecewise-domain functions).**
- **CP6 — `FactorService` compute + persist on cadence + Runner
  `asyncio.Task` wiring + lifecycle cleanup on all 4 exit paths
  (🟡 Lifecycle cleanup on all exit paths).**
- **CP7 — `/factors` dashboard page + `dashboard/app/api/pms/
  factors/` route + Playwright e2e.**

### 8.9 Effort estimate

**M** (5–7 CPs per §8.8; middle-ring schema + `FactorService` +
dashboard page; migration of the existing rules + statistical
detectors is the risk axis). S3's surface area is narrower than S1
(no sensor split, no storage rewrite, no CI infra) but broader than
a greenfield service because the migration must preserve existing
runtime behaviour exactly — the `StatisticalForecaster`'s
Beta-posterior blend of prior strength with observed counts has
enough interacting parameters to make a silent behavioural
regression plausible. The regression test at CP5 is load-bearing.

### 8.10 Risk register

| Risk | Likelihood | Impact | Mitigation | Trigger / early-warning |
|---|---|---|---|---|
| Migration silently drops a rule's behaviour (e.g. `StatisticalForecaster`'s Beta-posterior blend of prior strength with observed counts is decomposed into raw factors but the composition blob does not reproduce the blend; or `RulesForecaster` precedence between `_price_spread` and `_subset_violation` is silently inverted) | M | H | CP5 regression **matrix** (per AC5; not a single canonical fixture) pins post-migration forecaster output against pre-migration output across every piecewise regime — spread below/at/above `min_edge`, subset-violation below/at/above `min_edge`, both rules present to lock precedence, metaculus absent vs present, zero vs non-zero counts under seeded `prior_strength` (🟡 Comments are not fixes; 🟡 Piecewise-domain functions) | Any matrix cell red on CP5; a fix that only matches "close enough" silently changes runtime behaviour |
| Raw-factor design forces artificial granularity — `RulesForecaster.predict` (`src/pms/controller/forecasters/rules.py:14-18`) is a precedence chain (`_price_spread` short-circuits; `_subset_violation` runs only when the first returns `None`), and decomposing today's thresholded outputs into raw factors looks easy until a hypothetical future rule needs max/min selection across rules | M | M | Persist the **unthresholded** raw spreads as `FactorDefinition`s in S3 (`fair_value_spread = fair_value − yes_price`, `subset_pricing_violation = subset_price − superset_price`); leave thresholding (`< min_edge`) and precedence ordering in `StrategyConfig.factor_composition` consumed by S5. Do **not** persist "did this rule fire" as a raw factor — that bakes thresholded rule semantics into S3, violating Invariant 4. The hypothetical max/min-across-rules selection is a future stress test; if it ever forces strategy-specific weighting into a `FactorDefinition` body, **halt and escalate** via retro | A `FactorDefinition` module proposal encodes strategy-specific weighting or a threshold to preserve rule semantics; code review flags Invariant 4 Anti-pattern |
| Factor cache staleness vs orderbook update cadence — `FactorService` writes every N seconds while `price_changes` arrives sub-second; S5 reads the most recent `factor_values` row and sees stale derived signals | M | M | Start with a cadence short enough that staleness is bounded (e.g. 1 s) and instrument `FactorService` latency; surface staleness as a dashboard metric on `/factors`. A proper invalidation framework is §8.4 non-goal; if staleness regressions appear in S5, open a retro | `FactorService` write p95 latency > cadence target; `/factors` staleness metric exceeds configured budget |
| `/factors` dashboard page perf degrades on large `factor_values` tables | L | M | Query the `factor_values` table with an index on `(factor_id, market_id, ts DESC)` by default, extended to `(factor_id, param, market_id, ts DESC)` if CP1 adopts the optional `param` column (per §8.2 row 3 deferral), and cap the default time window; the dashboard API route accepts a `since` parameter for window narrowing; retention policy is deferred per §6.4 non-goal (a) — the original deferral text says the decision lands in **S3 or S6** once factor-panel / backtest-replay query shapes make the trade-offs concrete, and S3 explicitly defers the choice out of scope (§8.4 non-goal "no factor-cache staleness framework"; harness-spec CP-DQ-style review may revisit) | Dashboard `/factors` p95 render > 2 s; Next.js API route logs slow-query warnings |
| `StrategyConfig.factor_composition` becomes a stringly-typed DSL by accident (a contributor encodes composition rules as string expressions "because JSONB accepts strings") | M | H | The S2 projection is `@dataclass(frozen=True)`; `factor_composition` is typed as a nested Python structure (e.g. `list[FactorCompositionStep]` with each step a typed dataclass), serialised to JSONB at store boundary only. A code review rejection template names the Invariant 4 Anti-pattern "factor-expression DSL stored in the database" explicitly | A PR introduces a `str`-typed field inside the composition structure; the `FactorDefinition` registry gains an `eval`-like call site |
| `FactorService` imports from `pms.strategies.aggregate` to "look up which strategies need this factor" — Invariant 5 partial violation | L | H | `FactorService` computes every registered factor for every subscribed market unconditionally; pruning based on `strategy_factors` is explicitly out of scope for S3 (that is S5's `ControllerPipeline` concern — it reads only the factor values the strategy needs). Enforced as **S3 review/grep guidance** (`rg -n '^(from pms\.strategies|import pms\.strategies)' src/pms/factors` — zero matches expected); promoting this to a codified import-linter rule (S2-owned per §3.2.2 row 8) requires amending §3.2.2 in the same PR per §3.1 | Code review flag on a new `FactorService` constructor argument typed as `Strategy`; grep returns a non-zero match in `src/pms/factors/` |

### 8.11 Kickoff Prompt

The block below is **copy-paste-ready** for a fresh Claude session
whose job is to author `.harness/pms-factor-panel-v1/spec.md` — the
harness-executable spec with per-CP acceptance criteria,
files-of-interest, effort, and intra-spec dependencies. The future
session has **no memory** of this document; the prompt is
self-contained.

```
SCOPE:

You are starting harness task `pms-factor-panel-v1` in the
prediction-market-system repository at
/Users/stometa/dev/prediction-market-system. Your job this session
is to author the harness-executable spec file at
/Users/stometa/dev/prediction-market-system/.harness/pms-factor-panel-v1/spec.md
(the directory may not yet exist; you will create it).

REQUIRED READING (ordered — read in this order before touching
anything, all paths absolute):

1. /Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md
   — specifically §8 (this sub-spec's total-spec contract) and
   §3.2.3 (the 7 concepts S3 owns).
2. /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
   — focus on Invariants 4 (raw factors only) and 8 (onion-
   concentric storage, middle ring). Partial touches on 2 and 5.
3. /Users/stometa/dev/prediction-market-system/agent_docs/promoted-rules.md
   — especially Runtime behaviour > design intent (🔴), Comments
   are not fixes (🟡 — migration must preserve runtime behaviour
   exactly, not via comment), Lifecycle cleanup on all exit paths
   (🟡 — relevant for the FactorService asyncio.Task), and
   Fresh-clone baseline verification (🟡).
4. /Users/stometa/dev/prediction-market-system/docs/notes/2026-04-16-evaluator-entity-abstraction.md
   — the "Factor Entities" section. Understand the distinction
   between FactorDefinition (raw primitive), FactorConfig
   (strategy-scoped usage), FactorValue (computed value),
   FactorPanel (queryable matrix), and FactorAttribution (Evaluator
   artifact). Only raw primitives and computed values land in S3;
   FactorAttribution is deferred per §3.4 of the project spec.
5. /Users/stometa/dev/prediction-market-system/docs/notes/2026-04-16-repo-issues-controller-evaluator.md
   — load-bearing sections: "Persistence Decision: PostgreSQL, All
   Environments", "Controller: First-Class Layer, But Strategies
   Are Not First-Class Yet" (context for the rules-detector
   migration), and "Summary: Decisions Captured On 2026-04-16".
6. /Users/stometa/dev/prediction-market-system/src/pms/controller/CLAUDE.md
   — per-layer invariant enforcement for Controller. Lists today's
   RulesForecaster / StatisticalForecaster files and marks
   `stop_conditions` as a known violation (context for what
   migrates to raw factors vs what stays in Controller).
7. /Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
   — structural reference for harness-grade spec shape (CP shape,
   acceptance-criteria shape, files-of-interest shape).

CURRENT STATE SNAPSHOT:

S1 (pms-market-data-v1) and S2 (pms-strategy-aggregate-v1) are
complete. The §8.6 Intake items MUST be satisfied before any
authoring begins — specifically:

- S1's outer-ring tables (markets, tokens, book_snapshots,
  book_levels, price_changes, trades) exist in schema.sql and are
  populated by MarketDiscoverySensor + MarketDataSensor at
  runtime.
- S2's inner-ring aggregate tables (strategies, strategy_versions,
  strategy_factors) have their DDL applied; strategies and
  strategy_versions are populated (the "default" strategy row is
  seeded); strategy_factors ships in EMPTY shape per Boundary
  Matrix §3.2.2 row 5 Notes — S3 is the authorised consumer-side
  writer that populates it as FactorDefinition modules land.
  PostgresStrategyRegistry is operational.
- S2's StrategyConfig projection is a frozen dataclass; S2's
  import-linter rules are active in CI (pms.sensor and pms.actuator
  cannot import pms.strategies.* or pms.controller.*).

If any §8.6 Intake item fails, HALT and tell the user.

PREFLIGHT (boundary check — run before any authoring):

- Confirm §8.6 Intake items 1–5 all satisfied (verify outer-ring
  tables queryable; strategies + strategy_versions populated;
  strategy_factors DDL applied but empty per §3.2.2 row 5;
  StrategyConfig frozen; import-linter rules active; canonical
  gates green on a fresh clone).
- Confirm §3.2.3 of the project-level spec has exactly 7 rows.
  Recount. If more or fewer, STOP and reconcile — the scope-in
  concept list depends on that row count being stable.

TASK:

Create
/Users/stometa/dev/prediction-market-system/.harness/pms-factor-panel-v1/spec.md
by expanding §8.8 (Checkpoint skeleton) into harness-grade CPs
with:

- Per-CP acceptance criteria (observable, falsifiable, not
  implementation notes).
- Per-CP files-of-interest (absolute paths under
  /Users/stometa/dev/prediction-market-system/).
- Per-CP effort estimate (S / M / L).
- Intra-spec CP dependencies (which CPs block which).

Use
/Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
as the structural reference for shape. Draft only — stop and wait
for spec-evaluation approval before running any checkpoint.

CONSTRAINTS:

- New feature branch `feat/pms-factor-panel-v1` off `main`. Never
  commit to `main` directly.
- Respect §3 Boundary Matrix of the project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md).
  Never claim a concept owned by another sub-spec (S1, S2, S4–S6).
  The 7 concepts enumerated in §8.2 Scope-in are the complete
  authorised set for S3; anything else is scope drift.
- Every Strategy / Factor / ring-ownership claim must cite an
  invariant number from
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md.
- No `Co-Authored-By` lines in any commit
  (/Users/stometa/dev/prediction-market-system/CLAUDE.md §"Do
  not"; promoted rule "Commit-message precedence").
- Conventional-commit prefixes required (§5.6 of the project
  spec): `feat(<scope>):`, `fix(<scope>):`, `docs(<scope>):`,
  `test(<scope>):`, `refactor(<scope>):`, `chore(<scope>):`.
- Follow the promoted rule 🟡 Lifecycle cleanup on all exit paths
  for the FactorService asyncio.Task and any database connection
  helpers: acquire + release in the same commit, `try/finally` on
  all four exit paths.
- Follow the promoted rule 🟡 Comments are not fixes when migrating
  RulesForecaster / StatisticalForecaster detection into raw
  factors: the migration MUST produce equivalent runtime behaviour
  (CP5 regression test), not a code comment explaining the
  equivalence.

HALT CONDITIONS:

- Any invariant in
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
  cannot be satisfied by the design you are authoring. Do NOT
  silently amend the invariant. Open a retro under
  /Users/stometa/dev/prediction-market-system/.harness/retro/ and
  return to the user.
- Any attempt to add a `factor_type ENUM('raw','composite')` column
  to the `factors` table, a `composite_factors` table, or a
  factor-expression DSL column (e.g. a string like "a * b + c"
  stored as a `factors` column) — these are the canonical
  Invariant 4 Anti-patterns named in
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
  §Invariant 4. STOP immediately.
- Any attempt to add a `strategy_id` or `strategy_version_id`
  column to `factors` or `factor_values` — this is an Invariant 8
  violation. STOP immediately.
- The 7 concepts you author CPs for do not match §3.2.3 of the
  project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md),
  concept-for-concept. Reconcile first.
- The rules-detector migration requires strategy-specific
  weighting inside a `FactorDefinition` module body to preserve
  existing behaviour. This is an Invariant 4 stress test — the
  correct response is a retro, not a composite factor.

FIRST ACTION:

Run:

    cd /Users/stometa/dev/prediction-market-system \
      && git status && git branch --show-current \
      && git log --oneline -5

Then read the 7 files in the REQUIRED READING block, in order,
before drafting any content. After reading, report your
understanding of §8.8's 7 checkpoints (CP1–CP7) and wait for
go-ahead before drafting
/Users/stometa/dev/prediction-market-system/.harness/pms-factor-panel-v1/spec.md.
```

---

---

## 9. Sub-spec S4 — pms-active-perception-v1

S4 closes the **active-perception feedback edge** of the cybernetic
loop. With S1's two-layer sensor (`MarketDiscoverySensor` +
`MarketDataSensor`) and S2's `Strategy` aggregate + registry already
landed, the bidirectional edge from Controller/Strategy back into
Sensor subscription is still missing: `MarketDataSensor` today
receives its asset-id list from a stub loader (S1 §6.4 Non-goals
explicitly carves this out), and no module computes the merged
market-id set that registered strategies actually need. S4 wires
that edge. This section expands the `agent_docs/project-roadmap.md`
§S4 skeleton into the project-level contract that the harness run
under `.harness/pms-active-perception-v1/` will consume.

### 9.1 Goal

S4 wires the **Controller → Sensor feedback path** so Sensor
subscription is derived at runtime from the union of every registered
strategy's `select_markets(universe)` output, rather than from static
config. A new `MarketSelector` reads the market universe from the
outer-ring `markets` / `tokens` tables (owned by S1), applies each
registered strategy's `Strategy.select_markets(universe)` hook, and
produces a merged `market_ids: list[str]`. A new
`SensorSubscriptionController` **pushes** subscription updates into
the `MarketDataSensor` (the sensor never pulls — Invariant 6). The
`Strategy.select_markets` **method body** lands with S4 even though
the `Strategy` aggregate class itself was defined in S2: per the
single-owner rule in §3.2.4, this specific method ships in the same
commit as its first consumer (`MarketSelector`) so the hook and the
driver land together. Runner wiring honors the **Invariant 6 boot
order** — `MarketDiscoverySensor` populates the universe first,
`MarketSelector` then computes the merged list from the populated
universe, `SensorSubscriptionController` pushes the result into
`MarketDataSensor`, and strategy-config changes trigger
**incremental resubscribe** without a Runner restart. S4 closes
observable-capability item #9 of §1.1 (active perception wired
end-to-end, no sensor module imports `pms.strategies.*`).

### 9.2 Scope (in / out)

**Scope in.** Exactly the **4 rows** owned by S4 in §3.2.4, in the
same order, each with a one-line scope descriptor. Reviewers
verifying boundary integrity grep §3.2.4 against this list; the
lists must match concept-for-concept.

1. **`MarketSelector` (`src/pms/market_selection/selector.py`).**
   Sibling module to `pms.controller` — placement under
   `src/pms/market_selection/` rather than `src/pms/controller/`
   (both placements were left open by `src/pms/controller/CLAUDE.md`;
   S4 declares the sibling placement here). Rationale: the
   import-linter rule banning `pms.sensor → pms.market_selection`
   (Invariant 6 Enforcement, codified by S2) names
   `pms.market_selection` as a first-class package; placing the
   module under that package keeps the linter rule readable and
   avoids re-exporting from `pms.controller`. Reads universe from
   outer ring via `PostgresMarketDataStore` (S1), applies each
   strategy's `select_markets(universe)`, returns merged market-id
   list (Invariant 6).
2. **`SensorSubscriptionController`
   (`src/pms/market_selection/subscription_controller.py`).**
   Push-only channel: invokes `MarketDataSensor.subscribe(
   asset_ids)` / `.unsubscribe(asset_ids)` with the delta between
   the previous and current merged list. Sensor never pulls — the
   flow is always selector → controller → sensor (Invariants 6, 7).
3. **`Strategy.select_markets(universe)` method (declaration +
   body + per-strategy tests).** The aggregate class itself lives
   on S2's `Strategy` type; this **method** ships with S4 so the
   hook and its first consumer (`MarketSelector`) land in the same
   commit. Returns a strategy-specific subset of the universe as a
   `list[str]` of market ids (Invariants 2, 6).
4. **Runner wiring: boot order + recompute on universe-refresh
   and strategy-config change.** `src/pms/runner.py` is updated to
   instantiate the four components in the order
   `MarketDiscoverySensor → MarketSelector → SensorSubscription-
   Controller → MarketDataSensor`, then drives **two** recompute
   triggers: (a) **universe-refresh trigger** — every time
   `MarketDiscoverySensor` completes a poll cycle that changed the
   `markets` / `tokens` outer-ring rows (new listing, delisting, or
   token update), Runner re-invokes `MarketSelector.compute()` and
   pushes any delta through `SensorSubscriptionController`; this is
   what unblocks the cold-start empty-universe boot (§9.3 AC 4) and
   what catches newly-listed markets (§9.10 risk row 1); (b)
   **strategy-registry trigger** — on a strategy register /
   unregister / config change from `PostgresStrategyRegistry` (S2),
   Runner re-invokes `MarketSelector` and pushes only the delta
   through `SensorSubscriptionController`. Neither trigger causes a
   Runner restart or a full resubscribe; both emit only the
   symmetric difference of the two market-id sets (Invariant 6
   "Cold-start handling" block — the cold-start recovery path is
   trigger (a), incremental live evolution is the union of (a) and
   (b)).

**Scope out.** The following look S4-adjacent but are owned by
other sub-specs. A PR landing any of these under `.harness/pms-
active-perception-v1/` is scope drift.

- **`Strategy` aggregate class + projection types
  (`StrategyConfig`, `RiskParams`, `EvalSpec`, `ForecasterSpec`,
  `MarketSelectionSpec`) → S2** (§3.2.2). S4 depends on the
  aggregate class existing and being iterable via
  `PostgresStrategyRegistry`; S4 does not redefine it. The only
  strategy-side S4 delta is the `select_markets` **method body**,
  per §9.2 row 3.
- **Outer-ring `markets` / `tokens` tables (DDL + writes) → S1**
  (§3.2.1). `MarketSelector` reads these tables via the
  S1-provided `PostgresMarketDataStore`; S4 does not alter DDL or
  ingestion cadence.
- **`MarketDiscoverySensor` + `MarketDataSensor` classes → S1**
  (§3.2.1). S4 depends on both, configures their boot order, and
  pushes subscription updates into `MarketDataSensor`; S4 does
  not rewrite sensor internals. `MarketDataSensor` remains a
  subscription **sink** per its S1 contract.
- **`factor_values` table + `FactorService` → S3** (§3.2.3). S4
  does not consume factor values; selection is a universe → market-
  id projection, not a factor-ranking step. Any factor-driven
  filtering happens inside a strategy's `select_markets` body,
  but the reads live behind `StrategyConfig` — S4 itself never
  imports `pms.factors.*`.
- **Per-strategy `ControllerPipeline` dispatch that consumes the
  active subscription list → S5** (§3.2.5). S4 makes the
  subscription active; S5 is the first consumer that dispatches
  per-strategy pipelines against it. S4 ships with the existing
  global `ControllerPipeline` untouched — S5 does the per-strategy
  split.
- **`BacktestSpec` + `ExecutionModel` + market-universe replay
  → S6** (§3.2.6). Backtest replay of subscription changes is
  out of S4's scope (see §9.4 Non-goals).

### 9.3 Acceptance criteria (system-level)

Six observable-capability bullets. Each is verifiable at the system
level — not a checklist item for an individual CP.

1. **Subscription tracks `select_markets` output live.** Registering
   a new strategy whose `select_markets(universe)` returns market ids
   `[A, B]` causes the `MarketDataSensor` subscription to include
   both `A` and `B` within **5 seconds** of the registry write, with
   no Runner restart. Verified by an integration test that registers
   a stub strategy, reads the sensor's current subscription set, and
   asserts set-membership within the time bound (Invariant 6).
2. **Union semantics on shrink.** Unregistering a strategy — or
   reducing its `select_markets` output — causes the merged
   subscription set to shrink. A market still selected by **any**
   remaining registered strategy stays subscribed; a market **no
   longer** selected by any strategy is unsubscribed via an
   explicit `unsubscribe` call on `MarketDataSensor`, within the
   same 5-second bound. Verified by an integration test with two
   strategies where one drops a market the other still selects and
   a second market only the unregistered strategy selected
   (Invariant 6).
3. **Import-linter enforces the full Sensor import boundary.** The
   S2-codified import-linter rule that `pms.sensor` cannot import
   from `pms.strategies.*`, `pms.controller.*`, **or**
   `pms.market_selection` (§3.2.2 row 7 of the project-level spec)
   has zero violations after S4 lands. The first two legs already
   appear as informal "Do not" bullets in the current
   `src/pms/sensor/CLAUDE.md`; S2 is what formalises all three
   legs into a machine-checkable rule (S2 landed before S4 per the
   §4.1 canonical sequence), and the third leg
   (`pms.market_selection`) is the one S4 makes load-bearing for
   the first time since `pms.market_selection` does not exist as a
   package before S4. Machine check: `import-linter` run in CI
   produces a green report against all three targets; grep
   complement `rg -n
   '^(from pms\.strategies|from pms\.controller|from pms\.market_selection|import pms\.strategies|import pms\.controller|import pms\.market_selection)'
   src/pms/sensor` returns zero matches. No Sensor module references
   the selector, the controller, or the strategies aggregate
   (Invariants 5, 6).
4. **Cold-start boot order holds and recovers on universe
   refresh.** Invariant 6 "Cold-start handling" is verified by a
   two-part deterministic test: (a) if `MarketDiscoverySensor`
   returns **0 markets** (the universe is empty — a legitimate
   state at first boot before the Gamma poll completes),
   `MarketSelector.compute()` returns an empty list,
   `SensorSubscriptionController` issues **no** subscribe calls,
   and `MarketDataSensor` stays idle (no WebSocket subscribe
   message emitted, no book-snapshot row written); (b) the very
   next `MarketDiscoverySensor` poll that writes a non-empty
   `markets` set triggers the Runner's universe-refresh recompute
   (§9.2 row 4 trigger (a)), `MarketSelector.compute()` now
   returns the strategies' merged selection, and
   `SensorSubscriptionController` subscribes the resulting asset
   ids within the same 5-second bound as AC 1. The window of
   idleness is bounded by the `MarketDiscoverySensor` cadence; it
   is a documented boot-order ordering constraint, not an error
   state, and not a permanent stranding.
5. **Incremental resubscribe on strategy-config change.** Mutating
   a registered strategy's config (re-registering with a new
   `strategy_versions` row via `PostgresStrategyRegistry` per
   Invariant 3) triggers only **delta** subscribe / unsubscribe
   calls on `MarketDataSensor`, not a full unsubscribe-then-
   resubscribe. Verified by an integration test that counts
   `.subscribe` / `.unsubscribe` invocations across a config change
   and asserts the count equals the symmetric difference of the
   two market-id sets (Invariant 6).
6. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   passes with the baseline (≥ 70 tests + 2 skipped integration)
   and `uv run mypy src/ tests/ --strict` is clean, on a fresh
   shell per 🟡 Fresh-clone baseline verification (§5.1, §5.2).

### 9.4 Non-goals

Explicit deferrals. An S4 harness spec that lands any of these
items is scope drift.

- **No per-strategy `ControllerPipeline` dispatch.** S4 makes the
  subscription list driven by each strategy's `select_markets`,
  but the existing global `ControllerPipeline` continues to run
  unchanged. Per-strategy dispatch — the layer that consumes the
  active subscription and fans decisions out per strategy — is
  S5-owned (§3.2.5).
- **No strategy-specific WebSocket topics.** Subscription is
  **venue-level**, shared across strategies by construction: the
  merged market-id list is the union, so the same `book` /
  `price_change` event arrives once and is written once to the
  outer ring (Invariant 8 — market data stays strategy-agnostic).
  Per-strategy "my own stream" is explicitly rejected.
- **No backtest replay of subscription changes.** S6 owns the
  market-universe replay engine (§3.2.6). Backtest mode continues
  to use `HistoricalSensor`'s flat asset-id list; active perception
  applies only to live / paper modes in S4.
- **No strategy-selection caching layer.** Baseline is
  **compute-on-change** — `MarketSelector` recomputes the merged
  list on either of the two §9.2 row 4 triggers (universe-refresh
  from `MarketDiscoverySensor` poll-complete, or strategy-registry
  register / unregister / config change), and that is it. No
  memoisation of `select_markets(universe)` outputs, no
  incremental universe-diffing inside the selector. Both triggers
  are authorised; nothing else schedules a recompute. Add a cache
  only if a concrete perf finding demands it, with its own retro.
- **No multi-venue subscription coordination.** S4 covers
  **Polymarket only**. A Kalshi adapter pair (discovery + data)
  with its own selector entry is follow-on work — mentioned in §1.2
  "What the finished system does not do" and deferred past S6.
- **No runtime mutation of a registered strategy's `select_markets`
  implementation.** Changing the method body requires a new
  `strategy_versions` row (Invariant 3 — immutable versioning);
  S4 does not support in-place method replacement.

### 9.5 Dependencies

- **Upstream sub-specs.**
  - **S1** (§6) — `MarketDiscoverySensor` + `MarketDataSensor`
    classes, `markets` / `tokens` tables, `PostgresMarketDataStore`
    read helpers, `asyncpg.Pool`, Runner scaffolding. S4 wires
    itself into the existing two-sensor split as a **subscription-
    push channel** between them.
  - **S2** (§7) — `Strategy` aggregate class, `strategy_versions`
    immutability, `PostgresStrategyRegistry`, **import-linter
    rules (including the `pms.sensor → pms.market_selection` ban
    that S2 reserves for S4)**. S4 consumes the registry's iterable
    of registered strategies; S4's `select_markets` **method body**
    extends the aggregate surface that S2 declares.
- **Upstream invariants.** Primary: **6** (active perception —
  Controller-derived market ids feed back into Sensor subscription)
  and **7** (two-layer sensor, with `MarketDataSensor` as the
  subscription sink). Partial touches: **2** (the Strategy aggregate
  is read via `select_markets`; S4 is an aggregate-reader on one
  method only) and **5** (the import-linter rule extension
  `pms.sensor → pms.market_selection` codified in S2 becomes
  load-bearing once S4 introduces the `pms.market_selection`
  package — S4 is the first PR under which the rule can actually
  fire; the rule itself stays S2-owned).

### 9.6 Intake

Minimum set that must exist before a harness run opens under
`.harness/pms-active-perception-v1/`.

1. **S1 Leave-behind satisfied.** Per §6.7: `schema.sql` declares
   outer-ring `markets` / `tokens` / `book_snapshots` /
   `book_levels` / `price_changes` / `trades`; `MarketDiscovery-
   Sensor` populates `markets` + `tokens`; `MarketDataSensor`
   accepts a flat `asset_ids: list[str]` at start-up and exposes
   `.subscribe(asset_ids)` / `.unsubscribe(asset_ids)` as the
   subscription-sink contract; `PostgresMarketDataStore` provides
   the outer-ring read helpers `MarketSelector` needs;
   `asyncpg.Pool` is Runner-owned; the S1 stub loader for
   `MarketDataSensor`'s initial asset-id list is still in place
   (S4 replaces it with the push channel).
2. **S2 Leave-behind satisfied.** Per §7.7: `Strategy` aggregate
   class exists under `src/pms/strategies/aggregate.py` with
   `MarketSelectionSpec` projection declared on it;
   `strategy_versions` table carries the immutable config-hash row
   per registered strategy; `PostgresStrategyRegistry` exposes an
   iterable of registered `Strategy` aggregates and emits a
   change-notification hook (register / unregister / config-change)
   that S4's Runner wiring subscribes to; the `"default"` strategy
   + version row is seeded so the pre-S5 runtime has exactly one
   registered strategy to iterate over. **The S2 import-linter
   rule must already codify the full three-target ban before S4
   opens** — `pms.sensor` cannot import from `pms.strategies.*`,
   `pms.controller.*`, OR `pms.market_selection`. S2 is the
   sub-spec that introduces the machine-checkable rule (the
   import-linter config is a **new** addition that lands with S2;
   today's `pyproject.toml` has no `[tool.importlinter]` section,
   and `src/pms/sensor/CLAUDE.md` informally records only the
   first two legs). All three targets must be present in
   `pyproject.toml` (or `ruff.toml`, whichever S2 chooses) ahead of
   the S4 harness run; the `pms.market_selection` leg is the one
   S4 makes load-bearing for the first time since
   `pms.market_selection` does not exist as a package pre-S4, but
   the other two legs guard against regressions during S4 wiring
   (Invariants 5 + 6 Enforcement).
3. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   passes with ≥ 70 tests + 2 skipped; `uv run mypy src/ tests/
   --strict` is clean (§5.1, §5.2; 🟡 Fresh-clone baseline
   verification).
4. **No existing `src/pms/market_selection/` tree.** This is the
   first sub-spec to introduce the package; a pre-existing
   directory would indicate scope overlap with a parallel effort
   that must first reconcile. Mechanical check:
   `test ! -d src/pms/market_selection` (exits 0) — or equivalently
   `find src/pms -type d -name market_selection` returns zero lines.
   `fd` is deliberately **not** used: per the promoted rule 🟡
   Verify isolated-env tooling assumptions, the check must work in
   a minimal shell without third-party CLIs installed.

### 9.7 Leave-behind

Enumerated artefacts produced by S4, keyed back to §3.2.4 row
numbers so the §4.3 gate-4 boundary-integrity check can diff S5's
Intake (once §10.6 lands) against this list.

1. **`src/pms/market_selection/selector.py`** (§3.2.4 row 1)
   defining `MarketSelector` with:
   - Constructor taking `PostgresMarketDataStore` (for outer-ring
     universe reads) and `PostgresStrategyRegistry` (for iterating
     registered strategies).
   - A single public async method `compute() -> list[str]` that
     reads the current universe, invokes each registered
     `Strategy.select_markets(universe)`, and returns the merged
     market-id list (order deterministic — alphabetical by
     `market_id` — so diffs are stable).
2. **`src/pms/market_selection/subscription_controller.py`**
   (§3.2.4 row 2) defining `SensorSubscriptionController` with:
   - Constructor taking a `MarketDataSensor` reference.
   - A single public async method
     `apply(new_market_ids: list[str]) -> None` that computes the
     symmetric difference against the last applied list and emits
     exactly the delta `.subscribe` / `.unsubscribe` calls. Ordering
     contract: `.subscribe(delta_add)` runs **before**
     `.unsubscribe(delta_remove)` within the same `apply()` call —
     markets still in the union never briefly leave the active set
     (§9.10 row 3).
   - Internal state: the last applied list, scoped by instance; no
     cross-instance shared state.
3. **`Strategy.select_markets(universe: Sequence[MarketRow]) ->
   list[str]` method body** (§3.2.4 row 3) on the S2 aggregate
   class at `src/pms/strategies/aggregate.py`, with:
   - Default implementation that reads the strategy's
     `MarketSelectionSpec` projection and filters the universe
     accordingly.
   - Per-strategy tests covering: empty universe, universe with
     no matches, universe with partial matches, and a strategy
     whose spec rejects every row.
4. **Runner wiring in `src/pms/runner.py`** (§3.2.4 row 4):
   - Boot-order sequence `MarketDiscoverySensor.start() →
     MarketSelector.compute() → SensorSubscriptionController.apply()
     → MarketDataSensor.start()` — with `MarketSelector.compute()`
     tolerating an empty universe and returning `[]` cleanly (§9.3
     AC 4a).
   - A **universe-refresh trigger** (§9.2 row 4 trigger (a)): a
     `MarketDiscoverySensor` poll-complete hook that, whenever the
     completed poll changed the outer-ring `markets` / `tokens`
     rows, re-invokes `MarketSelector.compute()` +
     `SensorSubscriptionController.apply()` pushing only the delta.
     This is the trigger that closes the cold-start empty-universe
     window (§9.3 AC 4b) and that catches newly-listed markets
     (§9.10 risk row 1).
   - A **strategy-registry trigger** (§9.2 row 4 trigger (b)): a
     `PostgresStrategyRegistry` change-notification subscriber that
     triggers `MarketSelector.compute()` +
     `SensorSubscriptionController.apply()` again, pushing only
     the delta (§9.3 AC 5).
   - Lifecycle cleanup on **all four exit paths** (normal shutdown,
     signal, exception, test teardown) per 🟡 Lifecycle cleanup on
     all exit paths — `SensorSubscriptionController` releases its
     last-applied-list state and `MarketDataSensor` receives an
     explicit final `unsubscribe` for every active market id in a
     `try/finally` scoped to `Runner.stop()`.

S5's Intake (authored in the next drafting slot) reads from this
list; diffing the two is the §4.3 gate-4 mechanism.

### 9.8 Checkpoint skeleton

Flat one-line-each list. **Not harness acceptance criteria** — the
per-CP acceptance criteria, files-of-interest, effort estimates,
and test expectations land in `.harness/pms-active-perception-v1/
spec.md` when the kickoff prompt (§9.11) triggers that authoring
session.

- **CP1 — `MarketSelector` class + unit tests over synthetic
  registry + empty-universe cold-start test** (covers §9.3 AC 4a
  deterministically in isolation — empty universe yields empty
  list, no side effects).
- **CP2 — `Strategy.select_markets` method body on the S2
  aggregate + per-strategy tests** (empty universe / no-match /
  partial-match / reject-all).
- **CP3 — `SensorSubscriptionController` class + delta-push
  protocol tests** against a `MarketDataSensor` test double,
  including the ordering test that locks in row-3-of-§9.10
  (subscribe-first then unsubscribe for the delta-remove, so
  markets in the union never gap).
- **CP4 — Runner boot-order wiring + universe-refresh
  recompute trigger** (DiscoverySensor → Selector →
  SubscriptionController → DataSensor, plus the
  discovery-poll-complete hook that re-invokes
  `MarketSelector.compute()` + `SensorSubscriptionController.
  apply()` on a changed outer-ring row set) with cold-start
  ordering + cold-start recovery both verified end-to-end
  (§9.3 AC 4a + AC 4b) and lifecycle cleanup on all four
  exit paths.
- **CP5 — Strategy-registry change trigger** — incremental
  resubscribe on strategy register / unregister / config change
  wired into `PostgresStrategyRegistry`'s change-notification
  hook, verified by an integration test counting delta calls
  (§9.3 AC 5).
- **CP6 — Integration test: add/remove strategy observable
  subscription delta within 5 seconds** (§9.3 AC 1 + AC 2) under
  `@pytest.mark.integration` + `PMS_RUN_INTEGRATION=1` skipif
  (🟢 Integration test default-skip pattern).

### 9.9 Effort estimate

**M** (6 CPs per §9.8; new module tree `src/pms/market_selection/`
+ one method body on the S2 aggregate + Runner wiring + integration
tests; blast radius concentrated at the Controller ↔ Sensor
boundary). Smaller than S1 (L — 10 CPs, persistence backend + two
sensors + CI infra) and S2 (L — aggregate + registry + projections
+ import-linter rule codification) because the storage layer, the
aggregate surface, and the linter rules are all pre-existing. Most
of the design risk is concentrated in Runner lifecycle / boot-order
sequencing, not in new persistence or schema work.

### 9.10 Risk register

| Risk | Likelihood | Impact | Mitigation | Trigger / early-warning |
|---|---|---|---|---|
| Race between `MarketDiscoverySensor` universe update and in-flight `MarketSelector.compute()` — selector reads a stale `markets` snapshot, misses a newly-listed market, and the new market is not subscribed until the next config-change event | M | M | `MarketSelector.compute()` re-reads the universe at invocation time (no cached universe); discovery sensor writes `markets` rows via `INSERT ... ON CONFLICT` (S1 §3.2.1), so a concurrent read sees either the old row or the new one but not a partial write; selector also re-runs on discovery-sensor completion heartbeat (not only on strategy register / unregister) | Integration test: race harness that listens for a `markets` row insert and asserts the subscription set contains the new market within 5 seconds of the insert; CI flake on the race test |
| Oscillating subscriptions between two strategies disagreeing on a market with thin signal — strategy A selects then drops a market on every config-hash tick, producing a churn of subscribe / unsubscribe cycles on the WebSocket | L | M | Union semantics: unsubscribe only when **no** registered strategy selects the market (§9.3 AC 2); strategies cannot force an unsubscribe against another strategy's selection; `SensorSubscriptionController` does not emit a `.unsubscribe` for a market still in the union; the only loop that can form is strategy-vs-itself, which is bounded by the config-hash re-registration cadence | Metrics on `.subscribe` / `.unsubscribe` call rates per market id; rate > 1 per minute for any single market id during normal operation |
| `MarketDataSensor` drops frames during resubscribe — the subscribe / unsubscribe delta is emitted while a book event is in flight, and the stateful parser sees a delta for a market it has just unsubscribed | L | M | `SensorSubscriptionController.apply()` emits `.subscribe` for the delta-add set **before** `.unsubscribe` for the delta-remove set, so markets still in the union never briefly leave the active set (the union cover at every instant is a superset of both the old and the new active sets). `MarketDataSensor`'s stateful parser (S1 §6.2 row 11) ignores events for asset ids not currently in its active set — events for a cleanly-removed market arriving after `.unsubscribe` are dropped by design, not by race; reconnect reconciliation (S1 §6.2 row 13) re-issues subscribe on the current set so any WebSocket-side race resolves within one reconnect cycle. CP3 test double locks the ordering: `.subscribe(delta_add)` invocation count is non-zero before any `.unsubscribe(delta_remove)` invocation within the same `apply()` call | `MarketDataSensor` WARN log "event for unsubscribed asset" at rate > 0.01 per second; `book_snapshots` gap > 30 seconds on an active market; CP3 ordering assertion fails in CI |
| Cold-start timing window where `MarketDataSensor` has no subscription for the first N seconds after Runner boot — the universe is empty, `MarketSelector` returns `[]`, no subscribe is issued, and the dashboard briefly shows no live markets | **Expected, not a bug** | L | Documented in Invariant 6 "Cold-start handling" and in §9.3 AC 4 as the correct boot-order behaviour, not a failure mode; the window closes on the first `MarketDiscoverySensor` poll cycle (bounded by its configured cadence — seconds); dashboard copy on `/signals` should distinguish "no subscriptions yet (universe empty)" from "sensor error" | First-boot observer reports "no data on `/signals`"; resolve by citing this row + AC 4 — do not attempt to remove the window |
| Import-linter rule collision with existing controller code — `src/pms/controller/router.py` or `src/pms/controller/pipeline.py` already imports something the S2 linter rule flags once `pms.market_selection` exists (e.g., a shared helper that controller unwittingly re-exports into sensor reach) | L | M | Before writing the S4 module tree, run the S2 linter against a no-op stub `src/pms/market_selection/__init__.py` to confirm the baseline is green; existing `pms.controller.*` already avoids sensor reach per S5 Invariant 5 enforcement; §9.6 Intake item 2 is load-bearing here (the rule must exist in the tree before S4 starts so violations surface at CP1, not at CP6) | S4 CP1 lint run fails on a rule not introduced by S4 itself — stop and reconcile with S2 / controller code before proceeding |

### 9.11 Kickoff Prompt

The block below is **copy-paste-ready** for a fresh Claude session
whose job is to author `.harness/pms-active-perception-v1/spec.md` —
the harness-executable spec with per-CP acceptance criteria,
files-of-interest, effort, and intra-spec dependencies. The future
session has **no memory** of this document; the prompt is
self-contained.

```
SCOPE:

You are starting harness task `pms-active-perception-v1` in the
prediction-market-system repository at
/Users/stometa/dev/prediction-market-system. Your job this
session is to author the harness-executable spec file at
/Users/stometa/dev/prediction-market-system/.harness/pms-active-perception-v1/spec.md
(the directory may not yet exist; you will create it).

REQUIRED READING (ordered — read in this order before touching
anything, all paths absolute). NOTE on branch state: by the time
this kickoff fires, the project-level decomposition spec (item 1
below) and all S1/S2/S3 artefacts are assumed merged to `main`
(CURRENT STATE SNAPSHOT confirms this). If a file listed below is
NOT reachable from the current `main` checkout, run
`git log --all --oneline -- <path>` to locate the branch it lives
on, read it via `git show <branch>:<path>`, and HALT before
authoring — a missing REQUIRED READING artefact means the S1/S2/S3
retirement gates did not land correctly.

1. /Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md
   — specifically §9 (this sub-spec's total-spec contract). This
   file lands on `main` as part of the project-decomposition PR;
   if absent on `main`, fetch via
   `git show docs/pms-project-decomposition:docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md`
   and HALT with the user.
2. /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
   — focus on Invariant 6 (active perception — Controller-derived
   market ids feed back into Sensor subscription) and Invariant 7
   (two-layer sensor, with `MarketDataSensor` as subscription
   sink). Read the §"Cold-start handling" block of Invariant 6
   carefully — the boot-order ordering constraint is load-bearing
   for §9.3 AC 4 and §9.8 CP1 / CP4. Partial touches on
   Invariants 2 and 5.
3. /Users/stometa/dev/prediction-market-system/agent_docs/promoted-rules.md
   — especially Runtime behaviour > design intent (🔴), Review-loop
   rejection discipline (🔴), Lifecycle cleanup on all exit paths
   (🟡 — relevant for Runner boot order and SubscriptionController
   teardown), Fresh-clone baseline verification (🟡), and
   Integration test default-skip pattern (🟢 — applies to the
   §9.3 AC 1 / AC 2 / AC 5 integration tests).
4. /Users/stometa/dev/prediction-market-system/src/pms/sensor/CLAUDE.md
   — per-layer invariant enforcement for Sensor. Load-bearing:
   the file's §"Do not" list informally bans `pms.strategies.*` and
   `pms.controller.*` (Invariant 5). The third leg of the
   three-target ban — `pms.market_selection` — is grounded in
   Invariant 6 Enforcement (`agent_docs/architecture-invariants.md`
   §Invariant 6) and is mechanically codified by S2's import-linter
   rule per the §3.2.2 Boundary Matrix row; the sensor CLAUDE.md
   does not currently mention `pms.market_selection` directly. All
   three legs together form the rule the import-linter enforces
   from S2 onwards.
5. /Users/stometa/dev/prediction-market-system/src/pms/controller/CLAUDE.md
   — per-layer invariant enforcement for Controller. Load-bearing:
   §"Layer-relevant invariants" Invariant 6 bullet — MarketSelector
   placement is declared as `src/pms/market_selection/` (sibling
   module) in §9.2 of the project-level spec, not under
   `src/pms/controller/`.
6. /Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
   — structural reference for harness-grade spec shape (CP shape,
   acceptance-criteria shape, files-of-interest shape).

CURRENT STATE SNAPSHOT:

This snapshot describes the **state the future session expects at
the moment this kickoff fires** (after S1, S2, S3 have all
landed per the §4.1 canonical sequence). If any item below is not
true when the kickoff actually runs, HALT — the §4.3 between-spec
gates for S3 did not clear. Specifically at kickoff time:
- S1, S2, and S3 have all been merged to `main` per the
  between-spec gates.
- The project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md)
  is on `main` and reachable from the current checkout.
- The harness structural reference at
  /Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
  is on `main` and reachable from the current checkout. (If
  either file is absent from `main`, HALT — see REQUIRED
  READING NOTE.)
- S1 Leave-behind (§6.7 of the project-level spec) is satisfied:
  outer-ring tables, `MarketDiscoverySensor`, `MarketDataSensor`
  as subscription sink with `.subscribe` / `.unsubscribe`,
  `PostgresMarketDataStore`, Runner-owned `asyncpg.Pool`, stub
  loader for `MarketDataSensor`'s initial asset-id list still in
  place.
- S2 Leave-behind is satisfied: `Strategy` aggregate + projections,
  `strategies` + `strategy_versions` tables, `PostgresStrategy-
  Registry` with a change-notification hook, and the
  import-linter configuration S2 introduces to `pyproject.toml`
  (or `ruff.toml`) carries the full three-target ban (`pms.sensor`
  cannot import from `pms.strategies.*`, `pms.controller.*`, OR
  `pms.market_selection`). Note: the import-linter configuration
  is **net-new in S2** — pre-S2 `pyproject.toml` has no
  `[tool.importlinter]` section, and `src/pms/sensor/CLAUDE.md`
  informally records only the first two legs. S2 is what lands
  the mechanical enforcement of all three. `"default"` strategy
  + version is seeded.
- S3 Leave-behind is satisfied: `factors` + `factor_values` tables,
  `FactorService`, rules-detector heuristics migrated to raw
  factor definitions, `/factors` dashboard page.
- §9.6 Intake items are all satisfied. No `src/pms/market_selection/`
  directory yet exists in the tree.

PREFLIGHT (boundary check — run before any authoring):

- §9.6 Intake items must all be satisfied. If any fails, HALT and
  tell the user:
  * `uv run pytest -q` passes with ≥ 70 tests + 2 skipped
    (PMS_RUN_INTEGRATION=1 gate), and
    `uv run mypy src/ tests/ --strict` is clean, on a fresh shell.
  * S1 and S2 Leave-behind artefacts exist per §6.7 and §7.7 of
    the project-level spec.
  * The full three-target S2 import-linter rule is present in
    `pyproject.toml` or `ruff.toml`: `pms.sensor` cannot import
    from `pms.strategies.*`, `pms.controller.*`, OR
    `pms.market_selection`. All three legs must be in the rule —
    the first two guard against regressions during S4 wiring,
    the third is the one S4 makes load-bearing.
  * No `src/pms/market_selection/` directory pre-exists under
    /Users/stometa/dev/prediction-market-system/src/pms/.

TASK:

Create
/Users/stometa/dev/prediction-market-system/.harness/pms-active-perception-v1/spec.md
by expanding §9.8 (Checkpoint skeleton) into harness-grade CPs
with:

- Per-CP acceptance criteria (observable, falsifiable, not
  implementation notes).
- Per-CP files-of-interest (absolute paths under
  /Users/stometa/dev/prediction-market-system/).
- Per-CP effort estimate (S / M / L).
- Intra-spec CP dependencies (which CPs block which).

Use
/Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
as the structural reference for shape. Draft only — stop and wait
for spec-evaluation approval before running any checkpoint.

CONSTRAINTS:

- New feature branch `feat/pms-active-perception-v1` off `main`.
  Never commit to `main` directly.
- Respect §3 Boundary Matrix of the project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md).
  Never claim a concept owned by another sub-spec (S1, S2, S3,
  S5, S6). The 4 concepts enumerated in §9.2 Scope-in are the
  complete authorised set for S4; anything else is scope drift.
- Every Strategy / Sensor / selector / ring-ownership claim must
  cite an invariant number from
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md.
- No `Co-Authored-By` lines in any commit
  (/Users/stometa/dev/prediction-market-system/CLAUDE.md §"Do
  not"; promoted rule "Commit-message precedence").
- Conventional-commit prefixes required (§5.6 of the project
  spec): `feat(<scope>):`, `fix(<scope>):`, `docs(<scope>):`,
  `test(<scope>):`, `refactor(<scope>):`, `chore(<scope>):`.
- Follow the promoted rule 🟡 Lifecycle cleanup on all exit paths
  for `SensorSubscriptionController` state and `MarketDataSensor`
  subscription teardown: acquire + release in the same commit,
  `try/finally` on all four exit paths.
- Follow the promoted rule 🟢 Integration test default-skip pattern
  for the §9.3 AC 1 / AC 2 / AC 5 integration tests.

HALT CONDITIONS:

- Any sensor module ends up importing from `pms.strategies.*`,
  `pms.controller.*`, OR `pms.market_selection` — STOP
  immediately. The first two legs are the informal "Do not" rules
  in `src/pms/sensor/CLAUDE.md` §"Do not" (Invariant 5
  Enforcement); the third leg is grounded in Invariant 6
  Enforcement (`agent_docs/architecture-invariants.md` §Invariant
  6) and is mechanically codified by the S2 import-linter rule
  (project-level spec §3.2.2 row 7). Together the three legs are
  the full ban that S2 lands as a machine-checkable rule before
  S4 opens. The flow is always selector → controller → sensor;
  sensor never pulls. Do not author any CP whose files-of-interest
  include both `src/pms/sensor/` edits and a
  `from pms.market_selection`, `from pms.controller`, or
  `from pms.strategies` import (also reject the plain
  `import pms.market_selection` / `import pms.controller` /
  `import pms.strategies` syntaxes — the linter rule covers both
  forms).
- Any attempt to make `MarketDataSensor` compute its own
  subscription list (pull, rather than receive via push). This
  is an Invariant 6 violation — STOP.
- Any invariant in
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
  cannot be satisfied by the design you are authoring. Do NOT
  silently amend the invariant. Open a retro under
  /Users/stometa/dev/prediction-market-system/.harness/retro/ and
  return to the user.
- The 4 concepts you author CPs for do not match §3.2.4 of the
  project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md),
  concept-for-concept. Reconcile first.

FIRST ACTION:

Run:

    cd /Users/stometa/dev/prediction-market-system \
      && git status && git branch --show-current \
      && git log --oneline -5

Then read the 6 files in the REQUIRED READING block, in order,
before drafting any content. After reading, report your
understanding of §9.8's 6 checkpoints (CP1–CP6) and wait for
go-ahead before drafting
/Users/stometa/dev/prediction-market-system/.harness/pms-active-perception-v1/spec.md.
```

---

---

## 10. Sub-spec S5 — pms-controller-per-strategy-v1

S5 is the **fan-in** node of the §2.1 DAG: both S3 (factor panel) and
S4 (active perception) terminate here, and every load-bearing
strategy-aware concept this decomposition needs the runtime to honour
lands inside this sub-spec. By the end of S5 the system runs multiple
strategies concurrently through per-strategy `ControllerPipeline`
instances, every product-table row carries a real
`(strategy_id, strategy_version_id)` pair (Invariant 3 columns
upgraded to `NOT NULL` and populated end-to-end), the Evaluator's
metric surface partitions by strategy, the `Opportunity` entity
replaces the stringly-typed `stop_conditions` mix that today carries
routing + model_id, and the `/strategies` page becomes the
comparative-metrics dashboard. This section expands the skeleton in
`agent_docs/project-roadmap.md` §S5 into the project-level contract
that the harness run under `.harness/pms-controller-per-strategy-v1/`
will consume.

### 10.1 Goal

S5 closes Invariants 2, 3, and 5 simultaneously by upgrading the
runtime from "one global pipeline tagged `\"default\"`" to
"per-strategy dispatch with real version tags end-to-end". Concretely:
the `ControllerPipeline` becomes a per-strategy factory (one instance
per registered strategy, each with its own forecaster stack /
calibrator / sizer composition read from `StrategyConfig` and
`ForecasterSpec` projections); the Evaluator's `MetricsCollector`
rewrites every aggregation to `GROUP BY strategy_id,
strategy_version_id`; the `(strategy_id, strategy_version_id)`
columns reserved `NULLABLE` by S1 (§3.2.1 row 19) and seeded with
`"default"` by S2 are upgraded to `NOT NULL` on every inner-ring
product table (`feedback`, `eval_records`, `orders`, `fills` —
all 4 created by S1 per §3.2.1 rows 14 and 18); the
`TradeDecision` / `OrderState` / `FillRecord` / `EvalRecord` emit
paths populate real values from the per-strategy dispatch; the
`Opportunity` entity lands as the Controller's pre-execution output
and replaces the Invariant 2 violation where strategy provenance hides
inside `stop_conditions: list[str]` (`src/pms/controller/CLAUDE.md`
"known violation" note); the `/strategies` page upgrades from S2's
listing view to a comparative-metrics view (Brier / P&L / fill rate /
slippage / drawdown side-by-side per strategy); and the existing
`/metrics` page extends from a global rollup to a per-strategy
breakdown. S5 is the fan-in point of the DAG (§2.1) — Invariants 2,
3, and 5 all become enforceable end-to-end inside this sub-spec, not
"declared in S2 and partially satisfied".

### 10.2 Scope (in / out)

**Scope in.** Exactly the **7 concepts** owned by S5 in §3.2.5, in
the same order, each with a one-line scope descriptor. Reviewers
verifying boundary integrity grep §3.2.5 against this list; the lists
must match concept-for-concept.

1. **Per-strategy `ControllerPipeline` dispatch.** A per-strategy
   factory under `src/pms/controller/pipeline.py` (or successor
   module) that constructs one `ControllerPipeline` instance per
   registered strategy from the `StrategyConfig` + `ForecasterSpec` +
   `RiskParams` projections (Invariant 2 — projection-only consumption;
   Invariant 5 — strategy-aware boundary).
2. **Per-strategy `Evaluator` aggregation.** `MetricsCollector` and
   the relevant `metrics.py` queries rewritten to `GROUP BY
   strategy_id, strategy_version_id`, retiring the global
   `MetricsCollector.snapshot()` shape (Invariants 3, 5).
3. **`(strategy_id, strategy_version_id)` `NOT NULL` DDL upgrade on
   all inner-ring product tables.** Schema-change-only migration on
   `feedback`, `eval_records`, `orders`, `fills` with a `CHECK`
   constraint forbidding sentinel values (`agent_docs/architecture-
   invariants.md` §Invariant 3 Enforcement). Pre-S5 columns are
   `NULLABLE` with `"default"` tagging; the upgrade does not add new
   tables (Invariant 3).
4. **`TradeDecision` / `OrderState` / `FillRecord` / `EvalRecord`
   strategy-field population end-to-end.** S1 reserved the columns,
   S2 seeded `"default"`, S5 populates real values from the
   per-strategy dispatch. Each downstream emit path
   (Controller → Actuator → Evaluator) forwards the
   `(strategy_id, strategy_version_id)` pair without re-deriving it
   (Invariant 3).
5. **`Opportunity` entity (Controller output pre-execution).** New
   `@dataclass(frozen=True)` under `src/pms/core/models.py` (or
   `src/pms/controller/models.py` per harness-spec choice) carrying
   selected factor values + expected edge + rationale; replaces the
   stringly-typed `stop_conditions: list[str]` routing / model_id mix
   identified in `src/pms/controller/CLAUDE.md` as a known Invariant 2
   violation. Inspecting an `Opportunity` explains why the Controller
   made the decision (Invariant 2).
6. **`/strategies` comparative-metrics view.** Upgrades S2's
   listing-only page to display Brier / P&L / fill rate / slippage /
   drawdown side-by-side per strategy, each grouped by
   `(strategy_id, strategy_version_id)` (Invariant 3; observable-
   capability item §1.1.3).
7. **`/metrics` per-strategy breakdown.** Existing global `/metrics`
   page extends to a per-strategy rollup partitioned by
   `(strategy_id, strategy_version_id)`; the cross-strategy aggregates
   that exist for ops dashboards remain but must be explicitly
   annotated per `agent_docs/architecture-invariants.md` §Invariant 3
   Enforcement (Invariant 3).

**Scope out.** The following look S5-adjacent but are owned by other
sub-specs. A PR landing any of these under
`.harness/pms-controller-per-strategy-v1/` is scope drift.

- `Strategy` aggregate, projection types (`StrategyConfig`,
  `RiskParams`, `EvalSpec`, `ForecasterSpec`, `MarketSelectionSpec`),
  `strategies` / `strategy_versions` / `strategy_factors` tables, and
  `PostgresStrategyRegistry` → **S2** (§3.2.2). S5 *consumes* the
  aggregate as an authorised reader (Invariants 2, 5) but never
  modifies the aggregate's shape, the projection contracts, or the
  registry tables.
- `factors` / `factor_values` tables, `FactorService`, raw
  `FactorDefinition` modules, the rules-detector migration into raw
  factors, and `StrategyConfig.factor_composition` JSONB field →
  **S3** (§3.2.3). S5 reads `factor_values` (Invariant 8) but never
  declares new factors, never modifies `FactorService`, and never
  changes the `factor_composition` schema.
- `MarketSelector`, `SensorSubscriptionController`,
  `Strategy.select_markets(universe)` method, and the boot-order /
  incremental-resubscribe Runner wiring → **S4** (§3.2.4). S5
  consumes the merged subscription path (`MarketSelector` produces
  the merged set per §3.2.4 row 1; `SensorSubscriptionController`
  is the only subscriber-facing path per §3.2.4 row 2) and derives
  per-strategy market scope by calling `Strategy.select_markets(
  universe)` directly (S4-owned method per §3.2.4 row 3); S5 does
  not modify the selector, the controller, the method, or the
  wiring.
- `BacktestSpec`, `ExecutionModel`, `BacktestDataset`, `BacktestRun`,
  `StrategyRun` (the materialised per-strategy backtest entity),
  market-universe replay engine, parameter sweep,
  `BacktestLiveComparison`, `TimeAlignmentPolicy`,
  `SymbolNormalizationPolicy`, `SelectionSimilarityMetric`,
  `EvaluationReport`, `PortfolioTarget`, and `/backtest` ranked
  N-strategy view → **S6** (§3.2.6). Live / paper per-strategy
  tracking happens through the inner-ring product tables grouped by
  `(strategy_id, strategy_version_id)`; introducing a runtime
  `strategy_runs` table inside S5 would create a reverse dependency
  S5 → S6 that the §2.1 DAG forbids (§3.2.6 row 5 explicitly states
  this).
- Outer-ring DDL, `MarketDiscoverySensor` / `MarketDataSensor`,
  `PostgresMarketDataStore`, JSONL → PG migration, `/signals` page
  → **S1** (§3.2.1). S5's storage work is the inner-ring `NOT NULL`
  upgrade only; outer-ring schema is untouched.

### 10.3 Acceptance criteria (system-level)

Eight observable-capability bullets. Each is verifiable at the system
level — not a checklist item for an individual CP.

1. **Three concurrent per-strategy `ControllerPipeline` instances
   from a single Runner.** Registering 3 strategies in `strategies`
   (with distinct `strategy_versions` rows) and starting the Runner
   produces 3 concurrent `ControllerPipeline` instances; each reads
   the asset ids returned by its own `Strategy.select_markets(universe)`
   call (the S4-owned method on the aggregate, per §3.2.4 row 3) and
   filters incoming `MarketSignal`s to that scope before dispatching;
   each emits `TradeDecision`s tagged with that strategy's
   `strategy_version_id`. The Sensor subscription itself remains the
   merged set (`MarketSelector` produces the union, S4-owned per
   §3.2.4 row 1; `SensorSubscriptionController` is the only
   subscriber-facing path per §3.2.4 row 2). Verified by an
   asyncio-task introspection probe (3 distinct pipeline tasks
   running concurrently) plus a behavioural probe over the in-memory
   `TradeDecision` stream (`pms.runner.RunnerState.decisions`) and
   the inner-ring product tables (`SELECT DISTINCT strategy_id,
   strategy_version_id FROM eval_records` and the same query over
   `fills` once any decision has resolved) returning exactly the
   registered set (Invariants 1, 2, 5).
2. **Every aggregate query in `MetricsCollector` groups by
   `(strategy_id, strategy_version_id)`.** Verified per
   `agent_docs/architecture-invariants.md` §Invariant 3 Enforcement
   — the schema half by post-migration `information_schema` queries,
   the query-time half by §5.3 Mixed-invariant evidence (reviewer
   sign-off on every aggregation query, plus a code-review gate
   that rejects any SQL aggregation over `eval_records` or `fills`
   omitting the `GROUP BY` without an explicit cross-version
   justification comment) (Invariant 3).
3. **`/strategies` page renders comparative metrics side-by-side.**
   The dashboard `/strategies` page shows per-strategy Brier / P&L /
   fill rate / slippage / drawdown columns for every registered
   strategy, each grouped by `(strategy_id, strategy_version_id)`;
   a Playwright assertion confirms ≥ 2 distinct strategy rows
   render with non-null metric cells when ≥ 2 strategies have
   produced fills (Invariant 3; observable-capability §1.1.3).
4. **`Opportunity` carries decision rationale and replaces the
   `stop_conditions` mix.** Inspecting an `Opportunity` row exposes
   selected factor values + expected edge + rationale + target size
   + expiry / staleness policy + `(strategy_id,
   strategy_version_id)`. The Controller emits an `Opportunity`
   before constructing a `TradeDecision`; `stop_conditions` no
   longer carries routing / model_id semantics (the Invariant 2
   violation called out in `src/pms/controller/CLAUDE.md` is
   removed). Verified by a behavioural test that asserts on the
   `Opportunity` payload shape and a grep that fails the build if
   `stop_conditions` regains stringly-typed strategy semantics
   (Invariants 2, 3).
5. **No product-table row exists where `strategy_id` or
   `strategy_version_id` IS NULL post-S5.** Per
   `agent_docs/architecture-invariants.md` §Invariant 3 Enforcement
   (a — schema half), the `NOT NULL` constraint plus the `CHECK`
   forbidding sentinel values are active on every inner-ring product
   table after the migration; an `information_schema` probe in CI
   confirms both the constraint definitions and the `\d+` view
   matches what the spec declares. Pre-S5 rows already carry the
   `"default"` strategy tag seeded by S2 (per §3.2.2 row 8 and
   `agent_docs/architecture-invariants.md` §Invariant 3 Runtime
   evidence — `"default"` is a real strategy id, not a sentinel), so
   the `NOT NULL` upgrade is schema-change-only — no historical
   re-attribution to a different strategy is performed. Only S5-era
   and post-S5 writes populate real per-strategy
   `(strategy_id, strategy_version_id)` from the per-strategy
   dispatch (Invariant 3).
6. **`/metrics` per-strategy breakdown is the default view.** The
   existing `/metrics` page renders a per-strategy rollup keyed on
   `(strategy_id, strategy_version_id)` as the primary section;
   cross-strategy aggregates remain available but are explicitly
   annotated as "ops view" per the §Invariant 3 Enforcement
   review-gate guidance (Invariant 3).
7. **Strategy unregister releases pipeline lifecycle resources.**
   Unregistering a strategy from the registry (within a Runner that
   is already running) cancels the corresponding
   `ControllerPipeline` task and releases every tracked resource
   (subscription side-effects via `SensorSubscriptionController`'s
   S4 contract, in-flight `Opportunity` queue, calibrator state),
   verified by a behavioural test that asserts on
   `asyncio.all_tasks()` shrinking by exactly the unregistered
   strategy's pipeline count plus the absence of dangling per-
   strategy state in memory snapshots. The acquire / release pairing
   lands in the same commit per the promoted rule
   🟡 Lifecycle cleanup on all exit paths.
8. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   passes with the project baseline (≥ 70 tests + 2 skipped,
   `PMS_RUN_INTEGRATION=1`-gated) and `uv run mypy src/ tests/
   --strict` is clean, on a fresh shell per 🟡 Fresh-clone baseline
   verification (§5.1, §5.2).

### 10.4 Non-goals

Explicit deferrals. An S5 harness spec that lands any of these items
is scope drift.

- **No backtest-framework replay engine.** Market-universe replay,
  multi-day backtest history reconstruction, and the
  `BacktestSpec` → `BacktestRun` → `StrategyRun` materialisation
  chain are S6's headline scope (§3.2.6). S5 produces *runtime*
  per-strategy metrics that S6 reuses as the reference behaviour
  for backtest comparison.
- **No parameter sweep or `BacktestLiveComparison`.** Per-strategy
  parameter exploration (generate N `BacktestSpec`s with shared
  factor-panel cache) and live-vs-backtest equity / selection
  comparison both live in S6.
- **No multi-account dispatch / `StrategyBundle`.** The
  `StrategyBundle` entity (multi-strategy group mapping to one live
  account) is intentionally deferred per §3.4. Today each strategy
  runs independently with shared risk budget at the Runner level;
  multi-account work happens after S5 closes, before any Kalshi or
  multi-account expansion.
- **No strategy hot-reload without Runner restart.** Re-configuring
  a registered strategy at runtime (without process restart)
  requires careful pipeline-rebuild + state-handoff semantics that
  exceed S5's scope. Today's flow is: stop Runner → update
  `StrategyConfig` → restart Runner. Live reconfiguration is a
  follow-on concern after S6 closes.
- **No materialised `StrategyRun` runtime entity.** Per §3.2.6 row 5
  ("`StrategyRun`"), live / paper per-strategy tracking happens
  through the inner-ring product tables grouped by `(strategy_id,
  strategy_version_id)`; no separate runtime `strategy_runs` table
  is introduced. `StrategyRun` exists as a backtest-only entity
  owned by S6.
- **No automated feedback-loop strategy adjustment.** `Feedback`
  rows continue to be surfaced for human resolution per
  observable-capability §1.2 — automated strategy reconfiguration
  in response to threshold breaches is explicitly out of scope
  (carried forward from `.harness/pms-v2/` non-goals).

### 10.5 Dependencies

- **Upstream sub-specs (fan-in: S3 + S4).** S5 has **two** incoming
  edges in the §2.1 DAG (`S3 → S5` and `S4 → S5`), the only fan-in
  node in the decomposition. §4.3 gate 4 explicitly classifies
  S5's Intake check as "S5's Intake against **S3's Leave-behind ∪
  S4's Leave-behind**" (union semantics — both predecessor sets
  participate), so §10.6 below cites the union directly.
- **Transitive predecessors (referenced, NOT included in §10.6
  Intake).** S3 and S4 each carry their own dependency on S2 (per
  §2.1 DAG edges `S2 → S3` and `S2 → S4`); S2 in turn depends on
  S1. The S2 aggregate + registry tables (§3.2.2) and S1 outer-ring
  tables (§3.2.1) reach S5 transitively through S3 and S4 — but
  they are **not** part of S5's Intake, because §4.3 gate 4 mandates
  the union diff against the **immediate** predecessor edges only.
  Including transitive S1 / S2 concepts in §10.6 would force
  predecessor specs to duplicate concepts they do not own and
  would break the mechanical boundary-integrity diff. They appear
  here as documented references for spec readers, not as gate
  artefacts.
- **Upstream invariants.** Primary: **2** (aggregate reader +
  projection consumption — S5's per-strategy `ControllerPipeline`
  factory is the canonical example of layer-side projection
  consumption); **3** (`NOT NULL` upgrade on every inner-ring
  product table, plus end-to-end field population through
  Controller → Actuator → Evaluator emit paths); **5** (Controller
  + Evaluator are the two strategy-aware layers, and S5 is where
  that boundary becomes load-bearing in production code, not just
  an import-linter rule). Transitive: **8** (Controller reads
  middle ring written by S3, writes inner ring whose schema was
  reserved by S1 and seeded by S2) and **6** (per-strategy
  dispatch pairs each pipeline with the per-strategy market scope
  computed by `Strategy.select_markets(universe)` while
  `SensorSubscriptionController` continues to drive the merged
  subscription).

### 10.6 Intake

Minimum set that must exist before a harness run opens under
`.harness/pms-controller-per-strategy-v1/`. **S5's Intake is the
union of S3's Leave-behind (§3.2.3 / §8.7 once landed) AND S4's
Leave-behind (§3.2.4 / §9.7 once landed)** — both predecessor edges
participate, matching the §4.3 gate-4 union formula
(`S5's Intake against S3's Leave-behind ∪ S4's Leave-behind`). The
union-based check is what commit 3 of this document authoring
effort established for fan-in nodes; S5 is the only fan-in node in
the decomposition, so this Intake is the canonical exercise of that
gate semantic. **Transitive S1 / S2 prerequisites are deliberately
NOT listed here** — including them would break the §4.3 gate-4
mechanical diff against the immediate-predecessor union. Those
transitive predecessors are documented in §10.5 Dependencies as
references for spec readers.

**From S3's Leave-behind (factor panel — §3.2.3 owners).**

1. **`factor_values` table populated and queryable.** Reads against
   `factor_values(factor_id, market_id, ts, value)` return non-empty
   results for at least the migrated rules-detector factors — the
   per-strategy `ControllerPipeline` cannot dispatch without real
   factor reads (Invariants 4, 8).
2. **`FactorService` available for read.** `FactorService` exposes
   the read API the per-strategy pipeline calls during forecaster /
   sizer composition; `FactorService` itself is S3-owned, S5 is a
   consumer only (Invariant 4).
3. **`StrategyConfig.factor_composition` JSONB field readable.**
   The per-strategy projection passed into the
   `ControllerPipeline` factory exposes `factor_composition` so
   strategy-specific composition logic resolves at dispatch time;
   the field is S3-owned (per §3.2.3 row 6) but consumed by S5
   (Invariants 2, 4).
4. **Migration of existing rules-detector heuristics into raw
   factors complete.** The pre-S5 `RulesForecaster` /
   `StatisticalForecaster` heuristics are now expressed as raw
   `FactorDefinition` rows so the per-strategy pipeline can compose
   them via `factor_composition` (Invariant 4).

**From S4's Leave-behind (active perception — §3.2.4 owners).**

5. **`MarketSelector` produces the merged subscription set.**
   `MarketSelector` reads the universe, applies each strategy's
   `select_markets(universe)`, and returns the **merged** market-id
   list per §3.2.4 row 1 + Invariant 6 (this contract is S4-owned
   and S5 does not change its shape). The per-strategy
   `ControllerPipeline` reasons about its own market scope by
   calling `Strategy.select_markets(universe)` directly (item 7
   below), not by reading a per-strategy view off `MarketSelector`
   (Invariants 2, 6).
6. **`SensorSubscriptionController` is the only subscriber-facing
   path.** `MarketDataSensor` is driven by push from
   `SensorSubscriptionController` (per §3.2.4 row 2); per-strategy
   pipelines never write to the sensor directly. The Sensor stays
   strategy-agnostic per Invariant 5 (Invariants 5, 6, 7).
7. **`Strategy.select_markets(universe)` method present on the
   aggregate.** S4 owns the entire surface (declaration + body +
   per-strategy tests, per §3.2.4 row 3); S5's per-strategy
   `ControllerPipeline` calls this method directly to derive the
   per-strategy market scope it filters incoming `MarketSignal`s
   against (Invariants 2, 6).
8. **Boot-order Runner wiring intact.** DiscoverySensor → Selector →
   SubscriptionController → DataSensor wiring, plus incremental
   resubscribe on strategy-config change, are operational so the
   per-strategy pipeline can register / unregister strategies
   without breaking the cold-start path (Invariant 6).

S6's Intake (authored in the §11 / commit 9 effort) reads from S5's
§10.7 Leave-behind below; diffing the two is the §4.3 gate-4
mechanism that closes S5 → S6.

### 10.7 Leave-behind

Enumerated artefacts produced by S5, keyed back to §3.2.5 row
numbers so the §4.3 gate-4 boundary-integrity check can diff S6's
Intake (once §11.6 lands) against this list.

1. **Per-strategy `ControllerPipeline` dispatch** (§3.2.5 row 1) at
   `src/pms/controller/pipeline.py` (or successor module) — a
   factory that constructs one `ControllerPipeline` per registered
   strategy from `(StrategyConfig, ForecasterSpec, RiskParams)`
   projections; each instance runs as an independent `asyncio.Task`
   inside the Runner (Invariants 1, 2, 5).
2. **Per-strategy `Evaluator` aggregation** (§3.2.5 row 2) inside
   `src/pms/evaluation/metrics.py` — every `MetricsCollector` query
   rewritten to `GROUP BY strategy_id, strategy_version_id`; the
   global snapshot shape retired; cross-strategy aggregates remain
   available with explicit ops-view annotation per §5.3 Invariant 3
   Mixed-evidence gate (Invariants 3, 5).
3. **Inner-ring `NOT NULL` DDL migration** (§3.2.5 row 3) applied
   to `feedback`, `eval_records`, `orders`, `fills`; the pair
   `(strategy_id, strategy_version_id)` is `NOT NULL` post-migration,
   plus the `CHECK` constraint forbidding sentinel values per
   `agent_docs/architecture-invariants.md` §Invariant 3 Enforcement.
   Schema-change-only: the constraint succeeds because every pre-S5
   row already carries the `"default"` strategy id seeded by S2 (per
   §3.2.2 row 8); legacy rows retain that tag as their honest
   provenance — only S5-era and post-S5 writes carry real
   per-strategy values from the new dispatch (Invariant 3).
4. **`TradeDecision` / `OrderState` / `FillRecord` / `EvalRecord`
   strategy-field population end-to-end** (§3.2.5 row 4) — every
   Controller emit path stamps `(strategy_id, strategy_version_id)`
   from the dispatching pipeline; every Actuator emit path forwards
   the pair without re-deriving it; every Evaluator scoring path
   reads the pair from the upstream record. No emit path falls back
   to `"default"` (Invariant 3).
5. **`Opportunity` entity + emit path** (§3.2.5 row 5) as a
   `@dataclass(frozen=True)` carrying selected factor values +
   expected edge + rationale + target size + expiry / staleness
   policy + `(strategy_id, strategy_version_id)`; emitted by the
   per-strategy pipeline before `TradeDecision` construction;
   `stop_conditions: list[str]` no longer carries strategy
   provenance (the Invariant 2 violation called out in
   `src/pms/controller/CLAUDE.md` is removed) (Invariant 2).
6. **`/strategies` comparative-metrics view** (§3.2.5 row 6) at
   `dashboard/app/strategies/page.tsx` (or successor route) showing
   Brier / P&L / fill rate / slippage / drawdown side-by-side per
   strategy, each grouped by `(strategy_id, strategy_version_id)`;
   replaces S2's listing-only view (Invariant 3; observable-
   capability §1.1.3).
7. **`/metrics` per-strategy breakdown** (§3.2.5 row 7) at
   `dashboard/app/metrics/page.tsx` (or successor route) extending
   the existing global rollup with a per-strategy section keyed on
   `(strategy_id, strategy_version_id)`; cross-strategy aggregates
   remain with explicit ops-view annotation (Invariant 3).

S6's Intake reads from this list as its sole upstream contribution
(S5 is S6's only predecessor edge per §2.1); diffing the two is the
§4.3 gate-4 mechanism that S5 → S6 must satisfy.

### 10.8 Checkpoint skeleton

Flat one-line-each list. **Not harness acceptance criteria** — the
per-CP acceptance criteria, files-of-interest, effort estimates,
and test expectations land in
`.harness/pms-controller-per-strategy-v1/spec.md` when the kickoff
prompt (§10.11) triggers that authoring session.

- **CP1 — Per-strategy `ControllerPipeline` factory (one instance
  per registered strategy, projection-only construction).**
- **CP2 — `Opportunity` entity + emit path replacing stringly-typed
  `stop_conditions` strategy semantics.**
- **CP3 — Per-strategy Evaluator aggregation (`GROUP BY strategy_id,
  strategy_version_id`) + `MetricsCollector.snapshot` rewrite.**
- **CP4 — Inner-ring product-table `NOT NULL` DDL migration +
  `CHECK` constraint (schema-change-only; pre-S5 rows retain S2's
  `"default"` tag as legitimate provenance, no historical
  re-attribution).**
- **CP5 — `TradeDecision` / `OrderState` / `FillRecord` /
  `EvalRecord` field population end-to-end (Controller → Actuator →
  Evaluator forwarding).**
- **CP6 — `/strategies` comparative-metrics view (Brier / P&L /
  fill rate / slippage / drawdown side-by-side per strategy).**
- **CP7 — `/metrics` per-strategy breakdown extending the existing
  global page.**
- **CP8 — Strategy register / unregister lifecycle: pipeline
  acquire + release in the same commit, `try/finally` on all four
  exit paths per 🟡 Lifecycle cleanup.**

### 10.9 Effort estimate

**L** (6–8 CPs per §10.8; touches all four runtime layers except
Sensor — Controller and Evaluator are upgraded, Actuator forwards
the new strategy fields, and the dashboard gains two upgraded views;
concurrent per-strategy pipelines + schema-change-only `NOT NULL`
upgrade over pre-tagged `"default"` rows + dashboard upgrades
collectively make S5 the highest blast-radius sub-spec after S1). The runtime
behavioural surface is the largest of any post-S1 sub-spec: S5 is
where the per-strategy production runtime stops being a "tagged
default" and becomes a concurrent feedback web (Invariant 1) of
multiple strategies, each with its own dispatch chain.

### 10.10 Risk register

| Risk | Likelihood | Impact | Mitigation | Trigger / early-warning |
|---|---|---|---|---|
| `NOT NULL` migration fails on dev databases with rows where the S2 `"default"` seed never tagged historic data (e.g., a dev DB created before S2 landed and not re-seeded) | M | H | Pre-migration probe in CI verifies every pre-existing row already carries a non-NULL `(strategy_id, strategy_version_id)` pair (S2's `"default"` seed) before the `ALTER COLUMN ... SET NOT NULL` statement runs; if any NULL row survives, the migration aborts with a clear remediation ("re-run S2 seed against this DB before applying S5"); rollback to `NULLABLE` is a single statement if the gate trips | Pre-migration probe finds NULL rows on `(strategy_id, strategy_version_id)`; CI migration step fails with a `NOT NULL` constraint violation |
| Per-strategy pipeline memory footprint scales linearly with strategy count and crosses headroom under proliferation | M | M | Each `ControllerPipeline` owns only the per-strategy state it requires (calibrator / sizer / forecaster cache — projection-derived, bounded); a Runner-level `/health` endpoint publishes per-pipeline `sys.getsizeof` rollups and `pool.acquire()` p99 latency; if an upper bound becomes load-bearing, a future retro adds a per-strategy resource cap before §3.4's `StrategyBundle` lands | `/health` reports per-pipeline RSS growth > 50 MB / hour or `pool.acquire()` p99 > 100 ms; new strategies registered cause measurable startup slowdown |
| Race between pipeline dispatch and strategy unregister leaves `Opportunity` queue items, in-flight `TradeDecision` rows, or subscription side-effects orphaned | M | M | Acquire / release wired in the same commit per 🟡 Lifecycle cleanup, with `try/finally` covering every exit path (success, exception, cancel, registry remove); `asyncio.all_tasks()` introspection asserts pipeline-task count drops by exactly one per unregister; behavioural test exercises unregister-while-dispatching with an `Opportunity` mid-flight | Behavioural test reports a non-zero residual task count after unregister; `Opportunity` queue drains slower than registered strategies removed |
| `Opportunity` schema drift breaks Actuator consumers (Actuator silently accepts the change because `stop_conditions` was previously stringly-typed and forgiving) | L | H | `Opportunity` is `@dataclass(frozen=True)` per project convention (typed, mypy-strict-checked); Actuator consumes via Protocol interface in `pms.core.interfaces` so any field-set change at the boundary is a mypy-strict failure; an end-to-end behavioural test pins the `Opportunity` payload shape | mypy strict fails on Actuator consumer; behavioural payload-shape test fails after `Opportunity` field add / remove |
| Dashboard query performance on the multi-strategy comparative view degrades as `eval_records` and `fills` grow | M | M | The `/strategies` view runs against grouped queries with explicit `(strategy_id, strategy_version_id)` indexes added in the same migration (S5's DDL upgrade); query-plan capture in CI catches any seq-scan regression; if N-strategy aggregation latency grows past comfort, a future retro adds materialised views or adds a small precomputed `metrics_snapshot` table (S6 may absorb this work) | `/strategies` page p95 > 1 s in dev with ≥ 3 strategies; CI query-plan check finds an unexpected seq scan |
| Lifecycle cleanup gap on strategy unregister leaks subscription state in `SensorSubscriptionController` (S4 boundary side) | L | M | Unregister path explicitly calls the S4 `SensorSubscriptionController` decrement / removal API (S4 owns the contract per §3.2.4 row 2) inside the `try/finally`; the S4 contract documents idempotent decrement so a double-call cannot regress; reviewer grep on every `acquire` / `add` / `register` call in the diff verifies a matching cleanup call exists per the 🟡 Lifecycle cleanup checklist | Behavioural test asserts `MarketDataSensor` subscription set shrinks by exactly the unregistered strategy's selected markets; a missing cleanup leaves stale subscription entries |

### 10.11 Kickoff Prompt

The block below is **copy-paste-ready** for a fresh Claude session
whose job is to author
`.harness/pms-controller-per-strategy-v1/spec.md` — the harness-
executable spec with per-CP acceptance criteria, files-of-interest,
effort, and intra-spec dependencies. The future session has **no
memory** of this document; the prompt is self-contained.

```
SCOPE:

You are starting harness task `pms-controller-per-strategy-v1` in the
prediction-market-system repository at
/Users/stometa/dev/prediction-market-system. Your job this
session is to author the harness-executable spec file at
/Users/stometa/dev/prediction-market-system/.harness/pms-controller-per-strategy-v1/spec.md
(the directory may not yet exist; you will create it).

REQUIRED READING (ordered — read in this order before touching
anything, all paths absolute):

1. /Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md
   — specifically §10 (this sub-spec's total-spec contract), and
   §3.2.5 (the 7 concepts S5 owns). Cross-reference §3.2.3 + §3.2.4
   for upstream Leave-behind.
2. /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
   — focus on Invariants 2 (aggregate + projections), 3 (immutable
   version tagging — note the `NOT NULL` upgrade landing in S5),
   and 5 (Controller + Evaluator are the strategy-aware boundary).
   Transitive: 6 (active perception — `MarketSelector` produces
   the merged subscription; per-strategy scope comes from
   `Strategy.select_markets(universe)`) and 8 (inner-ring writes).
3. /Users/stometa/dev/prediction-market-system/agent_docs/promoted-rules.md
   — especially Runtime behaviour > design intent (🔴), Comments
   are not fixes (🟡 — applies to every behavioural finding in
   per-strategy dispatch / metrics code), Lifecycle cleanup on all
   exit paths (🟡 — load-bearing for strategy register / unregister
   and pipeline lifecycle), and Fresh-clone baseline verification
   (🟡).
4. /Users/stometa/dev/prediction-market-system/docs/notes/2026-04-16-evaluator-entity-abstraction.md
   — load-bearing sections: §"Execution and Risk Entities" (these
   pre-existing entities gain strategy provenance in S5) and
   §"Evaluation Entities" (the Evaluator-side run-oriented
   abstraction direction; S5 lands `Opportunity` as the bridge
   between Strategy and Actuator per §"Strategy Entities").
5. /Users/stometa/dev/prediction-market-system/src/pms/controller/CLAUDE.md
   — per-layer invariant enforcement for Controller; note the
   "known violation of Invariant 2" callout for stringly-typed
   `stop_conditions` that S5 fixes via the new `Opportunity`
   entity.
6. /Users/stometa/dev/prediction-market-system/src/pms/evaluation/CLAUDE.md
   — per-layer invariant enforcement for Evaluator; note the
   "Currently global; S5 makes per-strategy" callout on
   `metrics.py`.
7. /Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
   — structural reference for harness-grade spec shape (CP shape,
   acceptance-criteria shape, files-of-interest shape).

CURRENT STATE SNAPSHOT:

S1 (pms-market-data-v1), S2 (pms-strategy-aggregate-v1),
S3 (pms-factor-panel-v1), and S4 (pms-active-perception-v1) are
complete; their Leave-behind subsections are landed. §10.6 Intake
items are satisfied (fan-in from S3 + S4): `factor_values` queryable
via `FactorService`, `StrategyConfig.factor_composition` populated,
rules-detector heuristics migrated to raw factors (S3 side);
`MarketSelector` produces the merged subscription set per §3.2.4
row 1, `SensorSubscriptionController` push channel active per
§3.2.4 row 2, `Strategy.select_markets(universe)` method present
on the aggregate per §3.2.4 row 3, boot-order Runner wiring intact
per §3.2.4 row 4 (S4 side). Transitive (per §10.5 Dependencies, NOT
part of §10.6 Intake): S1 outer-ring tables populated, S2 aggregate
+ registry + import-linter rules in place. S5 is the fan-in node of
the §2.1 DAG — both predecessor edges (S3 → S5 and S4 → S5)
terminate here.

PREFLIGHT (boundary check — run before any authoring; every
shell command below assumes cwd is
/Users/stometa/dev/prediction-market-system):

- §10.6 Intake items must all be satisfied. If any fails, HALT and
  tell the user:
  * `uv run pytest -q` passes with ≥ 70 tests + 2 skipped
    (PMS_RUN_INTEGRATION=1 gate), and
    `uv run mypy /Users/stometa/dev/prediction-market-system/src/ /Users/stometa/dev/prediction-market-system/tests/ --strict`
    is clean, on a fresh shell.
  * `factor_values` returns non-empty results for the migrated
    rules-detector factors (S3's Leave-behind);
    `StrategyConfig.factor_composition` reads succeed.
  * `MarketSelector` returns the merged subscription set when
    invoked with the registered strategies; `SensorSubscriptionController`
    push channel is active; `Strategy.select_markets(universe)` is
    callable directly from the per-strategy `ControllerPipeline`
    factory (S5 derives per-strategy market scope from this
    method, not from a per-strategy view on `MarketSelector`).
  * S2's import-linter rules pass on a clean lint pass (transitive
    pre-condition; not part of §10.6 Intake but documented in
    §10.5 Dependencies).

TASK:

Create
/Users/stometa/dev/prediction-market-system/.harness/pms-controller-per-strategy-v1/spec.md
by expanding §10.8 (Checkpoint skeleton) into harness-grade CPs
with:

- Per-CP acceptance criteria (observable, falsifiable, not
  implementation notes).
- Per-CP files-of-interest (absolute paths under
  /Users/stometa/dev/prediction-market-system/).
- Per-CP effort estimate (S / M / L).
- Intra-spec CP dependencies (which CPs block which).

Use
/Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
as the structural reference for shape. Draft only — stop and wait
for spec-evaluation approval before running any checkpoint.

CONSTRAINTS:

- New feature branch `feat/pms-controller-per-strategy-v1` off
  `main`. Never commit to `main` directly.
- Respect §3 Boundary Matrix of the project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md).
  Never claim a concept owned by another sub-spec (S1–S4, S6). The
  7 concepts enumerated in §10.2 Scope-in are the complete
  authorised set for S5; anything else is scope drift.
- Every Strategy / Factor / Sensor / ring-ownership claim must
  cite an invariant number from
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md.
- No `Co-Authored-By` lines in any commit
  (/Users/stometa/dev/prediction-market-system/CLAUDE.md §"Do
  not"; promoted rule "Commit-message precedence").
- Conventional-commit prefixes required (§5.6 of the project
  spec): `feat(<scope>):`, `fix(<scope>):`, `docs(<scope>):`,
  `test(<scope>):`, `refactor(<scope>):`, `chore(<scope>):`.
- Follow the promoted rule 🟡 Lifecycle cleanup on all exit paths
  for the strategy register / unregister surface and the
  per-strategy `ControllerPipeline` lifecycle: acquire + release
  in the same commit, `try/finally` on all four exit paths
  (success, exception, cancel, registry remove).
- Follow the promoted rule 🟡 Comments are not fixes — every
  behavioural finding (per-strategy dispatch, metrics aggregation,
  field population) needs a behavioural fix plus a new test, not
  a docstring.

HALT CONDITIONS:

- Any attempt to add a `strategy_id` or `strategy_version_id`
  column to an outer-ring table (`markets`, `tokens`,
  `book_snapshots`, `book_levels`, `price_changes`, `trades`) or
  a middle-ring table (`factors`, `factor_values`). This is an
  Invariant 8 violation — STOP immediately.
- Any row where `strategy_id` IS NULL or `strategy_version_id`
  IS NULL exists when the `NOT NULL` migration runs. Pre-S5 rows
  must already carry the `"default"` strategy id seeded by S2; if
  a NULL row survives the pre-migration probe, STOP — re-run S2's
  `"default"` seed against the affected database before applying
  S5, and do NOT silently amend the constraint or perform
  invented-provenance re-attribution to a non-`"default"` strategy
  (Invariant 3 — `"default"` is a real strategy, not a sentinel,
  per `agent_docs/architecture-invariants.md` §Invariant 3 Runtime
  evidence).
- Any invariant in
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
  cannot be satisfied by the design you are authoring. Do NOT
  silently amend the invariant. Open a retro under
  /Users/stometa/dev/prediction-market-system/.harness/retro/ and
  return to the user.
- The 7 concepts you author CPs for do not match §3.2.5 of the
  project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md),
  concept-for-concept. Reconcile first.

FIRST ACTION:

Run:

    cd /Users/stometa/dev/prediction-market-system \
      && git status && git branch --show-current \
      && git log --oneline -5

Then read the 7 files in the REQUIRED READING block, in order,
before drafting any content. After reading, report your
understanding of §10.8's 8 checkpoints (CP1–CP8) and wait for
go-ahead before drafting
/Users/stometa/dev/prediction-market-system/.harness/pms-controller-per-strategy-v1/spec.md.
```

---

---

## 11. Sub-spec S6 — pms-research-backtest-v1

S6 is the **terminal node** of the DAG (§2.1): it has no downstream
sub-specs, every upstream sub-spec (S1–S5) has already landed by the
time S6 opens, and S6 closes **no new architecture invariants** —
instead it *uses* all eight (§2.2 row 6 of the node summary; §3.3
Gap-check shows S6 carries Invariants 3 and 8 only via reference, not
ownership). This section expands the skeleton in
`agent_docs/project-roadmap.md` §S6 into the project-level contract
that the harness run under `.harness/pms-research-backtest-v1/` will
consume.

### 11.1 Goal

S6 lands the **research-grade backtest engine** — the offline harness
that lets a researcher generate, compare, and rank N strategies
against a shared market-data window without ever touching the live
runtime. `BacktestSpec` defines a single run by combining a strategy
version (S2), a `BacktestDataset` reference (outer ring per S1, middle
ring per S3), an `ExecutionModel`, a risk policy, a date range, and a
config hash. `ExecutionModel` is the **only** place backtest / paper
/ live legitimately diverge — fill policy, fee model, slippage model,
latency model, and staleness policy live here so the selection path
stays shared (`Factor → StrategySelection → Opportunity →
PortfolioTarget` per §1.1 capability #8). A market-universe replay
engine reads the outer-ring tables for the replay window and drives
`FactorService` (S3) to precompute factor panels once; a parameter
sweep generates N `BacktestSpec` variants that share the same
factor-panel cache (cache-hit-rate becomes an acceptance criterion,
not a hope). `BacktestLiveComparison` reconciles a backtest run
against a live window and reports equity divergence, selection
overlap with an explicit denominator, plus the backtest-only / live-
only symbol sets — answering "which strategy worked, where, why, and
under what market conditions?" (the unanswered question called out in
`docs/notes/2026-04-16-repo-issues-controller-evaluator.md` §3
Evaluator gap). `TimeAlignmentPolicy` + `SymbolNormalizationPolicy`
make that comparison meaningful by aligning timestamps and
identifiers before the diff happens. `SelectionSimilarityMetric`
quantifies overlap with the denominator declared up-front (backtest
set / live set / union — never inferred). `EvaluationReport` ties
run metadata, metrics, benchmarks, and attribution commentary into a
single artifact. `PortfolioTarget` materialises time-indexed target
exposure as the research-side abstraction. The `/backtest` dashboard
page is upgraded from today's smoke-replay view into a ranked
N-strategy comparison page, closing observable-capability item #6 of
§1.1.

### 11.2 Scope (in / out)

**Scope in.** Exactly the 13 concepts owned by S6 in §3.2.6, in the
same order, each with a one-line scope descriptor. Reviewers
verifying boundary integrity grep §3.2.6 against this list; the
lists must match concept-for-concept.

1. **`BacktestSpec` (strategy version + dataset + execution model
   + risk policy + date range + config hash).** Frozen dataclass at
   `src/pms/research/specs.py` (harness spec may choose final path)
   that captures every input necessary to reproduce a backtest;
   `config_hash` is a deterministic hash over the full spec so
   identical specs collide and a sweep can de-duplicate runs
   (Invariant 3 — version provenance carries through into research
   artefacts).
2. **`ExecutionModel` (fill / fee / slippage / latency / staleness
   policy).** Frozen dataclass with named, defaulted fields so a
   single named profile (e.g. `ExecutionModel.polymarket_paper()`)
   can be referenced from multiple `BacktestSpec`s; this is the
   **only** place backtest / paper / live legitimately diverge
   (§1.1 capability #8 + §3.2.6 Notes column).
3. **`BacktestDataset` (source, version, coverage, data-quality
   gaps).** Frozen dataclass referencing outer-ring tables (`book_
   snapshots`, `book_levels`, `price_changes`, `trades`) and middle-
   ring tables (`factor_values`) **by ring**, not by strategy id
   (Invariant 8); records the date range, market-universe filter,
   and any known data-quality gaps so an `EvaluationReport` can warn
   when a backtest straddles a known gap.
4. **`BacktestRun` (materialized run with artifact paths).** Inner-
   ring entity (per `(strategy_id, strategy_version_id)` tagging)
   recording run id, spec hash, status, timestamps, and artifact
   paths. One `BacktestRun` parents many `StrategyRun`s when the
   spec spans multiple strategies (§3.2.6 Notes column).
5. **`StrategyRun` (materialized per-strategy run record for
   backtest runs).** Inner-ring entity capturing per-strategy
   results inside a multi-strategy `BacktestRun`. **Backtest-only**
   — live / paper per-strategy tracking continues to flow through
   inner-ring product tables (`fills`, `eval_records`, …) grouped
   by `(strategy_id, strategy_version_id)` per S5; introducing a
   live `strategy_runs` table would create a reverse S5 → S6 DAG
   edge that §2 forbids (§3.2.6 row 5 Notes verbatim).
6. **Market-universe replay engine (multi-day, multi-market outer-
   ring reader).** Module under `src/pms/research/replay.py`
   (harness spec final path) that reads `book_snapshots` /
   `book_levels` / `price_changes` / `trades` over the
   `BacktestSpec` date range and replays them into the per-strategy
   `ControllerPipeline` flow without touching live sensor adapters.
   Drives `FactorService` (S3) to precompute panels once for the
   replay window so the sweep cache (concept #7) can hit (Invariant
   8 — replay reads outer ring; never writes).
7. **Parameter sweep (generate N `BacktestSpec`s, compare results
   with shared factor-panel cache).** CLI or programmatic entry
   point (`uv run pms-research sweep ...` or equivalent) that
   takes a sweep spec (parameter grid + base `BacktestSpec`),
   materialises N variants, runs each through the replay engine,
   and emits the comparison artefact. The factor-panel cache is
   keyed so identical factor inputs across variants produce a hit
   (Invariant 4 — raw factors are reusable across strategies, hence
   across `BacktestSpec` variants that differ only on composition).
8. **`BacktestLiveComparison` (equity divergence + selection
   overlap + backtest-only / live-only opportunities).** Module +
   entity that aligns a backtest `BacktestRun` against a live
   window (filtered from inner-ring product tables) and emits
   equity-curve delta, the `SelectionSimilarityMetric` value,
   backtest-only symbol set, and live-only symbol set. Closes the
   "validation loop before live promotion" lesson from
   `docs/notes/2026-04-16-evaluator-entity-abstraction.md`
   §"Production/live framework lessons".
9. **`TimeAlignmentPolicy` + `SymbolNormalizationPolicy`.** Frozen
   policy dataclasses that the comparison module applies before
   diffing. `TimeAlignmentPolicy` resolves generated / exchange /
   ingest / evaluation timestamp skew (the select-coin
   `hour_offset` analogue called out in
   `docs/notes/2026-04-16-evaluator-entity-abstraction.md`
   §"Evaluation Entities"). `SymbolNormalizationPolicy` maps venue
   identifiers, token ids, and human-readable symbols across the
   backtest dataset and live artefacts.
10. **`SelectionSimilarityMetric` (denominator explicit: backtest
    set / live set / union).** Frozen metric definition that **must**
    declare its denominator at construction time; the metric value
    has no meaning without it (§Risk register row 4). Used inside
    `BacktestLiveComparison` and exposed as a column on the
    `/backtest` page.
11. **`EvaluationReport` (run metadata + metrics + attribution +
    benchmarks).** Inner-ring entity tying together a `BacktestRun`
    or `BacktestLiveComparison` with: run metadata (spec hash,
    dataset reference, execution model name), metric values
    (Brier / Sharpe / cumulative P&L / drawdown / fill rate /
    slippage), benchmark comparison rows, attribution commentary,
    warnings (data-quality gaps, time-alignment corrections), and
    a recommended-next-action field. `FactorAttribution` as a
    dedicated table is intentionally deferred (§3.4); attribution
    appears here as commentary, not as its own row type.
12. **`PortfolioTarget` (time-indexed target exposure per
    strategy).** Frozen dataclass capturing a strategy's desired
    exposure per `(market_id, token_id, side, ts)` over the
    backtest window. Research-side abstraction — the live runtime
    continues to produce `TradeDecision` directly through the per-
    strategy `ControllerPipeline` (§3.2.6 row 12 Notes verbatim).
13. **`/backtest` ranked N-strategy comparison view.** Dashboard
    page that ranks N strategies by a configurable metric (Brier /
    Sharpe / cumulative P&L) with the underlying `BacktestRun` +
    `StrategyRun` rows queryable by run id; upgrades today's
    `/backtest` page (§1.1 capability #6).

**Scope out.** The following are S6-adjacent but are owned by other
sub-specs **or** are intentionally deferred per §3.4. A PR landing
any of these under `.harness/pms-research-backtest-v1/` is scope
drift; entities listed in §3.4 are explicitly **deferred** by the
project decomposition and reviving them as first-class citizens
inside S6 is a §3.4 violation that requires a §3.2 amendment in the
same PR (per §3.1's "When adding a new concept not in the matrix").

Owned by other sub-specs:

- Outer-ring DDL + sensors + the `asyncpg.Pool` lifecycle + JSONL→PG
  unification → **S1** (§3.2.1). S6 is a **reader** of outer-ring
  tables; never writes them, never adds a `strategy_id` column to
  any of them (Invariant 8).
- `Strategy` aggregate, projection types (`StrategyConfig`,
  `RiskParams`, `EvalSpec`, `ForecasterSpec`,
  `MarketSelectionSpec`), `strategies` / `strategy_versions` /
  `strategy_factors` tables, `PostgresStrategyRegistry`, import-
  linter rules, `"default"` strategy seed → **S2** (§3.2.2). S6 is
  an aggregate **reader** (Controller / Evaluator allow-list per
  Invariant 5).
- `factors` / `factor_values` tables, `FactorService`, raw factor
  definitions, `StrategyConfig.factor_composition`, `/factors`
  page → **S3** (§3.2.3). S6 invokes `FactorService` to precompute
  panels; never adds new factors (any new factor required by a
  backtest must land via S3, not S6 — Invariant 4).
- `MarketSelector`, `SensorSubscriptionController`,
  `Strategy.select_markets(universe)`, Runner boot-order wiring →
  **S4** (§3.2.4). S6 reads `select_markets` output to compute the
  backtest's market universe; never writes a `market_ids` list
  through any other channel.
- Per-strategy `ControllerPipeline` dispatch, per-strategy
  `Evaluator` aggregation, `(strategy_id, strategy_version_id)`
  `NOT NULL` upgrade, `TradeDecision` / `OrderState` /
  `FillRecord` / `EvalRecord` strategy-field population, the
  `Opportunity` entity, `/strategies` comparative-metrics view,
  `/metrics` per-strategy breakdown → **S5** (§3.2.5). S6 reuses
  the per-strategy `Evaluator` aggregation shape inside the
  backtest evaluator; never re-implements it.

Intentionally deferred per §3.4 (NOT to be revived by S6):

- **`StrategyBundle`** (multi-strategy group mapping to one live
  account) — deferred per §3.4. S6 may run multiple strategies
  through one `BacktestRun` (one-to-many `BacktestRun` →
  `StrategyRun`), but the *bundle* abstraction with shared
  account / risk-budget semantics is post-S5 work and would expand
  the schema beyond S6's scope.
- **`MarketUniverse` as a first-class entity** — deferred per §3.4.
  S6 treats the backtest universe as a derived view: the rows in
  `markets` matching the `BacktestDataset.market_universe_filter`
  for the spec date range. Persisting a separate `market_universes`
  table is not S6 work; if S6 discovers it must be persisted (e.g.,
  for dated universe snapshots), §3.4 + §3.2 are amended in the
  same PR before any code lands.
- **`FactorAttribution` as a dedicated table** — deferred per §3.4.
  `EvaluationReport` may carry attribution **commentary** as a text
  field, but a dedicated `factor_attribution` row type is not S6
  work; if the research workflow proves the need, §3.4 + §3.2 are
  amended in a follow-on retro and matrix update.
- **`SelectionSnapshot` / `PriceLevel` / `MarketUpdate` as
  persisted event types** — deferred per §3.4. The outer-ring
  tables already encode equivalent state; S6 reads them as-is.
- **Live-execution infrastructure** — stays in S5 plus the existing
  `live_trading_enabled=false` guard (§1.2 first bullet). S6 is
  offline-research-only; flipping the live gate is a human decision
  outside this decomposition.
- **Kalshi venue integration** — venue-agnostic interfaces stay
  in place per §1.2; a Kalshi adapter pair is a follow-on after S6
  (§1.2 third bullet). S6 ships against Polymarket-only data.

### 11.3 Acceptance criteria (system-level)

Eight observable-capability bullets. Each is verifiable at the
system level — not a checklist item for an individual CP.

1. **Single-spec end-to-end run produces a ranked report.** A
   `BacktestSpec` with **3 strategies** + a **30-day date window**
   + a shared `ExecutionModel` runs end-to-end through the replay
   engine and produces a ranked `EvaluationReport` within a
   configurable time budget. The report includes per-`StrategyRun`
   Brier, cumulative P&L, drawdown, and fill-rate columns, all
   tagged with `(strategy_id, strategy_version_id)` (Invariant 3 —
   research artefacts inherit version provenance).
2. **Parameter sweep hits the shared factor-panel cache.** A sweep
   over **10 `BacktestSpec` variants** that share an
   `ExecutionModel` and a date range completes with a factor-panel
   cache **hit rate > 95%** measured across variants. The replay
   engine reuses the precomputed panel; only the strategy
   composition (`StrategyConfig.factor_composition`) varies across
   variants. A measured hit rate ≤ 95% **fails the gate** and
   stops the sweep with a diagnostic — the cache is the load-
   bearing performance contract for the sweep, not an optimisation
   (§Risk register row 1).
3. **`BacktestLiveComparison` against a live window emits the four
   required artefacts.** A comparison against a **7-day live
   window** reports: equity-curve delta (per-day), selection-
   overlap ratio with **explicit denominator** (backtest set /
   live set / union — declared up-front, never inferred per §Risk
   register row 4), backtest-only symbol set, live-only symbol
   set. Each artefact references the same
   `(strategy_id, strategy_version_id)` pair on both sides
   (Invariant 3).
4. **`/backtest` page ranks N strategies and links to underlying
   rows.** The dashboard page renders an N-row ranked table sorted
   by a **user-configurable metric** (default Brier; switchable to
   Sharpe and cumulative P&L); each row drills into the
   `BacktestRun` + `StrategyRun` rows queryable by run id
   (capability #6 of §1.1; observable via Playwright e2e against
   a fixture run).
5. **Replay engine is read-only against outer + middle rings.**
   Running the replay engine against a fresh PostgreSQL with read-
   only outer-ring + middle-ring credentials succeeds; any attempt
   to write to `book_snapshots` / `book_levels` / `price_changes`
   / `trades` / `factors` / `factor_values` from inside the replay
   engine fails the test (Invariant 8 — outer ring is shared and
   strategy-agnostic; middle ring is shared cache).
6. **`BacktestSpec.config_hash` is deterministic and stable.** Two
   `BacktestSpec` instances with byte-identical fields (modulo
   field ordering) produce the **same** `config_hash`; any field
   change produces a different hash; the hash is recorded on
   `BacktestRun.spec_hash` and is queryable for de-duplication
   inside the sweep (Invariant 3 — version hashing extends to
   research artefacts).
7. **No outer-ring or middle-ring DDL gains a `strategy_id` or
   `strategy_version_id` column under S6.** S6 ships zero schema
   migrations against `markets`, `tokens`, `book_snapshots`,
   `book_levels`, `price_changes`, `trades`, `factors`,
   `factor_values`. The mechanical schema grep that witnesses this
   invariant lives in §5.3 Mixed-invariant evidence; AC #7 names
   the obligation here so reviewers can spot scope drift early
   (Invariant 8).
8. **Canonical gates green on a fresh clone.** `uv run pytest -q`
   passes with ≥ 70 tests + 2 skipped (integration gated on
   `PMS_RUN_INTEGRATION=1`) and `uv run mypy src/ tests/ --strict`
   is clean, on a fresh shell per 🟡 Fresh-clone baseline
   verification (§5.1, §5.2).

### 11.4 Non-goals

Explicit deferrals. An S6 harness spec that lands any of these is
scope drift. Items marked **(§3.4)** are entities the project
decomposition has *intentionally deferred* — reviving them as
first-class citizens inside S6 requires a §3.2 + §3.4 amendment in
the same PR before any code lands.

- **Kalshi venue integration.** Venue-agnostic interfaces remain
  in place per §1.2, but a Kalshi sensor / actuator pair is a
  follow-on effort after S6 ships. S6 ships against Polymarket
  data only — both the replay engine and the comparison tool
  assume the outer-ring tables filled by S1's
  `MarketDataSensor`. Cross-venue replay is not S6 scope.
- **Portfolio-level risk optimization.** `PortfolioTarget` lands
  as a research-side dataclass (concept #12) capturing per-
  strategy time-indexed exposure, but a portfolio optimiser that
  reweights `PortfolioTarget`s across strategies is not S6 work.
  Each strategy's `PortfolioTarget` stands on its own; aggregation
  across strategies is left to a follow-on retro-triggered design.
- **Automated paper/live → backtest promotion workflow.**
  `BacktestLiveComparison` produces the artefacts a human reviewer
  needs to make a promotion decision (capability #3 above), but
  the **promotion itself** stays a human action — automated
  promotion is explicitly out of scope (matches §1.2 second
  bullet on "no automated feedback loop reconfigures
  strategies").
- **ML-driven parameter search.** The sweep is **grid-based** in
  S6 — generate N `BacktestSpec`s by enumerating a parameter grid
  declared up-front. Bayesian / RL / surrogate-model parameter
  search is not S6 work; the sweep entry point's API can be
  extended later, but the S6 deliverable is the grid baseline.
- **Streaming backtest.** A `BacktestRun` is **batch**: the spec
  defines a closed date range, the replay engine reads all
  matching outer-ring rows up-front (or in bounded chunks), and
  the run terminates when the window completes. Incremental /
  streaming backtest that consumes new outer-ring rows as they
  arrive is not S6 work — that pattern blurs the offline-research
  / live-runtime boundary the decomposition is built around.
- **Multi-account `StrategyBundle` mapping (§3.4).** Per §3.4,
  `StrategyBundle` is intentionally deferred. S6 supports
  multiple strategies inside one `BacktestRun` (one-to-many
  `BacktestRun` → `StrategyRun`) but does **not** introduce a
  bundle abstraction with shared account / risk-budget semantics.
  Reviving this entity requires the §3.2 + §3.4 amendment cited
  above.
- **Persisting `FactorAttribution` rows (§3.4).** Per §3.4,
  `FactorAttribution` as a dedicated table is intentionally
  deferred. `EvaluationReport` may carry attribution **commentary**
  as a text field — sufficient for the research workflows S6
  ships against — but a `factor_attribution` row type with its
  own DDL is post-S6 work.
- **`MarketUniverse` as a first-class entity (§3.4).** Per §3.4,
  the universe is the implicit set of `markets` rows matching the
  `BacktestDataset.market_universe_filter`. S6 does **not** add a
  `market_universes` table or a dated-universe-snapshot mechanism.
  If S6 discovers persistence is required, §3.2 + §3.4 are
  amended before any DDL lands.
- **`SelectionSnapshot` / `PriceLevel` / `MarketUpdate` as
  persisted event types (§3.4).** Per §3.4, the outer-ring tables
  already encode equivalent state; S6 reads them as-is. No new
  event-type tables are introduced.

### 11.5 Dependencies

- **Upstream sub-specs.** Direct edge: **S5** (per-strategy
  Controller dispatch + per-strategy Evaluator aggregation +
  `(strategy_id, strategy_version_id)` `NOT NULL` upgrade +
  `Opportunity` entity + `/strategies` comparative view +
  `/metrics` per-strategy breakdown). Transitive predecessors via
  the §2.1 DAG: **S1** (outer-ring tables for replay), **S2**
  (`Strategy` aggregate + projections + `strategy_versions` for
  `BacktestSpec` reproducibility), **S3** (factor panel +
  `FactorService` invocation for the shared sweep cache),
  **S4** (`select_markets` output for backtest universe filtering).
- **Upstream invariants.** S6 **uses every invariant** but
  **closes none new** (§2.2 row 6, §3.3 Gap-check row legend "S6
  carries Invariants 3 and 8 only via reference, not ownership").
  S6 introduces **no new mechanical enforcement hooks** — every
  Mechanically-checkable and Mixed invariant is enforced by a
  machine check owned by an earlier sub-spec, and S6 inherits
  those checks as-is. Behavioural invariants (1, 2, 4, 7) have
  no machine check by their nature (§5.3 "Behavioural" bucket);
  S6 discharges them through its own acceptance criteria and
  code review, just like every prior sub-spec does. The
  conformance bar applies to all 8 invariants per §5.3 buckets:
  - **Mechanically checkable**: 5 — the existing import-linter
    rule (S2-owned, §3.2.2) banning `pms.sensor` / `pms.actuator`
    from importing `pms.strategies.aggregate` or `pms.controller.*`
    is the authoritative enforcement; S6 does **not** introduce a
    new `pms.research.*` linter rule. S6's obligation is to author
    research code that does not push Sensor or Actuator across
    that boundary (e.g., the replay engine reads outer-ring
    tables directly via `PostgresMarketDataStore`, not by importing
    Sensor; the comparison module reads inner-ring tables, not
    Actuator).
  - **Mixed (machine + review)**: 3 (every research artefact
    carries `(strategy_id, strategy_version_id)` and every
    aggregation `GROUP BY`s on it; the schema mechanical check is
    the S5-owned `NOT NULL` + `CHECK` constraint per §5.3
    Invariant 3 evidence — S6 inherits it), 6 (replay engine
    derives the backtest universe via `Strategy.select_markets`
    (S4-owned), never via a sensor-side or research-side static
    list; the import-linter rule against Sensor importing
    `pms.market_selection` is S2-owned and unchanged), 8 (no
    `strategy_id` / `strategy_version_id` columns added to outer-
    or middle-ring tables under S6; AC #7 above is the local
    obligation, the S1 + S3 owned grep is the mechanical check).
  - **Behavioural**: 1 (replay engine runs the per-strategy
    `ControllerPipeline` flow concurrently across strategies in
    one `BacktestRun`, not phased), 2 (research code is an
    aggregate reader allowed by §3.2.6 — same allow-list shape as
    S5 + Controller / Evaluator; S6 introduces no new aggregate
    consumers beyond research code itself), 4 (every new factor
    required by a backtest must land via S3 first as a raw
    `FactorDefinition`; S6 never registers a research-only factor),
    7 (S6 does not add Sensor classes; the replay engine is a
    reader of outer-ring tables, not a third sensor).

### 11.6 Intake

Minimum set that must exist before a harness run opens under
`.harness/pms-research-backtest-v1/`. S5's Leave-behind (authored
in Commit 8 of this document authoring effort) is the **primary
fan-in**; S3, S4, S2, S1 contribute through the §2.1 DAG transitive
edges that funnel into S5. The list below is what reviewers diff
against the union of predecessor Leave-behinds during the §4.3
gate-4 boundary-integrity check.

1. **Per-strategy `ControllerPipeline` dispatch operational** (S5
   primary, §3.2.5 row 1). The `BacktestRun` execution loop runs
   the same per-strategy dispatch the live runtime uses; without
   this, the backtest compares a global controller against itself
   — the very pathology §4.2 names as the reason S6 must be last.
2. **Per-strategy `Evaluator` aggregation shape stable** (S5
   primary, §3.2.5 row 2). The backtest evaluator reuses this
   `GROUP BY (strategy_id, strategy_version_id)` shape inside
   `EvaluationReport` (Invariant 3); a different aggregation
   shape between live and backtest would break §1.1 capability #8
   ("shared selection path; divergence only in `ExecutionModel`").
3. **`(strategy_id, strategy_version_id)` `NOT NULL` on inner-
   ring product tables** (S5 primary, §3.2.5 row 3). Pre-S5 the
   columns are `NULLABLE` with `"default"` tagging; post-S5 every
   product row carries a real strategy version. S6's
   `BacktestRun` / `StrategyRun` populate the same columns — a
   pre-S5 backtest would write `NULL` (or `"default"`) and break
   reproducibility (Invariant 3).
4. **`Opportunity` entity stable in shape** (S5 primary, §3.2.5
   row 5). `BacktestRun` artefacts reference `Opportunity` rows
   with the same fields the live runtime produces (selected
   factor values, expected edge, rationale); changing the shape
   under S6 would re-litigate S5 work.
5. **`/strategies` comparative view + `/metrics` per-strategy
   breakdown shipped** (S5 primary, §3.2.5 rows 6–7). The
   `/backtest` page (concept #13) reuses the comparative-view
   patterns shipped in S5; without them, S6 would be re-
   inventing a UI baseline.
6. **`FactorService` reader interface stable** (S3 transitive,
   §3.2.3 row 4). The replay engine drives `FactorService` to
   precompute panels for the replay window; the reader signature
   must already exist (S3 owns the **definition**; S6 is a
   **consumer** — Invariant 4 is enforced by S3 rejecting any
   composite factor that the sweep tries to introduce as a "raw"
   one).
7. **Outer-ring tables populated with sufficient history** (S1
   transitive, §3.2.1 rows 1–6 + 11). The replay engine reads
   `book_snapshots` / `book_levels` / `price_changes` / `trades`
   over the spec date range; if the outer ring contains less than
   the requested window, the run terminates with a diagnostic, not
   a partial result.
8. **`Strategy` aggregate + `strategy_versions` table populated**
   (S2 transitive, §3.2.2 rows 1, 4). `BacktestSpec` references
   a `(strategy_id, strategy_version_id)` pair that must already
   exist in `strategy_versions`; the deterministic config hash on
   `BacktestSpec` (concept #1) is what makes this lookup stable
   across sweep variants (Invariant 3 — version hashing already
   established by S2).
9. **`MarketSelector` + `Strategy.select_markets(universe)` stable**
   (S4 transitive, §3.2.4 rows 1, 3). The replay engine derives
   the backtest universe by invoking the same `select_markets`
   method that `MarketSelector` uses to drive live subscription
   (Invariant 6 — active perception). Without this, the backtest
   would have to re-implement universe derivation, breaking §1.1
   capability #8 ("shared selection path; divergence only in
   `ExecutionModel`") and re-litigating the S4 design.
10. **Canonical gates green on a fresh clone.** `uv run pytest
    -q` passes with ≥ 70 tests + 2 skipped; `uv run mypy src/
    tests/ --strict` is clean (§5.1, §5.2; 🟡 Fresh-clone
    baseline verification).

### 11.7 Leave-behind

S6 is the **terminal sub-spec** of the §2.1 DAG: no downstream
sub-spec consumes its Leave-behind. The list below exists so the
§4.3 gate-4 boundary-integrity check can confirm S6's outputs match
its §3.2.6 ownership **row-for-row** (13 numbered items, each
keyed back to a specific §3.2.6 row), and so a future post-S6
retro can audit what landed. Documentation deliverables that are
not §3.2.6 rows are listed under **Companion artefacts** below the
row-matched list.

1. **`BacktestSpec`** (§3.2.6 row 1) at
   `src/pms/research/specs.py` (harness spec final path) as a
   frozen dataclass with deterministic `config_hash` and a
   `from_strategy_version(...)` constructor that pulls
   `(strategy_id, strategy_version_id)` from S2's
   `strategy_versions`.
2. **`ExecutionModel`** (§3.2.6 row 2) at
   `src/pms/research/execution.py` (or co-located with
   `BacktestSpec`) as a frozen dataclass with named profile
   constructors (`ExecutionModel.polymarket_paper()`,
   `ExecutionModel.polymarket_live_estimate()`); fields cover
   fill / fee / slippage / latency / staleness policy.
3. **`BacktestDataset`** (§3.2.6 row 3) as a frozen dataclass
   referencing outer-ring + middle-ring tables by ring (Invariant
   8); records date range, market-universe filter, and known
   data-quality gaps.
4. **`BacktestRun` DDL + entity** (§3.2.6 row 4) added to
   `schema.sql` as an inner-ring table carrying `(strategy_id,
   strategy_version_id)` `NOT NULL`; records run id, spec hash,
   status, timestamps, artifact paths. Parents `StrategyRun`
   (item 5) via a `run_id` foreign key for multi-strategy runs.
5. **`StrategyRun` DDL + entity** (§3.2.6 row 5) added to
   `schema.sql` as an inner-ring table carrying `(strategy_id,
   strategy_version_id)` `NOT NULL` plus the parent `run_id`.
   **Backtest-only** — no live `strategy_runs` table is created
   (§3.2.6 row 5 Notes; introducing one would create a reverse
   S5 → S6 DAG edge that §2 forbids).
6. **Market-universe replay engine** (§3.2.6 row 6) at
   `src/pms/research/replay.py` driving the per-strategy
   `ControllerPipeline` flow over outer-ring rows in the spec
   window; read-only against outer + middle rings (AC #5).
7. **Parameter sweep CLI / entry point** (§3.2.6 row 7) at
   `src/pms/research/sweep.py` (or `pms-research sweep` console
   script) that materialises N `BacktestSpec`s from a sweep spec,
   executes each via the replay engine, and emits a comparison
   artefact. Factor-panel cache hit rate observable + asserted in
   tests (AC #2).
8. **`BacktestLiveComparison`** (§3.2.6 row 8) as a module +
   inner-ring entity emitting equity-curve delta + selection-
   overlap (with declared denominator) + backtest-only / live-
   only symbol sets. Returns an `EvaluationReport` (item 11).
9. **`TimeAlignmentPolicy` + `SymbolNormalizationPolicy`**
   (§3.2.6 row 9) as frozen policy dataclasses applied inside
   `BacktestLiveComparison` before diffing.
10. **`SelectionSimilarityMetric`** (§3.2.6 row 10) as a frozen
    metric definition that **requires** an explicit denominator at
    construction; usable inside `BacktestLiveComparison` and
    surfaced on `/backtest`.
11. **`EvaluationReport`** (§3.2.6 row 11) as an inner-ring
    entity tying `BacktestRun` (or `BacktestLiveComparison`) to
    metrics + benchmarks + attribution commentary + warnings +
    next-action field. `FactorAttribution` rows are NOT created
    (§3.4).
12. **`PortfolioTarget`** (§3.2.6 row 12) as a frozen dataclass
    capturing time-indexed target exposure per strategy; emitted
    by the replay engine as a research-side artefact.
13. **`/backtest` ranked N-strategy comparison view** (§3.2.6
    row 13) on the dashboard, drilling from a ranked table into
    `BacktestRun` + `StrategyRun` detail; verified by Playwright
    e2e against a fixture run.

**Companion artefacts** (not §3.2.6 rows; documentation
deliverables that accompany the 13 row-matched items above):

- **Documented `BacktestSpec` config format.** Reference
  documentation (likely under `docs/research/` or `agent_docs/`)
  describing how to author a `BacktestSpec` / sweep spec by hand
  — sufficient for a future researcher to onboard without reading
  the code. Lands as part of CP-DC (§11.8). Not a §3.2.6 row, so
  not counted in the row-for-row boundary-integrity check; tracked
  separately so a reviewer can still confirm the doc shipped.

### 11.8 Checkpoint skeleton

Flat one-line-each list — the **largest sub-spec by CP count**
because S6 carries: research-grade entity DDL, a replay engine, a
sweep workflow with cache contract, comparison tooling, evaluation-
report wiring, and a dashboard rewrite. **Not harness acceptance
criteria** — the per-CP acceptance criteria, files-of-interest,
effort estimates, and test expectations land in
`.harness/pms-research-backtest-v1/spec.md` when the kickoff prompt
(§11.11) triggers that authoring session.

- **CP1 — `BacktestSpec` + `ExecutionModel` + `BacktestDataset`
  entities (frozen dataclasses + deterministic `config_hash`).**
- **CP2 — Market-universe replay engine reading outer-ring tables
  over a date range (read-only against outer + middle rings).**
- **CP3 — `FactorService` integration: precompute panels for the
  replay window; cache keyed for sharing across sweep variants.**
- **CP4a — `BacktestRun` + `StrategyRun` DDL (frozen-dataclass
  entities + `schema.sql` extension; columns include
  `(strategy_id, strategy_version_id)` `NOT NULL` per Invariant 3).**
- **CP4b — Run-execution loop populating `BacktestRun` and
  `StrategyRun` rows end-to-end (consumes CP4a's DDL, drives a
  single end-to-end backtest from `BacktestSpec` input to populated
  rows).**
- **CP5 — `EvaluationReport` generator (metrics + benchmarks +
  attribution commentary + ranking).**
- **CP6 — Parameter sweep CLI / entry point + shared-cache
  guarantee (cache-hit-rate > 95% asserted in tests).**
- **CP7 — `TimeAlignmentPolicy` + `SymbolNormalizationPolicy` +
  `SelectionSimilarityMetric` (with explicit denominator).**
- **CP8 — `BacktestLiveComparison` tooling (equity delta +
  selection overlap + backtest-only / live-only symbol sets).**
- **CP9 — `PortfolioTarget` materialisation in the replay engine
  output.**
- **CP10 — `/backtest` ranked N-strategy comparison dashboard
  page + Playwright e2e.**
- **CP-DC — `BacktestSpec` config-format documentation (spec-
  review checkpoint, no runtime code).**

### 11.9 Effort estimate

**L** (8–10 CPs by the §11.8 skeleton — actually 11 with CP-DC
included, the largest sub-spec by CP count of S1–S6). S6 carries
the broadest research surface area: research-grade entities and
their DDL (`BacktestSpec`, `ExecutionModel`, `BacktestDataset`,
`BacktestRun`, `StrategyRun`, `EvaluationReport`, `PortfolioTarget`,
plus three policies and two metric definitions), a market-universe
replay engine that drives `FactorService` against precomputed
panels, a parameter-sweep workflow with a load-bearing cache
contract, a comparison module with two policy classes plus the
`SelectionSimilarityMetric`, a dashboard rewrite, and config-format
documentation. As the **terminal** sub-spec of the §2.1 DAG, S6
benefits from the most stable foundation — every prior sub-spec's
invariants are already enforced, every dashboard baseline is
already shipped — which is what makes the broadest scope tractable
in one harness run.

### 11.10 Risk register

| Risk | Likelihood | Impact | Mitigation | Trigger / early-warning |
|---|---|---|---|---|
| Factor-panel cache invalidation correctness across sweep variants (a wrong cache hit silently corrupts results) | M | H | Cache key includes the full set of `(factor_id, param, market_id, ts_window)` inputs the variant reads; cache-key derivation is unit-tested for every sweep variant pair (variant A + variant B with one parameter difference must produce different cache keys for the affected factor); sweep run logs the cache-hit-rate plus the per-variant key set so manual audit is possible | Two sweep variants that should differ produce identical `EvaluationReport.metrics`; cache-hit-rate goes to 100% on a sweep that has at least one variant differing in a factor parameter (must be < 100% on such sweeps) |
| Replay-engine performance on multi-year windows (the engine reads every outer-ring row in the window; volume scales with date range × market count) | M | M | Replay engine reads in bounded chunks (week-sized by default, configurable on `BacktestSpec`); the chunk boundary is an explicit field on the spec so a researcher can tune; `BacktestSpec` time-budget gate (AC #1) fails the run if the chunk strategy is mis-sized | Single `BacktestRun` runtime exceeds the `BacktestSpec.time_budget` field; `pool.acquire()` p99 latency on the replay engine's read connection > 500 ms |
| `ExecutionModel` defaults drift from live reality (backtest success → live disappointment) | M | H | Named profile constructors (`ExecutionModel.polymarket_paper()`, `ExecutionModel.polymarket_live_estimate()`) carry the canonical defaults; profile values are sourced from a documented fixture (live Polymarket fee schedule, observed slippage from `/metrics` per-strategy slippage column shipped in S5, observed latency from sensor watchdog telemetry); `BacktestLiveComparison` AC (#3) explicitly surfaces equity-delta — a persistent positive delta across runs is the early warning that the model is too generous | `BacktestLiveComparison` equity-delta consistently positive across multiple comparison runs; reviewer notes a `BacktestSpec` overriding profile defaults without justification |
| `SelectionSimilarityMetric` denominator ambiguity surfacing as reviewer disagreement (overlap can be measured against backtest set, live set, or union — each yields a different number) | M | M | The metric class **requires** the denominator at construction time (concept #10 row text + AC #3 explicit-denominator language); attempting to construct without a denominator is a type error; `BacktestLiveComparison` records the chosen denominator on the report so two reviewers cannot read different numbers from the same artefact | A reviewer comments on a `BacktestLiveComparison` artefact citing a different overlap number than the report shows; an `EvaluationReport` is generated without the denominator field populated |
| `BacktestLiveComparison` requiring time-alignment corrections that themselves need validation (a wrong `TimeAlignmentPolicy` produces convincing-looking but incorrect comparisons) | M | M | `TimeAlignmentPolicy` is a frozen dataclass with named offsets (generated / exchange / ingest / evaluation skew, each separately settable); the comparison module records the applied policy on the `EvaluationReport.warnings` field whenever a non-identity policy is applied; per-policy unit tests cover an identity case + at least one non-trivial offset; reviewers can replay the policy against fixture timestamps | A `BacktestLiveComparison` shows zero equity-delta across two runs that should differ (suggests a policy is masking the divergence); `EvaluationReport.warnings` empty on a comparison that applied a non-identity policy |
| Scope creep into deferred entities (§3.4) during CP execution — a CP that "almost needs" `StrategyBundle` / `MarketUniverse` / `FactorAttribution` / `SelectionSnapshot` / `PriceLevel` / `MarketUpdate` quietly introduces it as a "small" entity | M | H | §11.4 Non-goals explicitly cite §3.4 entry-by-entry; reviewer workflow grep on every CP PR for new class / table names against §3.4 list (per §3.1 + §5.4 boundary-matrix audit); HALT condition in §11.11 Kickoff Prompt makes the rule explicit at session start; any §3.4 revival requires a §3.2 + §3.4 amendment in the same PR before any code lands | A PR introduces a `StrategyBundle` / `MarketUniverse` / `FactorAttribution` / `SelectionSnapshot` / `PriceLevel` / `MarketUpdate` class or table without an accompanying §3.2 + §3.4 amendment |
| Live + backtest artefact-shape divergence (selection path drifts so a live `Opportunity` and a backtest `Opportunity` no longer compare row-for-row) | L | H | `Opportunity` entity is owned by S5 (§3.2.5 row 5) — S6 reuses it verbatim; any S6 PR introducing a research-only "Opportunity-like" type fails the §5.4 Boundary Matrix audit; `BacktestLiveComparison` AC (#3) requires the same `(strategy_id, strategy_version_id)` pair on both sides — a shape mismatch fails the comparison test | A research-only entity shaped like `Opportunity` appears under `src/pms/research/`; `BacktestLiveComparison` test fails with a field-mismatch error |

### 11.11 Kickoff Prompt

The block below is **copy-paste-ready** for a fresh Claude session
whose job is to author `.harness/pms-research-backtest-v1/spec.md`
— the harness-executable spec with per-CP acceptance criteria,
files-of-interest, effort, and intra-spec dependencies. The future
session has **no memory** of this document; the prompt is
self-contained.

```
SCOPE:

You are starting harness task `pms-research-backtest-v1` in the
prediction-market-system repository at
/Users/stometa/dev/prediction-market-system. Your job this
session is to author the harness-executable spec file at
/Users/stometa/dev/prediction-market-system/.harness/pms-research-backtest-v1/spec.md
(the directory may not yet exist; you will create it).

REQUIRED READING (ordered — read in this order before touching
anything, all paths absolute):

1. /Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md
   — specifically §11 (this sub-spec's total-spec contract),
   §3.2.6 (the 13 concepts S6 owns), §3.4 (entities deliberately
   out of scope — DO NOT revive without a §3.2 + §3.4 amendment
   in the same PR), §4.3 gate 4 (Leave-behind / Intake diff with
   S5 as the primary fan-in).
2. /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
   — read all 8 invariants. S6 USES every invariant but CLOSES
   none new. The conformance bar in §5.3 of the project spec
   applies in full: mechanically checkable (5), Mixed (3, 6, 8),
   behavioural (1, 2, 4, 7).
3. /Users/stometa/dev/prediction-market-system/agent_docs/promoted-rules.md
   — especially Runtime behaviour > design intent (🔴), Review-
   loop rejection discipline (🔴), Comments are not fixes (🟡),
   Lifecycle cleanup on all exit paths (🟡 — relevant for the
   `asyncpg` read connections inside the replay engine and the
   factor-panel cache lifecycle), Fresh-clone baseline verification
   (🟡), Integration test default-skip pattern (🟢), Piecewise-
   domain functions (🟡 — relevant for `ExecutionModel` slippage /
   fill regimes).
4. /Users/stometa/dev/prediction-market-system/docs/notes/2026-04-16-evaluator-entity-abstraction.md
   — load-bearing sections: §"Evaluation Entities" block
   (BacktestSpec, ExecutionModel, BacktestRun, StrategyRun,
   EvaluationMetric, StrategyMetrics, EvaluationReport,
   BacktestLiveComparison, SelectionSimilarityMetric,
   TimeAlignmentPolicy, SymbolNormalizationPolicy);
   §"Reference Backtesting Repos" (select-coin lessons:
   factor-first, strategy-then, execution-last; backtest/live
   diverge only in execution).
5. /Users/stometa/dev/prediction-market-system/docs/notes/2026-04-16-repo-issues-controller-evaluator.md
   — §"Reference: Select-Coin Backtesting Framework" §"Core
   abstraction order" (Factor → Filter → Strategy → BacktestConfig
   → ExecutionSimulation → Evaluation; this is the architectural
   shape S6 is implementing).
6. /Users/stometa/dev/prediction-market-system/src/pms/evaluation/CLAUDE.md
   — per-layer invariant enforcement for Evaluator; S6 reuses the
   per-strategy aggregation shape inside the backtest evaluator
   (Invariant 3).
7. /Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
   — structural reference for harness-grade spec shape (CP shape,
   acceptance-criteria shape, files-of-interest shape). If this
   path does not exist on the current branch, fall back to
   /Users/stometa/dev/prediction-market-system/.harness/pms-v1/spec.md
   for the same structural reference.

CURRENT STATE SNAPSHOT:

S6 is the terminal node of the project decomposition DAG (§2.1 of
the project spec). All prior sub-specs (S1, S2, S3, S4, S5) have
landed; §11.6 Intake items are satisfied. The project-level spec
(/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md)
is the authoritative boundary contract: §3.2.6 enumerates the 13
concepts S6 owns, §3.4 enumerates the entities S6 must NOT revive
without a matrix amendment, and §11 is S6's total-spec subsection.

PREFLIGHT (boundary check — run before any authoring; every
shell command below assumes cwd is
/Users/stometa/dev/prediction-market-system):

- §11.6 Intake items must all be satisfied. If any fails, HALT
  and tell the user:
  * Per-strategy `ControllerPipeline` dispatch is operational
    (S5 §3.2.5 row 1; observable via the `/strategies` page
    rendering per-strategy decisions tagged with non-`"default"`
    `(strategy_id, strategy_version_id)`).
  * Per-strategy `Evaluator` aggregation is shipping per-
    strategy metrics on `/metrics` (S5 §3.2.5 rows 2 + 7).
  * `(strategy_id, strategy_version_id)` is `NOT NULL` on every
    inner-ring product table (S5 §3.2.5 row 3); SQL check:
    `\d+ feedback` / `\d+ eval_records` / `\d+ orders` /
    `\d+ fills` show both columns NOT NULL plus the CHECK
    constraint forbidding empty strings on all 4 product tables
    (Invariant 3 mechanical enforcement, project spec §5.3 Mixed
    bucket).
  * `Opportunity` entity shape is stable (S5 §3.2.5 row 5).
  * `/strategies` comparative view + `/metrics` per-strategy
    breakdown are shipped and rendering live data (S5 §3.2.5
    rows 6 + 7).
  * `FactorService` reader interface is stable and accepts a
    date-range / market-universe argument (S3 §3.2.3 row 4).
  * Outer-ring tables contain at least the date range S6 plans
    to replay across (S1 §3.2.1 rows 1–6; SQL check:
    `SELECT min(ts), max(ts) FROM book_snapshots;`).
  * `Strategy` aggregate + `strategy_versions` table contain at
    least one strategy with at least one version row (S2
    §3.2.2 rows 1, 4).
  * `MarketSelector` + `Strategy.select_markets(universe)` are
    operational and produce a non-empty market-id list for at
    least one registered strategy (S4 §3.2.4 rows 1, 3;
    Invariant 6 — replay engine derives the backtest universe
    via this same code path).
  * `uv run pytest -q` passes with ≥ 70 tests + 2 skipped
    (PMS_RUN_INTEGRATION=1 gate), and
    `uv run mypy /Users/stometa/dev/prediction-market-system/src/ /Users/stometa/dev/prediction-market-system/tests/ --strict`
    is clean, on a fresh shell.

TASK:

Create
/Users/stometa/dev/prediction-market-system/.harness/pms-research-backtest-v1/spec.md
by expanding §11.8 (Checkpoint skeleton) into harness-grade CPs
with:

- Per-CP acceptance criteria (observable, falsifiable, not
  implementation notes).
- Per-CP files-of-interest (absolute paths under
  /Users/stometa/dev/prediction-market-system/).
- Per-CP effort estimate (S / M / L).
- Intra-spec CP dependencies (which CPs block which).

Use
/Users/stometa/dev/prediction-market-system/.harness/pms-v2/spec.md
(falling back to
/Users/stometa/dev/prediction-market-system/.harness/pms-v1/spec.md
if pms-v2 is absent on the branch) as the structural reference
for shape. Draft only — stop and wait for spec-evaluation approval
before running any checkpoint.

CONSTRAINTS:

- New feature branch `feat/pms-research-backtest-v1` off `main`.
  Never commit to `main` directly.
- Respect §3 Boundary Matrix of the project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md).
  Never claim a concept owned by another sub-spec (S1–S5). The 13
  concepts enumerated in §11.2 Scope-in are the complete authorised
  set for S6; anything else is scope drift.
- Respect §3.4 of the project-level spec. The entities listed
  there (`StrategyBundle`, `MarketUniverse` as first-class,
  `FactorAttribution` as a dedicated table, `SelectionSnapshot` /
  `PriceLevel` / `MarketUpdate` as persisted event types) are
  intentionally deferred — DO NOT revive them as first-class S6
  citizens without first amending §3.2 + §3.4 in the same PR.
- Every Strategy / Factor / Sensor / ring-ownership claim must
  cite an invariant number from
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md.
- No `Co-Authored-By` lines in any commit
  (/Users/stometa/dev/prediction-market-system/CLAUDE.md §"Do
  not"; promoted rule "Commit-message precedence").
- Conventional-commit prefixes required (§5.6 of the project
  spec): `feat(<scope>):`, `fix(<scope>):`, `docs(<scope>):`,
  `test(<scope>):`, `refactor(<scope>):`, `chore(<scope>):`.
- Follow the promoted rule 🟡 Lifecycle cleanup on all exit paths
  for the asyncpg read connections inside the replay engine and
  the factor-panel cache: acquire + release in the same commit,
  `try/finally` on all four exit paths.

HALT CONDITIONS:

- Any attempt to revive a §3.4-deferred entity (`StrategyBundle`,
  `MarketUniverse` as first-class, `FactorAttribution` as a
  dedicated table, `SelectionSnapshot` / `PriceLevel` /
  `MarketUpdate` as persisted event types) as a first-class S6
  citizen WITHOUT a §3.2 + §3.4 amendment in the same PR. STOP
  and ask the parent before any DDL or dataclass for those types
  lands.
- A factor-panel cache design that does NOT share across sweep
  variants (e.g., per-variant cache namespaces, per-spec cache
  invalidation that defeats sharing). This violates §11.3 AC #2
  (cache-hit-rate > 95% across variants) — STOP immediately and
  reconsider the cache-key derivation.
- Any attempt to add a `strategy_id` or `strategy_version_id`
  column to an outer-ring table (`markets`, `tokens`,
  `book_snapshots`, `book_levels`, `price_changes`, `trades`) or
  middle-ring table (`factors`, `factor_values`). This is an
  Invariant 8 violation and §11.3 AC #7 — STOP immediately.
- Any invariant in
  /Users/stometa/dev/prediction-market-system/agent_docs/architecture-invariants.md
  cannot be satisfied by the design you are authoring. Do NOT
  silently amend the invariant. Open a retro under
  /Users/stometa/dev/prediction-market-system/.harness/retro/ and
  return to the user.
- The 13 concepts you author CPs for do not match §3.2.6 of the
  project-level spec
  (/Users/stometa/dev/prediction-market-system/docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md),
  concept-for-concept. Reconcile first.

FIRST ACTION:

Run:

    cd /Users/stometa/dev/prediction-market-system \
      && git status && git branch --show-current \
      && git log --oneline -5

Then verify the §11.6 Intake satisfaction items above (each one
is a concrete check against either a SQL query, a dashboard URL,
or a fresh-clone gate). After verifying, read the 7 files in the
REQUIRED READING block, in order, before drafting any content.
After reading, report your understanding of §11.8's 11
checkpoints (CP1–CP10 + CP-DC) and wait for go-ahead before
drafting
/Users/stometa/dev/prediction-market-system/.harness/pms-research-backtest-v1/spec.md.
```

---

---

## 12. Maintenance and change policy

This document is the project-level total spec for the six-sub-spec
decomposition. The architecture it describes is alive: invariants
get re-derived, sub-specs land and produce retros, the Boundary
Matrix grows as new concepts surface, and the DAG occasionally
shifts. §12 specifies **when** this document must be updated,
**who** owns each update, **how** the `status:` frontmatter field
progresses, and **how** the document relates to its sibling
documents under `agent_docs/` and `.harness/retro/`. The mechanism
parallels the **Change policy** at the bottom of
`agent_docs/architecture-invariants.md`: invariants govern the
positive architecture; this section governs the contract document
that decomposes those invariants into harness work.

§12 is deliberately shorter than §§6–11. Sub-spec scope is
authoritative there; here the only authority is the policy that
keeps the document honest as the work lands.

### 12.1 Update triggers

This document **must** be updated when any of the following events
occur. The trigger and the document edit land in the **same PR** —
no after-the-fact reconciliation (rationale in §12.2).

- **A sub-spec (S1–S6) completes.** Append the completion date
  and a link to the retro under `.harness/retro/<sub-spec-id>.md`
  to the relevant §§6–11 sub-spec header / metadata. The retro
  itself is gate 1 of §4.3 (between-spec gates), so the link
  target always exists by the time this update lands.
- **A Boundary Matrix row changes owner.** Owner reassignment is
  rare and must trigger an architectural review **before** the
  edit — open a retro under `.harness/retro/` documenting the
  observed boundary problem, then amend §3.2 in the same PR that
  records the retro outcome. The change cascades to the affected
  sub-specs' *Scope in / out*, *Dependencies*, and *Intake /
  Leave-behind* subsections, all in the same commit.
- **A new concept is introduced that does not appear in §3.2.**
  Per §3.1's rule "When adding a new concept not in the matrix",
  the §3.2 amendment lands in the **same commit** that introduces
  the concept — picking exactly one Owner, listing Consumers,
  noting which invariant(s) the concept touches. A PR that
  introduces a concept absent from §3.2 fails the §5.4 Boundary
  matrix audit gate.
- **An authoring-order swap is executed.** Per §4.3 gate 6, the
  only supported reorder is **S3 ↔ S4** (§2.4, §4.4); the swap
  changes authoring sequence only and **does not** edit §2.1
  edges or §4.3 gate-4 predecessor enumeration. The update for a
  swap is the gate-6 `reorder` decision row with the triggering
  condition cited (§4.4 names the two acceptable conditions).
- **A real DAG edge changes in §2.1.** Adding an edge, removing
  an edge, or re-pointing one — anything other than the S3 ↔ S4
  authoring swap above. The change requires updates to §2.1
  graph, §2.2 node summary, §4.2 per-edge rationale, §4.3 gate-4
  predecessor enumeration, and both endpoints' *Dependencies* and
  *Intake / Leave-behind* subsections in the same PR. Per §4.3
  gate 6, breaking any DAG edge other than the S3 ↔ S4 ordering
  also requires a new retro documenting the architectural reason
  the original §4.2 rationale no longer holds.
- **A new sub-spec is inserted.** Very rare. Requires a retro
  documenting the architectural gap that motivates it, plus
  amendments to §2.1 (new node + edges), §2.2 (node summary),
  §3.2 (new sub-matrix block), §4 (execution rationale + gate
  enumeration), and a new §§6–11-style sub-spec section. The
  retro is the prerequisite — no insertion without one.
- **An invariant in `agent_docs/architecture-invariants.md`
  changes.** Cascade-review every sub-spec that cites the
  invariant by number (the §3.2.* "Invariant" column makes this
  grep-checkable). Each affected sub-spec subsection is reviewed
  in the same PR as the invariant change; if a sub-spec must
  change behaviour, its Scope / Acceptance criteria / Intake /
  Leave-behind update lands in the same commit. The invariant
  document is the upstream authority — fix it there first, then
  cascade here, never the other way around (this is the same
  ordering rule as the **Change policy** in
  `agent_docs/architecture-invariants.md`).
- **The `status:` frontmatter field transitions.** Each move
  along the `draft → active → superseded` ladder of §12.3 is
  itself an update trigger. The PR performing the transition
  edits the frontmatter, plus — for the `superseded` transition
  — appends the §0 pointer to the replacement document.

### 12.2 Update ownership

- **The author of the PR triggering the update owns the update.**
  A sub-spec completion PR includes the §§6–11 header amendment;
  an invariant-change PR includes the cascading §§6–11 review; a
  new-concept PR includes the §3.2 row.
- **Updates are commit-atomic.** No "I'll fix the spec next
  week." The trigger and the edit live in one PR, reviewed as one
  unit. This is the same hygiene rule as 🟡 *Lifecycle cleanup on
  all exit paths* (`agent_docs/promoted-rules.md`): acquire and
  release ship in the same commit; here the trigger and the
  document update ship in the same commit. After-the-fact
  reconciliation is the failure mode this rule prevents.
- **Reviewers enforce the atomicity.** A PR that triggers a §12
  update without performing it fails the §5.4 Boundary matrix
  audit gate (for §3.2 changes) or fails review against §4.3
  gate 4 (for Leave-behind / Intake changes). The gate is the
  enforcement; ownership defines who is on the hook to satisfy it.

### 12.3 Status field semantics

The frontmatter `status:` field at the top of this document
progresses through three durable values. Each transition is a
§12.1 update trigger — the PR performing the transition edits
the frontmatter in the same commit. Transition criteria are
written in document-completeness terms, not in commit-numbering
terms, so rebases and squashes do not invalidate them.

- **`draft`** — initial state. The document is partial: §3.2
  Boundary Matrix may have rows pending, §§6–11 may be skeleton-
  only or missing canonical subsections, the §3.3 invariant
  Gap-check may not yet cover every invariant in
  `agent_docs/architecture-invariants.md`. The whole authoring
  effort lives in this state until the §12.3 `active` criteria
  are met.
- **`active`** — transitions to `active` when the document is
  complete and authoritative: §§0–5 are populated and
  consistent, §3.2 covers every load-bearing concept (the §3.3
  Gap-check coverage table shows every invariant has at least
  one owning row, with Invariant 1 excepted per §3.3), and
  §§6–11 each carry the eleven canonical subsections (.1 Goal
  through .11 Kickoff Prompt). `active` means **this document
  is the current authoritative project-level spec**: harness
  runs open against it (see the per-sub-spec Kickoff Prompts in
  §§6.11–11.11), and it stays `active` for the entire span from
  completion through S6 finishing — the meaning does not shift
  after S6 lands. §12.5 describes the post-S6 lifecycle without
  changing `active`.
- **`superseded`** — transitions to `superseded` when a successor
  decomposition spec lands at `docs/superpowers/specs/<date>-pms-
  project-decomposition-v<N+1>.md` and reaches its own `active`
  state. The old document is **kept for history** — never
  deleted, never rewritten in-place. The frontmatter changes to
  `status: superseded` and a one-line pointer to the replacement
  is appended at the top of §0 ("Superseded by `<path>` on
  `<date>`."). The successor document inherits the `status:
  draft` lifecycle from scratch.

### 12.4 Relationship to sibling documents

The four sibling documents under `agent_docs/` and `.harness/retro/`
each have a defined relationship to this document. None of them
are owned by §12 — they are owned by their own files — but the
direction of authority and the cascade rules live here.

- **`agent_docs/architecture-invariants.md`** is the **upstream
  authority** on invariant semantics, rationale, runtime
  evidence, anti-patterns, and enforcement. This document
  *consumes* invariants by number; it never redefines them. When
  an invariant changes there, every §3.2 row that cites the
  invariant in its **Invariant** column and every sub-spec
  acceptance criterion that names the invariant is reviewed in
  the cascade described in §12.1.
- **`agent_docs/project-roadmap.md`** is **partially superseded**
  by §§2–5 of this document. Its DAG, execution-order rationale,
  and between-spec gate skeleton survive there as the **short
  navigable summary** that contributors hit first; the detailed
  contract (Boundary Matrix, Intake / Leave-behind, per-edge
  rationale, mechanical and behavioural invariant evidence) lives
  here as the authoritative source. The roadmap's §"Maintenance"
  block is preserved because it is shorter than §12 and remains
  useful as a quick-reference; updates to the roadmap's
  Maintenance block must stay consistent with §12.1's trigger
  list.
- **`agent_docs/promoted-rules.md`** holds rules promoted from
  retros (per its §"Promotion process"). Per §5.5 Retro-promotion
  workflow, a promotion lands as a single PR that updates
  `agent_docs/promoted-rules.md`, `CLAUDE.md` §"Promoted rules
  from retros", and `.harness/retro/index.md` together — those
  three are the only mandatory cascade. This document does not
  store promoted rules; it only references them by name and
  severity-emoji (🔴 / 🟡 / 🟢) where the content reinforces a
  §12 rule (e.g., the parallel between 🟡 *Lifecycle cleanup on
  all exit paths* and §12.2's commit-atomic update rule).
- **`.harness/retro/<sub-spec-id>.md`** is the post-sub-spec
  retro. It is gate 1 of §4.3 (a sub-spec without a retro cannot
  clear the gate) and the source of any rule promotion that
  feeds `agent_docs/promoted-rules.md`. The retro drives the
  relevant §§6–11 header update (§12.1 first bullet) and may
  trigger a Boundary Matrix row addition if it surfaces a missed
  concept (§12.1 third bullet). `.harness/retro/index.md` is the
  central index; the entry's status moves to `active` in the
  same retro-promotion PR per §5.5.

### 12.5 Retirement process

When S6 lands with consensus, the document transitions from
"active roadmap that drives sub-spec authoring" to "historical
record of how the architecture was decomposed". The transition is
a §12.1 update trigger, not a separate ceremony.

- **The status field stays `active`** at S6 completion. Per
  §12.3, `active` covers the full span from document completion
  through S6 finishing without semantic shift. A new subsection
  appended to the bottom of §11 (the S6 sub-spec) records the
  completion date and any follow-on work the S6 retro triggered:
  the Kalshi adapter pair (deferred per §1.2 "What the finished
  system does not do"), and any of `StrategyBundle`,
  `MarketUniverse`, `FactorAttribution`, or finer-grained
  backtest entities (deferred per §3.4 "Entities deliberately
  out of scope for this decomposition").
- **A successor document is authored only when a major
  architectural shift warrants it.** Examples that would warrant
  one: a new venue layer that breaks the current Sensor / Actuator
  abstraction; a multi-account / multi-tenant requirement that
  invalidates the inner-ring per-strategy schema; a research
  framework that needs a fundamentally different `BacktestSpec`
  shape. Minor changes (a new strategy, a new factor, a new
  dashboard page) do not warrant a successor — they happen
  inside the existing inner-ring + middle-ring contract.
- **When a successor lands**, this document transitions to
  `status: superseded` per §12.3 with the §0 pointer to the
  replacement. The successor document inherits `status: draft`
  and runs its own §§0–12 lifecycle from scratch. Old document
  remains in the repository under the same path; it is the
  historical record contributors consult to understand why the
  architecture took its current shape.

### 12.6 Cross-cutting observations

Two observations are worth preserving here so future authors do
not have to re-derive them from sub-spec context.

- **The Boundary Matrix (§3) is the hinge for boundary-
  affecting changes.** Any §§6–11 edit that introduces a new
  concept, reassigns an Owner, or changes an *Intake* / *Leave-
  behind* contract has a §3 counterpart and must amend §3.2 in
  the same commit. Header-and-metadata edits (sub-spec
  completion date + retro link, §12.1 first bullet) and prose
  cleanups do not. Maintaining the boundary-affecting subset of
  §§6–11 updates without matching §3 edits creates silent
  overlap (two sub-specs both claim ownership of the same
  concept) or silent gaps (a downstream sub-spec's Intake names
  a concept no upstream sub-spec produces). The ordering rule
  for that subset is **always amend §3 first, or in the same
  commit** as the §§6–11 edit — never after.
- **Leave-behind insufficiency cascades.** §4.3 gate 4 enforces
  Leave-behind ⊇ Intake at every sub-spec boundary by diffing
  the union of all predecessor Leave-behinds against the
  downstream Intake. When that diff turns up a gap — sub-spec N
  produced X, but sub-spec N+1 needs X' — gate 4 itself names
  two valid reconciliations and §12 covers both:
  (a) **amend a predecessor to produce the missing concept** —
  update the predecessor's *Leave-behind* and back-propagate to
  its *Scope in*, *Acceptance criteria*, and the §3.2 rows that
  cover the new artefact, then amend the §3.2 Boundary Matrix
  row in the same PR;
  (b) **amend sub-spec N+1 to drop the dependency** — when
  review confirms the dependency was a false requirement (the
  downstream did not actually need the concept), update N+1's
  *Intake* and any *Acceptance criteria* that referenced the
  dropped concept, **and** amend the §3.2 row for that concept
  to remove N+1 from the *Consumers* column in the same PR (a
  stale *Consumers* entry would re-create the same boundary
  drift gate 4 exists to prevent). If removing N+1 leaves the
  concept with zero remaining consumers, decide in the same PR
  whether to retire it from §3.2 or leave it as orphaned-but-
  owned with a note.
  Either path lands in the same PR as the diff that surfaced
  the gap; per §12.2, the PR author owns the chosen path. The
  one thing both paths forbid is silently weakening N+1's
  Intake without acknowledging the gap — that hides the choice
  between (a) and (b) and re-creates the boundary drift §4.3
  gate 4 exists to prevent.

---
