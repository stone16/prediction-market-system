# Architecture Invariants

**Status:** active as of 2026-04-16.
**Scope:** These are the non-negotiable architectural invariants for
every spec written under `.harness/` and every module under
`src/pms/`. Any design that violates an invariant must either be
rejected at spec-evaluation time, or must trigger an explicit retro
that re-evaluates the invariant before proceeding.

**Source material:**
- `docs/notes/2026-04-16-repo-issues-controller-evaluator.md`
- `docs/notes/2026-04-16-evaluator-entity-abstraction.md`
- Socratic walkthrough captured in these notes (2026-04-16).

**Related documents:**
- `agent_docs/promoted-rules.md` — rules promoted from retrospectives.
  These are complementary (retros address past mistakes; invariants
  define the positive architecture).
- `agent_docs/project-roadmap.md` — the 6-spec decomposition that
  will implement these invariants.

---

## How to read this document

Each invariant has five sections:

- **Statement** — the rule, in one sentence.
- **Rationale** — why it is load-bearing for the architecture.
- **Runtime evidence** — concrete `file:line` references that either
  demonstrate the invariant holding today, or violate it and must be
  fixed by a specific sub-spec.
- **Anti-patterns** — what a violation looks like in practice.
- **Enforcement** — how the invariant is checked (schema constraint,
  mypy type, linter rule, code review gate, or test).

The `(forward-looking)` tag on a runtime-evidence entry means the
evidence will exist after the named sub-spec lands; today it is
absent, which is itself the driver for that sub-spec.

---

## Invariant 1 — Cybernetic loop is a concurrent feedback web

**Statement.** Sensor, Controller, Actuator, and Evaluator are
concurrent participants in a feedback web with bidirectional edges.
They are **not** phases in a linear pipeline, and the feedback graph
has more than one direction.

**Rationale.** System correctness depends on edges that cross time:
Evaluator output at t=N affects Controller decisions at t=N+1;
Controller market selection at t=N affects Sensor subscriptions at
t=N+1 (see Invariant 6). Modeling this as a linear pipeline
(Sensor → Controller → Actuator → Evaluator) silently drops the
reverse edges and produces bugs where feedback is never applied or
where one layer blocks on another that is not yet ready.

**Runtime evidence.**
- `src/pms/runner.py:137-138`: `_controller_task` and `_actuator_task`
  are independent `asyncio.Task`s running concurrently alongside
  `sensor_stream.tasks` and the evaluator spool.
- `src/pms/runner.py:208-250`: the two loops (`_controller_loop`,
  `_actuator_loop`) communicate via `asyncio.Queue`, not synchronous
  call — the queue is what enables concurrency.

**Anti-patterns.**
- Describing runtime in a spec as "phase 1 Sensor, phase 2
  Controller, …" or writing acceptance criteria that imply phased
  execution.
- Tests that assert Sensor finishes before Controller starts.
- Introducing a "SensorCompleted" event that gates Controller.
- Describing the Evaluator → Controller feedback as "step 5".

**Enforcement.**
- Spec-evaluation agent rejects any sub-spec whose acceptance criteria
  assume phased runtime.
- Code review rejects synchronous barriers between layers (the
  intentional `asyncio.gather` inside `ControllerPipeline.on_signal`
  for multi-forecaster fan-in is the documented exception, because
  it is a scatter-gather *within* a single layer, not across layers).

---

## Invariant 2 — Strategy is a rich aggregate; layers consume projections

**Statement.** `Strategy` is a DDD-style aggregate that owns all
strategy-related state: factor specs, risk params, eval spec, market
selection rules, forecaster composition, router gating, versioning.
Downstream layers never import the `Strategy` class nor hold
references to the aggregate; they receive immutable projection
value objects (`StrategyConfig`, `RiskParams`, `EvalSpec`,
`ForecasterSpec`, `MarketSelectionSpec`) passed in at call time.

**Rationale.** If each layer holds a slice of strategy state (Sensor
holds subscription rules, Controller holds forecaster composition,
Actuator holds risk params, Evaluator holds metric definitions),
adding a strategy touches 4 modules, and changing a strategy's
contract changes 4 modules. This is the anemic domain model
anti-pattern: strategy knowledge scatters across the codebase. By
keeping the aggregate intact, strategy changes are localized; by
handing only projections to downstream layers, the layers remain
strategy-agnostic in their type signatures.

**Runtime evidence (forward-looking).** Implemented in S2.
- `src/pms/strategies/aggregate.py` — the Strategy aggregate.
- `src/pms/strategies/projections.py` — frozen projection types.
- `src/pms/sensor/`, `src/pms/actuator/` — accept projection types
  only, never import `pms.strategies.aggregate`.

**Anti-patterns.**
- Sensor, Actuator, or Evaluator modules importing from
  `pms.strategies.aggregate`.
- Strategy aggregate methods that return mutable state.
- Projection classes with setters or mutable containers.
- Fields-on-`MarketSignal` or `TradeDecision` that store partial
  strategy state (e.g. stringly-typed `stop_conditions` carrying
  routing/model-id mixed in — which today's v2 code does; this is a
  known violation that S2/S5 fix).

**Enforcement.**
- Import-linter rule (to be added in S2 `pyproject.toml` or
  `ruff.toml`): `pms.sensor`, `pms.actuator` cannot import from
  `pms.strategies.aggregate`; they may import from
  `pms.strategies.projections`.
- Mypy: projection types are `@dataclass(frozen=True)`.
- Spec-evaluation reviewer rejects any sub-spec introducing a new
  field on a downstream entity that duplicates strategy state.

---

## Invariant 3 — Strategy version is immutable and tags every downstream record

**Statement.** Every `TradeDecision`, `OrderState`, `FillRecord`,
`EvalRecord`, and `Feedback` carries a `(strategy_id,
strategy_version_id)` pair. `strategy_version_id` is a deterministic
hash of the strategy's full config (factors, params, routing, risk,
eval spec). Once written, it is never mutated. Re-configuring a
strategy produces a new version row, not an in-place edit.

**Rationale.** Strategies iterate continuously in production:
weights change, thresholds move, factors are added. If historical
records are tagged only with `strategy_id`, then "Strategy A's
Brier over the last 30 days" silently averages across multiple
semantically-different versions. The resulting metric is
meaningless, and worse, the error is invisible. Binding each record
to a specific version makes metric computation honest and makes
rollback decisions concrete ("pin production to version X").

**Runtime evidence (forward-looking).** Implemented in S2 schema.
- `strategies(strategy_id PRIMARY KEY, ...)` — one row per strategy.
- `strategy_versions(strategy_id, version TEXT, config_json JSONB,
  created_at, PRIMARY KEY (strategy_id, version))` — version hash.
- `eval_records(strategy_id, strategy_version_id, ...)` — both
  columns NOT NULL once S5 lands; NULLABLE during S2–S4 with a
  `"default"` strategy tagging legacy rows.

**Anti-patterns.**
- Recording only `strategy_id` on a downstream entity.
- Mutating `strategy_versions.config_json` to "fix" an old version.
- Computing aggregate metrics (mean Brier) without grouping by
  `(strategy_id, strategy_version_id)`.

**Enforcement.**
- PostgreSQL schema: `strategy_version_id` is `NOT NULL` once S5
  completes. A `CHECK` constraint forbids known sentinel values
  (empty string).
- Code review: no SQL aggregation query over `eval_records` or
  `fills` may omit `GROUP BY strategy_version_id` without an
  explicit comment justifying the cross-version aggregation (e.g.
  total volume across all strategies for ops dashboards).

---

## Invariant 4 — Factor layer stores raw factors only; composite logic lives in strategy config

**Statement.** The `factors` table and `factor_values` table store
only **raw** factors (atomic, strategy-agnostic, reusable across
strategies). Composite factors (weighted sums, transforms, ensembles)
are computed inside strategy logic using raw factor values; they are
not persisted as first-class factor rows.

**Rationale.** Raw factors are shared infrastructure: one
`orderbook_imbalance` calculation serves every strategy that needs
it, and the factor cache (Invariant 8, middle ring) pays for itself.
Composite factors are strategy-specific by construction — a
"weighted sum of raw_A and raw_B" is meaningful only within the
strategy that defined the weights. Modelling composites as first-class
factors forces a DSL, doubles the schema, and creates the "is this
factor a raw or a composite?" ambiguity that makes the factor
registry an onboarding hazard. The select-coin research framework
(`/Users/stometa/dev/quant/select-coin-backtesting/select-coin-pro_v1.8.1`)
reached the same conclusion.

**Runtime evidence (forward-looking).** Implemented in S3.
- `src/pms/factors/definitions/` — one module per raw factor.
- `factors` table — one row per raw factor definition.
- `factor_values(factor_id, market_id, ts, value)` — only raw values.
- `StrategyConfig.factor_composition` — JSONB field holding
  per-strategy composition logic; read at strategy runtime.

**Anti-patterns.**
- A `composite_factors` table or a `factor_type ENUM('raw','composite')`
  column.
- A factor-expression DSL (`"a * b + c"`) stored in the database.
- A raw factor that encodes strategy-specific weighting.

**Enforcement.**
- Code review: any new factor PR must demonstrate the factor is
  used by at least one strategy and is reusable in principle by
  others. Strategy-specific derived quantities are rejected with a
  pointer to this invariant.

---

## Invariant 5 — Sensor and Actuator are strategy-agnostic; Controller and Evaluator are strategy-aware

**Statement.** Sensor and Actuator modules contain no references to
Strategy. Sensor accepts `market_ids: list[str]` as its subscription
input and emits `MarketSignal` unaware of which strategy consumes it.
Actuator accepts `TradeDecision` + `RiskParams` projection and emits
`OrderState`/`FillRecord` unaware of which strategy produced the
decision. Controller and Evaluator *are* strategy-aware: Controller
reads `StrategyConfig` to build its pipeline, Evaluator reads
`EvalSpec` to know which metrics to compute per strategy.

**Rationale.** Strategy proliferation is expected (the project's
central working assumption). Keeping Sensor and Actuator
strategy-agnostic means adding a new venue adapter, a new sensor
type, or a new execution adapter does not require any strategy
knowledge. The layers stay independently evolvable. Controller and
Evaluator are the natural homes for strategy awareness because
"which strategy's decision is this" is structurally part of the
decision-making and measurement semantics, not part of observation
or execution.

**Runtime evidence.**
- Today's `src/pms/sensor/adapters/polymarket_rest.py` imports
  nothing from `pms.strategies` or `pms.controller` — correct.
- Today's `src/pms/actuator/adapters/paper.py` imports nothing from
  `pms.strategies` — correct.
- Today's `src/pms/controller/pipeline.py` does not yet have
  per-strategy dispatch — this is the gap S5 closes.

**Anti-patterns.**
- `from pms.strategies.aggregate import Strategy` inside any
  `pms.sensor.*` or `pms.actuator.*` module.
- A Sensor config that takes a `Strategy` or `StrategyConfig`
  object (should take the subscription projection only — a list of
  market ids or a `MarketSubscription` value object).

**Enforcement.**
- Import-linter rule: `pms.sensor`, `pms.actuator` cannot import
  anything from `pms.strategies.aggregate` nor from
  `pms.controller.*`. This is codified in S2 (pyproject.toml or
  ruff.toml) — S2 is the first sub-spec where `pms.strategies.*`
  exists, so the rule cannot meaningfully run before S2. Enforced
  in every subsequent spec.

---

## Invariant 6 — Active perception: Controller-derived market ids feed back into Sensor subscription

**Statement.** The Sensor's subscription list is not static
configuration; it is derived at runtime from the union of all
active strategies' market-selection projections. A `MarketSelector`
component reads the market universe (from the outer ring), reads each
active strategy's `MarketSelectionSpec` projection from the strategy
registry, applies the projection filters, and produces a merged
`market_ids: list[str]` that a `SensorSubscriptionController` pushes
to the Market Data Sensor. On strategy change, the subscription
updates; on Sensor disconnect, the subscription is replayed.

**Rationale.** Static subscription lists waste bandwidth and miss
markets that become relevant only after a strategy is deployed. More
fundamentally, this makes the control loop bidirectional in the
cybernetics sense: the system does not only *observe*, it
*chooses what to observe*. This is active perception (as distinct
from passive perception), and it is how the project can support
strategy-driven market discovery without hardcoding market lists in
Sensor adapters. Keeping Sensor strategy-agnostic (Invariant 5)
while still achieving this requires a dedicated orchestration layer
(the `MarketSelector` + `SensorSubscriptionController` pair) that
lives above Sensor and below Strategy.

**Runtime evidence.** Landed in S4.
- `src/pms/market_selection/selector.py` — reads universe, applies
  `MarketSelectionSpec` projections, returns merged market-id list.
- `src/pms/market_selection/subscription_controller.py` —
  propagates updates to Sensor.
- `src/pms/storage/strategy_registry.py::list_market_selections()` —
  yields `(strategy_id, strategy_version_id, MarketSelectionSpec)` for
  the selector.
- `src/pms/runner.py::_wire_active_perception()` — wires
  `MarketSelector` + `SensorSubscriptionController` into the live
  runtime.

**Anti-patterns.**
- Sensor module reading Strategy directly (violates Invariant 5).
- Market-id list written into `config.yaml` as a static array.
- Subscription changes that bypass `SensorSubscriptionController`
  and write directly to a Sensor's internal state.

**Enforcement.**
- Import-linter: Sensor modules cannot import from
  `pms.market_selection` (the flow is *push from selector to
  sensor*, not pull).
- Spec-evaluation reviewer rejects any design that makes Sensor
  aware of strategies to avoid implementing the selector.

**Cold-start handling (important).** The `MarketSelector` needs the
market universe, and the universe comes from Sensor. This is
resolved by Invariant 7: the outer ring is filled by the
`MarketDiscoverySensor` (which runs unconditionally with no strategy
input) before market-selection projections are applied. Boot order:

1. `MarketDiscoverySensor` populates `markets` / `tokens` tables.
2. `MarketSelector` reads the tables, reads active
   `MarketSelectionSpec` projections, and applies the filters.
3. `SensorSubscriptionController` subscribes `MarketDataSensor` to
   the resulting market ids.
4. Subsequent strategy config changes trigger steps 2–3 again
   incrementally.

---

## Invariant 7 — Sensor is two-layered: Market Discovery + Market Data

**Statement.** The Sensor layer contains two distinct sensor types:

- **`MarketDiscoverySensor`** — low-frequency, strategy-agnostic,
  unconditional. Polls the venue's full-market endpoint (for example
  Polymarket Gamma `/markets`) on a coarse cadence.
  Writes to the outer ring `markets` and `tokens` tables. Has no
  subscription list; it scans everything the venue exposes.
- **`MarketDataSensor`** — high-frequency, strategy-driven,
  subscription-based. Connects to the venue's streaming endpoint
  (Polymarket CLOB WebSocket) and subscribes to a specific asset-id
  list provided by `SensorSubscriptionController` (Invariant 6).
  Writes to the outer ring `book_snapshots`, `book_levels`,
  `price_changes`, and `trades` tables.

**Rationale.** Today's `PolymarketRestSensor` conflates the two:
from one class it polls `/markets` (discovery) and emits
`MarketSignal`s whose `orderbook` is always `{"bids": [], "asks":
[]}` — a market-data-shaped output without real depth (data). This
coupling makes it impossible to answer the cold-start of
Invariant 6, because the sensor that provides the universe is the
same sensor that needs a subscription list. Splitting the two lets
the discovery sensor run unconditionally (no strategy dependency)
while the data sensor becomes fully driven by active perception.

**Runtime evidence.**
- Today: `src/pms/sensor/adapters/polymarket_rest.py` — conflated
  implementation. S1 splits this file into two sensor classes.
- Today: `src/pms/sensor/adapters/polymarket_stream.py:71-77` —
  `_message_dict_to_signal` (module-level helper) drops `book` and
  `price_change` events by accepting only messages that carry
  `price` + `market_id`. S1 (stream sensor upgrade) replaces this
  with a stateful parser that writes to the outer ring.

**Anti-patterns.**
- A Sensor class that both polls the market universe and maintains
  per-market orderbooks.
- A `MarketDataSensor` that internally decides which markets to
  subscribe to (must accept the list; cannot compute it).

**Enforcement.**
- S1 acceptance criteria: `MarketDiscoverySensor` and
  `MarketDataSensor` are separate classes, each with a single
  responsibility.
- Code review: any future venue adapter must land as a pair (one
  discovery, one data).

---

## Invariant 8 — Onion-concentric storage: outer / middle / inner

**Statement.** Persistent state is organised as three concentric
rings with strict ownership:

- **Outer ring (market data, strategy-agnostic, shared).** Tables:
  `markets`, `tokens`, `book_snapshots`, `book_levels`,
  `price_changes`, `trades`. Written by Sensor layer. Read by
  Factor service, Controller, Evaluator, Dashboard. No column in
  these tables references `strategy_id`.
- **Middle ring (factor panel, strategy-agnostic cache, shared).**
  Tables: `factors`, `factor_values`. Written by Factor service,
  reading from outer ring. Read by Controller. No column in these
  tables references `strategy_id`.
- **Inner ring (strategy products, per-strategy).** Tables:
  `strategies`, `strategy_versions`, `strategy_factors`, and the
  product tables `eval_records`, `feedback`, `orders`, `fills`.
  Every product-table row carries `(strategy_id,
  strategy_version_id)`. Written by Controller/Actuator/Evaluator.
  Read by Dashboard, research tools.

**Rationale.** Mixing the rings — e.g. stamping `strategy_id` onto
`price_changes` — creates artificial per-strategy duplication of
market data (the same delta arrives once but must be written N
times for N strategies). Keeping market data shared lets every
strategy and every backtest consume the same raw observation.
Symmetrically, keeping strategy products per-strategy is what makes
Invariant 3 enforceable: every product row carries its strategy
tag, so every aggregate query naturally partitions on it.

**Runtime evidence (forward-looking).** Implemented across S1–S5.
- S1: outer-ring tables + `MarketDataStore` write methods.
- S2: inner-ring aggregate tables + (id, version) columns on
  product tables.
- S3: middle-ring tables.

**Anti-patterns.**
- A `strategy_id` column on `price_changes`, `book_snapshots`,
  `trades`, `markets`, `tokens`, or `factor_values`.
- A `factor_values` row that encodes per-strategy weighting
  (violates Invariant 4).
- A `strategies` table without a paired `strategy_versions` table
  (violates Invariant 3).

**Enforcement.**
- Schema: S1 defines the outer-ring DDL; no column named
  `strategy_id` or `strategy_version_id` exists on any outer-ring
  or middle-ring table. This is a grep-checkable rule; spec
  evaluation verifies it mechanically.
- Code review: new-table proposals must declare their ring
  explicitly and justify the ring choice.

---

## Cross-invariant matrix

| Invariant | Sensor | Controller | Actuator | Evaluator | Schema |
|-----------|--------|------------|----------|-----------|--------|
| 1. Concurrent feedback web | ✓ | ✓ | ✓ | ✓ | — |
| 2. Strategy as rich aggregate | — (sees projections) | ✓ (aggregate reader) | — (sees `RiskParams`) | ✓ (aggregate reader) | inner ring |
| 3. Immutable version tags | — | ✓ (writes tag) | ✓ (forwards tag) | ✓ (reads + groups by tag) | inner ring |
| 4. Raw factors only | — | ✓ (consumer) | — | ✓ (attribution reader) | middle ring |
| 5. Strategy-awareness boundaries | strategy-agnostic | strategy-aware | strategy-agnostic | strategy-aware | — |
| 6. Active perception | subscription sink | selector home | — | — | — |
| 7. Two-layer sensor | ✓ | — | — | — | outer ring |
| 8. Onion-concentric storage | writes outer | reads middle + writes inner | writes inner | reads + writes inner | all rings |

---

## Change policy

These invariants are **load-bearing**. An invariant is not
overridden by a convenience argument in a single PR. Changing one
requires:

1. A retro entry under `.harness/retro/` that describes the
   observed problem the invariant is causing.
2. A replacement invariant proposal with explicit rationale and
   runtime evidence.
3. Review + approval before the invariant is edited here.

If an invariant turns out to be wrong, fix it here first, then
update the dependent sub-specs and code — not the other way around.
