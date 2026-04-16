# PMS Project Roadmap — 6-Spec Decomposition

**Status:** active as of 2026-04-16.
**Scope:** this document is the **project-level** roadmap. It lists
the 6 harness sub-specs that together implement the architecture
described in `agent_docs/architecture-invariants.md`, and it records
the dependency DAG that determines execution order.

**Not in this document:** per-sub-spec scope detail. The detailed
breakdown (checkpoint lists, acceptance criteria, boundary
definitions) lives in
`docs/superpowers/specs/2026-04-16-pms-project-decomposition-design.md`
(to be written in PR #2 of the architecture-foundation work). When
that document lands, this roadmap will link to it for depth.

---

## End state

A **research-grade strategy platform** where:

1. Multiple strategies run concurrently against live + paper + backtest
   modes under a single runtime.
2. Strategy performance is comparable across markets, time windows,
   factor cohorts, venues, and liquidity buckets.
3. New strategies onboard via config, not code rewrite — once the
   registry and aggregate are in place, a new strategy is a new row
   in `strategies` plus a module under `src/pms/strategies/<id>/`.
4. Backtest and live share the same selection path; divergence
   happens only at the execution / fill layer.

---

## Dependency DAG

```
                    ┌──────────────────────────────┐
                    │ S1: pms-market-data-v1       │
                    │ Outer ring data + 2 sensors   │
                    │ ———————————————————————————— │
                    │ • PG schema (all rings)       │
                    │ • asyncpg pool lifecycle      │
                    │ • MarketDiscoverySensor       │
                    │ • MarketDataSensor (book +    │
                    │   price_change + heartbeat +  │
                    │   reconnect reconciliation)   │
                    │ • PostgresMarketDataStore     │
                    │ • PG-backed Feedback/Eval     │
                    │ • Dashboard orderbook panel   │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │ S2: pms-strategy-aggregate-v1   │
                    │ Inner ring aggregate + registry │
                    │ —————————————————————————————— │
                    │ • Strategy aggregate + projections
                    │ • StrategyVersion (immutable)   │
                    │ • PostgresStrategyRegistry      │
                    │ • Import-linter rules (Inv 5)   │
                    │ • Seed "default" strategy       │
                    └──────┬──────────────┬───────────┘
                           │              │
           ┌───────────────▼──┐         ┌─▼───────────────────────┐
           │ S3: pms-factor-   │         │ S4: pms-active-          │
           │     panel-v1      │         │     perception-v1        │
           │ Middle ring       │         │ Controller → Sensor      │
           │ —————————————————│         │ —————————————————————— │
           │ • FactorDefinition│         │ • MarketSelector          │
           │ • FactorService   │         │ • SensorSubscription-     │
           │ • factor_values   │         │   Controller              │
           │ • Refactor existing│        │ • Strategy.select_markets │
           │   rules detectors │         │ • Runner wiring           │
           │   into raw factors│         │                           │
           └──────────────┬────┘         └──┬───────────────────────┘
                          │                 │
                          └────────┬────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │ S5: pms-controller-per-strategy-v1│
                    │ Strategy-aware runtime + metrics │
                    │ —————————————————————————————— │
                    │ • ControllerPipeline per-strategy
                    │ • Evaluator per-strategy aggregation
                    │ • (strategy_id, version_id) NOT NULL
                    │ • /strategies comparison dashboard
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ S6: pms-research-backtest-v1 │
                    │ Research framework            │
                    │ —————————————————————————————│
                    │ • BacktestSpec + ExecutionModel
                    │ • Market-universe replay       │
                    │ • Parameter sweep               │
                    │ • Backtest/live comparison      │
                    └──────────────────────────────┘
```

---

## Execution order

**Canonical sequence:** S1 → S2 → S3 → S4 → S5 → S6 (one at a time,
sequentially). Each sub-spec is a separate harness run under
`.harness/<sub-spec-id>/`.

### Why this exact order

- **S2 must follow S1 directly.** S1's schema reserves
  `(strategy_id, strategy_version_id)` columns on every inner-ring
  product table (see Invariant 8 + Invariant 3). If S2 came later,
  S1 would land with unused columns and the boundary between "outer
  ring data" and "strategy awareness" would blur. Keeping S2 second
  means the aggregate is defined while the schema rationale is still
  fresh.
- **S3 before S4.** Both depend only on S2, so order is discretionary
  between them. S3 delivers an *additive* capability (factor values
  persisted over time) with no behavioural side effects; S4 changes
  the Sensor subscription behaviour, which is a runtime side effect
  with larger blast radius. Scheduling S3 first lets the observable
  factor stream exist before the subscription mechanism changes,
  which makes debugging S4 regressions easier.
- **S5 consumes S3 + S4 output.** Controller per-strategy dispatch
  needs factor values (S3) to drive per-strategy decisions and
  needs active perception (S4) so that per-strategy subscriptions
  exist. Landing S5 before both means either mocking factor values
  or hardcoding subscriptions — both are known-bad precedents.
- **S6 must be last.** Research-grade backtest compares strategies;
  without the full strategy runtime (S5), the comparison has no
  reference behaviour. S6 also introduces the heaviest scope
  (parameter sweep, factor panel precomputation) and benefits from
  the most stable foundation.

### Between-spec gates

Each sub-spec ends with a retro under `.harness/retro/<sub-spec>/`.
Before starting the next sub-spec:

1. Retro must be written and indexed.
2. Architecture invariants reviewed — no violations introduced.
3. Top-level `CLAUDE.md` updated with any new rule promoted from
   the retro.
4. A human decision gate: proceed / pause / reorder. S3–S4 order
   can be swapped here without cost if conditions change.

---

## Sub-spec skeletons

Each skeleton below is a placeholder pointing to the canonical total
spec (forthcoming in PR #2). Today the skeletons exist so that
cross-document references (CLAUDE.md, per-layer CLAUDE.md, retros)
have stable anchor names.

### S1 — `pms-market-data-v1`

- **Goal:** outer-ring data layer + 2-sensor split.
- **Key invariants addressed:** 7 (two-layer sensor), 8 (outer ring).
- **Schema delivered:** outer ring fully, inner ring columns reserved
  (NULLABLE), middle ring tables empty-shell.
- **Observable capability at completion:** real orderbook depth
  stored in PG and visible on `/signals` dashboard panel.
- **Detail:** see total spec (PR #2).

### S2 — `pms-strategy-aggregate-v1`

- **Goal:** inner ring aggregate and projection types, strategy
  registry, import-linter enforcement.
- **Key invariants addressed:** 2 (aggregate), 3 (version), 5
  (boundary), 8 (inner ring).
- **Schema delivered:** inner-ring aggregate tables populated;
  product tables still NULLABLE on (strategy_id, version_id) to
  allow `"default"` tagging during the transition.
- **Observable capability at completion:** `/strategies` page lists
  registered strategies; legacy runtime writes tagged to
  `"default"`.
- **Detail:** see total spec.

### S3 — `pms-factor-panel-v1`

- **Goal:** middle-ring factor panel with FactorService + persistent
  `factor_values`.
- **Key invariants addressed:** 4 (raw only), 8 (middle ring).
- **Observable capability at completion:** `/factors` page shows
  factor values evolving over time; existing RulesForecaster
  detectors are now expressed as raw factor definitions.
- **Detail:** see total spec.

### S4 — `pms-active-perception-v1`

- **Goal:** Controller → Sensor feedback path via MarketSelector and
  SensorSubscriptionController.
- **Key invariants addressed:** 6 (active perception), 7 (subscription
  sink on data sensor).
- **Observable capability at completion:** changing a strategy's
  `select_markets` output changes the live Sensor subscription
  without code change.
- **Detail:** see total spec.

### S5 — `pms-controller-per-strategy-v1`

- **Goal:** Controller pipeline per-strategy; Evaluator aggregation
  per-strategy.
- **Key invariants addressed:** 2 (projections), 3 (version
  tagging becomes NOT NULL), 5 (strategy-awareness for Controller
  + Evaluator).
- **Observable capability at completion:** multiple strategies run
  concurrently; `/strategies` page shows comparative Brier / P&L
  per strategy.
- **Detail:** see total spec.

### S6 — `pms-research-backtest-v1`

- **Goal:** research-grade backtest with parameter sweep and
  backtest/live comparison tools.
- **Key invariants addressed:** none new; this sub-spec *uses* all
  prior invariants to make strategy research productive.
- **Observable capability at completion:** an offline run compares
  N strategies over a market universe and produces a ranked
  comparison report.
- **Detail:** see total spec.

---

## Relationship to `.harness/` tasks

Each sub-spec becomes one harness task with its own directory:

```
.harness/
├── pms-market-data-v1/
│   ├── spec.md                 (the detailed executable spec)
│   ├── spec-review/            (human + evaluator review rounds)
│   ├── checkpoints/            (per-CP generation + evaluation)
│   └── ... (full harness flow)
├── pms-strategy-aggregate-v1/
│   └── ...
...
```

The **total spec document** (to be written at PR #2 time) describes
the scope of all six sub-specs as seen from the project level. The
per-sub-spec `spec.md` files are written inside the relevant harness
run, informed by the total spec.

---

## Maintenance

This roadmap file is updated when:

- A sub-spec finishes (add completion date + link to retro).
- A dependency gate changes (e.g. S3/S4 order swap).
- A new sub-spec is inserted (requires retro documenting the
  architectural gap that motivates it).
- An invariant changes (triggers cascade review of affected sub-specs).
