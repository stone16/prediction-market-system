# 2026-04-16 Evaluator and Entity Abstraction Notes

Status: discovery note, not an implementation spec.

This note clarifies what the Evaluator should evaluate, which abstractions are
missing, and which entities should become first-class citizens before the next
Harness implementation spec.

## Why This Note Exists

The current cybernetic loop describes data flow:

`Sensor -> Controller -> Actuator -> Evaluator -> Feedback -> Controller`

That loop is directionally right, but it does not yet define the domain entities
that must survive across stages. A stage is a runtime role. An entity is a
stable domain object with identity, provenance, config, metrics, or persistence.

For strategy discovery, the important entities are not only `MarketSignal`,
`TradeDecision`, `OrderState`, `FillRecord`, and `EvalRecord`. The system also
needs entities such as `Factor`, `Strategy`, `Opportunity`, `BacktestRun`,
`StrategyRun`, and `EvaluationReport`.

## Standing Reference Repositories

These repositories should stay in the project documentation as continuing
architecture references:

- Research backtesting framework:
  `/Users/stometa/dev/quant/select-coin-backtesting/select-coin-pro_v1.8.1`
- Production/live strategy framework:
  `/Users/stometa/dev/quant/select-coin-prod/select-coin-pro`
- Prediction-market orderbook/backtest reference:
  `/Users/stometa/dev/PolyMarketArbitrage`

Use these repos as conceptual references only. Do not copy proprietary code from
the select-coin frameworks into PMS.

## Current Gaps

### 1. Evaluator only scores decisions, not strategies

Current code:

- `src/pms/core/models.py` defines `EvalRecord`.
- `src/pms/evaluation/spool.py` writes one `EvalRecord` per resolved decision.
- `src/pms/evaluation/metrics.py` aggregates decision-level Brier, PnL,
  slippage, fill rate, and win rate.

This answers: "Did this individual decision resolve well?"

It does not answer:

- Which strategy produced the decision?
- Which factor values caused the strategy to select it?
- Which strategy config/version was active?
- Which backtest dataset and execution assumptions produced the result?
- Did the strategy work across a market universe, or only in one fixture path?
- Did it fail because of alpha, liquidity, stale data, sizing, risk, or fill
  assumptions?

### 2. Backtest mode is replay-oriented, not research-oriented

Current PMS backtest mode:

1. `HistoricalSensor` replays one `MarketSignal` at a time.
2. `ControllerPipeline` gates the signal, runs forecasters, averages
   probabilities, sizes with Kelly, and emits one `TradeDecision`.
3. `BacktestActuator` uses fixture orderbooks to simulate that one decision.
4. `EvalSpool` writes a decision-level `EvalRecord` if an outcome exists.

This is useful for smoke testing the loop, but it is not enough for strategy
research. Strategy research needs a market universe, factor matrix, ranking,
selection, allocation, execution model, and run-level metrics.

### 3. Factor and Strategy are implicit

Current Controller concepts:

- `Forecaster`
- `Calibrator`
- `Sizer`
- `Router`

These are useful components, but they are not enough as top-level domain
objects. A `Forecaster` predicts a probability for one signal. A `Strategy`
should define how to observe a universe, compute factor values, rank/abstain,
size targets, and produce opportunities over time.

The current system has no first-class:

- `FactorDefinition`
- `FactorValue`
- `FactorPanel`
- `StrategyDefinition`
- `StrategyConfig`
- `StrategyVersion`
- `StrategyRun`
- `StrategySelection`
- `Opportunity`

### 4. Opportunity lifecycle is missing

Prediction-market systems need an object between "raw market data" and "order":

`Opportunity`

It should capture a candidate trade before risk and execution:

- market id and token ids
- side or legs
- factor values
- expected edge or expected profit
- confidence/rationale
- target size or allocation
- reject/abstain reason
- generated timestamp and expiry/staleness policy

Right now this information is compressed into `TradeDecision` fields such as
`prob_estimate`, `expected_edge`, `side`, `size`, and `stop_conditions`. That is
too late in the flow. The Evaluator cannot explain whether the strategy was bad
or whether execution later degraded a good opportunity.

### 5. Dataset and execution assumptions are not first-class

Backtest results are meaningless without:

- dataset version
- observation window
- market universe selection
- orderbook depth availability
- data-quality gaps
- stale-book policy
- fee model
- slippage/fill model
- latency model
- risk policy
- config hash

The current PMS `EvalRecord` has no place to preserve these assumptions.

### 6. Metrics are not versioned or attributed

Current `MetricsSnapshot` is a summary object. It does not represent a stable
metric definition, metric scope, or metric run.

Evaluator needs to distinguish:

- decision metrics
- opportunity metrics
- strategy-run metrics
- factor attribution metrics
- execution-quality metrics
- data-quality metrics
- portfolio/equity metrics

Without this separation, the dashboard may show numbers without making clear
what population or assumptions produced them.

## Reference Backtesting Repos: First-Class Citizens

### Select-coin backtesting framework

Reference inspected:

`/Users/stometa/dev/quant/select-coin-backtesting/select-coin-pro_v1.8.1`

Important first-class objects in that framework:

- `FactorConfig`: factor name, parameter, sort direction, weight.
- `FilterFactorConfig`: pre/post filter factor and condition.
- `StrategyConfig`: factor lists, filter lists, long/short selection counts,
  holding period, offsets, market scope, capital weights, config hash/name.
- `BacktestConfig`: global date range, account assumptions, costs, leverage,
  min order constraints, strategy list, factor cache requirements.
- Selection result: strategy output before execution, carrying target allocation
  ratios by time, symbol, side, strategy, and offset.
- Allocation matrix: time x asset target ratios after offset and multi-strategy
  aggregation.
- Simulation result/equity curve: account-level result after execution costs and
  rebalance rules.
- Evaluation report: annual return, max drawdown, return/drawdown ratio, win/loss
  periods, volatility, monthly/yearly returns.
- Parameter sweep: generated backtest configs and comparable result tables.

Most relevant lesson: factor calculation and strategy selection happen before
execution. Backtest/live divergence mainly belongs in the execution and fill
model, not in the strategy abstraction.

### Select-coin production/live strategy framework

Reference inspected:

`/Users/stometa/dev/quant/select-coin-prod/select-coin-pro`

Important first-class objects in that framework:

- Strategy module: a deployable strategy bundle imported from `strategy/`.
- Strategy group/account: `backtest_name` is treated as the strategy-combination
  name, and the config notes that one backtest group usually maps to one live
  account.
- Sub-strategy config: strategy id/name, hold period, offsets, spot/swap flag,
  long/short selection counts, capital weights, factor lists, pre-filters, and
  post-filters.
- Factor/filter config: factor name, parameter, sort direction, weight, and
  filter method/range.
- Selection result: persisted `选币结果.pkl` containing time, symbol, direction,
  strategy, offset, and target allocation ratios before execution simulation.
- Live selection snapshot: daily live `pkl` files under
  `data/实盘结果/<trading_name>/选币`.
- Live equity snapshot: `data/实盘结果/<trading_name>/equity.csv`.
- Time alignment policy: `hour_offset` and timezone adjustment used to align
  live timestamps with backtest timestamps.
- Symbol normalization policy: backtest symbols such as `BTC-USDT` are converted
  to live-style symbols such as `BTCUSDT` before comparison.
- Rebalance policy: `RebalanceMode` chooses whether to rebalance every period,
  by account-equity threshold, or by position-level threshold.
- Venue trading rules: public exchange metadata such as spot/swap minimum order
  quantities is stored as reusable lot-size/min-order data.
- Backtest/live comparison: equity-curve delta, selection overlap, backtest-only
  symbols, live-only symbols, and similarity metrics.

What "trading" means in this repo:

- I did not find an in-repo private order client, credential flow, balance sync,
  or order-placement call such as `create_order`.
- The repo appears to represent live operation through artifacts generated by a
  separate trading process: live equity CSV plus live selection pkl files.
- Its strongest live lesson for PMS is not the order API. It is the validation
  loop: run the same strategy-selection logic in backtest and live, then compare
  equity and selected instruments with explicit time and symbol alignment.

Backtesting vs production/live differences:

- The research backtesting repo has richer newer abstractions for market scope,
  order preference, and cross-section factors.
- The production/live repo has more operational strategy bundles, live-result
  comparison tools, pkl inspection utilities, and exchange trading-rule metadata.
- The core simulation and selection shape are still similar: load data, compute
  factors, select instruments, aggregate target allocation, simulate equity, and
  evaluate.
- The production/live repo is older/simpler around strategy scope: it mostly
  uses `is_use_spot`, while the newer backtesting repo generalizes scope with
  concepts such as market selection and order preference.
- For PMS, this argues for two linked loops: a research backtest loop and a
  paper/live comparison loop. Both should share `Factor -> StrategySelection ->
  Opportunity/PortfolioTarget`; only execution, fills, and live artifact
  ingestion should differ.

### PolyMarketArbitrage backtesting module

Reference inspected:

`/Users/stometa/dev/PolyMarketArbitrage`

Important first-class objects in that repo:

- `PriceLevel`: orderbook level.
- `MarketUpdate`: snapshot/delta update with timestamp and sequence.
- `OrderBook`: mutable book state reconstructed from snapshots/deltas.
- `MarketMetadata`: market id, YES/NO token ids, tick size, min order size,
  status, neg-risk flag, update rate, resolution time.
- `Opportunity`: strategy candidate before order submission, with size, VWAP,
  total cost, expected profit, confidence, and reason codes.
- `BacktestRunSpec`: strategy id, config hash, dataset version, time window,
  status.
- `BacktestV1Params`: fill/slippage/fee/staleness/gap assumptions.
- `BacktestV1Trade`: per-opportunity replay result.
- `BacktestV1Result`: total profit, expected profit, profit factor, max drawdown,
  staleness skips, gap skips, clock-drift skips.
- `Order`, `Fill`, `ExecutionReport`: execution lifecycle.
- `PnLRecord`, `RiskLimitSnapshot`, `Ledger`: audit and risk attribution.

Most relevant lesson: prediction-market backtests need orderbook-quality and
execution-quality entities, not only strategy PnL metrics.

## What Our First-Class Citizens Should Be

First-class means:

- It has identity.
- It can be persisted or replayed.
- It crosses at least one stage boundary.
- It appears in evaluation, dashboard, or config.
- It can be versioned, hashed, compared, or audited.

### Market Data Entities

`Market`

Stable market metadata: venue, market id, title, category, resolution time,
status, rules, token mapping.

`OutcomeToken`

Tradable outcome leg: token id, market id, outcome label, tick size, min order
size, neg-risk relationship.

`MarketObservation`

A timestamped normalized view of price, volume, status, external priors, and
metadata. Current `MarketSignal` is close, but it mixes observation data,
orderbook data, and untyped external fields.

`OrderBookSnapshot`

L2 book state at a timestamp. Needs token id, bids, asks, sequence/hash, source
timestamp, ingest timestamp, and data-quality flags.

`MarketUpdate`

Snapshot/delta event used to reconstruct orderbooks. It should be replayable and
auditable.

`MarketUniverse`

The set of markets/tokens eligible for a backtest or strategy run, with inclusion
and exclusion rules.

### Factor Entities

`FactorDefinition`

The reusable factor primitive. Defines id, name, description, input schema,
parameters, output type, direction semantics, and owner.

`FactorConfig`

A strategy-specific factor usage: factor id, param, weight, sort direction,
normalization/ranking method.

`FactorValue`

One computed value for `(factor_id, param, market_id/token_id, timestamp)`.

`FactorPanel`

A queryable factor matrix over a market universe and time range. This is what
backtests and strategies should consume.

`FactorAttribution`

Evaluator output that explains which factor values contributed to selection,
rejection, PnL, drawdown, or calibration errors.

### Strategy Entities

`StrategyDefinition`

The stable strategy type: id, name, description, supported inputs, output
contract, and version.

`StrategyConfig`

The declarative strategy instance: factor list, filter list, selection counts,
hold/rebalance schedule, allocation rules, risk budget, market universe, and
execution constraints.

`StrategyBundle`

A deployable group of one or more strategy configs that share account/risk
budget, universe assumptions, and evaluation scope. This mirrors the select-coin
production pattern where a strategy-combination/backtest group maps closely to a
live account.

`StrategyVersion`

Stable hash of the strategy definition plus config. This should be present in
every opportunity, decision, order, fill, backtest run, metric, and dashboard
payload.

`StrategySelection`

The strategy output before risk and execution: selected/abstained markets,
rankings, scores, target allocations, and reasons.

`Opportunity`

A concrete candidate trade produced by a strategy selection. This is the bridge
between Strategy and Actuator.

`PortfolioTarget`

Time-indexed target exposure or allocation by market/token/side. This is the
research equivalent of "what the strategy wants" before execution modeling.

`SelectionSnapshot`

Persisted strategy output for a timestamp or run. This should be comparable
between backtest, paper, and live before looking at final PnL.

### Evaluation Entities

`BacktestDataset`

Dataset identity and coverage: source, version, time window, market universe,
snapshot cadence, delta availability, data-quality gaps, and schema version.

`ExecutionModel`

The backtest execution assumptions: fill policy, fee model, slippage model,
latency model, staleness policy, tick/min-size rounding, partial-fill behavior.

`BacktestSpec`

The complete input to a backtest: strategy version, dataset, execution model,
risk policy, account assumptions, date range, and config hash.

`BacktestRun`

A materialized run with run id, spec hash, status, timestamps, artifact paths,
and summary state.

`StrategyRun`

Per-strategy run inside a backtest or live/paper session. A multi-strategy
backtest should have one `BacktestRun` and many `StrategyRun`s.

`EvaluationMetric`

Metric definition and scope. Examples: Brier, expected PnL, realized PnL,
drawdown, fill ratio, opportunity hit rate, calibration error, staleness skip
rate, factor IC, and slippage per notional.

`StrategyMetrics`

Metric values for a strategy run, grouped by market class, factor family,
liquidity bucket, venue, time window, and opportunity type.

`EvaluationReport`

Evaluator output that ties together run metadata, metrics, attribution,
warnings, benchmark comparisons, and recommended next actions.

`BacktestLiveComparison`

Evaluator artifact that aligns a backtest run with paper/live artifacts and
reports equity divergence, selection overlap, backtest-only opportunities,
live-only opportunities, and timestamp/symbol alignment warnings.

`SelectionSimilarityMetric`

Metric definition for comparing two selection snapshots. It should make the
denominator explicit, because overlap can be computed relative to the backtest
selection set, the live selection set, or the union.

`TimeAlignmentPolicy`

Rule for aligning generated, exchange, ingest, and evaluation timestamps across
backtest/paper/live. The select-coin production framework makes this explicit
with `hour_offset`; PMS needs the same concept for latency, stale books, and
resolution timing.

`SymbolNormalizationPolicy`

Rule for mapping venue identifiers, token ids, market ids, and human-readable
symbols across datasets and execution surfaces.

### Execution and Risk Entities

These already partially exist, but need strategy provenance:

- `TradeDecision`
- `OrderState`
- `FillRecord`
- `Position`
- `Portfolio`
- `RiskPolicy`
- `RiskCheckResult`
- `RiskLimitSnapshot`
- `RebalancePolicy`
- `VenueTradingRule`
- `Feedback`

The change is not to discard these objects. The change is to make sure each one
can be traced back to `StrategyVersion`, `Opportunity`, `BacktestRun` or
`StrategyRun`, and relevant factor values.

## Proposed Abstraction Direction

### 1. Separate stages from entities

Stages:

- Sensor observes.
- Controller selects.
- Actuator executes.
- Evaluator measures.

Entities:

- Market data entities describe what was observed.
- Factor entities describe derived reusable signals.
- Strategy entities describe selection logic and outputs.
- Execution entities describe what was attempted and filled.
- Evaluation entities describe how results were measured.

### 2. Make Factor the first Controller primitive

Controller should not start with a list of forecasters. It should start with:

1. Market universe.
2. Factor definitions/configs.
3. Factor panel.
4. Strategy configs that consume factor panels.
5. Opportunity/portfolio target output.

Forecasters can still exist, but they should become one type of factor producer
or strategy component, not the top-level abstraction.

### 3. Make Strategy the first user-facing primitive

Dashboard and config should be organized around strategies:

- What factors does this strategy use?
- Which markets did it select or reject?
- What did it believe?
- What allocation did it request?
- How did it perform in backtest/paper/live?
- Which factor or execution assumption explains failure?

### 4. Make Evaluator run-oriented

Evaluator should evaluate populations, not just single decisions:

- `BacktestRun`
- `StrategyRun`
- `Opportunity` cohort
- market cohort
- factor cohort
- execution cohort

Decision-level `EvalRecord` remains useful, but it becomes one raw material for
strategy-level evaluation.

### 5. Treat backtest and live as the same strategy path

The selection path should be shared:

`MarketUniverse -> FactorPanel -> StrategySelection -> Opportunity -> PortfolioTarget`

Backtest and live should diverge at:

- execution model
- fill source
- latency/staleness treatment
- result finalization
- artifact ingestion and reconciliation

They should not diverge at strategy computation.

### 6. Add a paper/live comparison loop before real-money execution

The select-coin production framework shows a useful intermediate loop: compare
backtest outputs with paper/live outputs before treating live execution as
validated. PMS should mirror that:

1. Persist backtest `SelectionSnapshot` and `PortfolioTarget`.
2. Persist paper/live `SelectionSnapshot`, `TradeDecision`, fills, and equity.
3. Align timestamps and identifiers with explicit policies.
4. Evaluate selection overlap, execution divergence, and realized PnL separately.
5. Promote a strategy only when the selection path and execution path both pass
   acceptance gates.

## Minimum Acceptance Criteria For The Next Tech Spec

The next Harness spec should not start implementation until it answers:

1. What are the required fields for `FactorDefinition`, `FactorConfig`,
   `FactorValue`, and `FactorPanel`?
2. What is the required output contract for `StrategySelection` and
   `Opportunity`?
3. How is `StrategyVersion` computed and propagated into decisions, orders,
   fills, metrics, and dashboard payloads?
4. What is the minimal `BacktestDataset` shape for replaying strategy research?
5. What is the minimal `ExecutionModel` for comparing backtest/paper/live?
6. Which metrics are decision-level, opportunity-level, strategy-level, and
   run-level?
7. How does Evaluator persist and compare `BacktestRun` and `StrategyRun`?
8. Which dashboard view is the source of truth for strategies and evaluation?
9. How do backtest, paper, and live selection snapshots get aligned and compared?
10. Which artifacts are required before a strategy is allowed to reach a live
    Actuator?

## Non-Goals For The Next Spec

- Do not implement real-money live execution first.
- Do not hide factor values inside `external_signal`.
- Do not encode strategy identity only in `stop_conditions`.
- Do not treat a dashboard chart as an evaluation artifact unless it is backed
  by versioned run data.
- Do not make the backtest path a separate strategy implementation from the live
  path.
