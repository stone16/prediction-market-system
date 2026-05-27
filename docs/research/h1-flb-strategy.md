# H1 FLB Strategy Implementation Contract

This note converts the prediction-methodology research into the first
implemented strategy slice. It intentionally implements H1 only.

## H1 Semantics

Favorite-longshot bias says low-probability YES contracts are often overpriced
and high-probability YES contracts are often underpriced.

For binary Polymarket contracts:

- YES price below 10%: treat the YES longshot as overpriced and buy NO.
- YES price above 90%: treat the YES favorite as underpriced and buy YES.
- Middle deciles: no H1 trade.

## Implemented Surface

- Factor: `favorite_longshot_bias`
  - negative value: low-YES longshot bucket, actionable side is buy NO
  - positive value: high-YES favorite bucket, actionable side is buy YES
  - catalog direction stays `neutral` to satisfy the current factor schema; the
    factor value itself carries the signed H1 semantics
- Strategy plugin: `pms.strategies.flb`
  - source: `LiveFlbSource`
  - controller: `FlbController`
  - agent: `FlbAgent`
  - module: `FlbStrategyModule`

## Edge Model

The runtime edge model has two modes:

- With `strategies.flb_calibration_path`, probability estimates come from a
  warehouse-calibrated CSV artifact keyed by H1 signal name.
- Without that artifact, probability estimate falls back to observed limit
  price + `min_expected_edge`; this path is paper-plumbing only.

The emitted `expected_edge` is net edge:

```text
probability_estimate
- limit_price
- (entry_execution_cost_bps / 10_000)
- fee_rate * (1 - limit_price)
```

Signals below `min_expected_edge` after these deductions are suppressed before
sizing. Before live trading, replace the placeholder probability path and the
static execution-cost buffer with data-driven decile/category/horizon estimates
from the historical warehouse plus paper/live telemetry.

## Out Of Scope

H2 anchoring lag and LLM/news replay are not implemented here. They should wait
until the historical data spine proves H1 has enough resolved Polymarket
coverage and measurable edge.
