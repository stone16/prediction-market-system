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

The first slice uses `min_expected_edge` as a placeholder edge model:

- probability estimate = observed limit price + `min_expected_edge`
- default `min_expected_edge` = 2%

This is intentionally conservative and deterministic for paper soak. Before
live trading, replace the placeholder with data-driven decile edge estimates
from the historical warehouse / Wilson CI pipeline so 1% longshots, 9%
longshots, and 95% favorites are not treated as having identical edge.

## Out Of Scope

H2 anchoring lag and LLM/news replay are not implemented here. They should wait
until the historical data spine proves H1 has enough resolved Polymarket
coverage and measurable edge.
