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
- Strategy plugin: `pms.strategies.flb`
  - source: `LiveFlbSource`
  - controller: `FlbController`
  - agent: `FlbAgent`
  - module: `FlbStrategyModule`

## Out Of Scope

H2 anchoring lag and LLM/news replay are not implemented here. They should wait
until the historical data spine proves H1 has enough resolved Polymarket
coverage and measurable edge.
