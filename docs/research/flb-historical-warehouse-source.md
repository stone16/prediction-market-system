# H1 FLB Historical Warehouse Source

Task #22 keeps the H1 feasibility work data-source focused. It does not add
H2 anchoring lag or LLM/news replay.

## Goal

Produce resolved Polymarket binary contract observations that can be replayed
through `scripts/flb_data_feasibility.py` and compared against PR #45's
contract-level `ContractObservation` semantics:

- YES contract at entry YES price `p`.
- NO contract at entry price `1 - p`.
- `pays_out` must come from explicit final settlement data.

The H1 data source is viable only when both extreme contract buckets meet the
sample gate:

- `[0%,10%)`: at least 100 contract observations.
- `[90%,100%]`: at least 100 contract observations.

## Warehouse CSV Contract

Export a CSV from Dune, a data warehouse, or a checked-in research fixture with
one row per resolved binary market. `market_id` must be unique; trade-level or
token-level exports must be aggregated to one entry snapshot per market before
running the gate.

The CSV requires these columns:

| Column | Meaning |
| --- | --- |
| `market_id` | Stable Polymarket condition/market id. |
| `question` | Human-readable market question. |
| `entry_yes_price` | YES price at the selected entry timestamp. Must be `0 < p < 1`. |
| `yes_payout` | Final YES payout. Must be exactly `1` or `0`. |
| `no_payout` | Final NO payout. Must be exactly `1` or `0`. |
| `volume` | Historical volume for liquidity filtering/reporting. |
| `liquidity` | Liquidity measure at or near the entry timestamp. |
| `entry_timestamp` | ISO-8601 timestamp for the entry price snapshot. |
| `resolved_at` | ISO-8601 settlement/resolution timestamp. |
| `category` | Market category used for reporting. |

Settlement correctness is strict: only `(yes_payout,no_payout)=(1,0)` or
`(0,1)` is accepted. Price-like or ambiguous vectors such as `0.995,0.005` and
`0.5,0.5` are rejected because they are not explicit binary settlement labels.

Timing correctness is also strict: `entry_timestamp` must be before
`resolved_at`. Same-time or post-resolution entry snapshots are rejected because
they can leak settlement truth into the FLB sample.

## Reproducible Run

```bash
uv run python scripts/flb_data_feasibility.py \
  --source warehouse-csv \
  --input exports/polymarket_resolved_binary.csv \
  --output docs/research/flb-report.md \
  --csv docs/research/flb-deciles.csv
```

The script returns:

- exit `0` when H1 data is viable and both extreme buckets have at least 100
  contract observations.
- exit `1` when H1 is not viable yet because the sample gate fails.
- exit `2` when no markets were loaded.

## Next Data Gap

If the warehouse export fails the sample gate, H1 is not rejected. The next gap
is historical source coverage: add a broader Dune query or warehouse table with
more resolved binary contracts and explicit payout vectors before building H2
LLM/news replay.
