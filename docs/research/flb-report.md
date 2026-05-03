# H1 FLB Data Feasibility Report

**Generated:** 2026-05-03T09:22:35.478539+00:00
**Total resolved markets analyzed:** 13

## Sample Gate: ❌

| Bucket | Count | Required | Status |
|--------|-------|----------|--------|
| Longshot [0%-10%) | 12 | ≥100 | ❌ |
| Favorite [90%-100%] | 1 | ≥100 | ❌ |

**H1 NOT VIABLE YET.** Insufficient resolved contracts in target buckets. Collect more data before proceeding with FLB strategy.

## FLB by Probability Decile

| Decile | Range | N | N_YES | Implied P | Actual Rate | FLB Gap | 95% CI | Side |
|--------|-------|---|-------|-----------|-------------|---------|--------|------|
| 0 | [0%-10%) | 12 | 0 | 0.3% | 0.0% | +0.3% | [0.0%, 24.3%] | ⚪ no edge |
| 1 | [10%-20%) | 0 | 0 | 0.0% | 0.0% | — | — | ⚪ no edge |
| 2 | [20%-30%) | 0 | 0 | 0.0% | 0.0% | — | — | ⚪ no edge |
| 3 | [30%-40%) | 0 | 0 | 0.0% | 0.0% | — | — | ⚪ no edge |
| 4 | [40%-50%) | 0 | 0 | 0.0% | 0.0% | — | — | ⚪ no edge |
| 5 | [50%-60%) | 0 | 0 | 0.0% | 0.0% | — | — | ⚪ no edge |
| 6 | [60%-70%) | 0 | 0 | 0.0% | 0.0% | — | — | ⚪ no edge |
| 7 | [70%-80%) | 0 | 0 | 0.0% | 0.0% | — | — | ⚪ no edge |
| 8 | [80%-90%) | 0 | 0 | 0.0% | 0.0% | — | — | ⚪ no edge |
| 9 | [90%-100%] | 1 | 1 | 99.9% | 100.0% | -0.1% | [20.7%, 100.0%] | ⚪ no edge |

## Side Semantics (H1 FLB Contrarian)

| Condition | Market Says | FLB Says | Action |
|-----------|-------------|----------|--------|
| YES price < 10% (longshot) | Low prob event likely | Overpriced (actual rate < implied) | **BUY NO** |
| YES price > 90% (favorite) | High prob event likely | Underpriced (actual rate > implied) | **BUY YES** |

## Market Categories

| Category | Count |
|----------|-------|
| sports | 8 |
| politics | 3 |
| other | 2 |

## Volume Statistics

- **Min volume:** $1,162
- **Median volume:** $30,917
- **Max volume:** $601,109
- **Total volume:** $1,369,972

## Limitations

1. **Entry price proxy:** Uses `lastTradePrice` (last trade before resolution), NOT a timestamped entry snapshot. For strategy P&L backtesting, we need price snapshots at a defined entry horizon.
2. **Data source:** Gamma API `closed=true` returns a small window of recently resolved markets. For ≥100 contracts per target bucket, Dune Analytics on-chain data or a historical warehouse is required.
3. **Feasibility only:** This is a bias-detection script, not a strategy backtest. It does not account for fees, slippage, or execution timing.
