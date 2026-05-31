# Paper Daily Report - 2026-05-25

## Report Provenance

| Field | Value |
|---|---|
| generated_by | scripts/paper_report.py |
| generated_at | 2026-05-25T00:00:00+00:00 |
| artifact_mode | persisted |
| output_path | docs/paper-reports/2026-05-25.md |

## Summary

| Metric | Value | Gate |
|---|---:|---|
| Strategy | default@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 | - |

## Go/No-Go Gate

**Decision:** GO

| Check | Status | Detail |
|---|---|---|
| soak_days | PASS | 30 >= 30 |
| decisions_accepted | PASS | 50 >= 30 |
| fills | PASS | 50 >= 50 |
| distinct_markets | PASS | 3 >= 3 |
| distinct_risk_groups | PASS | 3 >= 3 |
| max_market_fill_share | PASS | 0.4000 <= 0.6000 |
| max_risk_group_fill_share | PASS | 0.4000 <= 0.6000 |
| fill_rate | PASS | 0.4000 > 0.0000 |
| average_slippage_bps | PASS | 10.0000 <= 50.0000 |
| todays_pnl | PASS | 0.0000 >= -20.0000 |
| cumulative_pnl | PASS | 2.0000 > 0.0000 |
| max_drawdown_pct | PASS | 5.0000 <= 20.0000 |
| open_positions | PASS | 2 <= 5 |
| total_exposure | PASS | 10.0000 <= 50.0000 |
| brier_score | PASS | 0.18 <= 0.20 |
| brier_improvement | PASS | 0.05 >= 0.0 |
| hit_rate | PASS | 0.50 >= 0.45 |
| average_edge_bps | PASS | 50.0 >= 5.0 |
| average_net_edge_bps | PASS | 20.0 >= 0.0 |
| sharpe_ratio | PASS | 0.5000 > 0.0000 |
| strategy_evidence | PASS | default@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 |
| unresolved_incidents | PASS | 0 unresolved |
| risk_events | PASS | 0 risk event(s) |

## Baseline Evidence Coverage

| Baseline | Decisions | Coverage |
|---|---:|---:|
| market_implied | 50 / 50 | 100.0% |
| mid_quote | 50 / 50 | 100.0% |
| last_trade | 40 / 50 | 80.0% |
| category_prior | 50 / 50 | 100.0% |

## Secondary Baseline Brier

| Baseline | Baseline Brier | Brier improvement |
|---|---:|---:|
| market_implied | 0.2300 | 0.0500 |
| mid_quote | 0.2200 | 0.0400 |
| last_trade | 0.2400 | 0.0300 |
| category_prior | 0.2100 | 0.0200 |
