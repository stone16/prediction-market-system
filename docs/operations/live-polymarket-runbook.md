# Live Polymarket Runbook

LIVE mode is fail-closed. Do not paste private keys, API secrets, or
passphrases into chat, issues, PRs, logs, or config files.

## PAPER Soak

1. Start from the first-live soak config:
   `cp config.live-soak.yaml config.local.live-soak.yaml`.
2. Confirm the risk envelope before every soak run:
   `max_position_per_market=$5`, `max_total_exposure=$50`,
   `max_drawdown_pct=20%`, `max_open_positions=5`,
   `max_quantity_shares=500`, and `slippage_threshold_bps=50`.
3. Run PAPER mode against live market data with the soak config:
   `uv run pms-api --config config.live-soak.yaml`.
   For process managers that cannot pass CLI args, set
   `PMS_CONFIG_PATH=config.live-soak.yaml`.
4. Confirm `/status`, `/trades`, `/positions`, and evaluator metrics update.
5. Review order notional, slippage, rejected orders, and portfolio exposure.
6. Keep `live_trading_enabled=false` until the 30-day soak and compliance
   checklist are accepted.

## Auto-Halt Triggers

PMS fail-closes before order submission when any of these live-soak triggers
trip:

- Polymarket API auth failure: HTTP 401 or 403.
- Drawdown above `risk.max_drawdown_pct`.
- Five consecutive losing filled trades.
- Average slippage above 100 bps across the last 10 filled trades.
- Three HTTP 429 rate-limit responses inside 10 minutes.
- Any submitted order remains unfilled for more than 30 minutes.

The halt state is explicit and reversible. Operators should first reconcile
venue state, credentials, open orders, and portfolio exposure, then call the
runner/admin path that invokes `RiskManager.clear_halt()`. Do not clear a halt
only to retry the same failing order.

## Daily Paper Report

Generate the daily soak report after each paper run:

```bash
uv run python scripts/paper-report.py --date 2026-05-03
```

Reports are written under `docs/paper-reports/YYYY-MM-DD.md` by default. Use
`--dry-run` to print the report in CI or during review. The report includes
Gate 3 metrics: decisions, fills, slippage, daily and cumulative P&L, drawdown,
exposure, Brier score, hit rate, average edge, Sharpe ratio, and risk events.

## Credential Setup

Install the live SDK in the runtime environment:

```bash
uv sync --extra live
```

Export credentials in the operator shell or secret manager that launches PMS:

```bash
export PMS_MODE=live
export PMS_LIVE_TRADING_ENABLED=true
export PMS_LIVE_ACCOUNT_RECONCILIATION_REQUIRED=true
export PMS_CONTROLLER__TIME_IN_FORCE=IOC
export PMS_POLYMARKET__PRIVATE_KEY=...
export PMS_POLYMARKET__API_KEY=...
export PMS_POLYMARKET__API_SECRET=...
export PMS_POLYMARKET__API_PASSPHRASE=...
export PMS_POLYMARKET__SIGNATURE_TYPE=1
export PMS_POLYMARKET__FUNDER_ADDRESS=...
export PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH=/secure/pms/first-order.json
```

Required fields are validated before LIVE mode starts:
`private_key`, `api_key`, `api_secret`, `api_passphrase`, `signature_type`,
and `funder_address`.

## First Live Order

The Polymarket adapter requires a first-order operator approval before any
venue submission. The preview includes max notional, venue, market, token,
side, outcome, market slug/question when available, limit price, and max
slippage. If the approval gate is absent or denies the preview, the adapter
raises `OperatorApprovalRequiredError` and submits nothing.

For the built-in file gate, write a JSON approval file that exactly matches the
preview:

```json
{
  "approved": true,
  "max_notional_usdc": 10.0,
  "venue": "polymarket",
  "market_id": "market-condition-id",
  "token_id": "outcome-token-id",
  "side": "BUY",
  "outcome": "NO",
  "limit_price": 0.4,
  "max_slippage_bps": 50
}
```

Keep the first-order notional at the minimum production risk cap. The approval
file is not a credential, but it should still live outside the repo so stale
approvals are not committed or reused accidentally.

## Rollback

1. Stop the runner: `curl -X POST http://127.0.0.1:8000/run/stop`.
2. Restart with `PMS_MODE=paper` and `PMS_LIVE_TRADING_ENABLED=false`.
3. Verify `/status` reports `mode=paper` before resuming autonomous operation.

## Emergency Stop

1. Stop PMS immediately: `curl -X POST http://127.0.0.1:8000/run/stop`.
2. Revoke or rotate Polymarket API credentials in the venue console.
3. Remove all `PMS_POLYMARKET__*` secrets from the runtime environment.
4. Restart only in BACKTEST or PAPER mode until exposure and open orders are
   reconciled.
