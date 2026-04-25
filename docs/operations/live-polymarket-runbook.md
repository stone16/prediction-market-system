# Live Polymarket Runbook

LIVE mode is fail-closed. Do not paste private keys, API secrets, or
passphrases into chat, issues, PRs, logs, or config files.

## PAPER Soak

1. Run PAPER mode against live market data with production risk caps:
   `PMS_MODE=paper uv run pms-api`.
2. Confirm `/status`, `/trades`, `/positions`, and evaluator metrics update.
3. Review order notional, slippage, rejected orders, and portfolio exposure.
4. Keep `live_trading_enabled=false` until the soak is accepted.

## Credential Setup

Install the live SDK in the runtime environment:

```bash
uv sync --extra live
```

Export credentials in the operator shell or secret manager that launches PMS:

```bash
export PMS_MODE=live
export PMS_LIVE_TRADING_ENABLED=true
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
side, limit price, and max slippage. If the approval gate is absent or denies
the preview, the adapter raises `OperatorApprovalRequiredError` and submits
nothing.

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
