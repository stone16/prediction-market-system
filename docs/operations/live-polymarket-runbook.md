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

The temporary approved local path is a file-mounted secret outside the repo.
It is weaker than a real secret manager, but avoids shell history, dotfiles,
and `.env` files while we keep LIVE local. Do not export Polymarket
credentials in an operator shell, dotfile, `.env`, compose override, or normal
config file.

Create a private local secret file and edit it with an editor. The file must
be readable only by the operator account:

```bash
install -d -m 700 ~/.config/pms
install -m 600 /dev/null ~/.config/pms/polymarket.local-secrets.yaml
$EDITOR ~/.config/pms/polymarket.local-secrets.yaml
```

Use this YAML shape in the secret file:

```yaml
polymarket:
  private_key: <paste private key>
  api_key: <paste API key>
  api_secret: <paste API secret>
  api_passphrase: <paste API passphrase>
  signature_type: 1
  funder_address: <paste wallet address>
```

PMS refuses local LIVE startup if the file is missing, not a regular file, or
is group/world readable. Fix permissions with:

```bash
chmod 600 ~/.config/pms/polymarket.local-secrets.yaml
```

Required fields are validated before LIVE mode starts:
`private_key`, `api_key`, `api_secret`, `api_passphrase`, `signature_type`,
and `funder_address`.

Configure LIVE mode with non-secret runtime config, not with credential
exports. For example, `config.live.yaml` should include:

```yaml
mode: live
secret_source: local_file
local_secret_file: ~/.config/pms/polymarket.local-secrets.yaml
live_trading_enabled: true
live_account_reconciliation_required: true
controller:
  time_in_force: IOC
polymarket:
  first_live_order_approval_path: /secure/pms/first-order.json
```

Before the first live run, rotate any Polymarket credential that was ever
pasted into a shell, issue, PR, chat, local `.env`, or dotfile during
development. Treat those values as compromised.

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
