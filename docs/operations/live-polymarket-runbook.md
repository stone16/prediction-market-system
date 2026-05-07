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
7. Ratify the strategic exit criteria (the kill plan) defined in
   [live-exit-criteria.md](live-exit-criteria.md) **before** the first live
   order. Do not flip `live_trading_enabled=true` while any threshold is
   marked `TODO_DECISION:`.

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

## Pre-launch Operator Checklist (STO-10)

This is the human work that must happen before flipping
`live_trading_enabled=true`. Walk top to bottom. Items are tagged
**[setup]** (one-time), **[fill-in]** (replace `__FILL_IN__` in this
file), or **[confirm]** (verify each launch).

The first-order gate is fail-closed by code
(`src/pms/actuator/adapters/polymarket.py:584-660`), but it only does its
job when the human side is named, reachable, and accountable. Each item
below makes one piece of the human side concrete.

### 0. Prerequisites — Polymarket account [setup]

Done outside this repo.

- [ ] Polymarket account in good standing. The funder wallet
      (`PMS_POLYMARKET__FUNDER_ADDRESS`) holds USDC.e on Polygon
      (chain id 137). Minimum balance: at least the live-soak
      `max_total_exposure` (\$50) plus a buffer for slippage and gas.
- [ ] CLOB API credentials issued from the Polymarket dashboard:
      `private_key`, `api_key`, `api_secret`, `api_passphrase`,
      `signature_type` (use `1` for Polymarket-managed signing).
- [ ] Two-factor enabled on the Polymarket account; the funder wallet
      private key is held in a hardware wallet or 1Password vault, not
      in plaintext on a laptop.

### 1. Stage credentials in Fly secrets [setup]

The production target is the Fly app `pms-paper-soak` (`fly.toml:1`).
All secrets stay on Fly's encrypted secret store, never in the repo,
never in shell history. From a shell with `flyctl` authenticated:

```bash
fly secrets set \
  PMS_POLYMARKET__PRIVATE_KEY='0x...' \
  PMS_POLYMARKET__API_KEY='...' \
  PMS_POLYMARKET__API_SECRET='...' \
  PMS_POLYMARKET__API_PASSPHRASE='...' \
  PMS_POLYMARKET__SIGNATURE_TYPE='1' \
  PMS_POLYMARKET__FUNDER_ADDRESS='0x...' \
  --app pms-paper-soak
```

Confirm without revealing values:

```bash
fly secrets list --app pms-paper-soak
```

For local development (NOT production): export the same variables in
the shell that launches `uv run pms-api` (or use `direnv` with a
`.envrc.local` that is gitignored). Never commit a `.env` containing
these.

> Required-fields validation runs at LIVE-mode startup
> (`src/pms/config.py` →
> `validate_live_mode_ready`). A missing field fails closed before any
> venue call.

### 2. Provision a Fly volume for the approval-file path [setup]

The approval JSON must persist across deploys but live outside the
container image. On Fly that means a mounted volume.

```bash
fly volumes create pms_data --region iad --size 1 --app pms-paper-soak
```

Then add this block to `fly.toml` (anywhere after the `[env]` block):

```toml
[[mounts]]
  source = "pms_data"
  destination = "/data"
```

Set the canonical path as a Fly secret (it is not strictly secret, but
keeping it next to the credentials makes rotation simpler):

```bash
fly secrets set \
  PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH=/data/pms/first-order.json \
  --app pms-paper-soak
```

Empty value pitfall: `_first_live_order_gate`
(`src/pms/runner.py:2098-2104`) treats an empty string as
`DenyFirstLiveOrderGate` — the gate **locks shut**, it does not
"disable." Do not set the env to empty as a workaround.

For local development the path is freeform. Recommended:
`~/.local/share/pms/first-order.json`. Create the parent dir with
`umask 077` so only your user account can read it.

### 3. Name the primary and backup operators [fill-in]

Edit the lines below. The gate has no concept of "operator" — naming
is enforced socially, by this runbook, and audited via the sidecar
metadata file.

- **Primary operator**: `__FILL_IN__` — handle (e.g. GitHub username),
  contact (Slack DM, phone), and time-zone window of availability.
- **Backup operator**: `__FILL_IN__` — same fields. Covers when the
  primary is unreachable.
- **Reachability rule**: at least one named operator must be reachable
  for every first-order event during the configured operator window.
  First-order signals outside that window stall the strategy by design;
  there is no on-call escalation path until you explicitly fund one.

Whoever happens to be on Slack at the time is **not** the operator. An
anonymous gate is no gate.

### 4. Configure operator alerting [setup, recommended default]

**Recommended default**: tail the runner log for the literal string
`OperatorApprovalRequiredError` and post to a Slack webhook. Lightweight,
no extra paid service, sufficient at \$100 bankroll.

Suggested log shipper rule (Fly Log Shipper, Vector, or
`fly logs --app pms-paper-soak` piped through grep):

- Match: log line contains `OperatorApprovalRequiredError`.
- Action: POST to `SLACK_OPERATOR_WEBHOOK_URL` with the matched line.
- Throttle: 1 message per 60 s (the gate is one-shot per actuator
  lifetime so floods are unlikely, but the log line repeats per
  decision until the file is filed).

If you skip this step, the operator must actively poll
`/status`. That is acceptable only for the first cp-03 rehearsal.

### 5. Confirm the SLA threshold [confirm, recommended default]

**Recommended default**: 15 minutes from
`OperatorApprovalRequiredError` raise to `approval_consumed` event in
the audit JSONL. Below 15 minutes is normal; above 15 minutes triggers
a follow-up to streamline the procedure (or, if it happens twice in a
row, revert to PAPER until the bottleneck is fixed).

This is your risk tolerance call. Tighten to 5 minutes if you trade
short-lived markets; loosen to 60 minutes if you only trade long-dated
ones.

### 6. Run the cp-03 rehearsal before going live [confirm]

Before flipping `live_trading_enabled=true`, walk the procedure end to
end against the paper-soak config with a fake client. Do this from a
clean shell at the repo root:

```bash
# Terminal A — start the runner.
PMS_CONFIG_PATH=config.live-soak.yaml uv run pms-api

# Terminal B — drop a matching approval JSON + sidecar.
mkdir -p $(dirname $PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH)
cat > $PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH <<'JSON'
{ "approved": true, "max_notional_usdc": 5.0, "venue": "polymarket",
  "market_id": "<from preview>", "token_id": "<from preview>",
  "side": "BUY", "outcome": "YES",
  "limit_price": 0.4, "max_slippage_bps": 50 }
JSON
cat > "${PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH}.meta.json" <<'JSON'
{ "approver_id": "<your-handle>", "ts": "2026-05-07T00:00:00Z" }
JSON

# Confirm the audit log records matched -> consumed.
tail -n 5 .data/live-emergency-audit.jsonl
```

The rehearsal is acceptance-complete when:

- The audit JSONL shows exactly `approval_matched` followed by
  `approval_consumed` for the rehearsal decision (no spurious events).
- The approval file is unlinked after consume.
- Both primary and backup have run the rehearsal at least once.
- Elapsed time from raise to consume is below the chosen SLA from
  step 5.

Append a short sign-off entry to this runbook ("Rehearsal log
YYYY-MM-DD: primary X, backup Y, elapsed N minutes") on completion.

### 7. Final go/no-go

Only flip `PMS_LIVE_TRADING_ENABLED=true` when steps 0-6 are all
checked. The first live decision will hit the slow path; the gate will
deny; the operator follows the playbook in the **Reference** section
below.

---

## Reference

### Named operators

Replace the `__FILL_IN__` markers in step 3 above. The reachability
rule and "anonymous gate is no gate" framing apply once names are
recorded.

### Approval-file location

- **Production (Fly)**: `/data/pms/first-order.json` (provisioned in
  step 2). Read only by the runner container's process UID.
- **Local development**: a freeform path under the operator's home,
  for example `~/.local/share/pms/first-order.json`. Created with
  `umask 077`.
- **Sidecar metadata**: alongside the approval JSON, the operator
  writes `<approval-path>.meta.json` containing
  `{ "approver_id": "<id>", "ts": "<ISO 8601>" }`. The audit writer
  reads this and records the approver in the JSONL.
- **Never** commit either file to the repo. Both are gitignored under
  `.data/` and operator-specific paths.

### "First" order semantics

"First" means **first since the actuator was instantiated** (see the
in-memory state at
`src/pms/actuator/adapters/polymarket.py:571-576`). A process restart
resets the gate to denied — the next decision will re-prompt the
operator. **This re-prompt on restart is intentional**, not a bug: any
disruption that warrants a restart also warrants re-validating the
operating environment before the next live submit.

Concretely, an approval is consumed exactly once per actuator lifetime:

1. Operator drops the approval JSON at the configured path.
2. Adapter matches the next decision against the file and submits.
3. Adapter calls `consume()`, which unlinks the file
   (`polymarket.py:324-330`).
4. `_approval_state.approved = True` flips the fast path open
   (`polymarket.py:599-604`); subsequent orders skip the slow path.
5. On any process restart, step 1 must repeat with a freshly-filed
   approval.

### Audit trail

Every gate consultation appends one record to the JSONL at
`live_emergency_audit_path` (default `.data/live-emergency-audit.jsonl`,
configurable via `PMS_LIVE_EMERGENCY_AUDIT_PATH`). The audit writer is
the same `JsonlFirstOrderAuditWriter` wired in
`src/pms/storage/first_order_audit.py`.

Three event types:

| `event`              | When                                                  |
|----------------------|-------------------------------------------------------|
| `approval_matched`   | Gate returned True; submit is about to proceed.       |
| `approval_denied`    | Gate returned False; `OperatorApprovalRequiredError`. |
| `approval_consumed`  | Submit succeeded and `consume()` ran (file unlinked). |

A record carries: `ts`, `event`, `approver_id` (from sidecar, may be
`null`), `venue`, `market_id`, `token_id`, `side`, `outcome`,
`max_notional_usdc`, `limit_price`, `max_slippage_bps`, `market_slug`,
`question`. The audit writer is non-blocking — a write failure logs
WARN and the order proceeds, mirroring `runner.py:1307-1308`.

The runbook reuses `live_emergency_audit_path` as the single
consolidated authorization log; filter records by the `event` field
(first-order) vs the `phase` field (emergency-halt) when reading.
Switching to a dedicated path is a deferred decision; revisit if
authorization-event volume grows enough to make filtering noisy.

### End-to-end procedure (operator playbook)

When `OperatorApprovalRequiredError` is observed (in logs or via the
Slack alert from step 4):

1. Pull the preview details from the error message (venue, market,
   token, side, outcome, max_notional_usdc, limit_price,
   max_slippage_bps).
2. Validate the preview against current strategy intent and risk caps.
3. If approved, write the approval JSON (matching every field) and
   the `<path>.meta.json` sidecar with your `approver_id` to the
   configured path. On Fly: `fly ssh console --app pms-paper-soak`,
   then write to `/data/pms/first-order.json` and the matching
   sidecar.
4. Wait for the next decision; the gate consults the file, matches,
   submits.
5. Confirm `approval_consumed` lands in the audit JSONL.
6. The fast path is now open for the rest of the actuator's lifetime;
   if the runner restarts, repeat from step 1 on the next decision.

If at any step you decide **not** to approve, do nothing. The next
decision will trigger another `OperatorApprovalRequiredError`; the
audit log will record `approval_denied`. The strategy stalls — that
is the gate working as intended.

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
