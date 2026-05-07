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

## Operator Workflow (STO-10)

The first-order gate is fail-closed by code (see
`src/pms/actuator/adapters/polymarket.py:584-660`), but the gate only does its
job when the human side is named, reachable, and accountable. This section
defines that human side.

### Named operators

- **Primary operator**: `TODO_DECISION` — fill in the named human responsible
  for first-order approvals. Until this is filled in, the gate must remain
  denied for every first-order signal.
- **Backup operator**: `TODO_DECISION` — fill in the named human who covers
  when the primary is unavailable.
- **Reachability rule**: at least one named operator must be reachable for
  every first-order event during normal market hours. First-order signals
  outside named operator hours **stall the strategy by design** — there is no
  on-call escalation path for first-order events until the user explicitly
  funds one.

Whoever is on Slack at the time is **not** the operator. An anonymous gate is
no gate.

### Approval-file location

- **Canonical environment variable**:
  `PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH`
  (consumed at `src/pms/runner.py:2098-2104` →
  `_first_live_order_gate`). An empty value resolves to
  `DenyFirstLiveOrderGate` and **locks** the gate; do not set the env var to
  empty as a "disable" — the gate is already disabled if the env var is
  unset.
- **Path**: `TODO_DECISION` — choose one of:
  - encrypted directory on the runner host (e.g. `/secure/pms/first-order.json`
    on a LUKS-backed mount or tmpfs);
  - Fly secret mount if the runner is deployed on Fly (see `fly.toml`);
  - operator's local machine over an SSH-tunnel-mounted dir.
- **Permissions**: write the file with `umask 077`. The runner process UID
  must be the only reader. The file must never be committed to the repo.
- **Sidecar metadata**: alongside the approval JSON, the operator's tool
  writes a `<approval-path>.meta.json` containing `{ "approver_id": "<id>",
  "ts": "<ISO 8601>" }`. The audit writer reads this on the next event and
  records the approver. Format remains stable across runs.

### "First" order semantics

"First" means **first since the actuator was instantiated** (see the in-
memory state at `src/pms/actuator/adapters/polymarket.py:571-576`). A process
restart resets the gate to denied — the next decision will re-prompt the
operator. **This re-prompt on restart is intentional**, not a bug: any
disruption that warrants a restart also warrants re-validating the operating
environment before the next live submit.

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
configurable via `PMS_LIVE_EMERGENCY_AUDIT_PATH`).

Three event types, written by `JsonlFirstOrderAuditWriter`
(`src/pms/storage/first_order_audit.py`):

| `event`              | When                                                  |
|----------------------|-------------------------------------------------------|
| `approval_matched`   | Gate returned True; submit is about to proceed.       |
| `approval_denied`    | Gate returned False; `OperatorApprovalRequiredError`. |
| `approval_consumed`  | Submit succeeded and `consume()` ran (file unlinked). |

A record carries: `ts`, `event`, `approver_id` (from sidecar, may be `null`),
`venue`, `market_id`, `token_id`, `side`, `outcome`, `max_notional_usdc`,
`limit_price`, `max_slippage_bps`, `market_slug`, `question`. The audit
writer is non-blocking — a write failure logs WARN and the order proceeds,
mirroring `runner.py:1307-1308`.

> **TODO_DECISION (audit sink)**: This runbook reuses `live_emergency_audit_path` as
> the single consolidated authorization log. Filter records by the `event`
> field (first-order) vs the `phase` field (emergency-halt) when reading.
> Switch to a dedicated `first_order_audit_path` if you prefer separate
> sinks; this is recorded as a deferred decision in STO-10.

### Alerting and SLA

- **Alert channel**: `TODO_DECISION` — choose one of: Slack webhook fed by a
  log shipper that watches for `OperatorApprovalRequiredError`; PagerDuty;
  email digest; or no alerting (operator polls runner status). Until this is
  set, the operator must actively check `/status` before any first-order
  signal is expected.
- **Max acceptable elapsed time** from `OperatorApprovalRequiredError` to
  `approval_consumed`: `TODO_DECISION` minutes. Exceeding this in cp-03
  rehearsal is a signal to streamline the procedure before live.

### End-to-end procedure (operator playbook)

When `OperatorApprovalRequiredError` is observed (in logs or via alert):

1. Pull the preview details from the error message (venue, market, token,
   side, outcome, max_notional_usdc, limit_price, max_slippage_bps).
2. Validate the preview against current strategy intent and risk caps.
3. If approved, run the operator tool to write both the approval JSON
   (matching every field) and the `<path>.meta.json` sidecar with your
   `approver_id`.
4. Wait for the next decision; the gate consults the file, matches, submits.
5. Confirm `approval_consumed` lands in the audit JSONL.
6. The fast path is now open for the rest of the actuator's lifetime; if
   the runner restarts, repeat from step 1 on the next decision.

If at any step you decide **not** to approve, do nothing. The next decision
will trigger another `OperatorApprovalRequiredError`; the audit log will
record `approval_denied`. The strategy stalls — that is the gate working as
intended.

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
