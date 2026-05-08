# Handoff prompt — Polymarket LIVE first-smoke

Paste the block below into a brand-new Claude Code session at the project root (`/Users/stometa/dev/prediction-market-system`). The prompt is fully self-contained — the new session has no prior context.

---

## Prompt to paste

```
I want to (b) restart the PAPER soak with the latest code on `fix/live-readiness-hardening` (or `main` after PR #30 merges) and verify the runner-side hardening works end-to-end against live Polymarket data, then (c) graduate to a single $5–10 IOC/FOK LIVE order on Polymarket. Address me as Stometa.

## Background you need to know

This is the prediction-market-system repo. A PR (#30, branch `fix/live-readiness-hardening`) just landed an 8-round + 3-fresh-final cross-LLM review (Codex peer) that hardened the LIVE Polymarket execution path. Full review trail: `.review-loop/latest/summary.md`. Don't re-review the diff — Codex already approved CONSENSUS. Just verify the build runs and operator-gate the LIVE smoke.

The user's previous PAPER soak (DB `pms_dev_stometa`) accumulated 44 positions / $1984 locked. The new portfolio reconciliation will pick that up on restart and clamp `free_usdc=0` — that's correct behaviour, not a bug. To get a fresh PAPER baseline, drop the `pms_dev_stometa` DB and recreate.

## Hard constraints — do NOT violate

1. **Never accept Polymarket credentials in chat / config files / git.** Per `docs/operations/live-polymarket-runbook.md:3-4`. Tell the user to export them in their operator shell only. The 6 required env vars are listed in the runbook.
2. **First LIVE order MUST be IOC or FOK** (not GTC). f6 deferral is binding: GTC creates resting orders that the runner won't reconcile across restart. IOC/FOK = fill-or-kill = no resting state.
3. **First LIVE order notional MUST be $5–10**. f4 (PG retry) and f5 (decision expiry race) are deferred per Codex Tier 3 advisory only for this small envelope. Don't exceed $10.
4. **Operator must manually inspect Polymarket UI** before AND after the first LIVE order to cross-check venue state. f6 deferral compensates with this human-in-loop.
5. **Don't merge the PR for the user.** Ask them to merge after they accept the smoke result.

## Step-by-step recipe

### Stage 0 — preflight (read-only)

```bash
# Confirm we're on the right branch / commit
cd /Users/stometa/dev/prediction-market-system
git status -s    # should be clean
git branch --show-current  # fix/live-readiness-hardening OR main if PR merged
gh pr view 30 --json state,mergeable 2>&1 | head -20
```

### Stage 1 — install + gates

```bash
uv sync --extra live   # CRITICAL: live SDK gets removed by bare `uv sync`
uv run python -c "import py_clob_client_v2; print('SDK OK')"
uv run pytest -q     # expect 714 passed / 155 skipped
uv run mypy src/ tests/ --strict   # clean across 293 source files
```

### Stage 2 — clean PAPER soak start

```bash
# Stop any running pms-api
PID=$(lsof -nP -iTCP:8000 -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $2}' | head -1)
if [ -n "$PID" ]; then
  curl -sS -X POST --max-time 3 http://127.0.0.1:8000/run/stop > /dev/null
  sleep 1
  kill -TERM "$PID" 2>/dev/null
  sleep 2
fi

# Drop & recreate dev DB for a clean baseline (the previous soak had $1984 locked)
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d postgres \
  -c "DROP DATABASE IF EXISTS pms_dev_stometa WITH (FORCE);"
PGPASSWORD=postgres createdb -h localhost -p 5432 -U postgres pms_dev_stometa

# Apply migrations (will land 0009 too)
DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_dev_stometa \
  uv run alembic upgrade head

# Start in PAPER mode with TIGHT risk caps (preview of LIVE safety envelope)
mkdir -p /tmp/pms-soak
PMS_MODE=paper \
PMS_AUTO_START=1 \
DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_dev_stometa \
PMS_SENSOR__POLL_INTERVAL_S=30 \
PMS_RISK__MAX_POSITION_PER_MARKET=5.0 \
PMS_RISK__MAX_TOTAL_EXPOSURE=10.0 \
PMS_RISK__MAX_QUANTITY_SHARES=200 \
PMS_RISK__MAX_DRAWDOWN_PCT=10.0 \
  uv run pms-api > /tmp/pms-soak/api.log 2>&1 &
echo "started PID $!"
```

### Stage 3 — PAPER soak verification (15-minute observation)

Verify within 15 minutes:
- `/status` returns 200 with `autostart_attempted: true, autostart_error: null, running: true`
- Both sensors report a recent `last_signal_at` (MarketDiscoverySensor + MarketDataSensor)
- `decisions_total > 0`, `fills_total > 0`
- Feedback table has only `severity=warning` rows (no `error`); reasons are `insufficient_liquidity` / `max_total_exposure` (expected at tight caps)
- DB row counts grow: markets > 0, tokens > 0, price_changes growing, decisions/orders/fills consistent

If everything looks healthy, ask the user to spot-check the dashboard at http://127.0.0.1:3100 (start with `cd dashboard && PMS_API_BASE_URL=http://127.0.0.1:8000 npm run dev`).

### Stage 4 — LIVE prerequisites (operator only)

Before launching LIVE, the user MUST have:

1. **Polymarket CLOB API L2 credentials** generated (api_key, api_secret, api_passphrase) — these are NOT the wallet private key, they come from Polymarket's CLOB authentication flow. Confirm with user before continuing.
2. **USDC.e on Polygon** funded on the funder address. Not USDC native, not Ethereum mainnet. Confirm with user.
3. **Signature type decided**: 1 (EOA / direct wallet) or 2 (proxy / browser wallet). Depends on funding pattern.
4. **First-order approval JSON** written OUTSIDE the repo (e.g. `/Users/stometa/.config/pms/first-order.json`). Template in `docs/operations/live-polymarket-runbook.md:51-62`. Tell the user to write this file with the exact preview values matching the intended decision.

DON'T accept any of these values in chat. Ask the user to export them in their own shell.

### Stage 5 — LIVE smoke launch

Once Stages 1–4 are verified and the user confirms credentials are exported in their shell:

```bash
# In the same shell where the user already exported PMS_POLYMARKET__* secrets:
PMS_MODE=live \
PMS_LIVE_TRADING_ENABLED=true \
DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_dev_stometa \
PMS_RISK__MAX_POSITION_PER_MARKET=5.0 \
PMS_RISK__MAX_TOTAL_EXPOSURE=10.0 \
PMS_RISK__MAX_QUANTITY_SHARES=200 \
PMS_RISK__MAX_DRAWDOWN_PCT=10.0 \
PMS_CONTROLLER__TIME_IN_FORCE=IOC \
PMS_AUTO_START=0 \
  uv run pms-api > /tmp/pms-soak/live.log 2>&1 &
```

Note `PMS_AUTO_START=0` — the operator should start the runner manually via `curl -X POST http://127.0.0.1:8000/run/start` AFTER inspecting `/status` (and after the user has verified Polymarket UI shows zero open orders for their account).

When the runner emits its first decision, it'll go through the first-order operator gate. The gate reads the approval JSON. If the JSON exactly matches the preview, gate approves once and the order goes to venue. If not, `OperatorApprovalRequiredError` raises and nothing reaches the venue.

### Stage 6 — post-smoke verification

After the first LIVE order:
- Check Polymarket UI for the order/fill (manual cross-check, f6 deferral binding)
- Query DB: `select * from orders where venue='polymarket' order by ts desc limit 5;`
- Query DB: `select * from fills order by ts desc limit 5;` — fill should appear if matched
- Approval JSON file should be unlinked (consume-on-success)
- `/positions` should show the new exposure
- Stop the runner gracefully: `curl -X POST http://127.0.0.1:8000/run/stop`

### Hard stop conditions

Abort and tell the user immediately if any of these happen:
- `/status` returns `autostart_error: <something>`
- More than 1 LIVE order goes through (we want exactly 1 for the smoke)
- `feedback` table has any `severity=error`
- Polymarket UI shows an order PMS doesn't know about (or vice versa)
- Anything looks weird in the DB row counts

## Read this for context if needed

- `.review-loop/latest/summary.md` — full review trail
- `docs/operations/live-polymarket-runbook.md` — the canonical LIVE recipe
- `agent_docs/architecture-invariants.md` — system-level invariants
- `agent_docs/promoted-rules.md` — engineering rules promoted from retros
- `CLAUDE.md` — project conventions
```

---

That's the prompt. Copy from "I want to (b) restart..." down to the closing backticks of the bash blocks (the ` ``` ` markers). The new Claude session will have everything it needs.
