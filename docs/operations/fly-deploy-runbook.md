# PMS Fly.io Deployment Runbook

This runbook covers two separate Fly apps:

- `fly.toml` runs the supervised PAPER soak process.
- `fly.live.toml.example` is the true LIVE capital template. Copy it to the
  ignored `fly.live.toml`, replace every `__FILL_IN_*__` value, stage secrets
  through Fly, and deploy it as a separate app only after the paper-soak,
  rehearsal, compliance, and credentialed preflight gates pass.

PR verification does not deploy either machine and does not restart the active
runner.

## Launch

Run the one-time Fly bootstrap interactively:

```bash
fly launch --no-deploy
```

Keep the generated app name aligned with `fly.toml` or update `fly.toml` in the
same change.

## Secrets

Inject Fly deployment secrets through Fly, never through config files, shell
exports, `.env` files, chat, issues, or PRs.

For single-value supervisor secrets such as `PMS_API_TOKEN` or the alert
webhook, `fly secrets set` is fine:

```bash
fly secrets set \
  PMS_API_TOKEN='...' \
  PMS_DISCORD__WEBHOOK_URL='https://discord.com/api/webhooks/...'
```

Local LIVE uses the local secret-file path documented in
`docs/operations/live-polymarket-runbook.md`. Fly LIVE uses
`fly.live.toml.example`, which sets the non-secret `PMS_SECRET_SOURCE=fly`
marker and intentionally omits all credential-bearing values. Use stdin so
Polymarket values, the database URL, the API token, and the Discord webhook do
not land in shell history. Run this command against the copied live config,
paste the `NAME=VALUE` lines, then press Ctrl-D:

```text
fly secrets import -c fly.live.toml
DATABASE_URL=<paste production postgres URL>
PMS_API_TOKEN=<paste API bearer token>
PMS_DISCORD__WEBHOOK_URL=<paste Discord webhook URL>
PMS_POLYMARKET__PRIVATE_KEY=<paste private key>
PMS_POLYMARKET__API_KEY=<paste API key>
PMS_POLYMARKET__API_SECRET=<paste API secret>
PMS_POLYMARKET__API_PASSPHRASE=<paste API passphrase>
PMS_POLYMARKET__SIGNATURE_TYPE=1
PMS_POLYMARKET__FUNDER_ADDRESS=<paste wallet address>
```

The live template binds `PMS_API_HOST=0.0.0.0`, so LIVE validation and
`pms-api` startup both require `PMS_API_TOKEN` to be present and
non-placeholder. Do not treat the API token as optional for Fly LIVE capital.
The template also sets `PMS_DISCORD__ALERT_DIR=/secure/pms/alerts`; keep
dropped-alert fallback evidence on the mounted `/secure` volume, not in the
app working tree.

## Deploy

The Docker image syncs `--extra live --extra llm` at build time and starts with
`uv run --no-sync` so the optional Polymarket and LLM SDKs are not removed or
resolved at boot. The image command intentionally does not pass `--config`;
`fly.toml` selects the supervised paper-soak config with
`PMS_CONFIG_PATH=/app/config.live-soak.yaml`.

```bash
fly deploy
```

For LIVE capital, do not edit `fly.toml`. Prepare the separate ignored config
and persistent secure volume:

```bash
cp fly.live.toml.example fly.live.toml
# Replace every __FILL_IN_*__ value with real already-ratified operator data.
fly volumes create pms_live_secure --region iad -c fly.live.toml
fly secrets import -c fly.live.toml
```

Then copy the full non-secret launch artifact set to `/secure/pms` on the
mounted volume before starting the runner:

- `/secure/pms/paper-soak-go-report.md`
- `/secure/pms/operator-rehearsal-report.md`
- `/secure/pms/execution-model.json`
- `/secure/pms/paper-backtest-execution-diff.json`
- `/secure/pms/category-prior-observations.csv`
- `/secure/pms/flb-calibration.csv`
- `/secure/pms/credentialed-preflight.json`

The paper-soak report path must match
`PMS_LIVE_PAPER_SOAK_REPORT_PATH=/secure/pms/paper-soak-go-report.md` in
`fly.live.toml`. The live template intentionally omits `PMS_CONFIG_PATH`; all
launch-critical non-secret values come from `fly.live.toml`, and all
credential-bearing values come from Fly secrets. This prevents the LIVE app
from accidentally booting with the PAPER soak config.

After the credentialed preflight artifact has passed and been staged:

```bash
fly deploy -c fly.live.toml
```

`PMS_AUTO_START=1` is enabled in the live template. Startup still fails closed
unless `PMS_DISCORD__WEBHOOK_URL`, `DATABASE_URL`, all Polymarket credentials,
the `/secure/pms` artifacts, and the live validation gates are present. Write
the per-order approval JSON only after the live preview appears, using
`scripts/approve_first_order.py` inside `fly ssh console` against the
volume-backed `/secure/pms/first-order.json` path.

The Fly health check targets `/readiness`, not `/health`. A process that is
alive but has not started the runner and halt subscriber should stay unready.

## Paper soak: seed initial subscriptions

A freshly deployed paper soak idles at zero decisions until at least one market
is subscribed. This is by design, not a fault: active-perception auto-selection
only subscribes to markets that already have a streamed two-sided book
(`MarketSelector._market_passes_filters` filters out any market whose
`get_latest_book_summary` is `None`; the `missing-book` case in
`tests/unit/test_market_selector.py` pins this). A cold database has no streamed
books, so auto-selection has nothing to expand from.

Bootstrap it once per soak by seeding user subscriptions, which merge unfiltered
(`UnionMergePolicy`) and start the book stream; auto-selection expands from
there. Subscribe each token you want to soak (authenticate with `PMS_API_TOKEN`
because the soak binds `PMS_API_HOST=0.0.0.0`):

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $PMS_API_TOKEN" \
  "https://pms-paper-soak.fly.dev/markets/<token_id>/subscribe"
```

Then confirm the stream and decision flow are live before walking away:

```bash
# book_snapshots should climb; decisions_total should move off zero once the
# strategy factors warm up against the streamed book.
curl -fsS -H "Authorization: Bearer $PMS_API_TOKEN" \
  https://pms-paper-soak.fly.dev/status | jq '.controller, .actuator'
```

Pick the seed markets to match the strategy's edge (e.g. the H1 FLB longshot/
favorite price extremes), not arbitrarily — a soak of markets the strategy never
trades produces no usable Go/No-Go evidence.

## Logs

```bash
fly logs
fly logs -c fly.live.toml
```

Use logs to confirm `PMS_AUTO_START=1` started the runner and that the Discord
alert subscriber started.

## Restart

```bash
fly machine restart <machine-id>
fly machine restart <machine-id> -c fly.live.toml
```

## Rollback

Redeploy the previous image tag from Fly releases:

```bash
fly releases
fly deploy --image <previous-image>
fly releases -c fly.live.toml
fly deploy -c fly.live.toml --image <previous-image>
```

## Credential Rotation

Rotate a compromised or expired secret with `fly secrets set`, then force a
restart so the process reads the updated environment:

```bash
fly secrets set PMS_DISCORD__WEBHOOK_URL='https://discord.com/api/webhooks/...'
fly machine restart <machine-id>
fly secrets set -c fly.live.toml PMS_DISCORD__WEBHOOK_URL='https://discord.com/api/webhooks/...'
fly machine restart <machine-id> -c fly.live.toml
```

For Polymarket credential rotation, set all six Polymarket fields together to
avoid partial credential state.
