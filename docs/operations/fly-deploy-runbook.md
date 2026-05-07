# PMS Fly.io Deployment Runbook

This runbook is for the supervised paper-soak `pms-api` process. PR verification
does not deploy this machine and does not restart the active runner.

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

For non-sensitive values such as `PMS_API_TOKEN`, `fly secrets set` is fine:

```bash
fly secrets set \
  PMS_API_TOKEN='...' \
  PMS_DISCORD__WEBHOOK_URL='https://discord.com/api/webhooks/...'
```

Local LIVE currently uses the temporary local secret-file path documented in
`docs/operations/live-polymarket-runbook.md`. If LIVE later moves to Fly, set
the non-secret `PMS_SECRET_SOURCE=fly` marker in the Fly app environment and
use stdin so Polymarket values do not land in shell history. Run this command,
paste the `NAME=VALUE` lines, then press Ctrl-D:

```text
fly secrets import
PMS_POLYMARKET__PRIVATE_KEY=<paste private key>
PMS_POLYMARKET__API_KEY=<paste API key>
PMS_POLYMARKET__API_SECRET=<paste API secret>
PMS_POLYMARKET__API_PASSPHRASE=<paste API passphrase>
PMS_POLYMARKET__SIGNATURE_TYPE=1
PMS_POLYMARKET__FUNDER_ADDRESS=<paste wallet address>
```

## Deploy

```bash
fly deploy
```

The Fly health check targets `/readiness`, not `/health`. A process that is
alive but has not started the runner and halt subscriber should stay unready.

## Logs

```bash
fly logs
```

Use logs to confirm `PMS_AUTO_START=1` started the runner and that the Discord
alert subscriber started.

## Restart

```bash
fly machine restart <machine-id>
```

## Rollback

Redeploy the previous image tag from Fly releases:

```bash
fly releases
fly deploy --image <previous-image>
```

## Credential Rotation

Rotate a compromised or expired secret with `fly secrets set`, then force a
restart so the process reads the updated environment:

```bash
fly secrets set PMS_DISCORD__WEBHOOK_URL='https://discord.com/api/webhooks/...'
fly machine restart <machine-id>
```

For Polymarket credential rotation, set all six Polymarket fields together to
avoid partial credential state.
