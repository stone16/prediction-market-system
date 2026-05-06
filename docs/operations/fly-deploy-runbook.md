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

Inject secrets through Fly, never through config files or chat:

```bash
fly secrets set \
  PMS_API_TOKEN='...' \
  PMS_DISCORD__WEBHOOK_URL='https://discord.com/api/webhooks/...' \
  PMS_POLYMARKET__PRIVATE_KEY='...' \
  PMS_POLYMARKET__API_KEY='...' \
  PMS_POLYMARKET__API_SECRET='...' \
  PMS_POLYMARKET__API_PASSPHRASE='...' \
  PMS_POLYMARKET__SIGNATURE_TYPE='...' \
  PMS_POLYMARKET__FUNDER_ADDRESS='...'
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
