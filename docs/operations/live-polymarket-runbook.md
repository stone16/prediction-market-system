# Live Polymarket Runbook

LIVE mode is fail-closed. Do not paste private keys, API secrets, or
passphrases into chat, issues, PRs, logs, or config files.

## PAPER Soak

1. Start from a repo-ignored local copy of the first-live soak config. Local
   machines may not be able to create root-level `/secure`; use a private
   user-owned artifact directory and let the helper rewrite local artifact
   paths:

   ```bash
   export PMS_SECURE_DIR="${PMS_SECURE_DIR:-$HOME/.local/share/pms/secure}"
   uv run python scripts/prepare_local_paper_soak_config.py \
     --secure-dir "$PMS_SECURE_DIR"
   ```

   Re-run the helper with `--overwrite` only after preserving local edits in
   `config.local.live-soak.yaml`. Fly/LIVE volume staging still uses
   `/secure/pms`; this local helper is only for PAPER soak development hosts.
2. Confirm the risk envelope before every soak run:
   `max_position_per_market=$1`, `max_total_exposure=$50`,
   `max_drawdown_pct=20%`, `max_daily_loss_usdc=$20`,
   `max_open_positions=50`, `max_exposure_per_risk_group=$1`,
   `max_quantity_shares=500`, `slippage_threshold_bps=50`, and
   `llm.max_daily_llm_cost_usdc=$0.05`.
3. Start the PAPER soak API control plane against live market data with the
   soak config. Keep the token private; `scripts/paper_report.py` reads the
   same token from `PMS_API_TOKEN` when polling protected paper API endpoints.

   ```bash
   export PMS_API_TOKEN="$(openssl rand -hex 32)"
   uv run pms-api --config config.local.live-soak.yaml
   ```

   For process managers that cannot pass CLI args, set
   `PMS_CONFIG_PATH=config.local.live-soak.yaml`.
4. In another shell, start the runner explicitly. The `pms-api` command starts
   the API control plane; it does not start the runner until an authenticated
   `POST /run/start` succeeds.

   ```bash
   curl -X POST \
     -H "Authorization: Bearer $PMS_API_TOKEN" \
     http://127.0.0.1:8000/run/start
   ```

5. Confirm `/status` reports `running=true` and every active sensor as
   `running`, not `stale` or
   `failed`. `MarketDataSensor` must have a fresh `last_signal_at`; a runner
   process that is alive but has stale market-data signals is not a valid soak.
6. Confirm `/strategies` shows the intended active strategy. Use
   `paper_canary_v1` when the goal is to verify live-data -> controller ->
   paper-actuator plumbing. Use the real default strategy only after its
   required factors are populated; 0 decisions from missing factors is not a
   market signal. `/status.strategy` is only a legacy display fallback; an
   empty or versionless `/strategies` response blocks a final GO report.
7. Confirm `/trades`, `/positions`, and evaluator metrics update when the
   selected strategy emits paper decisions.
8. Review order notional, slippage, rejected orders, and portfolio exposure.
9. Keep `live_trading_enabled=false` until the 30-day soak and compliance
   checklist are accepted.
10. Ratify the strategic exit criteria (the kill plan) defined in
   [live-exit-criteria.md](live-exit-criteria.md) **before** the first live
   order. Do not flip `live_trading_enabled=true` until
   `live_exit_criteria_ratified_by` and `live_exit_criteria_ratified_at` are
   filled in the live config.

## Auto-Halt Triggers

PMS fail-closes before order submission when any of these live-soak triggers
trip:

- Polymarket API auth failure: HTTP 401 or 403.
- Drawdown above `risk.max_drawdown_pct`.
- Current UTC day net realized P&L at or below `-risk.max_daily_loss_usdc`.
- Five consecutive losing filled trades.
- Average slippage above 100 bps across the last 10 filled trades.
- Three HTTP 429 rate-limit responses inside 10 minutes.
- Any submitted order remains unfilled for more than 30 minutes.

The halt state is explicit and reversible except for the daily-loss cap, whose
same-day trade evidence remains armed until the next UTC day. Operators should
first reconcile venue state, credentials, open orders, and portfolio exposure,
then call the runner/admin path that invokes `RiskManager.clear_halt()`. Do not
clear a halt only to retry the same failing order.

The ordinary risk envelope also includes `risk.max_exposure_per_risk_group`.
When market metadata supplies `risk_group_id`, `risk_group`, `event_id`, or
`category`, the controller tags each decision and `RiskManager` caps combined
open exposure for that group. This prevents a cluster of related markets from
consuming the live bankroll while each individual `market_id` still appears
inside its per-market cap. In final LIVE config the group cap is mandatory, so
decisions without a risk group are rejected instead of bypassing the grouped
exposure cap.

## Daily Paper Report

Generate the daily soak report after each paper run:

```bash
export PMS_API_TOKEN="<load from operator secret store>"
uv run python scripts/paper_report.py --date 2026-05-03
```

Reports are written under `docs/paper-reports/YYYY-MM-DD.md` by default for
daily review. The final LIVE go/no-go report must instead be regenerated at the
exact private launch path referenced by `live_paper_soak_report_path`. Use
`--output` for that final artifact and `--dry-run` to print the report in CI or
during review. `scripts/paper_report.py` reads `PMS_API_TOKEN` from the same
environment as `pms-api` and sends it as a bearer token when polling protected
paper API endpoints. Add `--require-go` for the machine-checkable paper-soak
gate; it returns exit code 1 until every gate row passes:

```bash
export PAPER_SOAK_REPORT_DATE="$(date -u +%F)"  # use the completed soak report date
uv run python scripts/paper_report.py \
  --date "$PAPER_SOAK_REPORT_DATE" \
  --dry-run \
  --require-go
uv run python scripts/paper_report.py \
  --date "$PAPER_SOAK_REPORT_DATE" \
  --output /secure/pms/paper-soak-go-report.md \
  --require-go
```

Persisted report files include a `Report Provenance` section with
`artifact_mode` set to `persisted` and a parseable `generated_at` timestamp.
Dry-run output is marked `dry_run` and is rejected by true LIVE validation even
if redirected into a Markdown file.
The final `--require-go` report date must not be in the future; the report
generator refuses future-dated final GO artifacts, and LIVE validation rejects
any future-dated paper-soak report before startup. LIVE validation also rejects
a paper-soak report whose title date is later than its persisted
`generated_at` provenance.

The report includes Gate 3 metrics: decisions, fills, slippage, daily and
cumulative P&L, drawdown, exposure, Brier score versus the market-implied
baseline, hit rate, average edge, fee bps, net edge after spread/fees/slippage,
unresolved incidents, rejection reasons, Sharpe ratio, secondary baseline
evidence coverage including category-prior evidence when supplied by the
signal, secondary baseline source-level Brier/improvement from `/metrics`,
risk events, and a single Go/No-Go decision. The final Go/No-Go gate fails
when any available secondary baseline source has non-positive Brier
improvement. The strategy row is read from `/strategies` and the final LIVE
preflight requires it to list every active
`strategy_id@strategy_version_id` that will be allowed to trade live; a GO
report for a different strategy cannot justify the current active strategy set.
If `/strategies` returns no active version rows, the report records a risk
event even if `/status.strategy` contains a plausible legacy label.
Stale or failed sensors from `/status` are recorded as risk events and block
the Go/No-Go gate.
Daily P&L is derived from the cumulative `/metrics` `pnl_series` for the report
day. `/metrics` also emits `max_drawdown_pct` from the same windowed P&L path,
using `risk.max_total_exposure` as the launch capital base, and `sharpe_ratio`
from daily P&L values inside the requested soak window. A final GO report with
a missing, empty, malformed, or out-of-window `pnl_series` records a risk event
and fails the Go/No-Go gate instead of defaulting daily P&L to `$0.00`.
`sharpe_ratio` must also pass as a concrete positive gate row.
If `/status.controller.diagnostics_total > 0`,
`/status.controller.diagnostic_counts` must use non-empty reason-code keys and
its count sum must exactly match every recorded diagnostic/rejection. Missing,
malformed, under-counted, or over-counted diagnostic evidence is recorded as a
risk event and blocks GO.

Each persisted decision also carries a bounded `decision_evidence` payload in
`decision_payloads` and `/decisions`: decision-time top book levels, a stable
book hash, observed book token id, submitted decision token id and outcome,
book age, quote source, factor snapshot hash, spread, source signal timestamp,
and market-implied / mid-quote / last-trade baseline probabilities, plus
`category_prior_baseline_prob_estimate` when the signal supplies a calibrated
decision-time prior. Baseline probability fields are in the final evaluator's
YES-outcome coordinate (`baseline_probability_coordinate: YES`); for NO
orders, use `decision_outcome_market_implied_prob_estimate` when checking the
traded token's implied probability. Configure
`controller.category_prior_observations_path`
to load that prior from a historical resolution CSV export with columns
`market_id,category,yes_payout,no_payout,resolved_at`. Startup fails closed if
the file is missing, has duplicate `market_id` rows, or contains price-like
payouts such as `0.99,0.01`; only exact settled vectors `1,0` / `0,1` are
scored, while `0.5,0.5` refund rows are skipped. The loader feeds
`pms.controller.baselines.CategoryPriorBaselineEstimator`, which filters out
observations resolved at or after the signal timestamp and falls back from
category to global only when the configured sample gates are met. Resolved
fills copy those baseline probabilities into `eval_records` JSONB maps and
score per-source Brier improvement for `/metrics`. Use this when reconciling
paper fills, backtest replay, and live venue behavior; do not rely on current
orderbook state to explain an old decision. True LIVE preflight requires this
artifact and rejects files with fewer resolved rows than
`controller.category_prior_min_global_samples`. The credentialed preflight
fingerprint binds the staged CSV contents, so replacing the category-prior
artifact after preflight invalidates the launch artifact.

The launch paper-soak config (`config.live-soak.yaml`) is bound to H1 FLB and
requires `strategies.flb_calibration_path` to point at a staged warehouse model
artifact. The CSV schema is
`signal_name,probability_estimate,sample_count,source_label`, and it must
contain both `longshot_yes_overpriced_buy_no` and
`favorite_yes_underpriced_buy_yes`. Startup fails closed when either signal is
missing, `sample_count < strategies.flb_min_calibration_samples`, or the
probability is outside `(0, 1)`. When configured, FLB uses the artifact
probability and suppresses signals whose net edge is below `min_expected_edge`;
do not run the launch soak with a null FLB calibration path. Net edge subtracts
`strategies.flb_entry_execution_cost_bps`
and the configured `strategies.flb_fee_rate` fee estimate before sizing. Keep
the static fee estimate conservative until per-market fee telemetry is wired;
Polymarket fees are market/category specific and queryable per market.
`pms-live preflight` validates this artifact before it writes a credentialed
preflight JSON, so a missing or malformed FLB model cannot be discovered only
after deploy startup. The credentialed preflight fingerprint also binds the
calibration CSV contents, so changing the FLB model after preflight requires a
new preflight artifact.

```yaml
strategies:
  flb_calibration_path: /secure/pms/flb-calibration.csv
  flb_min_calibration_samples: 100
  flb_entry_execution_cost_bps: 15.0
  flb_fee_rate: 0.07
```

Replace the static cost fields with paper/live telemetry before promotion.

Generate the local PAPER artifact from the strict warehouse resolution export
with the commands below. The checked-in Dune SQL template lives at
`docs/research/flb_polymarket_resolved_binary_dune.sql`; the Dune API key is a
credential, but the exported CSV and generated calibration CSV are non-secret
launch artifacts. The exporter validates the downloaded CSV with the same
strict warehouse loader as `scripts/flb_data_feasibility.py` and refuses to
publish an under-sampled launch export unless `--allow-under-sampled` is
explicitly passed for diagnostics:

```bash
export DUNE_API_KEY="<load from operator secret store>"
uv run python scripts/export_flb_warehouse_from_dune.py \
  --output "$PMS_SECURE_DIR/polymarket_resolved_binary.csv" \
  --performance large
uv run python scripts/flb_data_feasibility.py \
  --source warehouse-csv \
  --input "$PMS_SECURE_DIR/polymarket_resolved_binary.csv" \
  --output "$PMS_SECURE_DIR/flb-feasibility.md" \
  --csv "$PMS_SECURE_DIR/flb-deciles.csv" \
  --calibration-csv "$PMS_SECURE_DIR/flb-calibration.csv" \
  --calibration-source-label warehouse-flb-v1
```

Generate the optional local category-prior artifact that
`scripts/prepare_local_paper_soak_config.py` wires into
`config.local.live-soak.yaml`:

```bash
uv run python scripts/export_category_prior_observations.py \
  --output "$PMS_SECURE_DIR/category-prior-observations.csv" \
  --min-observations 100
```

For Fly/LIVE volume staging, keep the same artifact filenames under
`/secure/pms`, including `/secure/pms/flb-calibration.csv`.

Before starting the paper-soak API, run the local artifact check. It uses the
same FLB calibration and optional category-prior CSV loaders as runtime
startup, and it also verifies each configured private artifact parent before
the API process gets as far as `Runner(...)` construction. A missing,
malformed, or permissively staged launch artifact fails here instead of during
paper-soak startup:

```bash
uv run python scripts/check_paper_soak_artifacts.py \
  --config config.local.live-soak.yaml
```

When using research backtests to justify a live rollout, do not leave the
execution profile at the optimistic paper defaults. Rebuild it with observed
paper/live telemetry and include both `displayed_depth_fill_ratio` and
`adverse_selection_bps`; the simulator applies adverse selection before limit
eligibility, so tight limits that would drift out of reach do not appear as
free fills in promotion reports.

Generate the execution-model artifact from paper/live telemetry with:

```bash
uv run python scripts/execution_model_from_telemetry.py \
  --input /secure/pms/paper-execution-telemetry.csv \
  --output /secure/pms/execution-model.json \
  --fee-rate 0.04 \
  --staleness-ms 120000 \
  --displayed-depth-fill-ratio 0.75 \
  --require-adverse-selection \
  --min-samples 30
```

The telemetry CSV must contain `slippage_bps` and `latency_ms`; live-promotion
artifacts should also include `adverse_selection_bps`. The JSON output is the
`execution_model` object to embed in the research backtest spec, plus telemetry
sample metadata (`min_samples`, `telemetry_sample_count`,
`adverse_selection_sample_count`, and `require_adverse_selection`). Stage the
same artifact at `live_execution_model_path`; true LIVE validation and
credentialed preflight reject missing artifacts, static calibration sources,
profiles with no positive `adverse_selection_bps`, artifacts without the sample
contract, or sample contracts below the LIVE floor of 10 observations, and the
preflight fingerprint binds the artifact contents.

Before treating a research backtest as launch evidence, compare the paper
execution export against the matching backtest replay export:

```bash
uv run python scripts/paper_backtest_execution_diff.py \
  --paper /secure/pms/paper-execution-export.csv \
  --backtest /secure/pms/backtest-execution-export.csv \
  --output /secure/pms/paper-backtest-execution-diff.json \
  --max-fill-rate-delta 0.05 \
  --max-rejection-rate-delta 0.05 \
  --max-avg-slippage-bps-delta 5 \
  --max-total-pnl-delta 1 \
  --min-matched-decisions 10 \
  --require-pass
```

Both CSVs must contain `decision_id`, `market_id`, `status`, `slippage_bps`,
`pnl`, and `rejection_reason`. The diff fails on unmatched decision ids,
fill/rejection status mismatches, thin matched samples, or threshold breaches.
A failing artifact means the current execution model is not trusted enough for
promotion. Stage the passing JSON at `live_paper_backtest_diff_path`; true LIVE
validation and credentialed preflight require it and bind its contents into the
preflight fingerprint.

## Credential Setup

Install the live SDK in the runtime environment. The committed paper-soak
config enables the LLM forecaster, so paper-soak environments also install the
LLM extra. The true LIVE template keeps `llm.enabled=false` so the first
real-money path does not require a second provider secret. True LIVE validation
always requires `py_clob_client_v2` to be importable before runner startup:

```bash
uv sync --extra live --extra llm
```

`PMS_LLM__API_KEY is required only if you explicitly enable LLM` in
`config.live.yaml`. If you opt in, set `llm.enabled: true`, choose the provider,
stage `PMS_LLM__API_KEY` through the runtime secret mechanism, and rerun
`pms-live preflight`; the runtime dependency row will then require the selected
provider SDK as well. Config loading rejects any `llm.api_key` key in YAML; use
`PMS_LLM__API_KEY` or the production secret manager instead.

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
  # Use 3 / POLY_1271 for new API deposit-wallet credentials.
  # Existing proxy/Safe users may keep 1 or 2.
  signature_type: 3
  funder_address: <paste wallet address>
```

PMS refuses local LIVE startup if the file is missing, is a symlink or hard
link, is not a single-link regular file, is under the repository working tree,
is group/world readable, or has a group/world-accessible or symlinked parent
directory.
Configure the outside secret file path directly. Repository placement and
placeholder path markers such as `__FILL_IN_SECRET_FILE__` are rejected before
the secret file is parsed. Fix permissions with:

```bash
chmod 700 ~/.config/pms
chmod 600 ~/.config/pms/polymarket.local-secrets.yaml
```

Required fields are validated before LIVE mode starts:
`private_key`, `api_key`, `api_secret`, `api_passphrase`, `signature_type`,
and `funder_address`. Placeholder values such as `<paste private key>` or
`__FILL_IN_POLYMARKET_API_SECRET__` are rejected even though they are non-empty.
For `secret_source: local_file`, placeholder credential values are rejected
while config loads, before `pms-live preflight` opens runtime connections.
`signature_type` must be one of Polymarket's known signing modes: `0`, `1`,
`2`, or `3`. Use `3` (`POLY_1271`) for new API deposit-wallet credentials;
existing proxy/Safe users may keep `1` or `2`.
`funder_address` must be a `0x`-prefixed 40-hex-character wallet or proxy
address.

Configure LIVE mode with non-secret runtime config, not with credential
exports. Copy the committed template, then fill the operator/compliance fields
and paper-soak report path:

```bash
cp config.live.yaml.example config.live.yaml
```

`config.live.yaml` is ignored by git. Do not add Polymarket credential fields
to it; credentials belong only in `local_secret_file` or the production secret
manager. Config loading rejects non-null Polymarket credential fields before
runtime validation. Keep the template's tight risk envelope
(`max_position_per_market=$1`, `max_exposure_per_risk_group=$1`) unless a new
paper-soak gate and operator ratification explicitly replace it. The template
includes `secret_source: local_file`, `live_account_reconciliation_required: true`,
`live_exit_criteria_ratified_by`, and `live_compliance_jurisdiction`; do not
remove those lines for LIVE. It also pins `time_in_force: IOC` for the initial
real-money phase.

True LIVE validation permits only `IOC` or `FOK` during the initial
real-money phase. Use `IOC` for launch; `GTC` and other unsupported values
fail closed until PMS has a durable live open-order ledger for resting
exposure. The Polymarket actuator repeats this check for every live
`TradeDecision`, so agent/runtime-injected decisions cannot bypass the
config-level default.

True LIVE validation also rejects `quote_source: postgres_snapshot`.
Use `quote_source: dual` for launch: it compares the latest persisted book
against a fresh venue quote before submission and fails closed when they differ
by more than `dual_quote_max_price_delta_bps`. `venue_direct` is allowed for a
venue-only fallback, but final go/no-go should prefer `dual` while the
Postgres market-data freshness gate is active.

True LIVE validation requires `strict_factor_gates: true` so strategy-required
raw factor evidence cannot be relaxed at launch. The paper-soak template may
leave this disabled only for exploratory PAPER runs; the final LIVE template
must keep it enabled. LIVE validation also rejects a missing, zero, negative,
or non-finite core risk envelope: `max_position_per_market`,
`max_total_exposure`, `max_drawdown_pct`, `max_daily_loss_usdc`,
`max_open_positions`, `max_exposure_per_risk_group`, `max_quantity_shares`,
and `min_order_usdc` must be finite and greater than zero before credentials
can start the runner. The minimum order must fit within the per-market cap,
the per-market cap must fit within total exposure, and the risk-group cap must
fit between the minimum order and total exposure.

Before the first live run, rotate any Polymarket credential that was ever
pasted into a shell, issue, PR, chat, local `.env`, or dotfile during
development. Treat those values as compromised.

The five `live_*` ratification/review fields are mandatory in true LIVE mode.
`validate_live_mode_ready()` and `pms-live preflight` reject missing values so
credentials alone cannot start live trading before the kill plan and
jurisdiction/venue review are recorded.
They also reject placeholder markers such as `__FILL_IN_*__`, `<reviewer-id>`,
`TODO`, or `placeholder`, and reject future-dated ratification/review
timestamps. Replace the example markers with real operator/reviewer identities
and timestamps that have already happened.

`polymarket.first_live_order_approval_path` is also mandatory in true LIVE
mode. Runtime validation rejects a missing or placeholder approval path before
runner startup so the system cannot build a permanently-denying operator gate.
Runtime validation and `pms-live preflight` reject repo-local approval and
audit paths, unusable approval and audit path parents, and permissive approval
and audit parent directories. Keep them as real private owner-writable
directories with `chmod 700`, not symlinked parent paths.
`pms-live preflight` fails if an approval JSON already exists there, and now
also fails if a stale approval sidecar exists there; runtime validation rejects
the same stale approval artifacts at startup. A final go/no-go preflight must
start with no approval JSON or sidecar file; create both only after the live
order preview has been reviewed.
If an approval artifact exists at startup/preflight, it must be a regular file;
symlinked approval paths are rejected. During live submission, the file gate also
rejects symlinked approval JSON and symlinked sidecar metadata before matching.

When `llm.enabled=true`, true LIVE validation also requires the selected
provider SDK module to be importable. A config that names `provider:
anthropic` or `provider: openai` without the matching package installed fails
closed before runner startup; rerun `uv sync --extra llm` in the runtime shell
or image before retrying.

`live_paper_soak_report_path` must point at the final generated paper report
artifact from `scripts/paper_report.py --require-go`. Runtime LIVE validation
and `pms-live preflight` read this non-secret Markdown file and require its
Go/No-Go section to say `GO` with no failed gate rows and with PASS rows for
every required gate check: `soak_days`, `fills`, `fill_rate`,
`average_slippage_bps`, `todays_pnl`, `cumulative_pnl`, `max_drawdown_pct`,
`open_positions`, `total_exposure`, `brier_score`, `brier_improvement`,
`hit_rate`, `average_edge_bps`, `average_net_edge_bps`, `strategy_evidence`,
`unresolved_incidents`, and `risk_events`. Each required PASS row must have a
non-empty `Detail` cell, because the launch gate treats blank evidence and
placeholder text such as `TODO` as an invalid report. The `strategy_evidence`
detail must match the Summary `Strategy` row, so the report cannot mix a
current strategy label with gate evidence from another run. The committed GO gate
requires at least 50 simulated fills before the report can pass. Runtime LIVE
validation also requires the generated `Baseline Evidence Coverage` and
`Secondary Baseline Brier` sections: `market_implied`, `mid_quote`, and
`category_prior` coverage must be complete over the reported decision set, and
every coverage row must use that same reported-decision denominator. Every
baseline source label must be concrete, non-placeholder, lowercase `snake_case`
starting with a letter, and every source with covered decision evidence must
also have a secondary Brier row with positive improvement. Do not point this at
`--dry-run` output in a terminal buffer; write the report to disk and keep it
with the deployment record. The report's persisted provenance `output_path` must match
`live_paper_soak_report_path`; if you move the report, regenerate it at the
final path. The configured report path must be in a real private
owner-writable parent directory (`chmod 700`), not a parent-directory symlink,
and must be a single-link regular file, not a symlink or hard link; runtime
validation and the preflight readiness fingerprint both reject linked,
symlink-parent, or shared-parent paper-soak evidence, and
`scripts/paper_report.py` creates missing output directories as `0700` and
refuses to write persisted reports through linked paths or existing shared
output directories. Persisted paper reports are written owner-only (`0o600`).
Runtime LIVE validation also requires the operator exit-criteria ratification
and compliance review timestamps to be at or after the paper-soak and
operator-rehearsal report `generated_at` timestamps. The committed
template rejects readiness reports older than
`live_readiness_report_max_age_s`; LIVE validation caps that window at seven
days so paper-soak and operator-rehearsal evidence cannot be reused
indefinitely. The committed
`tests/fixtures/*_report.md` files exist only for automated regression tests;
test fixture reports are rejected by true LIVE validation and cannot be used as
launch evidence.

`live_operator_rehearsal_report_path` must point at a non-secret Markdown
report from the operator approval rehearsal. Runtime LIVE validation and
`pms-live preflight` require its **Operator Approval Rehearsal** section to say
`PASS` and include passing `approval_denied`, `approval_matched`, and
`approval_consumed` rows, plus `strict_sidecar_provenance` to prove the gate
required sidecar approver id, timestamp, and approval hash, and
`fresh_approval_required` to prove every-order mode denied the next submit
after the approval was consumed. It also requires `unexpected_events` and
`operator_id` PASS rows with non-empty, non-placeholder details:
`unexpected_events` must record the exact expected audit sequence and
`operator_id` must name the operator who ran the rehearsal. The report proves
the gate denied before approval, accepted an exact preview match with strict
sidecar provenance, consumed the approval artefacts, and stayed armed for the
next live order. The
report must also include `Report Provenance` with `generated_by` set to
`scripts/rehearse_first_order.py`, a parseable `generated_at`, `artifact_mode`
set to `persisted`, and `output_path` matching
`live_operator_rehearsal_report_path`. Its title date must not be in the
future and must not be later than its persisted `generated_at` provenance. The
committed test fixture rehearsal report is rejected for true LIVE; use the
operator-generated report from `scripts/rehearse_first_order.py`. The configured
rehearsal report path must also be a single-link regular file, not a symlink or
hard link; runtime validation and the preflight readiness fingerprint reject
linked, symlink-parent, or shared-parent operator-rehearsal evidence, and the
rehearsal script refuses to write its persisted report through a linked output
path. Keep the report parent private and owner-writable (`chmod 700`).
Persisted rehearsal reports are written owner-only (`0o600`).

## Read-only Live Preflight

Before starting a LIVE runner, run:

```bash
export PAPER_SOAK_REPORT_DATE="$(date -u +%F)"  # use the completed soak report date
uv run python scripts/paper_report.py \
  --date "$PAPER_SOAK_REPORT_DATE" \
  --output /secure/pms/paper-soak-go-report.md \
  --require-go
uv run pms-live preflight \
  --config config.live.yaml \
  --output /secure/pms/credentialed-preflight.json
```

The report command writes the persisted 30-day paper-soak artifact and proves
the metrics passed. The preflight command validates the LIVE config, runtime SDK
dependencies, operator approval path, first-order audit path, database connection,
Alembic schema head, recent
`book_snapshots` market-data freshness, unresolved `submission_unknown`
incidents, persisted PMS live open-order rows, active strategy/controller
compatibility, and the read-only venue account snapshot. Neither command
submits or cancels orders. Use `--json` for preflight automation.
When `api_host` is non-loopback, LIVE config validation also requires a
concrete non-placeholder `PMS_API_TOKEN`; this catches a missing control-plane
bearer token before the supervised `pms-api` process refuses startup.
Database connection failures are reported as a structured
`database_connection` row; credentialed database URLs and `password=` fragments
are redacted from that row before it is printed. Venue reconciliation failures
and manual `POST /run/start` runtime refusals also scrub configured Polymarket
private key, API credential, passphrase, and funder address values from
operator-visible errors. Polymarket SDK warning logs use the same scrubber for
venue rejection and transport-failure details.

The `--output` file is the machine-checkable proof artifact for the final
credentialed preflight. It includes `generated_by`, `generated_at`,
`artifact_mode`, `final_go_no_go_valid`, `skip_venue`,
`database_url_override_used`, `settings_fingerprint`,
`readiness_reports_fingerprint`, `active_strategies_fingerprint`, and the
structured preflight result. The fingerprints bind the artifact to the current
launch-critical LIVE settings, paper-soak and operator-rehearsal report files,
and active Postgres strategy projections without storing raw secret values in the artifact.
A passing credentialed artifact has `artifact_mode: credentialed_preflight` and
`final_go_no_go_valid: true`. It must also have
`database_url_override_used: false`; artifacts generated with `--database-url`
are marked `artifact_mode: incomplete_preflight` because they do not prove the
database DSN the LIVE runner will use. Any persisted artifact that is not final
go/no-go valid exits nonzero even if the diagnostic checks themselves ran to
completion. LIVE startup also rejects stale credentialed artifacts older than
`live_preflight_artifact_max_age_s`; LIVE validation caps that window at one
hour so venue reconciliation and market-data freshness are near-startup
evidence. The preflight artifact must also be generated after the persisted
paper-soak and operator-rehearsal reports it fingerprints; an artifact that
predates either readiness report is invalid for final go/no-go and is rejected
at LIVE startup. It must also be newer than every record in
`live_emergency_audit_path`; any emergency-stop or hard-halt audit record
written after the artifact invalidates it and requires a fresh credentialed
preflight before the next LIVE restart. Each required
preflight check detail must be non-empty and
non-placeholder; a
`TODO` detail makes the artifact invalid for final go/no-go. The active
strategy fingerprint must also be a concrete SHA-256 hex digest, not a
placeholder. The
credentialed preflight artifact parent directory must be private and
owner-writable (`chmod 700`), because this JSON is a launch-control artifact.
The artifact path must also be concrete and live outside the repo working tree;
generation and LIVE startup reject placeholder path markers and in-repo paths,
including in-repo symlinks to another location. Any preflight output path,
including incomplete diagnostic artifacts, must live in a private
owner-writable parent directory and must be a single-link regular file, not a
symlink or hard link; the writer creates missing output
parents as `0700` and refuses shared or linked output paths before writing.
For a final go/no-go artifact, `live_preflight_artifact_path` must be
configured and `--output` must match it; use a different path only for
incomplete diagnostic preflight artifacts.
The artifact records only whether `--database-url` was used, not the database
URL value.

As an additional CI-style witness, run the default-skipped integration test in
the same shell and image that will start LIVE:

```bash
PMS_RUN_INTEGRATION=1 \
PMS_RUN_LIVE_PREFLIGHT=1 \
PMS_LIVE_PREFLIGHT_CONFIG=config.live.yaml \
uv run pytest -q tests/integration/test_live_credentialed_preflight.py
```

This test calls the same read-only `run_live_preflight(..., skip_venue=False)`
path and fails unless every check, including `active_strategies` and
`venue_reconciliation`, passes. It does not submit, cancel, or approve orders.

The `runtime_dependencies` row must pass before final go/no-go. It proves the
Polymarket live SDK module is importable, and when `llm.enabled=true`, that the
selected provider SDK (`anthropic` or `openai`) is importable. If this row
fails, rerun `uv sync --extra live --extra llm` in the exact runtime image or
shell that will start the LIVE runner.

The `market_data_freshness` row must pass before final go/no-go. It compares
the latest persisted `book_snapshots.ts` and the latest two-sided snapshot with
positive `book_levels` BUY and SELL depth against
`dashboard.stale_snapshot_threshold_s`; a passing runner with stale snapshots,
missing snapshots, or one-sided/empty depth is not a valid live launch state.
When `risk.max_exposure_per_risk_group` is configured, every fresh usable
market must also have a nonblank `markets.risk_group_id`; a missing market row
or blank group id fails preflight because LIVE risk-group caps would reject the
resulting decisions.
`Runner.start()` enforces the same freshness gate in LIVE mode, so `pms-api`
startup also refuses stale or unproven market-data ingestion.

The `active_strategies` row must pass before final go/no-go. It loads the
currently active strategy versions from Postgres and builds their controller
pipelines under LIVE settings. It also verifies that each stored
`strategy_version_id` still matches the hash of the stored strategy projection.
It cross-checks the paper-soak GO report's Summary `Strategy` row against the
active `strategy_id@strategy_version_id` labels, so switching strategies after
paper soak requires a new paper-soak GO report for the new active set.
The registry refuses activation targets unless the exact
`(strategy_id, strategy_version_id)` row exists, so a version from another
strategy cannot be activated silently.
Paper-only strategies such as `paper_canary_v1`, any strategy with
`metadata.live_allowed=false`, and any strategy missing an explicit
`metadata.live_allowed=true` opt-in fail here, so a credentialed preflight
artifact cannot be accepted for a strategy that LIVE startup would later
reject. The opt-in is not sufficient by itself: LIVE strategies must also carry
non-empty, non-placeholder metadata for `alpha_source`, `edge_model_source`,
`calibration_source`, and `evidence_source`. Strategy metadata labels such as
`model_source` are also rejected if they contain placeholder, uncalibrated, or
static markers. LIVE strategies must include `calibration.enabled=true` in
their stored strategy projection; legacy strategy rows without an explicit
calibration block are rejected before final go/no-go.
LLM may be part of the forecaster ensemble, but a final LIVE strategy must
include at least one non-LLM forecaster so LLM is additive rather than the sole
alpha path.
`Runner.start()` also recomputes the active-strategy fingerprint from the
Runner-owned strategy registry and rejects the artifact if the active strategy
set or active strategy projection content changed after preflight.

The `venue_reconciliation` row must also prove that venue pUSD balance and allowance must cover `risk.max_total_exposure` after persisted PMS exposure is
subtracted. The Polymarket account snapshot first calls the CLOB
balance/allowance sync endpoint for collateral before reading the cached
balance, so a sync failure aborts the preflight/startup path instead of
accepting stale cash evidence. If the venue SDK response cannot be parsed into
finite pUSD/collateral balance and allowance values, the row fails closed rather
than accepting unknown or non-finite cash evidence.
`Runner.start()` seeds its in-memory launch budget from the same
`risk.max_total_exposure` value, so a passing preflight and the actual LIVE
startup reconcile against the same cash budget.

`--skip-credentials` is only for local non-secret readiness audits: it fills
diagnostic placeholder secrets so config, artifact, strategy, database, and
market-data checks can still surface non-credential blockers. It intentionally
skips venue reconciliation, returns a nonzero CLI exit, and can only write
`artifact_mode: incomplete_preflight` with `final_go_no_go_valid: false`.
`--skip-venue` is likewise only for local config/debugging when venue network
access is unavailable. A final go/no-go preflight must use real credentials and
must include the venue check.

## Operator Approval Gate

The Polymarket adapter requires operator approval before live venue submission.
Direct `PolymarketActuator` use in true `mode: live` also validates the
configured credentialed preflight artifact and revalidates the current
execution-model, paper/backtest diff, category-prior, and FLB calibration
artifacts unless the runner has already marked the adapter as startup-validated.
Direct adapter wiring cannot bypass the
startup artifact gate.
The configured approval mode decides how long that gate stays armed:

- `operator_approval_mode: first_order` keeps the legacy behavior: one approval
  is required for the first successful order after actuator startup, then the
  in-process fast path opens until restart. This remains available for local
  drills and lower-level actuator tests, but final LIVE config validation
  rejects it during the initial real-money phase.
- `operator_approval_mode: every_order` requires a fresh approval before every
  order. Use this for production launch; the LIVE readiness check requires it
  until an explicit later retro relaxes the gate.

The preview includes max notional, venue, market, token, side, outcome, market
slug/question when available, limit price, and max slippage. If the approval
gate is absent or denies the preview, the adapter raises
`OperatorApprovalRequiredError` and submits nothing.

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

The LIVE runner requires the approval helper's sidecar
`<approval-path>.meta.json` as well. It must include a non-empty,
non-placeholder `approver_id` and a non-future `ts` timestamp no older than
`polymarket.operator_approval_max_age_s`; LIVE validation caps this window at
300 seconds. It
also includes `approval_sha256`, a canonical SHA-256 hash of the exact approval
JSON, so a fresh sidecar cannot be reused with a copied or modified approval
payload. A bare approval JSON may be useful in lower-level tests, but true LIVE
treats it as denied so every real order has current operator provenance in the
audit log.
The helper refuses to write either artifact through a symlink or hard link,
including with `--force`; remove the link and write a single-link regular file
in the private approval directory instead. It also refuses to write if the
approval directory is group/world accessible or not owner-writable, and the
runtime file gate treats approvals from such a directory as denied. It also
requires `--force` before overwriting an existing approval JSON or sidecar
metadata file.
If helper execution fails after writing a new sidecar but before publishing the
approval JSON, it removes that new sidecar so later preflight/startup checks are
not blocked by stale metadata from a failed approval attempt.

Keep the approved notional at the minimum production risk cap. The approval
file is not a credential, but it should still live outside the repo so stale
approvals are not committed or reused accidentally.

## Pre-launch Operator Checklist (STO-10)

This is the human work that must happen before flipping
`live_trading_enabled=true`. Walk top to bottom. Items are tagged
**[setup]** (one-time), **[fill-in]** (replace `__FILL_IN__` in this
file), or **[confirm]** (verify each launch).

The operator approval gate is fail-closed by code
(`src/pms/actuator/adapters/polymarket.py`), but it only does its
job when the human side is named, reachable, and accountable. Each item
below makes one piece of the human side concrete.

### 0. Prerequisites — Polymarket account [setup]

Done outside this repo.

- [ ] Polymarket account in good standing. The funder wallet
      (`PMS_POLYMARKET__FUNDER_ADDRESS`) holds pUSD on Polygon
      (chain id 137) and has collateral approval/allowance in place. The PMS
      preflight/startup path syncs the CLOB collateral balance/allowance cache
      before reading it. Minimum balance and allowance: at least the live-soak
      `max_total_exposure` (\$50) plus a buffer for slippage and gas.
- [ ] CLOB API credentials issued from the Polymarket dashboard:
      `private_key`, `api_key`, `api_secret`, `api_passphrase`,
      `signature_type` (`3` / `POLY_1271` for new API deposit-wallet
      credentials; `1` or `2` only for existing proxy/Safe wallets).
- [ ] Two-factor enabled on the Polymarket account; the funder wallet
      private key is held in a hardware wallet or 1Password vault, not
      in plaintext on a laptop.

### 1. Stage credentials in the local secret file [setup]

LIVE mode currently runs locally on the operator's machine — see the
"Credential Setup" section above for the canonical `secret_source:
local_file` workflow that PMS validates at startup
(`src/pms/config.py:233`+: `secret_source` must be `"fly"` or
`"local_file"`; `local_file` requires `local_secret_file` to point
at a 0o600 regular file outside the repo). Walk that section first,
then return here to set up the operator-side artifacts.

In short: install the credentials YAML at
`~/.config/pms/polymarket.local-secrets.yaml` with `chmod 600`, and
point `local_secret_file` in `config.live.yaml` at it. Do not export
`PMS_POLYMARKET__*` in shell history or `.env`. The `secret_source:
fly` branch exists for a future Fly deployment but is not the current
target.

### 2. Set up the approval-file path [setup]

The approval JSON lives on the operator's machine
alongside (but separate from) the credentials. The canonical config
example in the "Credential Setup" section uses `/secure/pms` for the
approval JSON and LIVE audit JSONL files; make sure that directory exists
and is writable only by the operator UID before flipping the gate on.

```bash
sudo install -d -m 700 -o "$USER" /secure/pms
```

Set the path on `config.live.yaml`:

```yaml
live_emergency_audit_path: /secure/pms/live-emergency-audit.jsonl
live_first_order_audit_path: /secure/pms/first-order-audit.jsonl
polymarket:
  operator_approval_mode: every_order
  first_live_order_approval_path: /secure/pms/first-order.json
```

Empty value pitfall: `_first_live_order_gate`
(`src/pms/runner.py:2110-2116`) treats an empty string as
`DenyFirstLiveOrderGate` — the gate **locks shut**, it does not
"disable." Do not set the field to empty as a workaround.

For dev work without `sudo`, a freeform path under your home is
fine. Recommended: `~/.local/share/pms/first-order.json`. Create the
parent dir with `umask 077` so only your user account can read it.

### 3. Name the primary and backup operators [fill-in]

Edit the lines below. The gate has no concept of "operator" — naming
is enforced socially, by this runbook, and audited via the sidecar
metadata file.

- **Primary operator**: `__FILL_IN__` — handle (e.g. GitHub username),
  contact (Slack DM, phone), and time-zone window of availability.
- **Backup operator**: `__FILL_IN__` — same fields. Covers when the
  primary is unreachable.
- **Reachability rule**: at least one named operator must be reachable
  for every approval event during the configured operator window.
  Approval signals outside that window stall the strategy by design;
  there is no on-call escalation path until you explicitly fund one.

Whoever happens to be on Slack at the time is **not** the operator. An
anonymous gate is no gate.

### 4. Configure operator alerting [setup, required]

Set `PMS_DISCORD__WEBHOOK_URL` before any real-money startup. LIVE validation
rejects missing Discord webhook config so auto-halts, submission-unknown events,
and EOD failures have a built-in operator alert path.

Recommended extra paging: tail the runner log for the literal string
`OperatorApprovalRequiredError` and post to a Slack webhook. Lightweight,
no extra paid service, sufficient at \$100 bankroll while approval paging stays
operator-driven.

Suggested log shipper rule (Vector, or `journalctl -fu pms-api | grep
OperatorApprovalRequiredError` piped through `curl` while LIVE runs
locally; switch to Fly Log Shipper if/when LIVE moves to Fly):

- Match: log line contains `OperatorApprovalRequiredError`.
- Action: POST to `SLACK_OPERATOR_WEBHOOK_URL` with the matched line.
- Throttle: 1 message per 60 s (the gate is one-shot per actuator
  lifetime so floods are unlikely, but the log line repeats per
  decision until the file is filed).

If you skip the extra Slack paging step, the operator must actively poll
`/status` for first-order approval. That is acceptable only for the first
cp-03 rehearsal; the Discord webhook remains required for LIVE startup.

Set `PMS_DISCORD__ALERT_DIR=/secure/pms/alerts` (or the matching
`discord.alert_dir` config key). LIVE validation rejects runs that leave
dropped-alert fallback evidence inside the working tree.

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

Before flipping `live_trading_enabled=true`, walk the procedure end
to end. The `scripts/rehearse_first_order.py` driver does this in
one command — drives the real `PolymarketActuator` slow path with a
real strict-sidecar `FileFirstLiveOrderGate` and a real
`JsonlFirstOrderAuditWriter` backed by inline fakes for the venue
client and quote provider. No network, no DB, no real money.

```bash
uv run python scripts/rehearse_first_order.py --approver-id <your-handle>
# ✓ PASS  events=['approval_denied', 'approval_matched', 'approval_consumed', 'approval_denied']
#   audit log:    /tmp/pms-rehearsal-…/audit.jsonl
#   report:       /tmp/pms-rehearsal-…/operator-rehearsal-report.md
```

For a manual end-to-end against `config.live-soak.yaml` (PAPER mode,
no submit), use the helper for the operator-side write:

```bash
# Terminal A — start the runner against the soak config.
PMS_CONFIG_PATH=config.live-soak.yaml uv run pms-api

# Terminal B — file the approval using the operator helper.
uv run python scripts/approve_first_order.py \
  --from-error '<paste the full OperatorApprovalRequiredError line>' \
  --approver-id <your-handle> \
  --path /secure/pms/first-order.json

# Confirm the approval audit log records matched -> consumed.
tail -n 5 .data/first-order-audit.jsonl
```

The rehearsal is acceptance-complete when:

- The audit JSONL shows exactly `approval_matched` followed by
  `approval_consumed` for the rehearsal decision (no spurious events).
- A follow-up submit without a new approval is denied and the report includes
  `fresh_approval_required`.
- The report includes `strict_sidecar_provenance`, proving the same
  sidecar approver id, timestamp, and approval-hash checks required by LIVE.
- The approval file is unlinked after consume.
- The generated `operator-rehearsal-report.md` says `**Decision:** PASS`.
- Both primary and backup have run the rehearsal at least once.
- Elapsed time from raise to consume is below the chosen SLA from
  step 5.

Copy the final generated report into the secure path referenced by
`live_operator_rehearsal_report_path` and append a short sign-off entry to this
runbook ("Rehearsal log YYYY-MM-DD: primary X, backup Y, elapsed N minutes")
on completion.

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

- **Local LIVE (current target)**: `/secure/pms/first-order.json`,
  matching the canonical example in the "Credential Setup" section.
  Read only by the operator UID; create the parent dir with
  `install -d -m 700 -o "$USER" /secure/pms`.
- **Local development**: a freeform path under the operator's home,
  for example `~/.local/share/pms/first-order.json`. Created with
  `umask 077`.
- **Future Fly deployment**: a `[[mounts]]`-backed volume path such
  as `/data/pms/first-order.json` so it persists across deploys. Not
  the current target.
- **Sidecar metadata**: alongside the approval JSON, the operator
  writes `<approval-path>.meta.json` containing
  `{ "approver_id": "<id>", "approval_sha256": "<hash>", "ts": "<ISO 8601>" }`.
  The hash binds the sidecar to the approval JSON; the audit writer reads this
  and records the approver in the JSONL.
- **Never** commit either file to the repo. Both are gitignored under
  `.data/` and operator-specific paths.

### Approval-mode semantics

In `first_order` mode, "first" means **first since the actuator was
instantiated** (see the in-memory state at
`src/pms/actuator/adapters/polymarket.py`). A process restart resets
the gate to denied — the next decision will re-prompt the operator.
**This re-prompt on restart is intentional**, not a bug: any disruption
that warrants a restart also warrants re-validating the operating
environment before the next live submit.

Concretely, in `first_order` mode an approval is consumed exactly once
per actuator lifetime:

1. Operator drops the approval JSON at the configured path.
2. Adapter matches the next decision against the file and submits.
3. Adapter calls `consume()`, which unlinks the file
   (`polymarket.py:358-373`).
4. `_approval_state.approved = True` flips the fast path open;
   subsequent orders skip the slow path.
5. On any process restart, step 1 must repeat with a freshly-filed
   approval.

In `every_order` mode, the fast path never opens. Each decision repeats
steps 1-3, and the operator must write a new JSON file that matches the
next preview before the next venue submission can happen. The filename
can stay `/secure/pms/first-order.json`; it is a legacy path name, not
a signal that only the first order is checked.

### Audit trail

Every gate consultation appends one record to the JSONL at
`live_first_order_audit_path`. The true LIVE template writes this under
`/secure/pms/first-order-audit.jsonl`; the `.data/first-order-audit.jsonl`
default is for local/PAPER drills unless the parent directory is made private.
It is also configurable via `PMS_LIVE_FIRST_ORDER_AUDIT_PATH`. The audit writer
is `JsonlFirstOrderAuditWriter`, wired in `src/pms/storage/first_order_audit.py`.

Four event types:

| `event`                    | When                                                  |
|----------------------------|-------------------------------------------------------|
| `approval_matched`         | Gate returned True; submit is about to proceed.       |
| `approval_denied`          | Gate returned False; `OperatorApprovalRequiredError`. |
| `approval_consumed`        | Submit succeeded and `consume()` ran cleanly (both approval JSON and sidecar unlinked). |
| `approval_consume_failed`  | Submit succeeded but `consume()` raised; the approval artefacts may still be on disk. |

A record carries: `ts`, `event`, `approver_id` (from sidecar, may be
`null`), `venue`, `market_id`, `token_id`, `side`, `outcome`,
`max_notional_usdc`, `limit_price`, `max_slippage_bps`, `market_slug`,
`question`. The audit writer is non-blocking — a write failure logs
WARN and the order proceeds, mirroring `runner.py:1319-1320`.

`approval_consume_failed` is the operator's signal to clean up
manually: the venue submit succeeded, but the approval JSON and/or
its sidecar are still on disk, so a process restart could replay the
approval against the next decision. Page on this event with the same
priority as a runner halt: stop the runner if it has not stopped on
its own, unlink any remaining files at the configured approval path,
and confirm the audit JSONL has no further `approval_matched` records
for the same `market_id` before resuming.

In `every_order` mode, the actuator also blocks future in-process
submits after `approval_consume_failed` so a stale approval file cannot
authorize the next decision. Recovery is manual cleanup, venue
reconciliation, then process restart.

`live_first_order_audit_path` must stay distinct from
`live_emergency_audit_path`; true LIVE validation and `pms-live preflight`
reject shared paths. Both parent directories must be private and
owner-writable real directories, not symlinked parent paths, so approval review
records and emergency halt records can be appended during live operation. If
either audit file already exists, it must be a single-link regular file, not a
symlink or hard link; startup, preflight, and the JSONL appenders all fail
closed on linked audit outputs and unsafe audit parent directories. Emergency
halt records use `phase` fields in `live_emergency_audit_path`; operator
approval records use `event` fields in `live_first_order_audit_path`.

### End-to-end procedure (operator playbook)

When `OperatorApprovalRequiredError` is observed (in logs or via the
Slack alert from step 4):

1. Pull the preview details from the error message (venue, market,
   token, side, outcome, max_notional_usdc, limit_price,
   max_slippage_bps).
2. Validate the preview against current strategy intent and risk caps.
3. If approved, write the approval JSON (matching every field) and
   the `<path>.meta.json` sidecar with your `approver_id` to the
   configured path. The `scripts/approve_first_order.py` helper
   handles both files in one command (see step 6 in the checklist
   for usage); for local LIVE the path defaults to
   `/secure/pms/first-order.json`. If LIVE later moves to Fly, the
   same helper runs inside `fly ssh console` against the volume-
   backed path. Keep the parent directory private (`chmod 700`) through the
   entire approval window. Do not pre-create either path as a symlink or hard
   link; the helper refuses linked approval and sidecar artifacts before
   writing.
4. Wait for the next decision; the gate consults the file, matches,
   submits.
5. Confirm `approval_consumed` lands in the audit JSONL.
6. If `operator_approval_mode` is `first_order`, the fast path is now
   open for the rest of the actuator's lifetime; if the runner restarts,
   repeat from step 1 on the next decision. If the mode is `every_order`,
   repeat from step 1 for the next order.

If at any step you decide **not** to approve, do nothing. The next
decision will trigger another `OperatorApprovalRequiredError`; the
audit log will record `approval_denied`. The strategy stalls — that
is the gate working as intended.

## First Live Order Reconciliation

After the first approved live order produces a fill, write a durable
post-live reconciliation artifact before increasing risk caps or leaving the
runner unattended:

```bash
uv run pms-live reconcile-live-order \
  --config config.live.yaml \
  --decision-id <decision-id> \
  --reconciled-by <operator-id> \
  --output /secure/pms/first-live-order-reconciliation.json
```

The command checks the Alembic schema head, loads the persisted
decision/order/fill evidence for the concrete decision id, requires a persisted
fill plus pre-submit quote hash/source, rebuilds portfolio exposure from the
fills table, snapshots the Polymarket account, and compares venue cash,
positions, and open orders against PMS state. For a final artifact, it also
revalidates the configured credentialed preflight artifact before opening the
database, then verifies the same artifact was generated before the live order's
persisted `submitted_at` timestamp and records the artifact path plus SHA-256
content hash. Final artifact generation rejects any reference whose path differs
from `live_preflight_artifact_path`, whose file no longer validates as a
credentialed preflight artifact, or whose SHA-256 no longer matches the current
file bytes. A passing artifact has
`generated_by: pms-live reconcile-live-order`, `artifact_mode:
post_live_order_reconciliation`, `final_post_live_valid: true`, the
`settings_fingerprint`, `credentialed_preflight_artifact`, persisted order/fill
details, portfolio summary, and `venue_reconciliation.ok: true`.

Use `--database-url` only for diagnostics. Artifacts generated with a database
URL override are marked `artifact_mode:
incomplete_post_live_order_reconciliation`, set `final_post_live_valid: false`,
and the command exits nonzero because they do not prove the same DSN the LIVE
runner used. Store the artifact in the same private, outside-working-tree
operator directory as the credentialed preflight artifact.

## Submission Unknown Recovery

If a timeout or transport failure leaves an order in `submission_unknown`, do
not restart LIVE until the venue is reconciled. Inspect the Polymarket account
directly, decide whether the order filled, remained open, or was not found, then
mark the incident:

```bash
uv run pms-live reconcile-submission-unknown \
  --config config.live.yaml \
  --decision-id <decision-id> \
  --venue-order-id <venue-order-id> \
  --status filled \
  --reconciled-by <operator-id> \
  --note "matched venue fill"
```

Allowed statuses are `filled`, `not_found`, and `open`. `--decision-id` must be the concrete incident decision id, not a placeholder. `--venue-order-id` is required when status is `filled` or `open` and must be the concrete venue order id, not a placeholder; omit it only when venue
reconciliation confirms `not_found`. `--reconciled-by` must be a nonblank, non-placeholder operator identity. After reconciliation, rerun `uv run pms-live preflight
--config config.live.yaml`; LIVE start is refused while any unresolved
`submission_unknown` incident remains. Use `--database-url` only as an explicit override for the config DSN. The
reconciliation command checks the Alembic schema head before writing the
resolution, so a stale database must be migrated before incident state changes.
If the command cannot connect, sees stale schema, or cannot write the
resolution, it exits nonzero and prints a JSON payload with
`updated: false`, `decision_id`, `status`, and a redacted `error` field.

## Rollback

`/run/stop` is protected by `PMS_API_TOKEN` whenever the API token is
configured. True LIVE validation requires a concrete non-placeholder token
when the API binds to a non-loopback host. Keep the token out of shell history
by exporting it from the operator secret store, then send it as a bearer
header:

```bash
curl -X POST \
  -H "Authorization: Bearer $PMS_API_TOKEN" \
  http://127.0.0.1:8000/run/stop
```

1. Stop the runner with the authenticated `/run/stop` command above.
2. Inspect Polymarket directly for open orders and positions; cancel or unwind
   outside PMS if the venue shows exposure the DB cannot reconcile.
3. Reconcile any `submission_unknown` incident with `pms-live
   reconcile-submission-unknown`.
4. Restart with `PMS_MODE=paper` and `PMS_LIVE_TRADING_ENABLED=false`.
5. Verify `/status` reports `mode=paper` before resuming autonomous operation.

## Emergency Stop

1. Stop PMS immediately with the authenticated `/run/stop` command above.
2. Revoke or rotate Polymarket API credentials in the venue console.
3. Remove all `PMS_POLYMARKET__*` secrets from the runtime environment.
4. Inspect and cancel venue open orders directly; record any fill/order state
   that PMS did not persist.
5. Reconcile DB state, including `submission_unknown` incidents, before any
   future LIVE preflight.
6. Restart only in BACKTEST or PAPER mode until exposure and open orders are
   reconciled.
7. Append a manual emergency-stop record to `live_emergency_audit_path`:

```bash
uv run pms-live record-emergency-stop \
  --config config.live.yaml \
  --stopped-by <operator-id> \
  --reason "venue reconciliation mismatch" \
  --runner-stopped \
  --credentials-rotated \
  --runtime-secrets-removed \
  --venue-open-orders-reviewed \
  --database-reconciled \
  --restart-mode paper
```

The command reads only `live_emergency_audit_path` from config/env and does not
open `local_secret_file`, live credentials, or the database; it still works
after credentials have been rotated or removed. It records the operator's
completed rollback checklist in the same private emergency JSONL used for
automated hard-halt records. Placeholder operator ids or incomplete checklists
are rejected. Once this record is appended, the previous credentialed preflight
artifact is no longer valid for LIVE startup; rerun `pms-live preflight` after
the restart mode, venue exposure, credentials, and DB reconciliation are settled.
