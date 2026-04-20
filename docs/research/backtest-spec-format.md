# Backtest Sweep Spec Format

This document is the operator guide for `uv run pms-research sweep ...` and
`uv run pms-research worker ...`.

Use it when you need to:

- author a YAML sweep spec that the CLI accepts as-is,
- construct `BacktestSpec` and `BacktestExecutionConfig` in Python,
- choose or recalibrate an `ExecutionModel`,
- run the worker as a long-lived background service, or
- debug the factor-panel cache gate.

## Quick Start

1. Install dependencies and point the shell at PostgreSQL:

```bash
uv sync
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/pms_dev
```

2. In one shell, start the worker:

```bash
uv run pms-research worker --poll-interval 1.0
```

3. In another shell, queue a sweep:

```bash
uv run pms-research sweep tests/fixtures/sweep_10variant.yaml --wait
```

The worker owns execution. The API and dashboard only read and write research
rows; they do not fork or host the research worker.

## YAML Schema Accepted by `pms-research sweep`

The CLI expects one YAML mapping with three top-level keys:

- `base_spec` (required): a serialized `BacktestSpec`
- `exec_config` (optional): a serialized `BacktestExecutionConfig`
- `parameter_grid` (optional): dotted-path overrides applied to a copy of
  `base_spec`

Pass `-` instead of a file path if you want `pms-research sweep` to read the
YAML payload from stdin.

Minimal shape:

```yaml
base_spec:
  strategy_versions:
    - ["strategy-id", "strategy-version-id"]
  dataset:
    source: "fixture"
    version: "v1"
    coverage_start: "2026-04-01T00:00:00+00:00"
    coverage_end: "2026-04-30T00:00:00+00:00"
    market_universe_filter:
      market_ids: ["market-a", "market-b"]
    data_quality_gaps: []
  execution_model:
    fee_rate: 0.04
    slippage_bps: 10.0
    latency_ms: 250.0
    staleness_ms: 120000.0
    fill_policy: "immediate_or_cancel"
  risk_policy:
    max_position_notional_usdc: 100.0
    max_daily_drawdown_pct: 2.5
    min_order_size_usdc: 1.0
  date_range_start: "2026-04-01T00:00:00+00:00"
  date_range_end: "2026-04-30T00:00:00+00:00"
exec_config:
  chunk_days: 7
  time_budget: 1800
parameter_grid:
  execution_model.slippage_bps: [5.0, 10.0, 15.0]
  risk_policy.max_position_notional_usdc: [50.0, 100.0]
```

Field notes:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `base_spec.strategy_versions` | array of 2-item arrays | yes | Each entry is `[strategy_id, strategy_version_id]`. |
| `base_spec.dataset.source` | string | yes | Free-form provenance label. |
| `base_spec.dataset.version` | string | yes | Dataset version or snapshot id. |
| `base_spec.dataset.coverage_start` | ISO-8601 datetime with offset | yes | Must be timezone-aware. |
| `base_spec.dataset.coverage_end` | ISO-8601 datetime with offset | yes | Must be timezone-aware. |
| `base_spec.dataset.market_universe_filter` | mapping | yes | Arbitrary JSON-like object. Use `market_ids` when you want a fixed universe. |
| `base_spec.dataset.data_quality_gaps` | array of 3-item arrays | yes | Each item is `[start, end, reason]`. |
| `base_spec.execution_model.fee_rate` | number | yes | Used by `ExecutionModel.fee_curve()`. |
| `base_spec.execution_model.slippage_bps` | number | yes | Average execution slippage in basis points. |
| `base_spec.execution_model.latency_ms` | number | yes | End-to-end order latency assumption. |
| `base_spec.execution_model.staleness_ms` | number | yes | Data freshness ceiling. |
| `base_spec.execution_model.fill_policy` | string | yes | `immediate_or_cancel` or `limit_if_touched`. |
| `base_spec.risk_policy.*` | numbers | yes | Matches `RiskParams`. |
| `base_spec.date_range_start` | ISO-8601 datetime with offset | yes | Must be timezone-aware. |
| `base_spec.date_range_end` | ISO-8601 datetime with offset | yes | Must be timezone-aware. |
| `exec_config.chunk_days` | integer | no | Defaults to `7`. |
| `exec_config.time_budget` | integer | no | Defaults to `1800` seconds. |

Parameter-grid rules:

- Keys are dotted mapping paths, such as
  `execution_model.slippage_bps` or `dataset.market_universe_filter.market_ids`.
- Each value must be a sequence.
- The path walker only traverses mappings. It does not support list indexes.
  Replace whole arrays instead of editing one element inside them.
- To sweep `strategy_versions`, replace the whole field with a sequence of
  full `[strategy_id, strategy_version_id]` arrays, exactly like
  `tests/fixtures/sweep_10variant.yaml`.
- The CLI enumerates the Cartesian product of every parameter-grid sequence.

The canonical example fixture lives at
`tests/fixtures/sweep_10variant.yaml`.

## Programmatic Construction

For notebooks, automation, or test helpers, build the value objects directly and
serialize them only at the boundary:

```python
from datetime import UTC, datetime

from pms.research.spec_codec import serialize_backtest_spec, serialize_execution_config
from pms.research.specs import (
    BacktestDataset,
    BacktestExecutionConfig,
    BacktestSpec,
    ExecutionModel,
)
from pms.strategies.projections import RiskParams

spec = BacktestSpec(
    strategy_versions=(("strategy-alpha", "strategy-alpha-v3"),),
    dataset=BacktestDataset(
        source="warehouse",
        version="2026-04-19-snapshot",
        coverage_start=datetime(2026, 4, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 4, 30, tzinfo=UTC),
        market_universe_filter={"market_ids": ["market-a", "market-b"]},
        data_quality_gaps=(),
    ),
    execution_model=ExecutionModel.polymarket_live_estimate(),
    risk_policy=RiskParams(
        max_position_notional_usdc=100.0,
        max_daily_drawdown_pct=2.5,
        min_order_size_usdc=1.0,
    ),
    date_range_start=datetime(2026, 4, 1, tzinfo=UTC),
    date_range_end=datetime(2026, 4, 30, tzinfo=UTC),
)

exec_config = BacktestExecutionConfig(chunk_days=7, time_budget=1800)

payload = {
    "base_spec": serialize_backtest_spec(spec),
    "exec_config": serialize_execution_config(exec_config),
    "parameter_grid": {
        "execution_model.slippage_bps": [8.0, 10.0, 12.0],
    },
}

print(spec.config_hash)
```

Important behavior:

- `BacktestSpec.config_hash` is derived automatically in `__post_init__`.
- The hash surface includes strategy versions, dataset, execution model, risk
  policy, and the backtest date range.
- `BacktestExecutionConfig` is intentionally separate from the hash surface, so
  changing `chunk_days` or `time_budget` does not create a new semantic spec.

## Choosing an `ExecutionModel`

Two built-in profiles exist today:

- `ExecutionModel.polymarket_paper()`
  - `fee_rate=0.0`
  - `slippage_bps=0.0`
  - `latency_ms=0.0`
  - `staleness_ms=inf`
  - `fill_policy="immediate_or_cancel"`
- `ExecutionModel.polymarket_live_estimate()`
  - `fee_rate=0.04`
  - `slippage_bps=10.0`
  - `latency_ms=250.0`
  - `staleness_ms=120000.0`
  - `fill_policy="immediate_or_cancel"`

Use `polymarket_paper()` when you are checking strategy logic, factor
composition, or replay correctness and want market-friction assumptions out of
the way.

Use `polymarket_live_estimate()` when you want backtests to reflect the current
production execution envelope. Its defaults are sourced from live operational
inputs noted in the code comments: venue fee schedule, recent slippage
telemetry, watchdog latency, and the local staleness ceiling.

Override the defaults when any of these are true:

- you are replaying a venue or fee schedule that differs from current
  Polymarket assumptions,
- `/metrics` or evaluation data shows the live desk has drifted materially away
  from the baked-in slippage or fill assumptions,
- the strategy uses a slower control loop or a longer market-data freshness
  tolerance than the default watchdog ceiling,
- you need `limit_if_touched` to model a different execution style.

When you override, create a new `ExecutionModel(...)` explicitly instead of
patching serialized YAML ad hoc. That keeps the assumption set reviewable.

## Recalibrating from Live `/metrics`

Recalibrate `ExecutionModel` when backtest assumptions and live metrics are no
longer within the same operating regime.

Use this loop:

1. Pull a recent live sample window from `GET /metrics` and the per-strategy
   research views. Focus on `slippage_bps`, `fill_rate`, `win_rate`, and the
   record count behind them.
2. Compare the live values against the `ExecutionModel` currently baked into the
   sweep spec.
3. If the difference is persistent, rebuild the profile and re-run the same
   sweep window before widening scope.

Practical mapping from telemetry to fields:

- `slippage_bps`: move this first. It is directly surfaced in `/metrics`.
- `latency_ms`: update from order round-trip or watchdog latency telemetry.
- `staleness_ms`: update from the point where live data is no longer trusted.
- `fee_rate`: update only when the venue fee schedule or account tier changes.
- `fill_policy`: update only when the execution style changed; it is not a
  tuning knob for making reports look better.

Recommended calibration discipline:

- use a trailing window large enough to avoid reacting to one thin market,
- keep the old and new profile names in the experiment notes,
- change one major assumption at a time,
- re-run the same date range before switching to a new market universe.

## Running `pms-research worker` as a Service

The worker is a separate long-lived process. It polls `backtest_runs` for
`status='queued'`, claims one run, executes it, writes the report, and then
loops again.

Operational notes:

- `SIGTERM` and `SIGINT` request a graceful stop. The worker finishes the active
  run before it exits.
- `--max-runs 1` is useful for smoke tests; omit it for a real daemon.
- Use absolute filesystem paths. Do not rely on `~` expansion in service
  managers.

### macOS launchd template

Save as `~/Library/LaunchAgents/com.stometa.pms-research-worker.plist`, then
run `launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.stometa.pms-research-worker.plist`.

Replace `/ABS/PATH/...` with real absolute paths:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.stometa.pms-research-worker</string>

    <key>WorkingDirectory</key>
    <string>/ABS/PATH/TO/prediction-market-system</string>

    <key>ProgramArguments</key>
    <array>
      <string>/bin/zsh</string>
      <string>-lc</string>
      <string>exec uv run pms-research worker --poll-interval 1.0</string>
    </array>

    <key>EnvironmentVariables</key>
    <dict>
      <key>DATABASE_URL</key>
      <string>postgresql://postgres:postgres@127.0.0.1:5432/pms_dev</string>
      <key>PATH</key>
      <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>

    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>

    <key>StandardOutPath</key>
    <string>/ABS/PATH/TO/logs/pms-research-worker.stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/ABS/PATH/TO/logs/pms-research-worker.stderr.log</string>
  </dict>
</plist>
```

Notes:

- `WorkingDirectory` must be absolute.
- `launchd` gives you a sparse environment; set `PATH` explicitly if `uv` is
  not in a system path.
- Check status with `launchctl print gui/$UID/com.stometa.pms-research-worker`.

### Linux systemd template

Save as `/etc/systemd/system/pms-research-worker.service`:

```ini
[Unit]
Description=PMS research worker
After=network.target postgresql.service

[Service]
Type=simple
User=stometa
WorkingDirectory=/ABS/PATH/TO/prediction-market-system
Environment=DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/pms_dev
Environment=PATH=/usr/local/bin:/usr/bin:/bin
ExecStart=/bin/bash -lc 'exec uv run pms-research worker --poll-interval 1.0'
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now pms-research-worker
sudo systemctl status pms-research-worker
```

If `uv` lives outside the default service `PATH`, either extend `PATH` or
replace the shell command with the absolute `uv` binary path from `command -v uv`.

## AC #2 Cache-Hit-Rate Gate and `--no-cache` Debugging

The sweep gate is enforced in the CLI before runs are enqueued:

- the sweep warms the in-process factor-panel cache,
- it computes `ParameterSweep.cache_hit_rate()`,
- if caching is enabled and the hit rate is `<= 0.95`, the CLI exits non-zero
  with a `cache_gate_failed` payload.

Expected behavior:

- `uv run pms-research sweep tests/fixtures/sweep_10variant.yaml`
  should report `cache_hit_rate > 0.95`
- the same command with `--no-cache` should report `cache_hit_rate == 0.0`
  because the in-process cache is disabled by design
- `--no-cache` skips the gate so you can debug the underlying factor-load path

Typical failure payload:

```json
{"cache_hit_rate":0.72,"error":"cache_gate_failed","required_hit_rate":0.95,"variant_count":10}
```

How to debug:

1. Run the sweep once with cache enabled and once with `--no-cache`.
2. If the cached run fails the gate but the `--no-cache` run succeeds, inspect
   cache-key divergence:
   - different `param` payloads,
   - different `market_ids`,
   - different `ts_start` or `ts_end`,
   - different factor composition in the loaded strategy versions.
3. If both runs fail, the bug is not the in-process cache gate. Check:
   - strategy-version rows exist in `strategy_versions`,
   - the market universe in `market_universe_filter.market_ids` exists in the
     market-data store,
   - PostgreSQL connectivity and permissions,
   - factor loading itself for the requested factor and date window.
4. Reduce to one variant by emptying `parameter_grid`. If one variant still
   fails, debug the serialized `base_spec` first, not cache reuse.

One subtle point: the cache key normalizes parameter mappings and market-id
order. Reordering `market_ids` or YAML mapping keys should not create a cache
miss by itself.
