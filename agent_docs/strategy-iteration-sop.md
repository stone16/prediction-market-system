# Strategy-Iteration SOP — idea → retro

**Status:** active as of 2026-06-10.
**Scope:** the standard operating procedure for taking a strategy
idea through channel decision, factor viability check, config
install, backtest gate, paper soak gate, LIVE promotion, and
iterate/retro. Every gate below is encoded in the repo; thresholds
are quoted from code, not invented. `file:line` references were
verified against HEAD in the 2026-06-10 R&D session and spot-checked
again before this doc landed.
**Notation:** `guide:NNN` abbreviates
`agent_docs/strategy-authoring-guide.md:NNN`.

**Related documents:**
- `agent_docs/strategy-authoring-guide.md` — the authoritative
  channel/recipe reference this SOP indexes into.
- `agent_docs/worldcup-strategy-portfolio-2026-06.md` — the first
  portfolio run through this SOP, including the time-budget math.
- `agent_docs/architecture-invariants.md` — Invariants 3 and 4 are
  load-bearing for Steps 3 and 7.

---

## Time budget — read this before scheduling a portfolio

The paper gate requires `min_soak_days=30`
(`scripts/paper_report.py:176`). Any candidate aimed at a bounded
trading window must be installed and soaking at least 30 days
before the window closes, or it cannot produce an in-window GO.
Install all surviving candidates in parallel on day 1; the runner
builds one ControllerPipeline per active strategy and hot-reloads
new versions without restart (`guide:385-397`).

---

## Step 0 — Substrate (once, before the window opens)

```bash
docker compose up -d postgres                       # or your dev PG
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/pms_dev
uv sync && uv run alembic upgrade head
uv run python scripts/prepare_local_paper_soak_config.py   # writes config.local.live-soak.yaml (scripts/prepare_local_paper_soak_config.py:16-44)
uv run python scripts/check_paper_soak_artifacts.py        # fail-fast preflight (scripts/check_paper_soak_artifacts.py:1-6)
PMS_AUTO_START=1 uv run pms-api                            # recorder + paper runtime
# Cold-start subscription seed — target token ids (merged by selector.py:54-70):
curl -X POST -H "Authorization: Bearer $PMS_API_TOKEN" http://127.0.0.1:8000/markets/<token_id>/subscribe   # app.py:370-373
```

Backtests replay only your own outer ring (Gap G2) — recording
uptime during the target window IS your future backtest dataset.
Every un-recorded day is unrecoverable backtest coverage.

---

## Step 1 — Idea → channel decision

Use the §2 decision table
(`agent_docs/strategy-authoring-guide.md:83-104`):

- Composable from raw factors + the 4 built-in forecasters →
  **Channel A** (~0.5 day, 2 files: builder + installer).
- Custom observation source or intent pipeline → **Channel B**
  (~2–3 days, 5 files, one-time `runner.py` registration).

Default to A.

---

## Step 2 — Factor check (the viability kill-gate)

1. Registered definitions:
   `src/pms/factors/definitions/__init__.py:14-23`.
2. Live-computability: `required_inputs` must exist on live
   signals — only `orderbook` and `yes_price` (plus
   `external_signal.last_trade_price` etc.) are populated
   (`market_data.py:697-726`). Today only `orderbook_imbalance`
   and `favorite_longshot_bias` produce rows.
3. Evidence over intent — check actual rows:

   ```bash
   psql "$DATABASE_URL" -c "SELECT factor_id, count(*), max(ts) FROM factor_values GROUP BY 1 ORDER BY 2 DESC;"
   ```

4. Missing factor → §8 recipe (`strategy-authoring-guide.md:743-775`):
   definition file, add to `REGISTERED`, piecewise tests per branch,
   then:

   ```bash
   uv run pytest tests/unit/factors/test_<factor>.py -q
   uv run mypy src/pms/factors/definitions/<factor>.py --strict
   ```

5. Remember: missing *required* factors hard-skip decisions only in
   LIVE or when `controller.strict_factor_gates` is set
   (`src/pms/controller/pipeline.py:387-413,1660-1661`); paper with
   `strict_factor_gates: false` (`config.live-soak.yaml:66`)
   degrades gracefully — don't let paper leniency hide a LIVE
   blocker.

---

## Step 3 — Config (builder + installer, content-hashed version)

```bash
# 1. Builder: src/pms/strategies/<id>.py — copy paper_multifactor.py (canonical Channel-A example, guide:96)
# 2. Installer: scripts/install_<id>_strategy.py — copy install_paper_multi_factor_strategy.py (guide:242-327)
uv run mypy src/pms/strategies/<id>.py scripts/install_<id>_strategy.py --strict
uv run pytest tests/unit/strategies/test_<id>.py -q       # 4 minimal tests per guide:670-683
uv run python scripts/install_<id>_strategy.py            # idempotent; same config → same sha256 version id (guide:344-360)
psql "$DATABASE_URL" -c "SELECT strategy_id, active_version_id FROM strategies WHERE strategy_id='<id>';"   # guide:367-371
```

The running API hot-adopts the version via registry change
callbacks — no restart (`guide:385-397`). Never edit
`strategy_versions.config_json` in place (Invariant 3 violation,
`guide:521-523`).

---

## Step 4 — Backtest gate (fill-mechanics gate, honestly scoped)

```bash
# Sweep YAML: sweeps/<id>.yaml per §3.8 (guide:415-459); schema: docs/research/backtest-spec-format.md
uv run pms-research worker --poll-interval 1.0            # terminal A (cli.py:72-75)
uv run pms-research sweep sweeps/<id>.yaml --wait         # terminal B (cli.py:64-70)
#   add --no-cache when grid K < 7 (cache hit-rate gate >0.95 needs K≥7: sweep.py:28-29, cli.py:125-134)
```

Read results: dashboard `/backtest/<run_id>/compare`,
`GET /research/backtest/{run_id}` (`src/pms/api/app.py:699`), or
SQL (`strategy_runs`, `strategy_run_slices`,
`backtest_execution_rows`, `evaluation_reports`).

**What this gate can and cannot certify (G1–G10):** Brier/P&L are
structurally NULL on real replayed data
(`research/runner.py:1232-1248`; rankings degenerate per
`report.py:331-335`), risk policy is hashed but not enforced
(`execution.py:68`), volume gates are skipped when
`volume_24h=None` (`router.py:36-39`).

**Pass criteria that ARE meaningful:**

- fill_rate ≳ 0.30 on `immediate_or_cancel` (`guide:495-497`);
- slippage stable across the parameter grid (fragility = kill,
  `guide:498-501`);
- ≥ 20 decisions per slice and ≥ 2 slices, else the report emits
  promotion-blocking warnings (`src/pms/research/report.py:417-514`);
- decision counts cluster where the thesis says they should.

After ≥1 week of paper telemetry, replace `static_live_estimate`
with `scripts/execution_model_from_telemetry.py` output
(`specs.py:225-275`) to clear that report warning.

---

## Step 5 — Paper soak gate

Strategy activation: only `paper_multi_factor_v1`/`h1_flb`
auto-install via `paper_soak_strategy_id` (Literal at
`config.py:414`); every other candidate activates by its install
script (Step 3). Daily:

```bash
uv run python scripts/paper_report.py --date $(date -u +%F) --config config.local.live-soak.yaml --dry-run
```

GO gate (exit code 1 on failure):

```bash
uv run python scripts/paper_report.py --require-go --config config.local.live-soak.yaml \
  --output ~/.local/share/pms/secure/paper-go-$(date -u +%F).md
# --require-go output MUST live outside the working tree (paper_report.py:841-847,866-885)
```

**Encoded thresholds — `PaperSoakGateConfig`
(`scripts/paper_report.py:175-188`):** soak_days ≥ 30 ·
accepted_decisions ≥ 30 · fills ≥ 50 · slippage ≤ 50 bps ·
Brier(7d) < 0.20 · Brier improvement vs baseline > 0 (plus
per-source secondary checks, `paper_report.py:249`) · hit_rate >
0.45 · avg edge ≥ 5 bps · avg net edge ≥ 0 · distinct markets ≥ 3 ·
distinct risk groups ≥ 3 · max market/risk-group fill share ≤ 0.60 ·
exposure within `risk.max_total_exposure`.

Known config-level paper-link blockers — `risk_group_id` wiring,
per-market cap, factor coverage — re-verify before relying on fills
appearing.

Mid-soak parity check (the G-mitigation tooling):
`scripts/export_paper_execution_from_api.py` +
`scripts/export_backtest_execution_from_db.py` +
`scripts/paper_backtest_execution_diff.py`.

---

## Step 6 — Promotion criteria (LIVE)

All fail-closed in config validation: `live_trading_enabled=true` +
credentials + `live_paper_soak_report_path` pointing at a
validated, persisted, non-future GO report
(`src/pms/config.py:423,670-685,1378-1430`, including per-strategy
evidence consistency at `config.py:1472-1476`); LIVE requires
`strict_factor_gates=true` (`config.py:607-609`); strategy metadata
`live_allowed=true` only after GO (`guide:790`); first real-money
phase runs `operator_approval_mode=every_order`
(`config.live-soak.yaml:102`).

Submission preflight: `scripts/check_live_submission_artifacts.py`
(it names the exact `--require-go` command at `:193`), then
`scripts/rehearse_first_order.py` /
`scripts/approve_first_order.py`.

Exit criteria: `docs/operations/live-exit-criteria.md`; runbook:
`docs/operations/live-polymarket-runbook.md`.

---

## Step 7 — Iterate / Retro

**Iterate** (per version, `guide:503-519`): edit builder →
`mypy --strict` → reinstall (new content-hash version) → runner
hot-adopts → re-queue sweep → compare versions in
`/backtest/<run>/compare`. Cross-version comparison is sound
because every downstream row carries
`(strategy_id, strategy_version_id)` (Invariant 3).

**Retro** (per strategy decision — kill, promote, or pivot): add
`.harness/retro/<date>-<task>.md` with `[category: tag]`
observations; update the frequency table in
`.harness/retro/index.md` (status ladder: observation → monitoring
at 2× → proposed at 3×/high severity → active in CLAUDE.md, per
`index.md` "How To Read"). Never bypass a promoted rule without
opening a new retro (root `CLAUDE.md`, "Do not").

Pre-register the kill criterion per candidate at install time — a
candidate without a written kill criterion cannot be retro'd
honestly. See the World Cup portfolio doc for worked examples.
