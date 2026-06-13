# Dry-Run / Paper Readiness Gap Audit - 2026-06-13

Owner: Stometa
Branch: `feat/dry-run-paper-readiness`
Scope: local run, public remote data reads, no-secret dry-run/PAPER execution readiness.

This audit did not load or request real API secrets, private keys, funder
authorization, production data, or real-money order capability. All runtime
smokes used PAPER mode, local PostgreSQL databases, public Polymarket market
data, and a disposable local API token.

## Decision

No runtime code change was needed for the P0 dry-run/PAPER path. The blocking
modules and safety switches already exist and were verified:

- Public remote data reads work through Gamma, CLOB `/book`, and CLOB websocket.
- PAPER mode uses `PaperActuator`, not the live venue client.
- The paper canary produced controller decisions and paper fills with no
  credentials and no real orders.
- LIVE remains fail-closed by default and requires mode, credentials, approved
  secret source, API auth, preflight artifacts, fresh quote evidence, strict
  factor gates, every-order operator approval, risk envelope, and readiness
  attestations before any real-money path can start.

The remaining blocker is an operator precondition for the H1 launch PAPER soak:
private local artifact files under `/secure/pms` are absent in this workstation.
That blocks the full H1 launch soak, not the no-secret paper canary dry-run
validation.

## Gap Table

| ID | Area | Severity | Status | Evidence | Next action |
| --- | --- | --- | --- | --- | --- |
| G-01 | Canonical gates | P0 | Closed | `uv sync` completed; `uv run pytest -q` -> `2754 passed, 203 skipped in 29.51s`; `uv run mypy src/ tests/ --strict` -> `Success: no issues found in 482 source files`. | Re-run after every code change. This audit added docs only. |
| G-02 | Local backend startup | P0 | Closed with local port adjustment | `pms-api` started on `127.0.0.1:8017` after ports `8000` and `8001` were occupied. `/health` returned `{"status":"ok"}`; `/status` required `Authorization`; `/run/start` and `/run/stop` succeeded. Lifecycle cleanup is in `src/pms/api/app.py:199` and run controls are in `src/pms/api/app.py:839`. | Use an open port or free `8000` before following README defaults. |
| G-03 | Local PostgreSQL persistence | P0 | Closed | Local Postgres accepted connections. `alembic upgrade head` reached `0023_backtest_execution_rows`. Fresh canary DB persisted `book_snapshots=271`, `markets=2641`, `decisions=2`, `orders=2`, `fills=2`, `runtime_heartbeats=1`. Runner boots PG for non-backtest at `src/pms/runner.py:660` and reconciles portfolio before sensors at `src/pms/runner.py:692`. | Docker daemon was unavailable locally, but local Postgres satisfied the smoke. |
| G-04 | Dashboard local availability | P1 | Closed | `dashboard/package.json:5` runs Next on `127.0.0.1:3100`. With `PMS_API_BASE_URL=http://127.0.0.1:8017` and local token, dashboard loaded and `/api/pms/status`, `/api/pms/metrics`, `/api/pms/feedback`, `/api/pms/stream/events` returned 200. Proxy injects `PMS_API_TOKEN` at `dashboard/lib/upstream.ts:16`; status proxy is `dashboard/app/api/pms/status/route.ts:7`. | `/api/readiness` is not a dashboard route; use backend `/readiness` directly or add a route only if UI needs it. |
| G-05 | Public remote market data path | P0 | Closed | Public Gamma request returned `gamma_status=200`, `gamma_rows=1`; CLOB `/book` returned `clob_book_status=200`, `bids=39`, `asks=27`; CLOB websocket returned `event_types=['book']`. Source: Gamma client `src/pms/runner.py:3914`, discovery fetch `src/pms/sensor/adapters/market_discovery.py:202`, websocket default and subscription `src/pms/sensor/adapters/market_data.py:70` and `src/pms/sensor/adapters/market_data.py:598`, direct CLOB book client `src/pms/sensor/adapters/direct_book.py:141`. | No credential required for this path. Network outages remain an operational dependency. |
| G-06 | Credential injection and secret handling | P0 for dry-run, P1 for future live | Closed for current boundary | Committed PAPER canary config keeps `live_trading_enabled: false` and all Polymarket credential fields null at `config.local.paper-canary.yaml:5` and `config.local.paper-canary.yaml:89`. Config loading rejects inline venue credentials/API keys at `src/pms/config.py:484`. LIVE local secret files must be outside the working tree with private file and parent permissions at `src/pms/config.py:572`. Tests cover local-secret rejection cases at `tests/unit/test_live_trading_blockers.py:927`. | Do not paste secrets. Future credentialed preflight must use operator-managed secret store or chmod 600 local secret file outside the repo. |
| G-07 | Mode isolation | P0 | Closed | Defaults are `mode=backtest` and `live_trading_enabled=false` at `src/pms/config.py:408`. Runner adapter selection is mode-specific: BACKTEST -> `BacktestActuator`, PAPER -> `PaperActuator`, LIVE -> `PolymarketActuator` at `src/pms/runner.py:1455`. LIVE startup validates `validate_live_mode_ready()` and preflight artifact before start at `src/pms/runner.py:630`. | Keep no-secret testing in BACKTEST/PAPER. |
| G-08 | Paper execution safety | P0 | Closed | `PaperActuator.execute()` reads in-memory orderbooks and returns `paper-*` matched states without a venue client at `src/pms/actuator/adapters/paper.py:21`. It enforces positive notional and strict limit-price executable depth at `src/pms/actuator/adapters/paper.py:65`. Tests cover VWAP, NO-token orderbooks, insufficient depth, malformed rows, and live-like limit behavior at `tests/unit/test_paper_actuator_cp12.py:79` and `tests/unit/test_paper_actuator_cp12.py:286`. | No missing safety switch found. |
| G-09 | Real order safety switch | P0 | Closed | `PolymarketActuator.execute()` first rejects when `live_trading_enabled` is false at `src/pms/actuator/adapters/polymarket.py:846`, then revalidates live mode, preflight, strict operator gate, and quote guard. `validate_live_mode_ready()` requires live enabled, `mode=live`, complete non-placeholder credentials, approved secret source, IOC/FOK, non-snapshot quote source, strict factor gates, every-order operator approval, API token, live artifacts, risk envelope, and readiness evidence at `src/pms/config.py:511`. Tests cover mode, token, preflight, risk, secret path, every-order approval, and paper-soak report blockers at `tests/unit/test_live_trading_blockers.py:574`, `tests/unit/test_live_trading_blockers.py:750`, `tests/unit/test_live_trading_blockers.py:790`, `tests/unit/test_live_trading_blockers.py:1177`, and `tests/unit/test_live_trading_blockers.py:1459`. | No live credential or real order was attempted. |
| G-10 | Risk limits | P0 | Closed | Runtime risk checks enforce positive notional, min order, per-market cap, total exposure, risk-group cap, drawdown, open-position cap, slippage threshold, free cash, and max quantity at `src/pms/actuator/risk.py:120`. PAPER canary config sets tight caps at `config.local.paper-canary.yaml:13`; README launch caps are documented at `README.md:60`. | Future soak report should include risk rejection distribution, not just fills. |
| G-11 | Monitoring, logs, readiness, stop/recovery | P0 | Closed | `/readiness` requires sensors, event loop, and required workers ready at `src/pms/api/health.py:16`. API lifespan redacts autostart failures and stops runner/pool on shutdown at `src/pms/api/app.py:169` and `src/pms/api/app.py:199`. Runtime heartbeats persist continuity in `src/pms/storage/runtime_heartbeat_store.py:41`. The canary verifier passed readiness, sensor activity, runtime continuity, selection funnel, and first-trade-time checks. | Direct fee-rate reads emitted transient `ConnectError` warnings during smoke, but paper canary still passed. Monitor if repeated in longer soak. |
| G-12 | H1 launch PAPER soak artifacts | P1 manual precondition | Open, not code-blocking for no-secret canary | `uv run python scripts/check_paper_soak_artifacts.py --config config.live-soak.yaml` passed `paper_mode` and `h1_flb_strategy` but failed `flb_calibration` and `category_prior` because `/secure/pms` does not exist. The verifier requires private out-of-tree artifacts at `scripts/check_paper_soak_artifacts.py:86` and `scripts/check_paper_soak_artifacts.py:144`; README documents the same at `README.md:15` and `README.md:56`. | Operator must generate `/secure/pms/flb-calibration.csv`, its provenance JSON, and `/secure/pms/category-prior-observations.csv` from allowed data sources. Do not commit these artifacts. |

## Paper Canary Evidence

Fresh DB: `pms_gap_canary_20260613`

Setup:

```bash
PGPASSWORD=postgres createdb -h localhost -p 5432 -U postgres pms_gap_canary_20260613
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pms_gap_canary_20260613 uv run alembic upgrade head
uv run python scripts/install_paper_canary_strategy.py \
  --database-url postgresql://postgres:postgres@localhost:5432/pms_gap_canary_20260613 \
  --archive-default \
  --sample-modulus 1
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pms_gap_canary_20260613 \
  PMS_API_TOKEN=local-audit-token \
  uv run pms-api --config config.local.paper-canary.yaml --port 8017
curl -fsS -X POST -H 'Authorization: Bearer local-audit-token' http://127.0.0.1:8017/run/start
```

Live status while running:

```text
running True healthy True decisions 1 fills 1
```

Smoke verifier:

```bash
uv run python scripts/check_paper_canary_smoke.py \
  --status-json /tmp/pms-paper-canary-audit-20260613-fresh/status.json \
  --readiness-json /tmp/pms-paper-canary-audit-20260613-fresh/readiness.json \
  --strategies-json /tmp/pms-paper-canary-audit-20260613-fresh/strategies.json \
  --markets-json /tmp/pms-paper-canary-audit-20260613-fresh/markets.json \
  --decisions-json /tmp/pms-paper-canary-audit-20260613-fresh/decisions.json \
  --trades-json /tmp/pms-paper-canary-audit-20260613-fresh/trades.json \
  --positions-json /tmp/pms-paper-canary-audit-20260613-fresh/positions.json \
  --metrics-json /tmp/pms-paper-canary-audit-20260613-fresh/metrics.json
```

Output:

```text
[PASS] paper_mode: mode=paper; actuator.mode=paper; running=true
[PASS] readiness: status='ready'; eod_scheduler=disabled; event_loop=ready; halt_subscriber=disabled; sensors=ready; workers=ready
[PASS] runtime_continuity: heartbeat_count=1; unhealthy_heartbeat_count=0
[PASS] sensor_activity: MarketDiscoverySensor:running, MarketDataSensor:running
[PASS] active_strategy: paper_canary_v1@3fd5644e9ff9edc867ef547ed58cd223ee8a870b75b2b85c7c18478a7b5816dd
[PASS] market_discovery: observed_markets=2059
[PASS] controller_decisions: decisions_total=1; accepted_decisions=decision-b9c83decf9474574bab449a57b19049e
[PASS] paper_trades: fills_total=1; trades=paper-6c38328642c249ef8dc6e52e5e7af8be
[PASS] open_positions: open_positions=1
[PASS] selection_funnel: pms_selection_funnel_discovered_total=2770; pms_selection_funnel_selected_total=2770; pms_selection_funnel_routed_total=241; pms_selection_funnel_forecasted_total=6; pms_selection_funnel_controller_emitted_total=1; pms_selection_funnel_traded_total=1
[PASS] first_trade_time: pms.ui.first_trade_time_seconds=0.05389
```

Persisted rows after stop:

```text
book_snapshots|271
decisions|2
fills|2
markets|2641
orders|2
runtime_heartbeats|1
```

Representative persisted orders:

```text
matched|polymarket|BUY|NO|IOC|1|1|paper_canary_v1|64
matched|polymarket|BUY|YES|IOC|1|1|paper_canary_v1|64
```

The venue value is the market venue, not evidence of a real order. In PAPER
mode, the adapter is `PaperActuator` by construction (`src/pms/runner.py:1458`).

## Remote Data Evidence

Public data probe:

```text
gamma_status=200
gamma_rows=1
condition_id_present=True question='New Rihanna Album before GTA VI?' token_count=2
clob_book_status=200
book_asset_match=True bids=39 asks=27
```

Public websocket probe:

```text
ws_status=message_received token_id_len=77 message_count=1 event_types=['book']
```

No credentials were supplied for either probe.

## Local Runtime Evidence

Backend:

```text
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pms_test uv run alembic upgrade head
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pms_test PMS_API_TOKEN=local-audit-token uv run pms-api --port 8017
GET /health -> {"status":"ok"}
GET /readiness before start -> HTTP 503 not_ready
POST /run/start -> {"status":"started","mode":"backtest",...}
POST /run/stop -> {"status":"stopped"}
```

Dashboard:

```text
PMS_API_BASE_URL=http://127.0.0.1:8017 PMS_API_TOKEN=local-audit-token npm run dev
Next.js ready at http://127.0.0.1:3100
Browser loaded dashboard, source=live, mode=backtest, runner idle.
Network: /api/pms/status, /api/pms/metrics, /api/pms/feedback, /api/pms/stream/events all 200.
```

Environment notes:

- Docker daemon was unavailable, so Compose Postgres was not used.
- Ports `8000` and `8001` were occupied, so API smokes used `8017`.
- A dashboard favicon 404 appeared in browser console; this is not a
  dry-run/PAPER blocker.

## Validation Log

```text
uv sync
Resolved 79 packages in 0.97ms

uv run pytest -q
2754 passed, 203 skipped in 29.51s

uv run mypy src/ tests/ --strict
Success: no issues found in 482 source files

uv run python scripts/check_paper_soak_artifacts.py --config config.live-soak.yaml
[PASS] paper_mode: mode=paper
[PASS] h1_flb_strategy: paper_soak_strategy_id=h1_flb; paper_soak_archive_default=true
[FAIL] flb_calibration: FLB calibration artifact parent directory does not exist: /secure/pms
[FAIL] category_prior: category-prior artifact parent directory does not exist: /secure/pms
```

Post-documentation gates:

```text
uv sync
Resolved 79 packages in 5ms
Audited 46 packages in 2ms

uv run pytest -q
2754 passed, 203 skipped in 27.07s

uv run mypy src/ tests/ --strict
Success: no issues found in 482 source files
```
