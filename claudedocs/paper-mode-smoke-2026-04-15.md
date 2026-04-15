# Paper-mode smoke observation — 2026-04-15

Observation log for Task 2. Branch: `chore/cleanup`. Backend isolated at
`PMS_DATA_DIR=/tmp/pms-dev`. Task 1 fill-rate fix already loaded.

---

## Setup

### Mode switch (Step 2.2)

```bash
curl -s -X POST http://127.0.0.1:8000/run/stop
curl -s -X POST http://127.0.0.1:8000/config \
  -H 'Content-Type: application/json' -d '{"mode":"paper"}'
curl -s -X POST http://127.0.0.1:8000/run/start
```

- `/run/stop`   → `{"status":"stopped"}`
- `/config`     → `{"mode":"paper"}`  (ConfigUpdate model: `mode: RunMode` — `"paper"` is valid)
- `/run/start`  → `{"status":"started","mode":"paper","runner_started_at":"2026-04-15T13:45:58.491790+00:00"}`

### Observation window

- 14 polling iterations × 9 s each = ~126 s total
- Baseline at switch: decisions=119, fills=60
- End of window:      decisions=613, fills=60
- Net new decisions in paper mode: **+494**

---

## Counts (at snapshot time, ~13:48:17 UTC)

| Metric              | Value | Notes |
|---------------------|-------|-------|
| decisions_total     | 613   | +494 added during 126 s paper run |
| fills_total         | 60    | **unchanged** — no new fills in paper mode |
| eval_records_total  | 100   | unchanged — evaluator did not produce new records |
| brier_overall       | 0.25  | carried from backtest epoch |
| fill_rate           | 0.6   | post-Task-1 value, carried over |
| win_rate            | 0.26  | |
| pnl                 | 69.39 | |

---

## Time-axis quality

```
brier_series:  series_len=100  span=0.104356s
               first=2026-04-15T13:44:29.663971+00:00
               last =2026-04-15T13:44:29.768327+00:00

pnl_series:    series_len=100  span=0.104356s
               first=2026-04-15T13:44:29.663971+00:00
               last =2026-04-15T13:44:29.768327+00:00
```

**Span is 0.10 s — far below the ≥30 s threshold for readable dashboard curves.**

### Root cause analysis

Both series timestamps are from `13:44:29` — the backtest epoch that pre-dated
the mode switch at `13:45:58`. The rolling ring-buffer in the evaluator/metrics
layer is **not reset or re-seeded when the runner switches modes**. All 100 entries
were inserted during the backtest burst and the paper-mode run did not add any new
`EvalRecord` entries (eval_records_total stayed at 100 throughout the 126 s window).

Consequence for dashboard UX:
- Line charts over `brier_series` / `pnl_series` will render as a dot or a nearly
  invisible 0.1-second blip, not a curve.
- Any chart that uses `recorded_at` as the x-axis will appear broken regardless
  of how many decisions accumulate.

This is a pre-existing condition, not introduced by Task 1. It will reproduce
reliably from the captured JSON at `/tmp/pms-paper-metrics.json`.

---

## Signals shape

- **Count**: 50 signals in `/signals` endpoint
- **Market IDs**: Real Polymarket hex identifiers — e.g.:
  - `0x3209617364a0d598435806b59d0d056b606022dc9028c466ad7912df94fc170c`
  - `0x3d495a3e05eaffe438bb1c2ab97ed57a79b0a6ab18a2ca6fa5b448e20ce70082`
  - `0xdee5db5410b362783a1405b66b9aa08a7d050ae1f99e8da85b9d1ae7962dad3b`
- **Signal keys**: `market_id`, `token_id`, `venue`, `title`, `yes_price`,
  `volume_24h`, `resolves_at`, `orderbook`, `external_signal`, `fetched_at`,
  `market_status`
- **Contrast with backtest**: Backtest used synthetic `pm-synthetic-XXX` ids.
  Paper mode correctly hits the live Polymarket REST API and returns real market ids.
- **No 429 / rate-limit errors** observed during the 126 s window (sensor kept
  updating `last_signal_at` continuously).

---

## Fills behaviour in paper mode

- `fills_total` stayed at 60 for the entire 126 s observation window.
- Decisions grew +494 but none produced fills.
- Hypothesis: the paper actuator requires resolved markets or a specific order
  state to register a fill; during a live run the positions are open and fills
  are recorded only when the position resolves or is closed. Alternatively the
  paper actuator's `execute()` path may not be wiring fills back to the counter
  for paper-mode trades. **Not investigated further** — Task 2 is observation only.
  This is a candidate bug for a future task.

---

## Per-page visual inspection

Requires a browser pointed at `http://127.0.0.1:3100`. Deferred to Task 3 —
no browser available in this session.

---

## fill_rate before/after Task 1 fix

| Scenario                   | fill_rate | Notes |
|----------------------------|-----------|-------|
| backtest pre-Task-1 fix    | 1.0       | Tautology: denominator == numerator (fills counted decisions, not orders) |
| backtest post-Task-1 fix   | 0.6       | Correct ratio: 60 fills / 100 decisions |
| paper-mode post-Task-1 fix | 0.6       | Carried over from backtest epoch — no new fills in paper mode during this run |

The 0.6 value in paper mode is the backtest value persisted in the evaluator
ring-buffer, not a freshly computed paper-mode fill rate. Cannot independently
confirm the fill_rate formula fires correctly in paper mode until fills actually
accumulate (see note in Fills behaviour above).

---

## Restore commands

```bash
curl -s -X POST http://127.0.0.1:8000/run/stop
curl -s -X POST http://127.0.0.1:8000/config \
  -H 'Content-Type: application/json' -d '{"mode":"backtest"}'
curl -s -X POST http://127.0.0.1:8000/run/start
```

---

## Captured artifacts

- `/tmp/pms-paper-status.json`  — full status snapshot at end of observation
- `/tmp/pms-paper-metrics.json` — metrics including brier_series / pnl_series
- `/tmp/pms-paper-signals.json` — 50 live Polymarket signals

These files exist on the dev machine only (not committed). The findings above
document the key numbers so they can be reproduced by re-running the same
curl commands.
