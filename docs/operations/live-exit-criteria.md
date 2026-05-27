# Live Trading Exit Criteria (the kill plan)

This document defines the **strategic** exit criteria that stop live trading
program-wide and require an explicit operator review before resumption. It is
distinct from the **intra-session** auto-halt triggers in
`src/pms/actuator/risk.py:21-30`, which pause a single live session. Most can
be cleared with `RiskManager.clear_halt()` once venue / credential / order
state is reconciled; the daily-loss cap remains armed until the next UTC day.

A halt pauses the session. A kill-plan threshold, when tripped, ends the live
program until the named owner runs the named resume gate.

## Scope

- **Applies once:** `live_trading_enabled=true` AND the operator approval gate
  (`docs/operations/live-polymarket-runbook.md` §Operator Approval Gate) has
  been used at least once.
- **Does not apply:** during BACKTEST or PAPER mode, or before the first live
  order. Intra-session halts (`risk.py`) continue to apply on every mode.
- **Out of scope:** scaling-up criteria. The decision to grow bankroll cannot
  be made before the kill criteria below are met. Tracked separately when the
  program survives long enough to need it.

## Pre-commitment

The thresholds below must be ratified **in writing, before the first live
order**, by the operator listed under each threshold's *Owner*. Once ratified,
they are not renegotiable while live capital is at risk; renegotiation
requires the program to first hit its resume gate.

The numeric defaults below are the committed v1 kill-plan thresholds. An
operator may amend them before first live capital, but LIVE mode must not start
until the final values are ratified via `live_exit_criteria_ratified_by` and
`live_exit_criteria_ratified_at` in `config.live.yaml`.

---

## Threshold 1 — Cumulative drawdown stop

**Definition.** Maximum cumulative drawdown from any historical equity peak
since live start. Measured against starting bankroll, not session start. This
is *distinct* from `risk.max_drawdown_pct` (intra-session circuit breaker
defined in `src/pms/config.py:128` and enforced at
`src/pms/actuator/risk.py:140-150`).

**Threshold.** **35%** of starting live bankroll. Alternative values must be
written into the ratified operator review before `live_trading_enabled=true`.

**Measurement source.** `Portfolio.max_drawdown_pct` as exposed via the
`/status` endpoint and surfaced in the daily paper/live report
(`scripts/paper_report.py:31`). Bankroll baseline is the equity at the
`runner_started_at` of the first live session.

**Owner.** Operator. No agent-only override.

**Resume gate.**
1. Pause: set `live_trading_enabled=false` and follow
   `docs/operations/live-polymarket-runbook.md` §Rollback.
2. Document the loss attribution in `docs/notes/<date>-live-stop.md` (per
   strategy version, per market regime).
3. Run a fresh PAPER soak of at least 14 days on current production data
   under the live-soak config (`config.live-soak.yaml`).
4. Re-ratify Threshold 1 (and revisit 2 and 3) before flipping the flag back.

---

## Threshold 2 — Brier-no-improvement stop

**Definition.** Maximum number of consecutive trading days during which the
strategy's Brier score does not show *positive improvement* over a fixed
**baseline forecaster** computed on the same resolved markets. Improvement is
measured as a non-trivial, sustained reduction in Brier under the v1 mechanical
test below.

**Threshold.** **14 consecutive trading days** without positive improvement.
The v1 mechanical test is rolling 14-day mean Brier improvement strictly
greater than zero.

**Baseline.** The mechanical baseline is the decision-time market-implied YES
probability carried by the accepted decision: `limit_price` for YES decisions,
and `1 - limit_price` for NO decisions. This keeps the baseline paired to the
same market, decision time, and resolved outcome as the strategy forecast.
Each persisted decision also carries secondary baseline evidence for the
decision-time mid-quote and last-trade probability when those prices were
available; resolved fills persist per-source baseline probability and Brier
maps on `EvalRecord`.

**Measurement source.** Each `EvalRecord` stores `baseline_prob_estimate` and
`baseline_brier_score` alongside the strategy Brier score. `/status` exposes
`evaluator.baseline_brier_overall` and
`evaluator.brier_improvement_overall`, plus
`evaluator.baseline_brier_14d` and `evaluator.brier_improvement_14d` for the
rolling kill-plan window. `/metrics` exposes the overall fields in the ops view
and per-strategy rollups, including `baseline_brier_by_source` and
`brier_improvement_by_source` maps for secondary baselines. The daily report
displays the market baseline Brier and improvement rows, secondary baseline
coverage from `/decisions` including category-prior evidence when supplied,
and a `Secondary Baseline Brier` table from `/metrics`. The Go/No-Go gate
fails when any available secondary baseline source has non-positive Brier
improvement.

**Owner.** Operator. (Research can recommend, operator decides.)

**Resume gate.**
1. Pause live trading.
2. Diagnostic review: is the failure a strategy-edge issue (the forecast is
   not better than the baseline) or an execution-cost issue (forecast is
   better, but slippage / fees eat the edge)? These have different fixes.
3. If strategy-edge: research iteration in PAPER. If execution-cost: revisit
   `risk.slippage_threshold_bps`, sizing, and venue selection.
4. Require 14 consecutive days of positive baseline-improvement in PAPER on
   live market data before flipping the flag back.

---

## Threshold 3 — Halt-recovery cycle ceiling

**Definition.** Maximum number of distinct intra-session halts (any
`HaltTriggerKind` other than `none`, see `src/pms/actuator/risk.py:21-30`)
followed by a `RiskManager.clear_halt()` recovery, observed within a rolling
**7-day window**. A halt that is not recovered (i.e. live remains paused) does
not count toward this threshold; only halt → recover → halt-again cycles do.

**Threshold.** **3 halt-recovery cycles per rolling 7 days**.

**Measurement source.** Count of `HaltRecoveryCycle` entries in
`RiskManager.halt_recovery_cycles` emitted when `clear_halt()` recovers an
active halt. `/status` exposes the rolling 7-day count as
`actuator.halt_recovery_cycles_7d`, excluding halts that have not been
recovered.

**Owner.** Operator. (Tech lead reviews root cause for each halt; operator
decides whether to keep going.)

**Resume gate.**
1. Pause live trading.
2. For each halt in the trailing window, attach a root cause to
   `docs/notes/<date>-halt-<trigger_kind>.md`. No grouping, no "we know
   what this one was": one note per cycle.
3. Land an operational fix or hardening PR for the dominant failure mode
   (credential rotation, rate-limit backoff, stale-order auto-reconciliation,
   etc.).
4. Run a 7-day PAPER soak with **zero** halt-recovery cycles before flipping
   the flag back.

---

## Verification

The operator and tech lead must be able to read the current value of each
threshold at any time. Verification commands:

| Threshold | Command / Source | Output to check |
|-----------|------------------|-----------------|
| T1 — Drawdown | `curl -s :8000/status \| jq '.portfolio.max_drawdown_pct'` | < ratified value |
| T1 — Drawdown (daily) | `uv run python scripts/paper_report.py --date <YYYY-MM-DD>` | `max_drawdown_pct` row |
| T2 — Brier vs baseline | `curl -s :8000/status \| jq '.evaluator.brier_improvement_14d'` and `uv run python scripts/paper_report.py --date <YYYY-MM-DD>` | > 0 |
| T3 — Halt-recovery cycles | `curl -s :8000/status \| jq '.actuator.halt_recovery_cycles_7d'` | < ratified value |

## Review cadence

- **Daily:** the daily paper/live report (`scripts/paper_report.py`) is read
  by the operator. Any threshold breach → immediate
  `live_trading_enabled=false` and rollback per
  `docs/operations/live-polymarket-runbook.md` §Rollback.
- **Weekly:** the operator and tech lead jointly review T2 and T3 trend
  lines, even when no threshold is breached. The purpose is to catch slow
  drift before it crosses the line.
- **Per resume:** before flipping `live_trading_enabled=true` after any
  pause, *all three* thresholds are re-evaluated against the current state
  and re-ratified in writing.

## Change history

| Date | Change | Author |
|------|--------|--------|
| 2026-05-07 | Initial draft for ratification (STO-11). Defaults per issue body. | Erdong (Tech Lead) |
| 2026-05-25 | Committed v1 default thresholds and linked T2/T3 to `/status` machine-observable fields. | Codex |
