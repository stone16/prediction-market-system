# Live Trading Exit Criteria (the kill plan)

This document defines the **strategic** exit criteria that stop live trading
program-wide and require an explicit operator review before resumption. It is
distinct from the **intra-session** auto-halt triggers in
`src/pms/actuator/risk.py:21-29`, which pause a single live session and can be
cleared with `RiskManager.clear_halt()` once venue / credential / order state
is reconciled.

A halt pauses the session. A kill-plan threshold, when tripped, ends the live
program until the named owner runs the named resume gate.

## Scope

- **Applies once:** `live_trading_enabled=true` AND the first-order operator
  gate (`docs/operations/live-polymarket-runbook.md` §First Live Order) has
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

The numeric defaults below come from the suggested values in STO-11. Each is
marked `TODO_DECISION:` until the operator confirms or amends. **Do not flip
`live_trading_enabled=true` while any threshold is unresolved.**

---

## Threshold 1 — Cumulative drawdown stop

**Definition.** Maximum cumulative drawdown from any historical equity peak
since live start. Measured against starting bankroll, not session start. This
is *distinct* from `risk.max_drawdown_pct` (intra-session circuit breaker
defined in `src/pms/config.py:128` and enforced at
`src/pms/actuator/risk.py:140-150`).

**Threshold (default).** **35%** of starting live bankroll.

> `TODO_DECISION:` confirm `35%` or amend.
> *Options:* 25% (more conservative; matches a one-stddev tail for many
> mean-reverting strategies), 35% (default per issue body), 50% (softer but
> closer to typical bankroll-management heuristics for high-variance
> strategies).
> *Who can resolve:* user (operator).

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
measured as a non-trivial, sustained reduction in Brier; the exact statistical
test is part of the decision below.

**Threshold (default).** **14 consecutive trading days** without positive
improvement.

> `TODO_DECISION:` confirm `14d` or amend, AND specify the baseline.
> *Baseline options:*
> a. **Mid-quote prior** — at decision time, use the venue mid as the
>    forecast. Naive but venue-priced and free.
> b. **Historical category prior** — use the long-run resolved frequency for
>    the market's category as the forecast.
> c. **Last-trade prior** — last trade price snapshot at decision time.
> *Improvement test options:* (i) mean Brier strictly lower over rolling
> window; (ii) one-sided paired test at α=0.10; (iii) bootstrapped 90%
> confidence interval excludes 0.
> *Who can resolve:* user (operator), with input from research.

**Measurement source — gap.** `evaluator.brier_overall`
(`src/pms/evaluation/metrics.py:16`) is currently **absolute**, not vs. a
baseline. The paired baseline-vs-strategy series is **not yet instrumented**.
A follow-up implementation issue is required before this threshold is
mechanically checkable; until then, the operator computes the comparison
manually from the daily report and stored decisions.

> `TODO_DECISION:` open a follow-up implementation issue to wire
> `brier_vs_baseline_<window>d` into the evaluator and `/status` once the
> baseline is chosen above.
> *Who can resolve:* tech lead (Erdong) once Threshold 2's baseline is fixed.

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
`HaltTriggerKind` other than `none`, see `src/pms/actuator/risk.py:21-29`)
followed by a `RiskManager.clear_halt()` recovery, observed within a rolling
**7-day window**. A halt that is not recovered (i.e. live remains paused) does
not count toward this threshold; only halt → recover → halt-again cycles do.

**Threshold (default).** **3 halt-recovery cycles per rolling 7 days**.

> `TODO_DECISION:` confirm `3` per rolling 7d, or amend.
> *Options:* 2/week (stricter — almost any operational instability stops the
> program), 3/week (default), 5/week (more permissive; appropriate only if
> early operational issues are expected and capped).
> *Who can resolve:* user (operator).

**Measurement source.** Count of `HaltEvent` entries in
`RiskManager.halt_events` (`src/pms/actuator/risk.py:74-75`) emitted within
the trailing 7-day window, excluding events that did not subsequently call
`clear_halt()`. Surfaced via the supervision alerting feed introduced in
PR #60 (`feat: add supervision alerting foundation`).

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
| T2 — Brier vs baseline | (pending instrumentation) — manual: `uv run python scripts/paper_report.py` and compare `brier_score_7d` to baseline computed from resolved-market history | strategy strictly lower |
| T3 — Halt-recovery cycles | `curl -s :8000/status \| jq '.actuator.halt_events_7d'` *(if exposed)* or count `HaltEvent` rows in the supervision feed for the trailing 7d | < ratified value |

> `TODO_DECISION:` confirm whether `/status` should expose `halt_events_7d`
> and `brier_vs_baseline_14d` directly. Recommended: yes — it removes the
> manual-counting failure mode and lets a single dashboard render the
> kill-plan health. Tracked as a follow-up to this issue.

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
| 2026-05-07 | Initial draft for ratification (STO-11). Defaults per issue body; all values marked `TODO_DECISION:` until operator confirms. | Erdong (Tech Lead) |
