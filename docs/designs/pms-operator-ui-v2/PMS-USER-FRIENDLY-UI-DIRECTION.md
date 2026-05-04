# PMS User-Friendly UI Direction

Date: 2026-05-03
Owner: @Designer-Yiko
Thread: #proj-prediction-market:1e9db269
Status: v2 approved by @PM-Derik for implementation planning

## Goal

Make the prediction-market system easier for a human operator to understand and control.

The dashboard should answer four questions immediately:

1. **Am I making or losing money?**
2. **What did the system do?**
3. **Which strategy is working?**
4. **Do I need to approve, stop, or change anything now?**

## Current UI Assessment

The current dashboard has a solid Cathedral v1 visual system and useful pages:

- `/` Today
- `/markets`
- `/ideas`
- `/trades`
- `/positions`
- `/strategies`
- `/metrics`
- `/backtest`

The issue is not that the UI lacks data. The issue is that the user's mental model is split across pages.

Current fragmentation:

- P&L is split between metrics, positions, trades, and strategy rows.
- System actions are visible only after navigating into trades/decisions/event logs.
- Strategy performance exists as a table, but does not tell the operator what to do.
- Risk/compliance state is not surfaced as the main operating constraint.
- The user has to infer whether the system is healthy, profitable, or waiting for approval.

## Target User

Primary user: operator/founder who supervises an autonomous trading system.

Pain points:

- They need fast confidence before allowing more automation.
- They want to know whether the system made money and why.
- They need to see every action the system took, especially before live trading.
- They need strategy-level performance, not just aggregate metrics.
- They need explicit human decision points for compliance, risk, and live-mode transitions.

## Design Principles

1. **P&L first**
   - The first screen must show realized P&L, unrealized P&L, exposure, daily loss room, and bankroll state.
   - This should be visible before market tables or raw strategy metrics.

2. **Action ledger as trust surface**
   - Every system action should show: sensor input, strategy signal, risk decision, actuator output, and result.
   - Users should not have to search logs to understand what happened.

3. **Strategy scorecard, not just strategy table**
   - Strategy page should rank strategies by trust/performance.
   - Each strategy needs P&L, Brier, fill quality, drawdown, and recommended action.

4. **Human decision queue**
   - Pending decisions should be explicit: approve trade, reject trade, confirm bankroll, pause strategy, promote strategy.
   - Each action card needs evidence, risk impact, and expected P&L attribution.

5. **Risk is a first-class UI object**
   - Bankroll, max market exposure, daily loss cap, live/paper mode, and stop status must appear as stable controls/status chips.

6. **Separate paper confidence from live readiness**
   - Paper P&L can be green while live readiness remains blocked.
   - UI should show both states at once to avoid false confidence.

## Proposed Information Architecture

### 1. Operator Home

Purpose: daily control room.

Required surfaces:

- Portfolio P&L summary:
  - realized P&L
  - unrealized P&L
  - open exposure
  - daily loss room
  - bankroll usage
- Needs Attention queue:
  - compliance numbers missing
  - pending trade approval
  - strategy needs review
  - runner unhealthy
- System Action Timeline:
  - scan markets
  - generate strategy opportunity
  - risk gate result
  - paper/live actuator result
  - fill/settlement event

Primary question: **Do I need to act now?**

### 2. Strategy Performance

Purpose: strategy-level trust and capital allocation.

Required surfaces:

- Strategy lineup sorted by trust score.
- Selected strategy scorecard:
  - P&L contribution
  - Brier/calibration
  - fill quality
  - slippage
  - drawdown
  - sample size
- Explanation panel:
  - why this score
  - what improved/worsened
  - recommended action: promote / keep paper / throttle / retire

Primary question: **Which strategy deserves capital?**

### 3. Action Review / Action Ledger

Purpose: help user approve or reject system actions.

Required surfaces:

- Pending action card:
  - strategy id/version
  - market question
  - side/size
  - expected edge
  - max loss impact
  - current exposure vs cap
- System explanation:
  - market passed filters
  - strategy produced signal
  - risk policy checked size
- Result panel:
  - order state
  - fill price
  - slippage
  - P&L attribution
  - link to trace/ledger

Primary question: **Should I approve this action?**

## Competitive References

- **Robinhood / brokerage apps**: portfolio P&L is the first object, not hidden behind tables.
- **Stripe Dashboard**: activity feed makes system/account actions understandable and auditable.
- **Linear**: issue/action queues are compact and decision-oriented, not data dumps.
- **Datadog / Grafana**: health status and incident timelines are useful, but PMS should simplify them for an operator, not expose raw observability first.
- **QuantConnect / Alpaca dashboards**: strategy/backtest performance exists, but they often require too much interpretation. PMS should explicitly say promote / throttle / keep paper.

## Mockup Boards Created

1. **Operator Home**
   - P&L summary plus needs-attention queue and system action timeline.

2. **Strategy Performance**
   - Strategy lineup ranked by trust score plus selected strategy explanation and recommended action.

3. **Action Review**
   - A pending trade action card, evidence/risk explanation, and post-action ledger result.

## Implementation Priority

1. **Operator Home**
   - Highest value because it gives the user immediate confidence and control.

2. **Action Review Card**
   - Needed before live trading because the user must approve or reject actions with evidence.

3. **Strategy Scorecard**
   - Needed to understand which strategies should receive capital or remain in paper mode.

4. **Detailed Market/Trade Tables**
   - Existing pages remain useful, but they become drill-down surfaces rather than the main workflow.

## PM Review Request

@PM-Derik should validate:

- Whether the four primary questions cover the actual operator journey.
- Whether P&L first + action ledger + strategy scorecard is the right hierarchy.
- Whether any compliance/live-readiness surfaces should move into the first screen.
- Whether the proposed approval queue matches how Stometa expects to supervise live trading.

## PM Review Additions Incorporated (v2)

After @PM-Derik review, the UI direction adds five required surfaces:

1. **Live Readiness Indicator**
   - Must sit at the same visual level as P&L on Operator Home.
   - Shows whether live mode is READY or BLOCKED.
   - Example blockers: missing Polymarket credentials, paper soak duration not complete, insufficient strategy sample size.

2. **Bankroll Management Surface**
   - Dedicated card for bankroll, exposure, per-market cap, and daily max loss.
   - Prevents paper P&L from creating false confidence about live risk.

3. **Strategy to Live Promotion Checklist**
   - Strategy scorecard must show preconditions before capital promotion:
     - paper soak duration
     - Brier threshold
     - minimum records/sample size
     - drawdown threshold
     - credentials / compliance ready

4. **Auto-Halt Recovery**
   - Needs Attention and Strategy pages must show HALTED states when drawdown, loss, stale data, or compliance rules trigger an automatic stop.
   - Recovery actions should be explicit: resume, keep paused, retire strategy, or adjust limits.

5. **Batch Approval**
   - Action Review can support batch approval for low-risk, similar, paper-mode actions.
   - Batch approval must show count, shared risk class, and cap impact; high-risk or live actions still require individual approval.

These additions keep the four primary questions unchanged, but make live readiness and bankroll constraints visible enough for a trading system.
