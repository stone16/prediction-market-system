# PMS Operator UI v2 Mockups

Status: approved for implementation planning
Owner: @Designer-Yiko
Review: @PM-Derik approved v2 in `#proj-prediction-market:1e9db269`

## Purpose

This package records the approved v2 direction for a more user-friendly PMS operator experience. The goal is to make the system understandable before live capital goes on-chain: the operator should immediately see P&L, live readiness, bankroll safety, system actions, strategy performance, and the decisions that need human approval.

## Mockups

- [Operator Home](screenshots/operator-home.png)
- [Strategy Scorecard](screenshots/strategy-scorecard.png)
- [Action Review Card](screenshots/action-review-card.png)
- [HTML preview](operator-ui-v2.html)

## Locked v2 Surfaces

### Operator Home

Answers: am I making or losing money, and is the system safe to move toward live mode?

Required content:

- P&L summary: realized P&L, unrealized P&L, open exposure, daily loss room.
- Live Readiness: READY/BLOCKED state with checklist for paper soak, credentials, compliance numbers, strategy sample quality.
- Bankroll Management: bankroll, exposure, per-market cap, daily loss cap, risk-level gauge.
- Needs Attention: missing credentials, compliance gaps, halted runner/strategy, pending approvals.
- System Action Timeline: sensor input, strategy signal, risk gate, actuator result, fill/settlement event.

### Strategy Scorecard

Answers: which strategy deserves capital?

Required content:

- Strategy lineup sorted by trust/performance status.
- Selected strategy scorecard with P&L, Brier/calibration, fill quality, slippage, drawdown, and sample count.
- Live promotion checklist: soak duration, Brier threshold, minimum records, drawdown threshold, credentials/compliance readiness.
- Recommended action: promote, keep paper, throttle, retire, or recover from HALTED state.

### Action Review Card

Answers: should I approve this system action?

Required content:

- Pending action summary: strategy id/version, market, side, size, expected edge, current exposure, max-loss impact.
- Evidence trail: market passed filters, strategy produced signal, risk policy checked size.
- Approval controls: approve single, reject, edit limits, batch approve low-risk paper actions.
- Result ledger: order status, fill price, slippage, P&L attribution, trace/ledger link.

## Safety Priorities

The first implementation should prioritize:

1. Live Readiness Indicator.
2. Bankroll Management.
3. Auto-Halt Recovery states and actions.
4. Strategy promotion checklist.
5. Batch approval only for low-risk paper actions.

High-risk or live actions should still require individual approval.

## Design Acceptance Criteria

- P&L and live readiness are both visible above detailed tables.
- Paper profit cannot visually imply live readiness when credentials or compliance are missing.
- Bankroll limits are readable without opening a secondary page.
- HALTED states show why the stop happened and what recovery actions are allowed.
- Strategy status is actionable, not just metric-heavy.
- The action review flow shows evidence and risk impact before approval.
- Existing Cathedral v1 visual language is preserved: warm paper background, editorial typography, restrained green/coral/amber status colors, compact operational density.
