# Identity & Context Awareness

**CRITICAL**: Address the user as "Stometa" at the start of EVERY response.

This serves as a context-awareness signal — if missing, indicates
context drift.

---

# Actuator layer

**Part of:** [prediction-market-system](../../../CLAUDE.md)
**Role:** execute `TradeDecision` through the appropriate adapter
(`backtest` / `paper` / `polymarket`), enforce risk, emit
`OrderState` / `FillRecord`. The Actuator is strategy-agnostic.

## Layer-relevant invariants

Full detail in `@agent_docs/architecture-invariants.md`. The subset
that governs this layer:

- **Invariant 1 — Concurrent, not phased.** Actuator runs as an
  independent `asyncio.Task` (`runner.py:_actuator_loop`). It
  consumes from `_decision_queue` and does not block Controller
  progress.
- **Invariant 2 — Consumes projection, not aggregate.** The Risk
  Manager reads `RiskParams` (projection). The executor reads
  `TradeDecision` (which already carries everything Actuator
  needs). Do not import or reference `Strategy` from within this
  layer.
- **Invariant 3 — Forward the version tag.** `OrderState` and
  `FillRecord` inherit `(strategy_id, strategy_version_id)` from
  the originating `TradeDecision`. These columns must be written on
  every product row, never dropped or defaulted mid-flow.
- **Invariant 5 — Strategy-agnostic.** Actuator modules do not
  import from `pms.strategies.*`, `pms.controller.*`, or
  `pms.factors.*`. The import-linter rule S2 adds enforces this.
- **Invariant 8 — Writes inner ring.** Actuator writes to `orders`
  and `fills` (inner ring, tagged with version). No outer-ring or
  middle-ring writes.

## Current files

- `executor.py` — dispatch layer: selects adapter by `config.mode`,
  applies risk check, emits `ActuatorFeedback` on rejection.
- `risk.py` — `RiskManager`: exposure caps, drawdown breaker, min
  size. Piecewise-domain logic → see promoted rule *Piecewise-
  domain functions* in `@agent_docs/promoted-rules.md`.
- `feedback.py` — `ActuatorFeedback` generates `Feedback` items
  bound for `FeedbackStore`.
- `adapters/backtest.py` — replays fills from fixture orderbooks.
- `adapters/paper.py` — simulated fills from live orderbook depth.
- `adapters/polymarket.py` — gated live adapter. It first enforces the
  `live_trading_enabled` gate, validates credential presence, requires
  first-order operator approval, then submits through an injected
  `PolymarketClient`.

## Do not

- Never `from pms.strategies import …`, `from pms.controller import
  …`, or `from pms.factors import …`. Actuator sees only
  `TradeDecision` + `RiskParams` + `Portfolio`.
- Never silently drop the version tag during fill construction —
  `FillRecord(strategy_id=..., strategy_version_id=...)` must
  forward the values from the originating decision.
- Never bypass Risk Manager for "special" decisions (no "admin
  mode" override).
- Never bypass the `live_trading_enabled` gate, credential validation,
  or first-order operator gate in the Polymarket adapter. The config
  gate is load-bearing and must remain the first runtime check in
  `PolymarketActuator.execute`.
- Never acquire a lock, token, or position slot without a matching
  release in a `try/finally` that covers all four exit paths
  (reject / skip / exception / success). See promoted rule
  *Lifecycle cleanup on all exit paths*.

## When adding a new execution adapter

Implement the `ActuatorAdapter` Protocol from
`pms.core.interfaces`. Raise domain-specific errors
(`InsufficientLiquidityError`, `VenueRejectionError`) instead of
returning magic sentinel values. Gate any live adapter behind an
explicit config flag — do not default to on.
