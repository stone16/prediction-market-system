---
task_id: cathedral-v1
checkpoints_verified: 12
verdict: PASS
---

# Cathedral v1 End-to-End Evaluation Report

## Scope

End-to-end verification of the complete Cathedral branch after CP12, using:

- the final checkpoint artifact bundle through
  `.harness/cathedral-v1/checkpoints/12/iter-1/`
- a fresh current-head cross-checkpoint integration slice (`11 passed`)
- the full dashboard Playwright suite on the closeout head
  (`18 passed, 1 skipped`)
- current-head Cathedral-specific grep checks proving the old runtime
  vocabulary and `layer-card` surface remain absent

## Verdict Summary

| Flow / success theme | Evidence | Status |
| --- | --- | --- |
| Markets browse surface (`/markets`) is durable end to end | `tests/integration/test_markets_route.py`, `dashboard/e2e/markets-page.spec.ts` | PASS |
| Accept flow is durable, stale-safe, and idempotent | `tests/integration/test_decision_emission_cp07.py`, `tests/integration/test_api_decisions_cp08.py`, `dashboard/e2e/cathedral-accept.spec.ts` | PASS |
| Persisted fills back `/positions` and `/trades` | `tests/integration/test_positions_trades_route.py`, `dashboard/e2e/onboarding-ledger.spec.ts` | PASS |
| Event stream powers the drawer without breaking replay | `tests/integration/test_api_event_stream_cp10.py`, `dashboard/e2e/event-log-drawer.spec.ts` | PASS |
| Public share route/page remains allowlist-only | `tests/integration/test_share_route_cp11.py`, `dashboard/e2e/share-page.spec.ts` | PASS |
| Today hero + first-trade metric landed on the narrative home surface | `tests/integration/test_first_trade_metric_cp12.py`, `dashboard/e2e/cathedral.spec.ts` | PASS |
| Old architecture vocabulary stays off the runtime UI surface | `evidence/forbidden-vocab.txt`, `evidence/layer-card-grep.txt` | PASS |

## Data-Flow Audit

| Flow | Producer -> Consumer | Boundary type | Shape match? | Staleness risk? |
| --- | --- | --- | --- | --- |
| Candidate markets | `MarketDiscoverySensor` persistence -> `GET /markets` -> `/markets` page | PostgreSQL -> JSON -> React table | Yes | Low; request-scoped reads |
| Durable idea acceptance | `Opportunity` + `TradeDecision` -> `decisions` table -> `/decisions/{id}/accept` -> actuator queue | PostgreSQL row + idempotent POST | Yes | Low; stale hash + dedup explicitly gate reuse |
| Fill ledger | `FillStore.insert` -> `/positions` + `/trades` -> ledger pages | PostgreSQL -> aggregation/query JSON | Yes | Low; reads are derived directly from persisted fills |
| Event log | `Runner.event_bus.publish()` -> `/stream/events` -> `EventLogDrawer` | in-memory ring buffer -> SSE -> browser state | Yes | Low; replay resumes from `Last-Event-ID` |
| Public share projection | strategy metadata + eval/fill aggregates -> `/share/{id}` -> public share page | allowlist projection JSON | Yes | Low; route is read-only and explicitly projected |
| First-trade metric | `decisions` + `fill_payloads` + `fills` SQL join -> `/metrics` -> Today hero | SQL aggregate -> JSON metric | Yes | Low; computed on demand |

## Findings

### Blocking

None.

### Informational

- The only skipped Playwright spec is `e2e/source-indicators.spec.ts` mock-mode,
  which is already marked as skipped in the suite. The closeout run produced
  `18 passed, 1 skipped`.
- Cathedral-specific grep checks were rerun on the closeout head, not inherited
  blindly from CP12.

## Verdict: PASS

The complete Cathedral branch passes cross-checkpoint integration and the full
dashboard browser suite on the closeout head. No branch-level seam failure
remains across the durable markets browse flow, idea acceptance, persisted fill
ledgers, SSE event log, public share projection, or the Today hero / first
trade metric path.
