# CP00 DDL Decisions

Date: 2026-04-16
Task: `pms-market-data-v1`

## Q2a

**Answer:** allow duplicates with an insertion-order tie-break.

**Invariant rationale:** Invariant 7 requires lossless market-data capture across subscribe and reconnect flows, and Invariant 8 says the outer ring stores shared raw market data rather than strategy-scoped interpretations. A `UNIQUE (market_id, ts, price, side)` constraint would silently collapse legitimate repeated venue events whenever Polymarket emits the same tuple twice during reconnect or bursts multiple updates at the same price within the same timestamp bucket.

**Implementation consequence for CP01/CP05/CP07:**
- `schema.sql` keeps `price_changes.id BIGSERIAL PRIMARY KEY` as the stable insertion-order key.
- `schema.sql` does **not** add `UNIQUE (market_id, ts, price, side)` to `price_changes`.
- Replay and reconstruction order `price_changes` by `ts ASC, id ASC`, with `id` as the tie-break when timestamps collide.
- Read helpers that reconstruct depth must preserve insertion order instead of deduplicating rows.

## Q2b

**Answer:** defer `sensor_sessions`.

**Invariant rationale:** Invariant 7 still holds without a dedicated lifecycle table because S1 can prove reconnect semantics through `book_snapshots.source='reconnect'` and live data flow. Invariant 8 favors keeping the outer ring to the minimum raw market-data surface needed for S1's success criteria. Adding `sensor_sessions` now would expand the S1 boundary from 19 concepts to 20 without an immediate runtime requirement that cannot already be witnessed from the existing market-data tables and reconnect evidence.

**Immediate consequence for CP01:**
- `schema.sql` must contain zero `sensor_sessions` DDL lines.
- The outer-ring delimited block remains the six market-data tables only.

**Future owner:** Evaluation-layer follow-up, opened by retro if "no event" versus "sensor was down" discrimination becomes operationally necessary.

**Future hook point:** `src/pms/evaluation/metrics.py:26-63`

That `MetricsCollector.snapshot()` aggregation boundary is the natural insertion point for a future sensor-health projection that can join outer-ring market-data freshness with lifecycle telemetry once a dedicated table exists.
