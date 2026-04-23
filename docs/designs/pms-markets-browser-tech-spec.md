---
task_id: pms-markets-browser-v1
title: "PMS Markets Browser v1 — prices, filters, detail drawer, subscriptions"
version: 1
status: draft
branch: feat/pms-markets-browser-v1
created: 2026-04-23T21:10:00+08:00
updated: 2026-04-23T21:10:00+08:00
---

<!--
  This file is the git-tracked canonical Markets Browser v1 tech spec.
  .harness/pms-markets-browser-v1/spec.md is a symlink to this path so the
  Harness engine + Spec Evaluator read the same file. Do not duplicate content.

  Canonical source documents (referenced — do not copy):
    - src/pms/sensor/adapters/market_discovery.py       (existing Discovery sensor)
    - src/pms/storage/market_data_store.py              (outer-ring store)
    - src/pms/api/app.py                                (current /markets route)
    - src/pms/api/routes/markets.py                     (current response shape)
    - dashboard/components/MarketsTable.tsx             (current column layout)
    - dashboard/components/MarketsPageClient.tsx        (hardcoded limit=20)
    - dashboard/lib/useLiveData.ts                      (5s polling hook)
    - agent_docs/architecture-invariants.md             (Invariants 1–8)
    - agent_docs/promoted-rules.md                      (promoted retros)
    - CLAUDE.md                                         (canonical gates, baseline)
-->

## Goal

Turn `/markets` from a 14-row NHL-and-GTA-VI curiosity into a usable trading
browser: surface live Gamma prices on every row, give the user strong
filtering + pagination over the 485+ candidate set, let them inspect any
market in a detail drawer with a price-history mini-chart, and give them a
first-class "subscribe" affordance that persists across runner restarts
and participates in the `MarketSelector` union-merge.

The bundle executes as a single Harness task on `feat/pms-markets-browser-v1`;
post-harness, the 12 commit ranges cherry-pick onto 12 per-PR branches for
independent review. The starting HEAD is the tip of
`fix/sensor-gamma-query-params` (commit `7607959`), which raises the Discovery
sensor's Gamma `/markets` page size from the default 20 to `limit=500`
`active=true` `closed=false`. Without that fix the development experience
is indistinguishable from main's 14-row state; with it the dev DB has 485+
unresolved markets to exercise every filter and pagination boundary.

Concretely, this spec turns four independently observed product gaps into
testable deliverables:

- **Price gap.** `src/pms/sensor/adapters/market_discovery.py:119-144`
  extracts only `condition_id / slug / question / venue / volume_24h /
  resolves_at / created_at / last_seen_at` from the Gamma row, even though
  the same response already contains `outcomePrices` (YES/NO live),
  `lastTradePrice`, `bestBid`, `bestAsk`, `liquidity`, `spread`. The UI at
  `dashboard/components/MarketsTable.tsx:44-76` therefore cannot show any
  price — rendering every row effectively non-actionable. Fix: extend
  Gamma-row parsing and persist 8 price fields on `markets` + append a
  time-series row to a new `market_price_snapshots` table on every poll.
- **Discovery gap.** `dashboard/components/MarketsPageClient.tsx:9`
  hard-codes `/markets?limit=20` with no pagination control and no filters,
  so the user sees 20 of 485+ unresolved markets with no way to narrow.
  Fix: URL-synced filter set (volume, liquidity, spread, YES price band,
  resolution window, subscribed state, free-text search) + offset/limit
  pagination with page-size selector.
- **Inspection gap.** Clicking a market jumps to `/signals?market_id=…`
  which is an operations console, not a per-market detail view. Fix: an
  inline right-side detail drawer (`?detail=<condition_id>` URL sync) with
  large YES/NO price display, 24h-delta, a YES-only price-history line
  chart backed by `market_price_snapshots`, full metrics, a subscribe
  toggle, and metadata collapsed by default.
- **Subscription gap.** The user has no way to say "pay attention to this
  market" that the Runner will honor across restarts. `MarketSelector`
  (`src/pms/market_selection/selector.py`) builds its subscription list
  from strategy-derived eligibility only. Fix: add a persisted
  `market_subscriptions(token_id, source)` table; `MarketSelector` reads
  it and union-merges its output with `source='user'` rows so a user pick
  is sticky across runner lifecycle and never evicted by strategy
  reselection.

**Scope discipline.** This spec codifies the decisions brainstormed 2026-04-23
(price-source Option A — Gamma-poll only; subscribe-button Option A —
force-subscribe with sticky `source='user'`; filter set of 8 items; filter
UX Option C — top search + Advanced popover). It introduces NO new product
decisions. The Spec Evaluator may flag missing implementation-level decisions;
the Generator will escalate per Planning Protocol step 5.

## Success Criteria

System-level, observable, falsifiable. Each criterion references the
checkpoint(s) whose combined output produces the observable behavior.

1. **Gamma price fields persist and surface through `/markets`.** After
   CP05 lands, `curl http://127.0.0.1:8000/markets?limit=1` against a
   paper-mode runner that has completed ≥1 Discovery poll returns a
   response whose first element exposes non-null `yes_price`, `no_price`,
   `best_bid`, `best_ask`, `last_trade_price`, `liquidity`, `spread_bps`,
   and `price_updated_at` fields. (CP01, CP03, CP05)

2. **Price history is queryable for any market.** After CP07 lands, against
   a runner that has completed ≥3 Discovery polls,
   `curl http://127.0.0.1:8000/markets/{condition_id}/price-history?since=...`
   returns a JSON array of ≥3 `{snapshot_at, yes_price, …}` rows ordered
   by `snapshot_at ASC`. (CP02, CP04, CP07)

3. **`/markets` UI shows live YES/NO prices on every row.** After CP08
   lands, visiting `http://127.0.0.1:3100/markets` against the same paper
   runner renders every row with a `YES` and `NO` column populated with a
   percentage (e.g., `52.5%` / `47.5%`) or — for rows where Gamma returned
   null — the explicit `—` placeholder. Zero rows render `undefined` or a
   raw decimal. (CP08)

4. **User-initiated subscription sticks across runner restart.** After
   CP06 lands, the flow: (i) user clicks Subscribe on market X in the UI
   → `market_subscriptions` table has a row `(token_id=X.yes_token_id,
   source='user')`; (ii) restart the runner via
   `POST /run/stop` + `POST /run/start`; (iii) on next Discovery poll,
   `MarketSelector` emits a subscription list including X's tokens;
   (iv) the UI row for X shows `⭐ subscribed (user)`. (CP02, CP06)

5. **Detail drawer opens without route change and is shareable.** After
   CP09 lands, clicking a market row updates the URL to include
   `?detail=<condition_id>` without a page navigation; reloading the page
   at that URL reopens the drawer on the same market. Pressing Esc or
   clicking the backdrop closes the drawer and strips the query param.
   (CP09)

6. **Price-history mini chart renders in the drawer.** After CP10 lands,
   the drawer for a market with ≥3 snapshots shows a rendered SVG line
   chart (YES price only); markets with <2 snapshots show an explicit
   "building price history…" empty state, not a broken chart. (CP10)

7. **Filter popover narrows 485 rows to an actionable subset.** After CP11
   lands, from an initial /markets state with 485 rows, setting
   `volume_min=$100K` + `resolves_within_days=30` + `subscribed=only`
   produces a filtered count visible in the hero (`"485 → 3 shown"`) and
   each filter renders a chip with × to remove. Reloading the page with
   those URL params restores the exact same filter state. (CP11)

8. **Pagination exposes the full set.** After CP12 lands, page size 50 +
   a 485-row set produces 10 pages; `«` / `»` / page number input all
   work; switching page size resets to page 1; the "X of Y" counter
   matches server-side `total`. (CP12)

9. **Canonical gates remain green throughout.** Each checkpoint's commit
   passes `uv run pytest -q`, `uv run mypy src/ tests/ --strict`,
   `cd dashboard && npm run test`, `cd dashboard && npm run lint` before
   handoff to the Evaluator. (CP01 — CP12)

## Technical Approach

### §1. Scope & Out of Scope

**In scope (v1):**

- Price ingestion via Gamma `/markets` poll (no WebSocket real-time)
- 10-second polling granularity for `markets` + `market_price_snapshots`
- Denormalized "current snapshot" columns on `markets`
- Time-series `market_price_snapshots` with no retention policy
- User-initiated persisted subscriptions with `source='user' | 'selector'`
- `MarketSelector` union-merge that never evicts `source='user'`
- 8-column `/markets` UI redesign + detail drawer + YES-only line chart
- 8-filter popover (search, volume, liquidity, spread, YES band,
  resolution window, subscribed state, plus text search)
- URL-synced filter + pagination state
- Server-side SQL WHERE for every filter; zero client-side filtering

**Explicitly out of scope (v2+):**

- WebSocket real-time price feed for subscribed markets
- Category / tags filter (Gamma `/markets` default response has these
  fields null; would require a different endpoint call)
- Saved filter presets / named searches
- Multi-venue markets (Kalshi remains a stub; only `venue='polymarket'`)
- `market_price_snapshots` retention policy (monitoring only in v1)
- Optimistic UI for filter changes (every filter change hits the server)
- Drawer price-history charts beyond YES line (no bid/ask overlay, no NO line)
- NO-token independent subscription (subscribing a market subscribes both
  tokens atomically)

### §2. Architecture Overview

Data flow, left to right:

```
Polymarket Gamma API
        │ HTTPS, every 10s
        ▼
MarketDiscoverySensor.poll_once  (src/pms/sensor/adapters/market_discovery.py)
        │ parse 8 price fields  (new in CP03)
        ├──► PostgresMarketDataStore.write_market     (extended in CP03)
        │            │ UPSERT into markets (new price cols)
        │            ▼
        │        markets
        │
        └──► PostgresMarketDataStore.write_price_snapshot  (new in CP04)
                     │ INSERT into market_price_snapshots
                     ▼
                 market_price_snapshots

MarketSelector.current_asset_ids  (src/pms/market_selection/selector.py)
        │ read strategy-eligible tokens                (existing)
        ├─ read market_subscriptions WHERE source='user' (new in CP06)
        └─ UnionMergePolicy.merge → subscription list   (existing, same API)
                     │
                     ▼
        SensorSubscriptionController → MarketDataSensor

FastAPI routes  (src/pms/api/app.py)
   ├─ GET  /markets                   (extended in CP05, CP11)
   ├─ POST /markets/{token_id}/subscribe    (new in CP06)
   ├─ DEL  /markets/{token_id}/subscribe    (new in CP06)
   └─ GET  /markets/{condition_id}/price-history  (new in CP07)

Next.js dashboard  (dashboard/app/markets/page.tsx)
   ├─ MarketsPageClient → MarketsTable (new columns CP08)
   ├─ MarketsFilterPopover + chips + URL sync        (new in CP11)
   ├─ MarketsPagination                               (new in CP12)
   └─ MarketDetailDrawer (opens on row click)         (new in CP09, CP10)
         ├─ FreshnessDot, PriceBars
         ├─ PriceHistoryChart (fetch /price-history)
         └─ SubscribeToggle → POST/DEL /subscribe
```

**Invariant preservation:**

| Invariant | How this spec preserves it |
|-----------|------|
| #1 Concurrent not phased | Discovery → DB → API → UI are already concurrent; this spec adds edges to existing layers, adds no synchronous barriers. |
| #5 Sensor strategy-agnostic | New `market_price_snapshots` + `market_subscriptions` tables are strategy-agnostic (no `strategy_id`). Sensor reads neither. |
| #6 Controller pushes subscription | `MarketSelector` remains the only writer to the sensor subscription sink. It gains a second **input** (user subscriptions) but preserves the "one writer" contract. |
| #7 Two-layer sensor | Price fields persisted via the **HTTP Discovery poll**, not WebSocket. No work lands in `MarketDataSensor`. |
| #8 Outer-ring only | All three schema changes are outer-ring tables. None contain `strategy_id`. |

### §3. Data Model Changes

Three migrations, landing in CP01 + CP02. Alembic revision IDs continue the
existing sequence (`0005_strategies_share_metadata` is current tip).

#### Migration `0006_markets_price_fields` (CP01)

```sql
ALTER TABLE markets
  ADD COLUMN yes_price           NUMERIC(6,4),
  ADD COLUMN no_price            NUMERIC(6,4),
  ADD COLUMN best_bid            NUMERIC(6,4),
  ADD COLUMN best_ask            NUMERIC(6,4),
  ADD COLUMN last_trade_price    NUMERIC(6,4),
  ADD COLUMN liquidity           NUMERIC,
  ADD COLUMN spread_bps          INTEGER,
  ADD COLUMN price_updated_at    TIMESTAMPTZ;

-- Partial index only on rows that actually have a price, to keep the
-- index cheap while the sensor catches up.
CREATE INDEX idx_markets_price_updated_at
  ON markets (price_updated_at DESC)
  WHERE price_updated_at IS NOT NULL;
```

All new columns nullable. Downgrade drops columns + index. No data backfill
(next Discovery poll populates them).

#### Migration `0007_market_price_snapshots` (CP02)

```sql
CREATE TABLE market_price_snapshots (
  condition_id         TEXT        NOT NULL,
  snapshot_at          TIMESTAMPTZ NOT NULL,
  yes_price            NUMERIC(6,4),
  no_price             NUMERIC(6,4),
  best_bid             NUMERIC(6,4),
  best_ask             NUMERIC(6,4),
  last_trade_price     NUMERIC(6,4),
  liquidity            NUMERIC,
  volume_24h           NUMERIC,
  PRIMARY KEY (condition_id, snapshot_at),
  FOREIGN KEY (condition_id) REFERENCES markets(condition_id) ON DELETE CASCADE
);

CREATE INDEX idx_price_snapshots_recent
  ON market_price_snapshots (condition_id, snapshot_at DESC);
```

No retention in v1. Observability (see §8) tracks row count; a cron-based
retention migration is deferred to v2.

#### Migration `0008_market_subscriptions` (CP02)

```sql
CREATE TYPE subscription_source AS ENUM ('user', 'selector');

CREATE TABLE market_subscriptions (
  token_id    TEXT NOT NULL PRIMARY KEY,
  source      subscription_source NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  FOREIGN KEY (token_id) REFERENCES tokens(token_id) ON DELETE CASCADE
);
```

`PRIMARY KEY (token_id)` enforces "one subscription row per token" —
user and selector cannot both claim the same token simultaneously.
UPSERT on insert: if `source='selector'` row exists and a user clicks
Subscribe, the row flips to `source='user'` (user always wins; see §2
`MarketSelector` union-merge rule). Conversely when a user unsubscribes,
MarketSelector re-scores on the next reselection and re-inserts with
`source='selector'` if still strategy-eligible.

### §4. API Surface Changes

#### `GET /markets` — extended response (CP05, CP11)

New fields on each row of `markets[]`:

| Field | Type | Source |
|-------|------|--------|
| `yes_price` | `number \| null` | `markets.yes_price` |
| `no_price` | `number \| null` | `markets.no_price` |
| `best_bid` | `number \| null` | `markets.best_bid` |
| `best_ask` | `number \| null` | `markets.best_ask` |
| `last_trade_price` | `number \| null` | `markets.last_trade_price` |
| `liquidity` | `number \| null` | `markets.liquidity` |
| `spread_bps` | `integer \| null` | `markets.spread_bps` |
| `price_updated_at` | `string \| null` | `markets.price_updated_at` (ISO-8601) |
| `subscription_source` | `"user" \| "selector" \| null` | `market_subscriptions.source` or null if idle |

New query parameters (CP11):

| Param | Type | Default | Semantics |
|-------|------|---------|-----------|
| `q` | string | `""` | Case-insensitive substring match on `question` |
| `volume_min` | number | `0` | `volume_24h >= volume_min` (null volumes excluded when `>0`) |
| `liquidity_min` | number | `0` | `liquidity >= liquidity_min` (null excluded when `>0`) |
| `spread_max_bps` | integer | unlimited | `spread_bps <= spread_max_bps` (null excluded when set) |
| `yes_min` | number | `0` | `yes_price >= yes_min` (null excluded when `>0`) |
| `yes_max` | number | `1` | `yes_price <= yes_max` (null excluded when `<1`) |
| `resolves_within_days` | integer | unlimited | `resolves_at <= NOW() + interval 'N days'` |
| `subscribed` | enum | `all` | `all` / `only` / `idle` |

**NULL-handling convention.** A row with `yes_price IS NULL` falls outside
any non-default `yes_min`/`yes_max` band — i.e., unknown data cannot pass
a price filter. This matches trader intuition: "show me markets priced
20-80%" should NOT include markets whose price we don't know yet.
See §7 test cases `test_read_markets_null_price_excluded_from_band`.

Existing `limit` / `offset` pagination unchanged; `MarketsListResponse.total`
reflects the filtered count.

#### `POST /markets/{token_id}/subscribe` — new (CP06)

- Auth: `require_api_token`
- Effect: UPSERT `market_subscriptions(token_id, source='user')`. If a
  `source='selector'` row exists for the same token, UPDATE to `'user'`
  (user wins).
- Response: `{ token_id, source: 'user', created_at }` (the resulting row)
- 404 if the token_id does not exist in `tokens` table.

#### `DELETE /markets/{token_id}/subscribe` — new (CP06)

- Auth: `require_api_token`
- Effect: DELETE row WHERE `token_id=$1 AND source='user'` (does not
  touch selector rows). Selector will re-insert on next reselection if
  still eligible.
- Response: `{ token_id, deleted: boolean }` where `deleted` reflects
  whether a user row existed.

#### `GET /markets/{condition_id}/price-history` — new (CP07)

- Query params: `since` (ISO-8601, default = 24h ago), `limit` (default
  1440 — enough for 24h at 1-min resolution).
- Response: `{ condition_id, snapshots: [{snapshot_at, yes_price, no_price,
  best_bid, best_ask, last_trade_price, liquidity, volume_24h}, ...] }`
- Order: `snapshot_at ASC`.
- 404 if condition_id has no rows.

### §5. Frontend Components

New / modified components:

| Component | File | CP |
|-----------|------|----|
| `MarketsTable` (rewrite) | `dashboard/components/MarketsTable.tsx` | CP08 |
| `FreshnessDot` | `dashboard/components/FreshnessDot.tsx` (new) | CP08 |
| `PriceBar` | `dashboard/components/PriceBar.tsx` (new) | CP08 |
| `SubscribeStar` | `dashboard/components/SubscribeStar.tsx` (new) | CP08 |
| `MarketDetailDrawer` | `dashboard/components/MarketDetailDrawer.tsx` (new) | CP09 |
| `PriceHistoryChart` | `dashboard/components/PriceHistoryChart.tsx` (new) | CP10 |
| `MarketsFilterPopover` | `dashboard/components/MarketsFilterPopover.tsx` (new) | CP11 |
| `MarketsFilterChips` | `dashboard/components/MarketsFilterChips.tsx` (new) | CP11 |
| `MarketsPagination` | `dashboard/components/MarketsPagination.tsx` (new) | CP12 |
| `useMarketsFilters` | `dashboard/lib/useMarketsFilters.ts` (new) | CP11 |

`MarketsPageClient.tsx` rewires to wire these together; `useLiveData`
continues as the data source (5s polling against `/markets?...` with
current filter/pagination URL state).

**URL-as-state invariant.** All filter + pagination state lives in the URL.
`useMarketsFilters` is a thin hook that reads `useSearchParams()` and
writes via `router.replace()`. No component holds a local filter state
that isn't mirrored in the URL within the same tick.

### §6. PR Sequencing

Single Harness task on `feat/pms-markets-browser-v1`. Post-harness
cherry-pick onto 12 per-PR branches against `main`:

| PR | CPs | Title |
|----|-----|-------|
| 1 | CP01 | `feat(schema): markets price columns migration 0006` |
| 2 | CP02 | `feat(schema): price snapshots + subscriptions migrations 0007 0008` |
| 3 | CP03 | `feat(sensor): gamma row price extraction + markets upsert` |
| 4 | CP04 | `feat(sensor): write price snapshot per discovery poll` |
| 5 | CP05 | `feat(api): /markets response exposes price fields + subscription source` |
| 6 | CP06 | `feat(api): subscribe/unsubscribe endpoints + selector union merge` |
| 7 | CP07 | `feat(api): /markets/{id}/price-history endpoint` |
| 8 | CP08 | `feat(dashboard): markets table new columns + freshness dot` |
| 9 | CP09 | `feat(dashboard): market detail drawer shell + URL sync` |
| 10 | CP10 | `feat(dashboard): price history chart + subscribe toggle wiring` |
| 11 | CP11 | `feat(dashboard): markets filter popover + chips + URL state` |
| 12 | CP12 | `feat(dashboard): markets pagination + e2e happy path` |

### §7. Testing Strategy

Per CLAUDE.md canonical gates; each checkpoint's commit must pass:

- `uv run pytest -q` (unit + integration; integration gated on
  `PMS_RUN_INTEGRATION=1` + `PMS_TEST_DATABASE_URL`)
- `uv run mypy src/ tests/ --strict` (all 277+ files clean)
- `cd dashboard && npm run test` (Vitest)
- `cd dashboard && npm run lint`
- `cd dashboard && npx playwright test` (only CPs touching UI flows)

**Baseline.** As of `fix/sensor-gamma-query-params` tip: 601 passed, 138
skipped. This spec adds ~55 new tests (~20 per backend phase, ~15 per
frontend phase, ~20 for filters). End state target: ~656 passed.

**Critical test coverage per CP** (deep-dives in each checkpoint's
Acceptance Criteria):

- CP01, CP02: alembic upgrade + downgrade cycles apply cleanly; idempotent.
- CP03: every Gamma price field mapped correctly; missing fields fallback
  to `None` without raising; `spread_bps` derivation `(ask-bid)*10000`
  rounded to integer.
- CP04: one snapshot row per poll per market; concurrent polls don't
  double-insert (primary key enforces).
- CP05: existing `/markets` consumers (dashboard pre-CP08) still work —
  new fields are additive, no breaking change.
- CP06: subscribe/unsubscribe idempotency; selector row never overwrites
  user row; user can't unsubscribe a selector row.
- CP07: empty-history case returns empty array + 200, not 404 for markets
  that exist but have no snapshots yet.
- CP08: every row renders all 8 columns; null prices show `—`, not
  `undefined` or `NaN%`; freshness dot color thresholds match spec.
- CP09: URL round-trip (open → reload → closed via Esc → URL clean).
- CP10: empty/single/multi-snapshot chart states; subscribe button
  optimistic state + rollback on 4xx/5xx.
- CP11: URL ↔ state bidirectional sync; chip removal updates URL + data
  in one render; null-price filter exclusion rule from §4.
- CP12: pagination boundary (page 1 ⇄ last page); page size reset to 1.

**Happy-path e2e** in CP12: a single Playwright spec walks the full flow
— load /markets → apply 2 filters → verify chip render → open drawer on
filtered row → subscribe → close drawer → paginate to page 2 → unsubscribe
— with zero console errors throughout.

### §8. Observability

**Metrics (Prometheus-exposed via `/metrics`; CP03 + CP04):**

- `pms_sensor_discovery_price_fields_populated_ratio` (gauge, 0-1) —
  fraction of Gamma rows in the last poll that had non-null `outcomePrices`
- `pms_sensor_discovery_snapshots_written_total` (counter) — per-poll
  count of rows written to `market_price_snapshots`
- `pms_markets_total` (gauge) — `SELECT COUNT(*) FROM markets`
- `pms_market_price_snapshots_total` (gauge) — row count of snapshots
  table; monitor for storage drift

**Structured logs:**

- `subscription.user_add` / `subscription.user_remove` — token_id,
  condition_id, request metadata (CP06)
- `discovery.price_parse_failure` — Gamma row `id` + reason, when any
  price field fails to parse (CP03; log + continue, do not fail the row)

**Alerts (documented in CP04's spec; wired up is out-of-scope for v1):**

- `market_price_snapshots_total > 10_000_000` → manual review for
  retention policy
- `price_fields_populated_ratio < 0.5` for 10 minutes → Gamma API schema
  drift suspected

### §9. Risks & Mitigations

| # | Risk | Probability | Severity | Mitigation |
|---|------|-------------|----------|------------|
| R1 | `market_price_snapshots` grows 4M+ rows/day, PostgreSQL perf degrades | M | M | v1: observability alert + documented manual pgsql archive script. v2: cron retention migration. |
| R2 | Gamma schema change renames fields silently → all new prices NULL | L | H | CP03 ships `price_parse_failure` log; CP08 includes explicit `—` render for null so regression is user-visible not silent. |
| R3 | MarketSelector union-merge logic has a race where user row deleted mid-reselection → UI shows idle for one tick | L | L | CP06 uses a single SQL read inside the selector's existing lock; accept ≤5s flicker as acceptable UX. |
| R4 | Detail drawer `?detail=` URL survives across logical page changes and confuses back-button | M | L | CP09 strips `?detail=` on unmount of MarketsPageClient (route change) via `useEffect` cleanup. |
| R5 | Filter popover `volume_min` slider's non-linear buckets mis-lead user ("why does $100K jump to $1M") | M | L | CP11 uses log-scale slider labeled in buckets (`$1K, $10K, $100K, $1M, $10M+`); see UI mock in §5. |
| R6 | `/markets` query with all 8 filters + large offset becomes slow | L | M | CP11 adds a single composite index `(volume_24h, liquidity, price_updated_at, resolves_at)` if EXPLAIN shows seq scan on filtered queries. Measure before indexing (PRINCIPLES.md "Measure First"). |
| R7 | Subscribe button called concurrently from 2 tabs → PK violation | L | L | CP06 response is idempotent: POST on existing user row returns 200 with the existing row, not an error. |
| R8 | Existing /signals deep-link from `MarketsTable.tsx:58` breaks when the column is removed | L | L | CP08 preserves a "Metadata → Open in Signals" link inside the detail drawer; no link outright disappears. |

### §10. Success Criteria (human-visible recap)

A user browsing `/markets` can:

- See live YES/NO prices on every one of ~485 rows (CP08)
- Filter to markets with volume ≥ $100K and resolving within 30 days and
  see the filtered count in the hero (CP11)
- Page through the filtered result, 50 rows at a time, with a page-number
  jump input (CP12)
- Click any row to open a right-side drawer showing full metrics + a
  24h YES-price line chart (CP09, CP10)
- Star/subscribe a market from the drawer; the subscription persists
  across runner restarts and survives strategy reselection (CP06)
- Share the exact filtered-and-detail-open view via URL (CP09, CP11)

## Checkpoints

### Checkpoint CP01: Alembic `0006_markets_price_fields`

- Scope: Add migration `alembic/versions/0006_markets_price_fields.py`
  per §3 Migration `0006`. Include upgrade (8 columns + partial index) and
  downgrade (reverse). No sensor / API / UI changes. `Market` dataclass at
  `src/pms/core/models.py` extended with the 8 new optional fields.
- Depends on: —
- Type: backend (schema)
- Acceptance criteria:
  - [ ] `uv run alembic upgrade head` on a fresh clone completes without
    error; `\d markets` shows the 8 new columns + partial index
  - [ ] `uv run alembic downgrade -1` reverses cleanly; re-`upgrade head`
    is idempotent
  - [ ] Unit test `test_market_dataclass_accepts_price_fields` — construct
    a `Market(...)` with all 8 fields, assert immutability (frozen
    dataclass invariant per CLAUDE.md project conventions)
  - [ ] Integration test `test_migration_0006_apply_and_reverse` (gated on
    `PMS_RUN_INTEGRATION=1`) — apply, check column list, reverse, confirm
    columns gone
  - [ ] mypy strict clean (277+ files)
  - [ ] `uv run pytest -q` still 601+ passing
- Files of interest: `alembic/versions/0006_markets_price_fields.py` (new),
  `src/pms/core/models.py`
- Effort estimate: S

### Checkpoint CP02: Alembic `0007_market_price_snapshots` + `0008_market_subscriptions`

- Scope: Migrations per §3 `0007` and `0008`. Pair them in one CP
  because they are both new tables with no sensor/API consumers until
  later CPs, and landing them together keeps CP count at 12.
- Depends on: CP01
- Type: backend (schema)
- Acceptance criteria:
  - [ ] `uv run alembic upgrade head` creates both tables + indexes; `\dt`
    shows `market_price_snapshots` and `market_subscriptions`
  - [ ] Downgrade cycle clean for both
  - [ ] Integration test `test_migration_0007_0008_apply_and_reverse`
  - [ ] Integration test `test_market_subscriptions_pk_rejects_duplicate` —
    INSERT twice with same `token_id`, expect `UniqueViolationError`
  - [ ] Integration test `test_market_price_snapshots_cascade_on_market_delete`
    — INSERT snapshot, DELETE parent market, assert snapshot cascaded
  - [ ] mypy strict clean; pytest baseline unchanged
- Files of interest: `alembic/versions/0007_market_price_snapshots.py`
  (new), `alembic/versions/0008_market_subscriptions.py` (new)
- Effort estimate: S

### Checkpoint CP03: Gamma row price extraction + `write_market` UPSERT of new columns

- Scope: Extend `_gamma_market_to_market` at
  `src/pms/sensor/adapters/market_discovery.py:119` to parse the 8 price
  fields from `outcomePrices / lastTradePrice / bestBid / bestAsk /
  liquidity`. Compute `spread_bps = round((bestAsk - bestBid) * 10000)`
  when both present. On any parse failure, emit a `discovery.price_parse_failure`
  log and fall back to `None` for the affected field only (do not skip the
  row). Extend `PostgresMarketDataStore.write_market` SQL to UPSERT the 8
  new columns. Add Prometheus gauge
  `pms_sensor_discovery_price_fields_populated_ratio` computed per poll.
- Depends on: CP01
- Type: backend (sensor)
- Acceptance criteria:
  - [ ] Unit test `test_gamma_row_to_market_extracts_all_price_fields` —
    fixture containing all 5 source Gamma fields, assert all 8 output
    fields correct (including derived `spread_bps`)
  - [ ] Unit test `test_gamma_row_missing_outcome_prices_falls_back_to_none`
    — field absent from row, assert `yes_price=no_price=None` and no
    exception
  - [ ] Unit test `test_gamma_row_outcome_prices_string_encoded_parsed` —
    Gamma sometimes returns `outcomePrices` as a JSON string instead of a
    list; parser handles both
  - [ ] Unit test `test_spread_bps_rounded_to_integer` — ask=0.525,
    bid=0.519 → 60 bps
  - [ ] Integration test `test_discovery_poll_persists_price_fields` —
    MockTransport serving a fixture, `poll_once()`, assert rows in
    `markets` table have non-null `yes_price` etc.
  - [ ] mypy strict clean
  - [ ] Metric visible at `/metrics` endpoint
- Files of interest: `src/pms/sensor/adapters/market_discovery.py`,
  `src/pms/storage/market_data_store.py`, `src/pms/metrics.py` (or
  wherever Prometheus gauges live — check before implementing)
- Effort estimate: M
- **Learning-mode contribution point:** `_parse_outcome_prices` helper —
  the user decides the fallback rule when Gamma returns a malformed
  `outcomePrices` (e.g., wrong length, non-numeric string). Options:
  (a) return `(None, None)`, (b) raise and skip the whole row, (c) return
  `(None, None)` + warning log. See comment in the file.

### Checkpoint CP04: `write_price_snapshot` per Discovery poll

- Scope: Add `PostgresMarketDataStore.write_price_snapshot(condition_id,
  snapshot_at, yes_price, ..., volume_24h) -> None` that INSERTs into
  `market_price_snapshots`. Wire it into `MarketDiscoverySensor.poll_once`
  to call once per successfully-parsed market, using the same `fetched_at`
  timestamp as `write_market`. Add Prometheus counter
  `pms_sensor_discovery_snapshots_written_total`.
- Depends on: CP02, CP03
- Type: backend (sensor)
- Acceptance criteria:
  - [ ] Unit test `test_write_price_snapshot_inserts_row` — store mock,
    call method, assert SQL INSERT is issued with correct values
  - [ ] Integration test `test_discovery_poll_writes_one_snapshot_per_market`
    — 3-market fixture, 1 poll, count `market_price_snapshots` = 3
  - [ ] Integration test `test_discovery_poll_idempotent_on_duplicate_timestamp`
    — force 2 calls with same `snapshot_at`, expect one to no-op (ON
    CONFLICT DO NOTHING) or raise unique-violation captured as a warning
  - [ ] Integration test `test_discovery_poll_continues_after_snapshot_write_failure`
    — simulate snapshot insert failure for 1 row of 3; assert other 2
    rows still persisted, error logged
  - [ ] mypy strict clean
  - [ ] Metric counter increments
- Files of interest: `src/pms/storage/market_data_store.py`,
  `src/pms/sensor/adapters/market_discovery.py`
- Effort estimate: S

### Checkpoint CP05: `/markets` response surfaces price fields + subscription_source

- Scope: Extend `MarketCatalogRow` + `MarketRow` pydantic models and
  `read_markets` SQL to return the 8 price fields from `markets` plus a
  `subscription_source` derived by a `LEFT JOIN market_subscriptions`.
  No filter params yet (that is CP11). No API breaking change — all new
  fields optional.
- Depends on: CP02, CP03
- Type: backend (api)
- Acceptance criteria:
  - [ ] Unit test `test_list_markets_response_includes_price_fields` —
    fake store returns a row with prices, assert pydantic serializes them
  - [ ] Integration test `test_markets_route_returns_price_fields`
  - [ ] Integration test `test_markets_route_returns_subscription_source_user`
    — INSERT into `market_subscriptions`, assert response row has
    `subscription_source='user'`
  - [ ] Integration test `test_markets_route_subscription_source_null_when_idle`
  - [ ] Existing `tests/integration/test_markets_route.py` tests still
    green (no breaking change)
  - [ ] mypy strict clean
- Files of interest: `src/pms/api/routes/markets.py`,
  `src/pms/storage/market_data_store.py`
- Effort estimate: S

### Checkpoint CP06: Subscribe/unsubscribe endpoints + MarketSelector union-merge

- Scope: New `POST/DELETE /markets/{token_id}/subscribe` endpoints per
  §4. Extend `MarketSelector.current_asset_ids` (or its equivalent entry
  point — verify at implementation) to read `market_subscriptions` WHERE
  `source='user'` and union-merge the token set with the strategy-derived
  set. `UnionMergePolicy` should already do this; the code change is to
  add the `user` input to the merge call, not to change the policy.
- Depends on: CP02, CP05
- Type: backend (api + controller)
- Acceptance criteria:
  - [ ] Unit test `test_subscribe_endpoint_upserts_user_row`
  - [ ] Unit test `test_subscribe_on_existing_selector_row_overrides_to_user`
  - [ ] Unit test `test_subscribe_idempotent_returns_200`
  - [ ] Unit test `test_unsubscribe_only_removes_user_source_not_selector`
  - [ ] Integration test `test_subscription_survives_runner_restart` —
    per Success Criterion 4; start runner, subscribe, stop, start, verify
    token in selector output on next poll
  - [ ] Integration test `test_market_selector_union_includes_user_subscriptions`
  - [ ] mypy strict clean
- Files of interest: `src/pms/api/app.py`,
  `src/pms/market_selection/selector.py`,
  `src/pms/storage/market_subscription_store.py` (new)
- Effort estimate: M
- **Learning-mode contribution point:** `MarketSelector` merge call site —
  the user decides where in the selector's existing flow to inject the
  user-subscription input. Options: (a) inside `current_asset_ids` at
  query time, (b) as an input to `UnionMergePolicy.merge`. Tradeoff is
  coupling vs. testability.

### Checkpoint CP07: `GET /markets/{condition_id}/price-history` endpoint

- Scope: New endpoint per §4. SQL: `SELECT ... FROM market_price_snapshots
  WHERE condition_id=$1 AND snapshot_at >= $2 ORDER BY snapshot_at ASC
  LIMIT $3`.
- Depends on: CP02, CP04, CP05
- Type: backend (api)
- Acceptance criteria:
  - [ ] Unit test `test_price_history_endpoint_returns_empty_array_for_no_snapshots`
    — market exists in `markets` but no snapshots yet; response is `{snapshots: []}`, NOT 404
  - [ ] Unit test `test_price_history_endpoint_404_for_unknown_market`
  - [ ] Unit test `test_price_history_endpoint_default_since_is_24h_ago`
  - [ ] Integration test `test_price_history_endpoint_returns_chronological_order`
  - [ ] mypy strict clean
- Files of interest: `src/pms/api/routes/markets.py`,
  `src/pms/storage/market_data_store.py`
- Effort estimate: S

### Checkpoint CP08: `MarketsTable` 8-column rewrite + `FreshnessDot` + `PriceBar` + `SubscribeStar`

- Scope: Per §5. Rewrite `MarketsTable.tsx` to the new column set. Drop
  the Market ID column (primary) and the Token IDs column entirely;
  remove Venue as a standalone column. Add YES / NO / Vol 24h /
  Liquidity / Spread / Resolves / ⭐ columns. New helper components:
  `FreshnessDot` (color based on `price_updated_at` age),
  `PriceBar` (percentage with 0-100% bar底色), `SubscribeStar` (status-
  only in this CP; click behavior comes in CP09).
- Depends on: CP05
- Type: frontend
- Acceptance criteria:
  - [ ] Vitest: `MarketsTable.test.tsx` — fixture row with all fields,
    assert each column renders
  - [ ] Vitest: `MarketsTable.test.tsx` — row with all prices null renders
    `—` not `undefined` or `NaN%`
  - [ ] Vitest: `FreshnessDot.test.tsx` — `<60s → green`, `60s–5min → amber`,
    `>5min → gray`
  - [ ] Vitest: `PriceBar.test.tsx` — `0.525 → 52.5% + 52.5%-wide bar`
  - [ ] Vitest: `SubscribeStar.test.tsx` — `subscription_source=user →
    filled star`, `selector → filled with different tint`, `null → outline`
  - [ ] Playwright: `/markets` page renders with new columns at viewport
    ≥1280px, zero console errors, screenshot saved to evidence/
  - [ ] `rg -n 'yes_token_id|no_token_id' dashboard/components/MarketsTable.tsx`
    returns zero matches (columns removed)
  - [ ] mypy strict clean (backend); lint clean (frontend)
- Files of interest: `dashboard/components/MarketsTable.tsx`,
  `dashboard/components/FreshnessDot.tsx` (new),
  `dashboard/components/PriceBar.tsx` (new),
  `dashboard/components/SubscribeStar.tsx` (new),
  `dashboard/lib/types.ts` (extend `MarketRow`)
- Effort estimate: M

### Checkpoint CP09: `MarketDetailDrawer` shell + URL sync

- Scope: Right-slide drawer opened by clicking a row. URL updates to
  `?detail=<condition_id>` without triggering route change (use
  `router.replace()`, not `router.push()`). Closing via Esc or backdrop
  click strips the param. Drawer renders static metrics for now (no
  chart, no subscribe — those are CP10).
- Depends on: CP08
- Type: frontend
- Acceptance criteria:
  - [ ] Vitest: `MarketDetailDrawer.test.tsx` — given `?detail=X`, drawer
    renders with market X's data; given no param, drawer closed
  - [ ] Vitest: `MarketDetailDrawer.test.tsx` — Esc key triggers
    `onClose`; onClose strips `?detail` from URL
  - [ ] Vitest: `MarketDetailDrawer.test.tsx` — clicking backdrop (not
    inner content) closes
  - [ ] Playwright: click a row, verify drawer opens with correct market,
    URL has `?detail=`, reload page → same drawer state
  - [ ] Zero console errors
  - [ ] Accessibility: `Esc` closes, focus trap within drawer, `aria-modal`
- Files of interest: `dashboard/components/MarketDetailDrawer.tsx` (new),
  `dashboard/components/MarketsPageClient.tsx` (wire)
- Effort estimate: M

### Checkpoint CP10: `PriceHistoryChart` + drawer metrics + subscribe toggle wiring

- Scope: Inside the drawer, fetch `GET /markets/{id}/price-history`,
  render a YES-only line chart (SVG-native or Recharts, prefer the
  existing dashboard charting stack — check `FactorSeriesChart.tsx` for
  precedent). Handle empty/single/multi-point states. Wire the drawer's
  Subscribe button to `POST/DELETE /markets/{token_id}/subscribe`;
  optimistic UI with rollback on 4xx/5xx (the learning-mode contribution
  point).
- Depends on: CP06, CP07, CP09
- Type: frontend
- Acceptance criteria:
  - [ ] Vitest: `PriceHistoryChart.test.tsx` — 3-point fixture renders
    path with 3 segments
  - [ ] Vitest: `PriceHistoryChart.test.tsx` — empty fixture renders
    empty state ("Price history not available yet")
  - [ ] Vitest: `PriceHistoryChart.test.tsx` — 1-point fixture renders
    dot, not line
  - [ ] Vitest: `MarketDetailDrawer.test.tsx` (extended) — subscribe click
    issues POST, ⭐ updates; failure rolls back
  - [ ] Playwright: full drawer flow — open drawer, verify chart, click
    subscribe, close, reopen, still subscribed
  - [ ] Zero console errors
- Files of interest: `dashboard/components/PriceHistoryChart.tsx` (new),
  `dashboard/components/MarketDetailDrawer.tsx` (extend),
  `dashboard/app/api/pms/markets/[id]/subscribe/route.ts` (new proxy),
  `dashboard/app/api/pms/markets/[id]/price-history/route.ts` (new proxy)
- Effort estimate: M
- **Learning-mode contribution point:** `MarketDetailDrawer.tsx`
  `handleSubscribeToggle` — the user decides optimistic-update semantics.
  Options: (a) immediately flip ⭐, rollback on error (Linear/GitHub-
  modern style), (b) show spinner on star until 200, then flip (banking-
  safe style), (c) flip + undo toast for 3s (Gmail style).

### Checkpoint CP11: Filter popover + SQL filter params + URL state

- Scope: `MarketsFilterPopover` component with 7 filter controls per
  §4 query params (search is separate, in the top bar, not in the
  popover). Filter chips render below the top bar. `useMarketsFilters`
  hook bridges URL ↔ state. Backend `read_markets` SQL extended to
  accept the 7 filter params + free-text `q`.
- Depends on: CP05
- Type: full-stack
- Acceptance criteria:
  - [ ] Unit (backend): `test_read_markets_filter_volume_min`,
    `test_read_markets_filter_liquidity_min`,
    `test_read_markets_filter_spread_max_bps`,
    `test_read_markets_filter_yes_price_band`,
    `test_read_markets_filter_resolves_within_days`,
    `test_read_markets_filter_subscribed_only`,
    `test_read_markets_filter_q_substring`,
    `test_read_markets_null_price_excluded_from_band` (per §4 NULL rule),
    `test_read_markets_combined_filters`
  - [ ] Vitest: `useMarketsFilters.test.tsx` — URL `?volume_min=100000`
    loaded → state has `volumeMin=100000`; state change → URL updates
  - [ ] Vitest: `MarketsFilterPopover.test.tsx` — each control triggers
    URL update
  - [ ] Vitest: `MarketsFilterChips.test.tsx` — active chip renders,
    × removes
  - [ ] Playwright: apply 2 filters, verify `shown` count updates,
    remove 1 chip, verify count adjusts
  - [ ] Reload preserves all filter state
  - [ ] mypy strict + pytest + vitest + lint all clean
- Files of interest:
  `dashboard/components/MarketsFilterPopover.tsx` (new),
  `dashboard/components/MarketsFilterChips.tsx` (new),
  `dashboard/lib/useMarketsFilters.ts` (new),
  `dashboard/components/MarketsPageClient.tsx` (wire),
  `src/pms/storage/market_data_store.py` (SQL extension),
  `src/pms/api/routes/markets.py` (pydantic params)
- Effort estimate: L

### Checkpoint CP12: Pagination controls + page size + e2e happy-path

- Scope: `MarketsPagination` component with «, 1, 2, 3, ..., N, »
  controls, page-number input, page-size selector (20 / 50 / 100,
  default 50). Filter change resets to page 1. Page size change
  resets to page 1. e2e happy-path spec walks the full user journey.
- Depends on: CP11
- Type: frontend
- Acceptance criteria:
  - [ ] Vitest: `MarketsPagination.test.tsx` — page boundaries (1,
    middle, last), page number input, size selector
  - [ ] Vitest: integration — changing a filter resets page to 1
  - [ ] Playwright: `markets-happy-path.spec.ts` full flow from §7
  - [ ] Vocabulary check: `rg -n 'Sensor|Controller|Actuator|Evaluator'
    dashboard/components/Markets*.tsx` returns zero (per cathedral-v1
    precedent; markets-browser inherits the anti-slop rule)
  - [ ] All canonical gates clean; baseline now ~656 passed
- Files of interest: `dashboard/components/MarketsPagination.tsx` (new),
  `dashboard/components/MarketsPageClient.tsx` (wire),
  `dashboard/e2e/markets-happy-path.spec.ts` (new)
- Effort estimate: M

## Out of Scope

- WebSocket real-time per-market price (MarketDataSensor integration)
- Category / tags filter
- Saved filter presets
- Retention / archive of `market_price_snapshots`
- Subscribe-NO-only (every subscribe subscribes both YES and NO tokens
  atomically via the market's `condition_id` lookup)
- Multi-venue UI (Kalshi stub unchanged)
- Drawer chart: bid/ask overlay, NO line, volume on secondary axis
- Custom alert rules ("notify me when market X yes_price > 0.8")

## Open Questions

None at spec time. If the Spec Evaluator surfaces unresolved decisions,
the Generator escalates to the Planner per Planning Protocol step 5.

## Appendix A — Baseline references

Cloneable, as-of `fix/sensor-gamma-query-params` tip `7607959`:

- `uv run pytest -q` → 601 passed, 138 skipped
- `uv run mypy src/ tests/ --strict` → clean, 277 source files
- `cd dashboard && npm run test` → existing Vitest green
- `/markets?limit=1` has `total=485`
- `SELECT COUNT(*) FROM markets` returns ~600 in paper-mode dev DB
- `SELECT COUNT(*) FROM market_price_snapshots` returns "relation does
  not exist" (table created in CP02)

End spec.
