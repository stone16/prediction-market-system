# Research Foundation

This directory contains the authoritative research documents produced by the
`auto-research` framework. They form the **architecture text bank** — the
single source of truth for entity definitions, tool selection rationale, and
design decisions.

All implementation work in this repo should be traced back to one of these
documents. When in doubt about a field name, a venue difference, or a tool
choice, read here first.

---

## Documents

### [`schema-spec.md`](schema-spec.md) — Platform Schema & Entity Catalog
*auto-research run: `pms-schema-design` — score 1.0 / 1.0, 5 iterations*

The definitive specification for all internal data types. Use this as the
reference when writing or reviewing any model under `src/pms/core/models.py`.

**What it contains:**
- **8 fully annotated entities** — every field has: type, required/nullable,
  source repo + original field name, why it was kept, when it is used, gotcha notes
- **Polymarket → internal mapping** — 30+ market fields, 14 order fields, 18
  trade fields; explicit dropped-field list with reasons
- **Kalshi → internal mapping** — 18 market fields, 13 order fields, 12 fill
  fields (from `GET /portfolio/fills`); Jan 2026 breaking-change fields marked
- **Directory structure** — locked to file level with one-line descriptions
- **5 adapter contracts** — Polymarket sensor/actuator, Kalshi sensor/actuator,
  paper actuator; each with input shape → transform → output shape
- **7 architecture decisions** — with rationale and OSS-research evidence
- **6 anti-patterns** — from real GitHub issues, each with issue number, what
  breaks, and how the adapter prevents it
- **`decimal_handling_invariant`** — Kalshi fixed-point precision rule that
  applies throughout sensor, actuator, and reconciliation code

**Key entities:**

| Entity | Boundary | Primary use |
|--------|----------|-------------|
| `MarketSignal` | Sensor → Controller | Normalized snapshot of one market |
| `TradeDecision` | Controller → Actuator | Full trading intent with prob + edge |
| `OrderState` | Internal (Actuator) | Order lifecycle state machine |
| `FillRecord` | Actuator → Evaluation | Fill data + calibration metrics |
| `Position` | Internal (Portfolio) | Per-market holdings |
| `Portfolio` | Internal | Capital allocation + risk caps |
| `VenueCredentials` | Config | Auth per venue |
| `EvalRecord` | Evaluation output | Aggregated performance metrics |

---

### [`tool-catalog.md`](tool-catalog.md) — Cybernetic Tool Selection Catalog
*auto-research run: `pms-cybernetic-eval` — score 0.98, 16 iterations*

The scored tool catalog organized by the four cybernetic control layers.
Use this when selecting or swapping the concrete implementation behind a
Protocol interface.

**What it contains:**
- **Sensor layer**: pmxt (primary, multi-venue), real-time-data-client
  (Polymarket WebSocket), prediction-market-analysis (historical)
- **Controller layer**: Polymarket/agents (LLM skeleton) + netcal==1.3.6
  (calibration) + pymc==5.28.4 (Bayesian) + kelly-criterion==1.2.0 (sizing)
- **Actuator layer**: py-clob-client==0.34.6 (primary), rs-clob-client (Rust),
  nautilus_trader==1.225.0 (institutional)
- **Evaluation layer**: prediction-market-backtesting, properscoring==0.1,
  calibration-belt==0.1.41
- **Minimum viable stack** — one tool per layer with install commands, latency
  budget (3450–3800 ms), bottleneck identification (Controller / LLM call)
- **Integration architecture** — hop-by-hop runtime wiring, failure modes per
  hop, circuit breaker, anomaly flag spec
- **Cross-layer adjunct signals** — GDELT, Dune Analytics, Metaculus API

**Each tool entry has:**
- `readiness_score` (1–5) — engineering maturity
- `dimension_fitness` (1–5) — fit for its cybernetic role
- `integration_grade` (A/B/C) — interface compatibility
- `verdict` — primary / backup / reference-only / reject

---

## How to use these documents

### When adding a new model field
1. Check `schema-spec.md` → `entities` section for the entity.
2. Verify the field exists there with `source_repo` and `source_field`.
3. If the field is not in the spec, it needs to be justified before adding.

### When choosing a concrete implementation
1. Check `tool-catalog.md` for the relevant layer.
2. Prefer `verdict: primary` tools over `backup` or `reference-only`.
3. Every tool has an `install` command — use the pinned version.

### When implementing a venue adapter
1. Read the `venue_mappings` section in `schema-spec.md`.
2. Follow the `adapter_contracts` — input shape, transforms, dropped fields.
3. Apply `decimal_handling_invariant` for all Kalshi `*_fp` and `*_dollars` fields.

### When hitting a production issue
1. Check `anti_patterns` in `schema-spec.md` — the six documented failure
   modes cover auth (AP-01), silent orders (AP-02), WebSocket freeze (AP-03),
   SELL size semantics (AP-04), Kalshi side/action (AP-05), and Decimal
   precision loss (AP-06).

---

## Research provenance

Both documents were produced by the `llm-autoresearch` framework
(`/Users/stometa/dev/auto-research`) using Codex as producer and Claude as
judge. Sources were grounded in real code:

- `py-clob-client/py_clob_client/clob_types.py`
- `Polymarket/agents/agents/utils/objects.py`
- `nautilus_trader/adapters/polymarket/common/enums.py`
- `nautilus_trader/adapters/polymarket/schemas/order.py`
- `docs.kalshi.com/api-reference/portfolio/get-fills`
- GitHub issues: py-clob-client #258, #278, #292, #294, #327

Last updated: 2026-04-12
