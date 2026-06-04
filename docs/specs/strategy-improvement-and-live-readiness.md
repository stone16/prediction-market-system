# PMS Strategy Improvement & Live-Readiness Spec

> **Date:** 2026-05-03
> **Canonical Authors:** @PM-Derik (process + operations), @Researcher-Ciga (algorithms + strategy), with review from @claude (strategy code audit) and @codex (repo/runtime verification)
> **Status:** CTO repo-validated (msg `b908ac8d`) — **NO-GO for live today** (infra solid, alpha layer empty). Patching per @codex's 7 spec deltas.
> **Scope:** What needs to change before the prediction market system can trade real money on Polymarket
> **This file:** `docs/specs/strategy-improvement-and-live-readiness.md` (canonical — do not create a duplicate)

---

## Goal

Replace the placeholder prediction layer with real alpha-generating models, add production-grade risk management, and establish operational gates so the system can safely transition from paper to live trading.

## What's Already Good (Don't Touch)

The engineering foundation is solid. These are **not** in scope for changes:

- Concurrent Sensor/Controller/Actuator/Evaluator feedback architecture (Invariant 1 — not a phased pipeline)
- Strategy Protocol interface and plugin boundary (`src/pms/strategies/`)
- Actuator risk gate — 8 checks (max position, total exposure, slippage, min order, etc.)
- Frozen dataclasses + `Decimal` for calculation internals
- Protocol-first module boundaries (interfaces in `src/pms/core/interfaces.py`, concrete impls in subpackages)
- Test baseline: **1387 tests collected** (2026-05-11 audit), mypy strict, import-linter 8 contracts kept, dashboard build clean *(previously: 874 passed, 161 skipped at v2)*
- Live mode fail-closed design (`live_trading_enabled=true` + first-order operator gate + missing-credentials rejection)
- Emergency stop + rollback runbook (`docs/operations/live-polymarket-runbook.md`)
- 8 architecture invariants (documented in `agent_docs/architecture-invariants.md`)

---

## P0 — Live-Readiness Blockers (Must Fix Before Any Real Money)

### P0-1: Activate LLM Forecaster (moved up from P1 — critical to paper soak validity) — split into P0-1a/b/c per F-3

**Status (2026-05-11 audit):**
- P0-1a (LLM): ✅ DONE — DeepSeek active in 60h soak, producing prob_estimates
- P0-1b (RulesForecaster): ❌ TODO — `rules.py:15` still returns `(signal.yes_price, 0.0, "pre-s5-neutral")`
- P0-1c (StatisticalForecaster): ❌ TODO — `statistical.py:22` still returns `pre-s5-neutral`

**Why P0:** Paper soaking with zero-alpha forecasters wastes 30 days. At least one real predictor must be active before paper evidence collection.

**File:** `src/pms/controller/forecasters/llm.py` (lines ~46-52)

**Current behavior:**
```python
def predict(self, signal: MarketSignal) -> LLMForecastResult | None:
    if self.config is None or not self.config.enabled:
        return None
    return LLMForecastResult(
        prob_estimate=signal.yes_price,  # ← Returns market price, zero alpha
        confidence=0.0,
        rationale="pre-s5-neutral",
        model_id="neutral",
    )
```

**Fix:**
1. Activate existing prompt template (`_prompt` function already exists) — market title + YES price + orderbook top-5 + Metaculus prior
2. Wire LLM provider config (`config.yaml` → `llm:` section already exists)
3. Parse JSON response: `{"prob_estimate": 0.XX, "confidence": 0.XX, "rationale": "..."}`
4. Add calibration post-processing (LLM probabilities are overconfident — isotonic regression or Platt scaling)
5. Add cost budget gate: `max_daily_llm_cost_usdc` (default $50/day). Cost per check ~$0.0045 (500 input + 200 output tokens at Sonnet 4 rates). Checking 100 markets every 5 min = ~$54/day — exceeds budget, so default poll cadence must be reduced or budget raised.

**Configuration** (align with current `LLMSettings`):
```yaml
llm:
  enabled: true
  provider: "anthropic"
  base_url: null              # null = default Anthropic endpoint
  model: claude-sonnet-4-20250514
  api_key: "${ANTHROPIC_API_KEY}"  # env var, not in config file
  timeout_s: 30
  cache_ttl_s: 300
  max_tokens: 200
  max_daily_llm_cost_usdc: 50.0  # NEW field to add to LLMSettings
```

**Note:** Current `LLMSettings` supports `provider`, `api_key`, `base_url`, `model`, `timeout_s`, `cache_ttl_s`, `max_tokens`. `max_daily_llm_cost_usdc` does **not** exist yet — adding it is part of P0-1 implementation scope.

**Run path:** `uv sync --extra live --extra llm` installs both the Polymarket SDK and LLM SDKs. `--extra live` alone does not install LLM dependencies.

**Acceptance criteria:**
- `LLMForecaster` returns probability ≠ market price in at least 30% of cases (differ by >5 bps)
- LLM calls log cost info (model, token count, estimated cost) to existing observability path — no new cost ledger table needed
- Daily cost report available from log aggregation or simple SQL query
- Calibration layer adjusts raw LLM output
- LLM calls traced with `trace_id` (existing observability infrastructure)

**Owner:** TBD (eng) | **Effort:** S (1-2 days)

---

### P0-2: Replace Fixture-Driven Strategy with Real Data Source — ✅ DONE

**Status (2026-05-11 audit):** `LiveRippleSource` implemented at `source.py:222` with `factor_reader` (PostgresFactorSnapshotReader), `market_reader`, and `position_sizer` injection. Fixture source retained for unit tests only.

**File:** `src/pms/strategies/ripple/source.py`

**Problem:** `RippleObservationFixture` — hardcoded test data, not live market signals. Does NOT consume the existing factor library.

**Fix:**

**Day 1 prerequisite:** The actual factor data path is:
- `FactorService.compute_once(...)` / `FactorService.get_panel(...)` at `src/pms/factors/service.py:90` and `:122` populate factor rows
- `PostgresFactorSnapshotReader.snapshot(market_id)` at `src/pms/controller/factor_snapshot.py:38-90` reads factor snapshots for controller/strategy decisions
- **Do NOT use** `factor_service.snapshot()` — that method does not exist

Then implement a real `StrategyObservationSource`:

```python
class LiveRippleSource(StrategyObservationSource):
    async def observe(self, market_id: str) -> RippleObservation:
        factor_snapshot = self.factor_reader.snapshot(market_id)  # PostgresFactorSnapshotReader
        book = await self.book_reader.latest(market_id)
        return RippleObservation(
            market_id=market_id,
            probability_estimate=factor_snapshot.get("metaculus_prior", book.yes_price),
            orderbook_imbalance=factor_snapshot.get("orderbook_imbalance", 0.0),
            fair_value_spread=factor_snapshot.get("fair_value_spread", 0.0),
            ...
        )
```

Pull from:
- `orderbook_imbalance` factor (existing, in `src/pms/factors/definitions/`)
- `fair_value_spread` factor (YES + NO mispricing)
- `metaculus_prior` factor (external probability reference)
- Polymarket order book snapshot (depth, spread, last price)

Keep `RippleObservationFixture` only for unit tests, not production path.

**Acceptance criteria:**
- `LiveRippleSource` produces observations from factor service, not fixtures
- Observation source identifier changes from `"fixture"` to `"live_factor_service"` in production
- Unit test with factor-service mock shows observation pipeline works end-to-end
- No hardcoded probability estimates in production code path
- Existing tests that depend on fixture still pass

**Owner:** TBD (eng) | **Effort:** M (1-2 weeks)

---

### P0-3: Replace Deterministic Threshold Evaluator with Probabilistic Model — 🟡 PARTIAL

**Status (2026-05-11 audit):** `RippleEvidenceEvaluator` at `evaluator.py:32` has `posterior_probability`, `expected_edge`, and `entry_edge_threshold` fields with an edge gate. However, this is NOT a full Beta-Binomial conjugate prior — it's a posterior-flavored wrapper around the existing threshold logic. The `_posterior_from_candidate()` helper needs to be verified for mathematical correctness.

**File:** `src/pms/strategies/ripple/evaluator.py` (~78 lines)

**Problem:** `if confidence < 0.6: reject; if any contradiction: reject` — deterministic thresholds, no Bayesian update, no posterior, no calibration check, no edge quantification.

**Fix:**

Use **Beta-Binomial conjugate prior** for binary event markets (the correct mathematical model):

```
prior = Beta(α, β)  # default: Beta(1, 1) = uniform
likelihood = evidence from orderbook_imbalance + metaculus_prior
posterior = Beta(α + yes_count, β + no_count)
expected_edge = E[posterior] - market_price
```

Where:
- `prior` = Metaculus prior mapped to Beta parameters, or uniform Beta(1,1) default
- `likelihood` = derived from `orderbook_imbalance` factor (Kyle/Glosten-Milgrom microstructure)
- `expected_edge` = posterior mean minus market price
- Dynamic threshold: `reject if expected_edge < base_threshold / √(time_remaining)`
- Add entry gate: `only enter if expected_edge >= 0.02 AND posterior_confidence >= min_confidence`

**Acceptance criteria:**
- Evaluator produces continuous posterior probability, not just accept/reject
- `expected_edge` is logged per decision for post-hoc analysis
- Reject threshold is configurable in risk config, not hardcoded
- Entry gate requires positive expected edge (≥ 2 bps minimum)
- Tests cover: prior-only (no book data), book-only (no prior), full posterior, near-resolution markets

**Owner:** TBD (eng + @Researcher-Ciga for algorithm) | **Effort:** S-M (1 week)

---

### P0-4: Implement Proper Position Sizing (Fractional Kelly) — 🟡 PARTIAL

**Status (2026-05-11 audit):** `KellySizer` class exists at `sizers/kelly.py:15`. `LiveRippleSource` has a `position_sizer: RipplePositionSizer` slot, but Ripple strategy does NOT reference `KellySizer`. The sizer is built but not wired into the strategy.

**Files:** Extend `src/pms/controller/sizers/kelly.py`, wire into Ripple strategy

**Problem:** `notional_usdc` is hardcoded in fixture. `KellySizer` exists but Ripple doesn't use it. No portfolio-level concentration awareness.

**Fix:**

```python
# Fractional Kelly sizing (only called when expected_edge >= min_edge_threshold)
f_star = (p - q) / b          # p = posterior_prob, q = 1-p, b = odds (price/(1-price))
position = kelly_fraction * f_star * bankroll
# where kelly_fraction = 0.25 (conservative default)
```

Portfolio caps:
- Per-trade max: 2% of bankroll
- Per-market max: 10% of bankroll
- Total open positions max: configurable (default 5 for first live run per F2)

**Acceptance criteria:**
- Ripple strategy calls `KellySizer` for every trade, not using fixture notional
- Tests verify: small edge → small position, large edge → capped position, zero/negative edge → zero position
- Entry gate prevents Kelly from sizing into zero or negative EV trades
- Config override for Kelly fraction (0.25 default)

**Owner:** TBD (eng) | **Effort:** S (3-5 days)

---

### P0-5: Tighten Risk Configuration for First Live Run — ✅ DONE

**Status (2026-05-11 audit):** `config.live-soak.yaml` committed with correct values: $5/market, $50 total, 20% drawdown, 5 max positions, 50bps slippage. Also includes calibration section, LLM config, and Polymarket connection config. Commit `9708908` later doubled exposure to $100 and lifted max_open_positions to 60 for paper soak — original tight values remain in the committed file.

**File:** `config.yaml.example` (add documentation) + new `config.live-soak.yaml` (commit to repo, no secrets)

**Problem:** Default risk caps too permissive:
- `max_position_per_market: 100.0` → $100/market (too high for first live run)
- `max_total_exposure: 1000.0` → $1000 total (too high)
- `max_drawdown_pct: null` → no circuit breaker
- `max_open_positions: null` → unlimited concurrent positions

**Fix — `config.live-soak.yaml`:**
```yaml
risk:
  max_position_per_market: 5.0     # $5 per market (first live run)
  max_total_exposure: 50.0         # $50 total
  max_drawdown_pct: 20.0           # halt at 20% drawdown
  max_open_positions: 5            # max 5 concurrent positions
  min_order_usdc: 1.0
  slippage_threshold_bps: 50.0
  max_quantity_shares: 500         # prevent low-price token blowout ($5 / $0.01 = 500 shares)
```

**Acceptance criteria:**
- `config.live-soak.yaml` committed to repo (no secrets)
- `config.yaml.example` defaults remain production-level but documented as "not for first live run"
- Runbook updated to reference `config.live-soak.yaml` as the first-live-start config

**Owner:** PM + eng | **Effort:** XS (1-2 days)

---

### P0-6: 30-Day Paper Soak with Evidence Collection — ⏳ IN PROGRESS

**Status (2026-05-11 audit):** 60h completed (2026-05-10 → 2026-05-13). 1130 decisions, 10 fills, 0 eval_records. ~58 hours of 720 hours required. Blocked on: calibration not wired (F-2), Rules/Stats placeholder (F-3), no exit logic (F-7), CLOB staleness (F-1). Paper soak should restart after these fixes land.

**Problem:** No documented paper-trading evidence. Cannot skip — it's the primary signal that the system has real edge, not noise.

**Prerequisites:** P0-1 through P0-4 must be complete first. Soaking on a zero-alpha strategy wastes 30 days.

**Run mode:** `PMS_MODE=paper`

**Daily metrics to collect:**
- Number of decisions made, accepted, rejected
- P&L trajectory (cumulative and per-day)
- Average edge per accepted trade
- Hit rate (% of trades that resolved in predicted direction)
- Average slippage vs. limit price
- Rejected order rate (from risk gates)
- Max drawdown (daily)
- Strategy version ID per decision (for attribution)
- Brier score (prediction calibration metric)

**Acceptance criteria (30-day minimum):**
- Sharpe ratio > 0 (doesn't need to be great, just positive)
- Max drawdown < 30%
- Hit rate > 45% (better than coin-flip)
- Average edge > 5 bps
- Brier score < 0.20 (better than random 0.25)
- At least 30+ accepted trades (statistical significance)
- At least 50 fills (for slippage analysis)
- No system crashes or credential failures during soak

**Owner:** Eng | **Duration:** 30 days (cannot be shortened)

---

### P0-7: Legal/Compliance Gate — ⏳ PENDING (Stometa-owned)

**Problem:** No legal review of whether prediction market trading is legal in the operator's jurisdiction, and no tax planning.

**Fix (can run in parallel with technical work):**
1. Verify Polymarket accessibility from the operator's jurisdiction (varies by state/country)
2. Determine tax treatment (personal vs. company funds)
3. Set up trade ledger export for tax reporting (PMS already stores `OrderState` and `FillRecord` in Postgres)
4. Document the bankroll plan: initial deposit, monthly budget, loss tolerance
5. Prepare Polymarket deposit path (USDC on Polygon)
6. Identify legal contact person

**Acceptance criteria:**
- Legal review documented (even if just "confirmed legal in jurisdiction X")
- Bankroll plan written: "Start with $500 on Polymarket. Max loss $200/month. Stop if >3 consecutive weeks in the red."
- Trade export query ready (`SELECT * FROM orders, fills WHERE strategy_id = ?` for tax period)
- Polymarket account funded with test amount ($500 USDC on Polygon)

**Owner:** Stometa | **Effort:** 3-5 days (may require external advice)

---

## P1 — Alpha Improvements (After First Live Soak)

### P1-1: Feature-Weighted Ensemble Forecaster

**New file:** `src/pms/controller/forecasters/feature_weighted.py`

**Problem:** Even with LLM activated, having multiple independent predictors is better than one.

**Algorithm:**
```
prob = w1 * metaculus_prior
     + w2 * fair_value_adjusted_price
     + w3 * orderbook_imbalance_prob
     + w4 * subset_pricing_consensus
     + w5 * market_price          (baseline, max 20%)
     + w6 * yes_ratio
```

Where:
- `fair_value_adjusted_price = clamp(yes_price + fair_value_spread * 0.5, 0.01, 0.99)`
- `orderbook_imbalance_prob = clamp(0.5 + orderbook_imbalance * 0.3, 0.01, 0.99)`
- `yes_ratio = yes_count / (yes_count + no_count)` if both > 0 else 0.5
- `subset_pricing_consensus = 1.0 - subset_pricing_violation` (from external signal)

**Weight constraints:** `w1+w2+w3+w4+w5+w6 = 1.0`, `w5 ≤ 0.2` (market price max 20%), `w1 ≥ 0.1` (Metaculus minimum 10% so optimizer can't zero it out)

**Optimization:** Use existing backtest framework for grid search or Bayesian optimization.

**Acceptance criteria:**
- Brier score < 0.20 (better than random 0.25)
- Predictions' Spearman correlation with market price < 0.7 (truly independent)
- `src/pms/factors/definitions/` factors are consumed (not just defined)

**Owner:** TBD (eng + @Researcher-Ciga) | **Effort:** M (1-2 weeks)

---

### P1-2: Additional Strategy Diversity

**New strategies:**
1. `book_imbalance` — pure data-driven microstructure strategy
   - Input: `orderbook_imbalance` + `fair_value_spread` + `subset_pricing_violation`
   - No external signal dependency, purely market microstructure
2. `calendar_spread` — time-structure arbitrage on near/far month contracts for same event

**Acceptance criteria:**
- Each strategy has its own `StrategyObservationSource` and evaluator
- Strategies can run concurrently with weighted capital allocation
- Per-strategy P&L view in dashboard
- Capital allocation between strategies respects total `max_total_exposure` cap (e.g., 2 strategies × $25 each = $50 total)

**Owner:** TBD (eng + @Researcher-Ciga) | **Effort:** L (2-3 weeks)

---

### P1-3: Automated Safety Halts

**File:** Extend `src/pms/actuator/risk.py` with `RiskManager.check_auto_halt()`

**Problem:** Emergency stop runbook is manual-only. Need automated circuit-breaker triggers.

**Trigger conditions (any triggers → all new orders halted, existing positions preserved):**
1. **Consecutive losses:** 5 losing trades in a row → halt, require operator restart
2. **Slippage spike:** Average slippage > 100bps over last 10 trades → halt
3. **Credential failure:** API key expired/rotated (401/403 response) → halt immediately
4. **Order without fill:** 30 min with ≥ 1 active order placed AND zero fills → halt (distinguishes from "strategy correctly filtered all markets")
5. **Rate limit:** Polymarket API returns 429 three times in 10 minutes → halt (likely high-volatility event)
6. **Drawdown:** Portfolio drawdown exceeds `max_drawdown_pct` (existing gate, but must be explicit in halt log)

**Acceptance criteria:**
- Circuit-breaker events are logged to evaluator
- Halt sets `risk.halted: true` → Runner stops sending new orders within 1 tick
- Halt is reversible via operator command (`/run/restart`)
- Tests cover each trigger condition independently
- Runner crash → alert via Slack/email (webhook config needed)

**Owner:** TBD (eng) | **Effort:** S-M (1 week)

---

### P1-4: Regime Detection (Market State Filter)

**New file:** `src/pms/controller/market_regime.py`

**Problem:** Ripple treats every market the same. Should distinguish: pre-event (high uncertainty), settlement-imminent (high info), illiquid (avoid).

**Three states:**
1. **DISCOVERY** — high volume, wide spreads, fast price changes → information being injected, allow trading, use Kelly conservative (0.25 fraction)
2. **NOISE** — low volume, narrow spreads, price stable → no new information, reduce trading or skip
3. **ILLIQUID** — spread > 100bps or depth < $50 → avoid entirely, no position

**Algorithm:** HMM or simple rule-based classifier using `orderbook_imbalance`, `fair_value_spread`, volume, time-to-resolution.

**Integration:** `ControllerPipeline.gate()` checks regime state — `ILLIQUID` and low-confidence `NOISE` markets are skipped.

**Acceptance criteria:**
- Trading volume in NOISE state reduced 40%+ compared to no-filter baseline
- Edge (Brier score) not significantly degraded (stays within 10% of baseline)
- All ILLIQUID markets are skipped (zero positions)

**Owner:** TBD (eng + @Researcher-Ciga) | **Effort:** M (1-2 weeks)

---

### P1-5: Cross-Market Correlation Cap (F6)

**New file:** `src/pms/actuator/correlation_guard.py`

**Problem:** Buying YES on 5 correlated markets = same trade 5x. No portfolio-level correlation awareness.

**Fix:**
- Compute rolling 30-day Spearman correlation between markets in same vertical
- Risk gate: total exposure to markets with `|correlation| > 0.7` capped at 50% of `max_total_exposure`
- If max total is $50, correlated cluster max is $25

**Acceptance criteria:**
- Correlation matrix computed daily from resolved market outcomes
- Risk check rejects new orders that would exceed correlated-cluster cap
- Tests verify: uncorrelated markets pass, highly correlated markets are capped

**Owner:** TBD (eng) | **Effort:** S (3-4 days)

---

### P1-6: Adverse Selection / Quote Fade Detection (F7)

**File:** Extend `src/pms/actuator/risk.py` (reuses existing `orderbook_imbalance` factor)

**Problem:** Polymarket has informed flow (insiders, poll insiders). The system needs to detect when it's being traded against.

**Fix:**
- If our limit order is filled and the book moves against us > 50bps within 60 seconds, mark the trade as "adverse-selected"
- Track `adverse_selection_rate` per (market, strategy)
- If rate > 30% over last 20 trades → disable that strategy for that market

**Acceptance criteria:**
- Adverse selection events are logged to evaluator
- Strategy disable is reversible via operator command
- Tests simulate informed-flow scenario and verify detection

**Owner:** TBD (eng + @Researcher-Ciga) | **Effort:** S (2-3 days)

---

### P1-7: Paper Trading Daily Report

**New script:** `scripts/paper-report.py`

**Daily output:** `scripts/paper_report.py` writes archival Markdown to
`docs/paper-reports/YYYY-MM-DD.md`; notifier jobs may post a compact
Discord/Slack summary derived from the same metrics.

```markdown
# Paper Daily Report - 2026-05-03

## Summary

| Metric | Value | Gate |
|---|---:|---|
| Strategy | ripple_v2 | - |
| Day of soak | 0 | 30 required |
| Decisions made | 0 | - |
| Decisions accepted | 0 | - |
| Decisions rejected | 0 | - |
| Fills | 0 | - |
| Average slippage (bps) | N/A | <= 50 |
| Today's P&L | +$0.00 | >= -daily limit |
| Cumulative P&L | +$0.00 | > 0 by soak end |
| Max drawdown | N/A | <= 20.0% |
| Open positions | 0 | <= 5 |
| Total exposure | $0.00 | <= $50.00 |
| Brier score (7d rolling) | N/A | < 0.20 |
| Hit rate (all trades) | N/A | > 45% |
| Average edge (bps) | N/A | > 5 |
| Sharpe ratio (cumulative) | N/A | > 0 |

## Risk Events

| Time | Trigger | Status |
|---|---|---|
| (none today) | - | - |

## Trade Notes

No trades today.
```

**Integration:** GitHub Actions daily cron or local cron job.

**Acceptance criteria:**
- Report auto-generates daily during paper mode
- Output committed to `docs/paper-reports/` or posted to Discord/Slack
- Contains all Gate 3 acceptance metrics for tracking

**Owner:** PM + eng | **Effort:** S (1 day)

---

## Gate Structure (Go/No-Go Checkpoints)

### Gate 1: Prediction Independence (Week 2 end)

- [ ] LLM forecaster returns non-market-price probability ≥ 30% of cases (differs by >5 bps)
- [ ] Ripple uses `PostgresFactorSnapshotReader` (not fixture) — source field = `"live_factor_service"`
- [ ] FactorService API verified (`compute_once()` / `get_panel()` / `PostgresFactorSnapshotReader.snapshot()`)
- [ ] **Runtime smoke test**: `POST /run/start` in PAPER/backtest mode produces **> 0 decisions** (not 0 decisions / 100 diagnostics)
- [ ] Run path: `uv sync --extra live --extra llm` installs all dependencies cleanly

### Gate 2: Risk Readiness (Week 4 end)

- [ ] Kelly sizing active in Ripple strategy with edge gate (≥ 2 bps minimum)
- [ ] Beta-Binomial posterior evaluator replaces threshold checker
- [ ] `config.live-soak.yaml` committed with $5/market, $50 total, 20% drawdown guard
- [ ] Automated safety halts tested (each trigger independently)
- [ ] Paper daily report auto-generates
- [ ] Compliance checklist all green

### Gate 3: Paper Evidence (after 30+ calendar days)

- [ ] ≥ 30 days of paper trading data
- [ ] ≥ 50 fills
- [ ] Sharpe ratio > 0 (or Brier score < 0.20)
- [ ] Max drawdown < 30%
- [ ] Hit rate > 45%
- [ ] Average edge > 5 bps
- [ ] No system crashes during soak

### Gate 4: Live Ready (Week 12 end)

- [ ] Gates 1-3 all passed
- [ ] Live soak config committed and tested
- [ ] Emergency stop runbook tested end-to-end
- [ ] Polymarket account funded with $500 USDC on Polygon
- [ ] Human monitoring schedule confirmed

---

## Implementation Roadmap

```
Week 1-2: Core Prediction (P0-1 through P0-2)
├── P0-1: Activate LLM Forecaster (1-2 days)
├── P0-2: Replace Ripple fixture source (Day 1: verify FactorService API)
├── P0-7: Compliance checklist (parallel, Stometa-owned)
└── Gate 1 review → proceed or iterate

Week 3-4: Strategy Substance + Risk (P0-3 through P0-5 start)
├── P0-3: Beta-Binomial posterior evaluator
├── P0-4: Fractional Kelly sizing with edge gate
├── P0-5: Tighten risk config + commit live-soak config
├── P1-7: Paper daily report script
└── Gate 2 review → start paper soak

Week 5-9: Paper Soak (30 days minimum, cannot skip)
├── Daily: paper mode + auto report
├── Weekly: Brier/P&L/Sharpe assessment
├── Strategy iteration: weight tuning, threshold optimization
└── Gate 3 review after day 30 → proceed to live or extend paper

Week 9-10: Advanced Alpha (P1 items, parallelizable after Gate 3 evidence is sufficient)
├── P1-1: Feature-weighted ensemble (optimized weights)
├── P1-2: Additional strategy diversity (2+ strategies)
├── P1-3: Automated safety halts
├── P1-4: Regime detection
├── P1-5: Cross-market correlation cap
└── P1-6: Adverse selection / quote fade detection

Week 11-12: Post-Paper Parameter Tuning + Gated LIVE
├── Adjust parameters based on paper evidence
├── $5/market, $50 total, 2-week live soak
├── Daily human review + auto-halts armed
└── Gate 4 review → scale or iterate
```

---

## Role Assignments

| Lane | Suggested Owner | Items |
|------|----------------|-------|
| Strategy code (P0-2, P0-3, P0-4) | TBD (eng) | Replace fixture source, Bayesian evaluator, Kelly integration |
| LLM forecaster (P0-1) | TBD (eng) | Activate existing skeleton, add calibration, cost budget |
| Algorithm design (P0-3, P1-1, P1-4, P1-6) | @Researcher-Ciga | Beta-Binomial model, ensemble weights, regime detection, quote fade |
| Risk/config (P0-5, P1-3, P1-5) | TBD (eng) + @PM-Derik | Config tightening, auto-halts, correlation guard |
| Paper soak ops (P0-6 daily reports) | Eng (automation) + @PM-Derik (review) | Daily report script, weekly assessment |
| Compliance (P0-7) | @stometaverse-2 | Legal review, bankroll plan, account setup |
| Repo verification + integration | @codex | Local bring-up, end-to-end tests, CTO review at each gate |
| Gate Go/No-Go decisions | @stometaverse-2 | Final approval at each gate |

---

## Empirical Findings from 60h Paper Soak (2026-05-10 → 2026-05-13)

> Soak ran on `feat/realtime-quality-signals` (commit `25617e5`) with `paper_multi_factor_v1` strategy. Config: `config.live-soak.yaml` ($50 total exposure, $5/market, 5 max positions, 50bps slippage). LLM: DeepSeek via Anthropic-compatible endpoint, 90s timeout. Result over 60h: **1130 decisions, 10 fills (0.9% fill rate), 0 eval_records, 5 open positions, locked $20 / cap $50, pnl swung -$1.74 to +$2.46 (CLOB-based mark)**.

The findings below either confirm existing P0 items with empirical evidence, or surface new gaps that must enter the P0 list. Items are labeled `F-N` (Finding-N) so they can be referenced from spec deltas and PRs.

---

### F-1: CLOB book staleness corrupts mark-to-market — ❌ TODO (mapped to P0-9)

**Severity:** P0 (LIVE-blocking — kill-plan T1 drawdown gate cannot fire reliably)
**Status (2026-05-11 audit):** ❌ Unresolved. `fill_store.py` has no staleness check. No `mark_source` field in `/positions` response.

**Evidence (Colombia outright YES position `0xab6fb278`):**

| Source | Value | Notes |
|---|---|---|
| avg_entry | $0.1048 | from `fills` table |
| CLOB latest `book_levels` best_bid | $0.12 | written by `MarketDataSensor` WebSocket pushes |
| `markets.best_bid` | $0.05 | refreshed by Gamma snapshot REST poll |
| `markets.yes_price` (Gamma) | $0.055 | Gamma last-trade |
| `/positions` upnl (CLOB-based) | +$0.20 | uses `fill_store.py` LATERAL JOIN to `book_levels` |
| Gamma-reconciled upnl | **-$1.91** | 38.18 shares × ($0.055 - $0.1048) |

`/positions` and Gamma disagree by **$2.11 on a single $4 position** because the CLOB book in `book_snapshots` is materially older than the Gamma poll. Across all 5 positions the discrepancy aggregates to roughly **$2.5-$3.0 of pnl** at the time of writing — the difference between "we're up 10%" and "we're down 8%".

**Hypothesized root cause** (needs verification before PR):
- `MarketDataSensor` writes a new `book_snapshots` row on every WebSocket book update, but if the market goes quiet (few quotes), the latest snapshot can be hours old.
- `fill_store.py:read_positions` LATERAL JOIN picks the most recent `book_snapshots` for the token without checking staleness.
- Gamma snapshot poll continues independently and is more current for low-liquidity markets.

**Fix proposed (P0):**
1. Add a `staleness_seconds` check to `fill_store.py` LATERAL JOIN — if `latest_snapshot.ts > N seconds old`, fall back to `markets.best_bid` (Gamma).
2. Surface staleness in `/positions` response so the dashboard can render a "stale mark" badge.
3. Add an alert: if any held position's CLOB-vs-Gamma divergence exceeds `staleness_alert_bps` (default 200bps), emit Feedback.

**Acceptance:**
- `/positions` upnl never disagrees with Gamma `yes_price`-derived pnl by more than 50bps when the underlying book_snapshot is < 60s old.
- For positions with stale book (> 60s), `/positions` falls back to Gamma and surfaces a `mark_source` field.

**Owner:** TBD (eng) | **Effort:** XS-S (0.5-1 day)

---

### F-2: Calibration pipeline (PR #71) is NOT wired into `paper_multi_factor_v1` — ❌ TODO (mapped to P0-10)

**Severity:** P0 (LIVE-blocking — the entire extreme_prob_clamp + logit_shrinkage layer is dead code for this strategy)
**Status (2026-05-11 audit):** ❌ Confirmed unresolved. `paper_multifactor.py:build_paper_multi_factor_strategy()` does not pass `calibration=CalibrationSpec(enabled=True, ...)`. Default `CalibrationSpec(enabled=False)` at `projections.py:55`. Fix: add one field to strategy builder + re-install strategy.

**Evidence:**
- `src/pms/controller/calibrators/extreme_clamp.py` exists; default `CalibrationSpec` has `enabled: bool = False`.
- `src/pms/controller/pipeline.py:465` reads `strategy.calibration`; falls back to default-disabled `CalibrationSpec()` when None.
- `strategy_versions.config_json -> 'calibration'` for `paper_multi_factor_v1` is **NULL**.
- `scripts/install_paper_multi_factor_strategy.py` and `src/pms/strategies/paper_multifactor.py:build_paper_multi_factor_strategy()` do not construct a `CalibrationSpec(enabled=True, ...)`.

**Empirical consequence:**
- **592 of 1116 decisions (53%) have `prob_estimate ≥ 0.99`** — `decision_payloads.payload -> 'prob_estimate'` distribution from DB.
- LLM "I'm 100% certain" outputs flow through to the actuator unfiltered.
- Counter-intuitive: PR #71 was meant to make paper soak quality go up, but observed pre-#71 and post-#71 strategy behavior is indistinguishable.

**Fix (P0):**
1. `src/pms/strategies/paper_multifactor.py:build_paper_multi_factor_strategy()` adds a non-default `CalibrationSpec(enabled=True, min_resolved_for_extreme=20, extreme_clamp_low=0.08, extreme_clamp_high=0.92, ...)`.
2. Re-install strategy via `scripts/install_paper_multi_factor_strategy.py` (creates new `strategy_version_id` — immutable per Invariant 3).
3. Restart runner; existing 5 positions are tagged with the old version_id (preserved per architecture invariant), new decisions use the new version with calibration active.

**Acceptance:**
- After re-install, % of decisions with `prob_estimate ≥ 0.99` should drop below 5%.
- A new unit test in `tests/unit/test_install_paper_multi_factor_strategy.py` asserts `config_json.calibration.enabled == True`.

**Owner:** TBD (eng) | **Effort:** XS (1-2 hours)

---

### F-3: Only the LLM forecaster is real — Rules and Statistical forecasters return placeholders — ❌ TODO

**Severity:** P0 (extends P0-1 scope)
**Status (2026-05-11 audit):** ❌ Confirmed. `rules.py:15` returns `(signal.yes_price, 0.0, "pre-s5-neutral")`. `statistical.py:22` returns same. Zero forecaster diversity — system is LLM-only.

**Evidence:** Every `paper_multi_factor_v1` opportunity row's `rationale` field equals:

```text
RulesForecaster:pre-s5-neutral | StatisticalForecaster:pre-s5-neutral | DeepSeek
```

P0-1 was about activating the LLM forecaster — done; DeepSeek is producing prob_estimates. But `src/pms/controller/forecasters/rules.py` and `src/pms/controller/forecasters/statistical.py` both still return `pre-s5-neutral` (= market price, zero alpha).

**Implication:**
- The "multi_factor" in `paper_multi_factor_v1` is currently a misnomer — it is LLM-only.
- The system has **zero forecaster diversity**. If DeepSeek hallucinates (which the 53% extreme-prob rate suggests it does), nothing in the ensemble offsets it.
- P1-1 (feature-weighted ensemble) cannot meaningfully start until Rules + Stats have real outputs.

**Spec delta:** Split P0-1 into three sub-items:
- P0-1a: ✅ LLM forecaster active (was P0-1)
- P0-1b: ⬜ RulesForecaster real implementation (new)
- P0-1c: ⬜ StatisticalForecaster real implementation (new)

**Owner:** TBD (eng + @Researcher-Ciga for algorithm) | **Effort:** S-M (3-5 days each)

---

### F-4: `default` strategy producing 676 dead decisions, 0 fills

**Severity:** P2 (housekeeping; not LIVE-blocking but wastes resources)
**Status:** newly observed

**Evidence:**
- `strategies` table contains both `default` (auto-created at runner startup with hash-based version_id) and `paper_multi_factor_v1`.
- `default` produced 676 decisions across 60h, all rejected (per `feedback` table breakdown: 114 `max_open_positions`, 62 `max_position_per_market`, 2 `insufficient_liquidity`).
- Zero fills for `default` — the strategy generates decisions but is rejected by its own RiskManager pass.

**Cost:** ~676 LLM calls × $0.0045 ≈ **$3.04 wasted in 60h** (≈ $36/month if left running).

**Fix:** archive `default` strategy when `paper_multi_factor_v1` is the operative strategy. Either via the install script (set `archived=true` on `strategies.default`) or via a runner config flag that suppresses auto-creation of `default`.

**Owner:** TBD (eng) | **Effort:** XS (30 min) | Can land alongside F-2.

---

### F-5: Decision throughput degrading over the soak

**Severity:** P2 (monitor, do not act yet — could be normal market selector convergence)
**Status:** newly observed; root cause unclear

**Evidence (paper_multi_factor_v1 decisions per 5min window):**

| Window | Rate |
|---|---|
| 5/10 13:35-14:43 (first 70min, all 10 fills) | **10-15 / 5min** |
| 5/11 hour 10-15 | 5-9 / 5min |
| 5/12 evening | 2-3 / 5min |
| 5/13 21:48 → 00:07Z (latest at status check) | **~0.4 / 5min** (2 decisions in 2h 18min) |

**Hypotheses (need investigation):**
- (a) LLM rate limit / cost budget hit at DeepSeek.
- (b) Controller queue backpressure as more book_snapshots accumulate.
- (c) Calibration filters (even though F-2 shows they're not wired in) — irrelevant given F-2.
- (d) WebSocket subscription churn or stale subscriptions reducing usable signal.
- (e) Market discovery sensor narrowing universe as low-volume markets fall off (normal).
- (f) `RUNNER_STATE_LIMIT = 1000` (in `src/pms/runner.py:128`) cap creating display artifacts but unclear if it also affects controller scheduling.

**Action (P2 monitor):**
1. Add `decisions_per_minute` gauge to `/metrics`.
2. Add alert if rate drops below `min_decisions_per_5min` (default 1).
3. Spend a focused 1-hour debug session next week to disambiguate the hypotheses above.

**Owner:** TBD (eng) | **Effort:** S (instrumentation 0.5 day, root-cause 0.5-1 day)

---

### F-6: Zero `eval_records` after 60h — by design, but kill-plan T2 has structural latency

**Severity:** P0-known-issue (acknowledged constraint, not a bug; affects kill-plan timing)
**Status:** validates existing P0-6 timeline

**Evidence:**
- `src/pms/evaluation/spool.py:67`: `if fill.resolved_outcome is None: continue` — `Scorer` requires final resolution before emitting an `EvalRecord`.
- All 10 fills' underlying markets resolve between 2026-05-31 (Colombia) and 2026-07-20 (World Cup).
- Until 2026-05-31, Brier score is mathematically unavailable for this paper soak.

**Implication for kill-plan:**
- Kill-plan threshold T2 = "14 consecutive trading days without Brier improvement vs. baseline" (`docs/operations/live-exit-criteria.md`).
- The T2 evaluation can only **begin** ~5 calendar days after the first resolution (need at least 5 daily windows of resolved data).
- For a paper soak starting 2026-05-10, T2 cannot fire before ~2026-06-05, and meaningful T2 enforcement is closer to 2026-06-15.
- This means the first 21+ days of paper soak have no T2 signal — Brier kill is structurally lagged.

**Mitigation options** (one of these should likely become P0-known-issue-mitigation):

**Option A: Mid-quote pseudo-Brier scorer (recommended, ~1-2 day)**
- Add a `MidQuoteScorer` alongside `Scorer` that scores each decision at T+1h using CLOB mid as pseudo-outcome.
- Emits a `quasi_eval_records` row (separate table, not `eval_records`).
- Daily report computes "mid-quote Brier" and a "resolution Brier" — operator can see early signal without breaking the strict-Brier invariant.

**Option B: Constrain market_selection to 30-day-soak-compatible markets**
- Set `resolution_time_max_horizon_days: 31` in `paper_multi_factor_v1` strategy config.
- MarketSelector filters out long-horizon markets (World Cup, presidential races).
- Trades only markets expected to resolve inside the 30-day paper-soak window,
  with a one-day UTC end-of-day buffer for markets resolving just after the
  report cutoff.
- Downside: smaller universe, possibly less diverse alpha.

**Runtime update (2026-05-31):** a 7-day horizon produced no live
opportunities in the current Polymarket discovery pool; the 31-day horizon
retains near-term NHL markets and adds NBA Finals / MegaETH risk groups while
excluding 49-day World Cup outrights.

**Recommendation:** Do Option B first (zero infrastructure cost), evaluate edge after 14 days, then decide whether to invest in Option A.

**Owner:** TBD (eng + @Researcher-Ciga) | **Effort:** XS (Option B) or S (Option A)

---

### F-7: No exit path — confirms P0-3/P0-4 gap, plus drawdown stop is mark-dependent — ❌ TODO (mapped to P0-8)

**Severity:** P0 (confirms existing P0-3/P0-4 + new dependency on F-1)
**Status (2026-05-11 audit):** ❌ Confirmed. No stop-loss, no profit-take, no time-decay exit in actuator. Colombia position at -48% with no system response. `grep -rn "exit\|stop.loss\|profit.take" src/pms/actuator/` returns empty.

**Evidence (across 60h, none of the 5 positions saw any exit logic invoked):**

| Position | Entry | Current mark (Gamma) | % move | System response |
|---|---|---|---|---|
| Colombia Outright YES | $0.105 | $0.055 | **-48%** | none (no stop-loss) |
| Abelardo Colombia YES | $0.108 | $0.116 | +7% | none (no profit-take) |
| Portugal World Cup YES | $0.075 | $0.0755 | 0% | none |
| James Bond YES | $0.72 | $0.715 | -1% | none |
| Spain World Cup NO | $0.843 | $0.840 | 0% | none |

**Implication:**
- For LIVE, kill-plan T1 (35% portfolio drawdown stop) is the **only** exit mechanism.
- There's no per-position risk management — a 50% adverse move on one position cannot be cut.
- Combined with F-1 (mark unreliability), the drawdown stop itself may misfire — either firing late (CLOB book stale, underestimating loss) or never firing (mark is wrong, drawdown looks fine).

**Connection to existing P0 items:**
- P0-3 (Beta-Binomial posterior evaluator) addresses entry quality but not exit.
- P0-4 (Kelly sizing with edge gate) addresses entry size but not exit.
- **There is no existing P0 item for exit logic.** This is a gap.

**Spec delta:** add P0-8 (exit logic):
- Per-position stop-loss: trigger if mark drops > 30% from entry (configurable).
- Per-position profit-take: trigger if mark rises > 50% from entry (configurable).
- Time-decay stop: cut position if no movement > 5% within `max_holding_days` (default 7).
- Action on trigger: place opposing IOC at near-best to flatten; if it doesn't fill within 30s, cancel and re-evaluate.

**Owner:** TBD (eng + @Researcher-Ciga) | **Effort:** M (1-2 weeks)

---

## Spec deltas from 60h soak

The findings above translate into these concrete spec changes:

1. **New P0-8: Exit logic** (per F-7) — was missing entirely; LIVE without this is unsafe.
2. **Split P0-1 → P0-1a / P0-1b / P0-1c** (per F-3) — LLM is done; Rules + Stats are not.
3. **New P0-9: CLOB book staleness fix in `fill_store.py`** (per F-1) — quick win, ~1 day.
4. **New P0-10: Wire `CalibrationSpec` into `paper_multi_factor_v1` config** (per F-2) — quickest win in the entire spec, ~1 hour.
5. **Add Option B from F-6 mitigation to P0-6** — constrain paper soak universe to ≤ 31-day resolution horizon, matching `resolution_time_max_horizon_days: 31`, so Gate 3 evidence is achievable in 30 days rather than 50+ with the UTC cutoff buffer.

## Updated LIVE Readiness Verdict (after F-1 through F-7)

Original spec estimated 8-12 weeks dominated by 30-day paper soak. With findings:

| Critical-path item | Effort | Blocker tier |
|---|---|---|
| F-2 / new P0-10: wire CalibrationSpec | 1 hour | hard |
| F-1 / new P0-9: CLOB staleness fix | 0.5-1 day | hard |
| F-3 / P0-1b: RulesForecaster real impl | 3-5 days | hard |
| F-3 / P0-1c: StatisticalForecaster real impl | 3-5 days | hard |
| F-7 / new P0-8: exit logic | 1-2 weeks | hard |
| Existing P0-3: Beta-Binomial evaluator | 1 week | hard |
| Existing P0-4: Kelly sizing | 3-5 days | hard |
| F-6 mitigation in P0-6: short-horizon universe | 30 min | enabler |
| Existing P0-6: 30-day paper soak | 30 days | structural |
| Existing P0-7: legal/compliance | 3-5 days | parallel |

**Revised minimum path to first $5 LIVE order**: ~5-7 weeks from 2026-05-13 (the 30-day soak still dominates; technical fixes parallelize). Soft blockers (F-4, F-5) can land during the soak.

---

## Overall Acceptance Criteria

The system is "ready for live trading" when ALL P0 items are complete:

1. ✅ LLM forecaster active — returns non-market-price predictions ≥ 30% of cases (P0-1)
2. ✅ Strategy reads from `PostgresFactorSnapshotReader`, not fixtures (P0-2)
3. ✅ Beta-Binomial posterior evaluator replaces threshold checker (P0-3)
4. ✅ Kelly sizing active with edge gate (P0-4)
5. ✅ Risk config tightened + `config.live-soak.yaml` committed (P0-5)
6. ✅ 30-day paper soak with positive Sharpe, <30% drawdown, >45% hit rate, 30+ trades, 50+ fills (P0-6)
7. ✅ Legal/compliance review + bankroll plan documented (P0-7)
8. ✅ **Runtime smoke test**: `POST /run/start` in PAPER mode produces > 0 decisions (not 0/100) with explainable evidence

**Estimated timeline:** 8-12 weeks total (dominated by 30-day paper soak).

**CTO validation baseline** (msg `b908ac8d`): Current system runs but produces 0 decisions in backtest mode. All tests pass (874 passed, 161 skipped, mypy 331 files, 8 import-linter contracts, dashboard build clean). Live mode fail-closed confirmed. The transition from 0 → nonzero decisions on local smoke is the North Star for Phase 1.

---

## Open Questions for @stometaverse-2

1. **Who owns P0-1 through P0-4 implementation?** This is the critical path. @claude reviewed the code but was assigned spec-only scope for ContentGenerator. We need an engineering owner — is @codex available, or should we assign to @Eng-Darwin?
2. **Bankroll amount for paper soak and first live run?** $500 Polymarket deposit? Different amount? This affects Kelly sizing parameters.
3. **Is legal review a hard gate?** Some prediction markets operate in a gray area. Formal legal opinion or just jurisdiction check?
4. **Review cadence during paper soak?** Weekly check-ins, or only at Gate 3 (end of 30 days)?
5. ~~**Should we pull P1-1 (ensemble forecaster) into P0?**~~ Resolved — no, keep ensemble in P1. LLM activation alone is sufficient for Gate 1 (per @Researcher-Ciga's correction).

---

## Changelog

- **v4 (2026-05-11):** Status audit — code-verified each P0 item against current codebase. Marked: P0-1a ✅ (LLM active), P0-2 ✅ (LiveRippleSource implemented), P0-5 ✅ (config.live-soak.yaml committed). Partial: P0-3 🟡 (posterior framework exists, not full Beta-Binomial), P0-4 🟡 (KellySizer exists, not wired into Ripple). Confirmed unresolved: P0-1b/c ❌ (Rules/Stats placeholder), P0-8 ❌ (exit logic absent), P0-9 ❌ (CLOB staleness), P0-10 ❌ (CalibrationSpec not wired). Updated test baseline: 874→1387 tests collected. Note: commit `9708908` doubled paper-soak exposure caps ($50→$100, positions 5→60) — diverges from spec's conservative first-live-run values.
- **v3 (2026-05-14):** Added "Empirical Findings from 60h Paper Soak (2026-05-10 → 2026-05-13)" section with findings F-1 through F-7. New P0 spec deltas: **P0-8 (exit logic)**, **P0-9 (CLOB book staleness fix in `fill_store.py`)**, **P0-10 (wire CalibrationSpec into `paper_multi_factor_v1` config)**. Split P0-1 into P0-1a (LLM, ✅ done), P0-1b (RulesForecaster, ⬜), P0-1c (StatisticalForecaster, ⬜). Revised LIVE timeline: 5-7 weeks from 2026-05-13. Key empirical signals: 1130 decisions / 10 fills / 0 eval_records over 60h, 53% of decisions have `prob_estimate ≥ 0.99` (calibration not wired in), CLOB-vs-Gamma pnl divergence up to $2.11 on single position (book staleness). Findings include hypothesized root causes that need code-level verification before PR.
- **v2 (2026-05-03):** Applied @codex's 7 spec deltas from CTO repo validation: (1) `factor_service.snapshot()` → `PostgresFactorSnapshotReader.snapshot()` + `FactorService.compute_once()` (correct API path), (2) LLM config aligned with current `LLMSettings` fields, (3) LLM cost ledger requirement removed (no such surface exists — replaced with log-based cost tracking), (4) Gate 1 inconsistency fixed (ensemble moved out of Gate 1; LLM activation alone sufficient), (5) test baseline updated to current observed: 874 passed, 161 skipped, mypy 331 files, 8 import-linter contracts, (6) runtime smoke test added (0 → nonzero decisions on `POST /run/start`), (7) `uv sync --extra live --extra llm` added to LLM run path. Q5 (ensemble in P0) resolved: no, keep in P1. CTO validation: system runs but 0 decisions in backtest — the 0→nonzero transition is Phase 1 North Star.
- **v1 (2026-05-03):** Initial unified spec — merged from @PM-Derik process doc, @Researcher-Ciga algorithm spec, @claude's 7 refinements, @codex's P0/P1 separation requirement. LLM forecaster promoted from P1 to P0 (critical to paper soak validity). Beta-Binomial conjugate prior specified for P0-3. Kelly edge gate added (P0-4). F6 (correlation cap) and F7 (quote fade) added as P1-5/P1-6. Auto-halt trigger #4 refined to avoid false positives. LLM cost budget added to P0-1. Gate structure formalized (4 gates with concrete pass criteria).
