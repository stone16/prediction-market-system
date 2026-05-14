# Strategy Live-Readiness P0 — Harness Tech Spec

> **Date:** 2026-05-11
> **Author:** Stometa + Claude (brainstorm → spec)
> **Status:** Draft — pending review
> **Parent:** `docs/specs/strategy-improvement-and-live-readiness.md` (v4)
> **Scope:** All remaining P0 blockers for LIVE readiness. 7 checkpoints, ~3-4 weeks execution.

---

## Goal

Close every remaining P0 gap identified in the live-readiness spec
(v4 status audit, 2026-05-11) so the system can enter a meaningful
30-day paper soak. The paper soak cannot produce valid evidence until
these items land: calibration is dead code, two of three forecasters
are placeholders, Kelly sizing is disconnected, position marks are
unreliable, and there is no exit path for held positions.

---

## Pre-Conditions

- Branch: `feat/strategy-live-readiness-p0` (from current `main`)
- Gates must pass before and after: `uv run pytest -q` + `uv run mypy src/ tests/ --strict`
- Test baseline at start: 1387 tests collected
- Each checkpoint is an atomic commit (or small commit sequence) that leaves gates green

---

## Checkpoint 1: Wire CalibrationSpec into paper_multi_factor_v1

**Priority:** P0-10 (from F-2) | **Effort:** XS (~1 hour)
**Why first:** Quickest win. Eliminates 53% extreme-probability
decisions immediately. Unblocks meaningful paper soak data.

### What to change

**File:** `src/pms/strategies/paper_multifactor.py`

Add `calibration` parameter to `build_paper_multi_factor_strategy()`:

```python
from pms.strategies.projections import CalibrationSpec

# In the Strategy(...) constructor call, add:
calibration=CalibrationSpec(
    enabled=True,
    shrinkage_factor=0.35,
    shrinkage_bias=0.0,
    extreme_clamp_low=0.08,
    extreme_clamp_high=0.92,
    min_resolved_for_extreme=20,
),
```

**Design decision:** `min_resolved_for_extreme=20` (not the default
500). With only 10 fills in 60h, 500 would keep the clamp active for
months. At 20, the clamp graduates after ~20 resolved markets — a
reasonable sample for early-stage paper trading.

### What NOT to change

- `CalibrationSpec` dataclass — already correct at `projections.py:54`
- `pipeline.py:460-489` — already reads `strategy.calibration` correctly
- `extreme_clamp.py`, `shrinkage.py` — already implemented and tested

### Acceptance criteria

1. `build_paper_multi_factor_strategy().calibration.enabled` is `True`
2. New unit test: assert `config_json` roundtrip preserves `calibration.enabled == True`
3. After re-install, % of decisions with `prob_estimate >= 0.99` drops below 5% (verify via paper soak log query; this is a runtime acceptance, not a test gate)
4. `uv run pytest -q` and `uv run mypy src/ tests/ --strict` pass

### Dependencies

None. Can start immediately.

---

## Checkpoint 2: CLOB Book Staleness Fix in fill_store.py

**Priority:** P0-9 (from F-1) | **Effort:** S (~0.5-1 day)
**Why:** Mark-to-market is unreliable. CLOB-vs-Gamma divergence
reached $2.11 on a single $4 position. Kill-plan T1 (drawdown
stop) cannot fire reliably.

### What to change

**File:** `src/pms/storage/fill_store.py` — `read_positions()` method

1. Add staleness filter to LATERAL JOIN subquery:

```sql
WHERE book_snapshots.market_id = aggregated_positions.market_id
  AND book_snapshots.token_id = aggregated_positions.token_id
  AND book_snapshots.ts > NOW() - INTERVAL '60 seconds'  -- NEW
ORDER BY book_snapshots.ts DESC, book_snapshots.id DESC
LIMIT 1
```

When no snapshot within 60s exists, `clob_marks.best_bid` becomes
NULL and the existing `COALESCE(clob_marks.best_bid, markets.yes_price)`
fallback activates automatically.

2. Add `mark_source` and `mark_age_seconds` to the SELECT:

```sql
CASE
    WHEN clob_marks.best_bid IS NOT NULL THEN 'clob'
    ELSE 'gamma'
END AS mark_source,
CASE
    WHEN clob_marks.snapshot_ts IS NOT NULL
    THEN EXTRACT(EPOCH FROM NOW() - clob_marks.snapshot_ts)
    ELSE NULL
END AS mark_age_seconds
```

(Requires also selecting `book_snapshots.ts AS snapshot_ts` in the
LATERAL subquery.)

**File:** `src/pms/core/models.py` — `Position` dataclass

Add two optional fields:

```python
mark_source: str | None = None      # "clob" or "gamma"
mark_age_seconds: float | None = None
```

**File:** API layer — ensure `/positions` response includes the new fields.

### Acceptance criteria

1. When `book_snapshots.ts` is older than 60s, `current_price` uses `markets.yes_price` (Gamma)
2. `mark_source` field correctly reports `"clob"` or `"gamma"`
3. `mark_age_seconds` is populated when source is `"clob"`
4. Integration test: insert a stale book_snapshot (>60s old), verify fallback
5. Unit test: `Position` model accepts `mark_source` and `mark_age_seconds`
6. Gates pass

### Dependencies

None. Independent of Checkpoint 1.

---

## Checkpoint 3: Wire KellySizer into Ripple Strategy

**Priority:** P0-4 | **Effort:** S (~1-2 days)
**Why:** Position sizing is currently fixture-driven. Without Kelly,
the system either oversizes (risk) or undersizes (missed edge).

### What to change

**Key insight:** `KellySizer.size(*, prob, market_price, portfolio) -> float`
already satisfies the `RipplePositionSizer` Protocol at
`source.py:105-112`. No adapter code needed.

**File:** Wherever `LiveRippleSource` is constructed (strategy install
script or runner wiring)

```python
from pms.controller.sizers.kelly import KellySizer

LiveRippleSource(
    ...
    position_sizer=KellySizer(fraction=Decimal("0.25")),
    ...
)
```

**Configuration:** `fraction=0.25` (quarter-Kelly). The
`max_position_per_market` cap in `RiskSettings` provides the hard
ceiling — Kelly sizes within that envelope.

### What NOT to change

- `KellySizer` class — already correct and tested
- `RipplePositionSizer` Protocol — already matches
- `RiskManager.check()` — continues to enforce caps independently

### Acceptance criteria

1. `LiveRippleSource` is constructed with a `KellySizer` instance
2. Test: small edge (prob barely above market price) → small position
3. Test: large edge → position capped at `max_position_per_market`
4. Test: zero/negative edge (prob ≤ market price) → zero position
5. Test: verify `KellySizer` satisfies `RipplePositionSizer` Protocol via `isinstance` or structural check
6. Gates pass

### Dependencies

None. Independent of Checkpoints 1-2.

---

## Checkpoint 4: Fix Beta-Binomial Confidence Calculation

**Priority:** P0-3 (remaining gap) | **Effort:** XS (~0.5 day)
**Why:** `_posterior_from_candidate()` reads `confidence` from
upstream metadata rather than computing it from the posterior
evidence mass. This means the evaluator trusts the forecaster's
self-reported confidence instead of deriving confidence from
Bayesian evidence — undermining the posterior model.

### What to change

**File:** `src/pms/strategies/ripple/evaluator.py`

In `_posterior_from_candidate()` (line ~221), replace:

```python
confidence = _metadata_float(candidate.metadata, "confidence")
```

with:

```python
confidence = posterior_confidence(
    prior_strength=prior_strength,
    yes_count=yes_count,
    no_count=no_count,
    degraded=("metaculus_prior" not in candidate.metadata),
)
```

The `degraded=True` path (no Metaculus anchor) caps confidence at
0.55 — conservative when the only prior is the market price itself.

### What NOT to change

- `beta_binomial_posterior_probability()` — already correct
- `posterior_confidence()` — already correct
- `entry_edge_threshold()` — already correct
- `RippleEvidenceEvaluator.assess()` — logic remains the same

### Acceptance criteria

1. `_posterior_from_candidate()` calls `posterior_confidence()` instead of reading metadata
2. Test: with `yes_count=5, no_count=2, prior_strength=2` → confidence > 0.5 (evidence present)
3. Test: with `yes_count=0, no_count=0, prior_strength=2` → confidence == 0.5 (prior-only)
4. Test: degraded mode (no Metaculus prior) → confidence ≤ 0.55
5. Existing evaluator tests still pass
6. Gates pass

### Dependencies

None. Independent of Checkpoints 1-3.

---

## Checkpoint 5: RulesForecaster Real Implementation

**Priority:** P0-1b | **Effort:** M (~3-5 days)
**Why:** Currently returns `(signal.yes_price, 0.0, "pre-s5-neutral")`
— zero alpha. The "multi_factor" strategy is LLM-only. If DeepSeek
hallucinates, nothing offsets it.

### Design

**Architecture:** Delta-based composable rules engine.

Each rule:
1. Reads a factor value from the signal/factor snapshot
2. Computes a probability adjustment (delta) in [-0.5, +0.5]
3. Delta is scaled by the rule's `strength` (weight field in
   `FactorCompositionStep`)

Rules are configured via existing `FactorCompositionStep` with a new
role: `"rule_delta"`.

**Protocol constraint:** `IForecaster.predict(signal: MarketSignal)`
takes only `signal` — no `factors` parameter. The pipeline reads
factor data separately at `pipeline.py:153`. Two options for
accessing factor data inside the forecaster:

- **(A) Inject a factor reader** at construction: `RulesForecaster`
  holds a `PostgresFactorSnapshotReader` reference and reads factors
  in `predict()`. Matches `LiveRippleSource` pattern. Downside:
  `predict()` becomes I/O-bound (already runs in `asyncio.to_thread`
  per `pipeline.py:453`, so acceptable).
- **(B) Extract from signal fields**: use `signal.orderbook` and
  `signal.external_signal` dicts which already carry some factor
  data. Limited to factors available in the signal — not all factors
  are present.

**Recommended: Option (A)** — inject factor reader. It gives access to
all registered factors and matches the existing dependency injection
pattern in `LiveRippleSource`.

**Composition formula:**

```text
base_prob = signal.yes_price  (market price as anchor)
for each enabled rule with role="rule_delta":
    delta = rule_function(factor_value, threshold)
    base_prob += delta * weight
final_prob = clamp(base_prob, 0.01, 0.99)
confidence = max(abs(delta_i * weight_i) for all rules)  # strongest signal
```

### What to change

**File:** `src/pms/strategies/projections.py` — `FactorCompositionStep`

Add field: `enabled: bool = True` (backward-compatible default).

**File:** `src/pms/controller/forecasters/rules.py`

Replace placeholder with real implementation. The forecaster holds an
injected factor reader and a composition config:

```python
@dataclass(frozen=True)
class RulesForecaster:
    factor_reader: FactorSnapshotReader
    composition: tuple[FactorCompositionStep, ...] = ()
    min_edge: float = 0.02

    def predict(self, signal: MarketSignal) -> ForecastResult | None:
        if not self.composition:
            return None
        snapshot = self.factor_reader.snapshot(signal.market_id)
        factors = dict(snapshot.values) if snapshot else {}
        base_prob = signal.yes_price
        max_abs_contribution = 0.0
        for step in self.composition:
            if step.role != "rule_delta" or not step.enabled:
                continue
            factor_val = factors.get(step.factor_id)
            if factor_val is None:
                if step.required:
                    return None  # required factor missing → abstain
                continue
            delta = _compute_delta(step.factor_id, factor_val, step.threshold)
            contribution = delta * step.weight
            base_prob += contribution
            max_abs_contribution = max(max_abs_contribution, abs(contribution))
        final_prob = max(0.01, min(0.99, base_prob))
        confidence = min(max_abs_contribution * 5.0, 0.95)
        return (final_prob, confidence, "rules-v1")
```

**Delta functions** (per factor_id):
- `orderbook_imbalance`: delta = factor_value (already in [-1, 1] range)
- `fair_value_spread`: delta = factor_value (positive = YES underpriced)
- `metaculus_prior`: delta = metaculus_prior - signal.yes_price (divergence from market)
- `favorite_longshot_bias`: delta = factor_value (contrarian signal)
- `anchoring_lag_divergence`: delta = factor_value (LLM vs market divergence)
- `subset_pricing_violation`: delta = -factor_value (violation = negative signal)

**File:** `src/pms/strategies/paper_multifactor.py`

Add `rule_delta` steps to `factor_composition`:

```python
FactorCompositionStep(
    factor_id="metaculus_prior",
    role="rule_delta",
    param="",
    weight=0.3,
    threshold=None,
    required=False,
    allow_neutral_fallback=True,
    enabled=True,
),
FactorCompositionStep(
    factor_id="favorite_longshot_bias",
    role="rule_delta",
    param="",
    weight=0.2,
    threshold=None,
    required=False,
    allow_neutral_fallback=True,
    enabled=True,
),
```

### Acceptance criteria

1. `RulesForecaster.predict()` returns probability ≠ market price when factors have non-zero values
2. Test: single rule with known delta → verify exact output
3. Test: multiple rules compose additively
4. Test: disabled rule is skipped
5. Test: required factor missing → returns None (abstain)
6. Test: all factors zero → returns market price (no adjustment)
7. `enabled` field on `FactorCompositionStep` defaults to `True`, existing configs unaffected
8. Gates pass

### Dependencies

None technically, but should land after Checkpoint 1 (calibration)
so the rules output goes through the calibration pipeline.

---

## Checkpoint 6: StatisticalForecaster Real Implementation

**Priority:** P0-1c | **Effort:** M (~3-5 days)
**Why:** Same as Checkpoint 5 — currently zero alpha. Adds
forecaster diversity orthogonal to Rules and LLM.

### Design

**Architecture:** Weighted factor fusion model.

Unlike the RulesForecaster (delta-based), the StatisticalForecaster
produces independent probability estimates from each factor and
combines them via weighted average.

**Formula:**

```text
prob_estimates = []
weights = []
for each factor with config:
    factor_prob = factor_to_probability(factor_id, factor_value, market_price)
    prob_estimates.append(factor_prob)
    weights.append(config_weight)

if not prob_estimates:
    return None  # abstain

final_prob = sum(p * w for p, w in zip(prob_estimates, weights)) / sum(weights)
final_prob = clamp(final_prob, 0.01, 0.99)
confidence = 1.0 - std(prob_estimates)  # high agreement → high confidence
```

**Factor-to-probability mappings:**
- `metaculus_prior`: directly a probability (pass through)
- `orderbook_imbalance`: `clamp(0.5 + imbalance * 0.3, 0.01, 0.99)`
- `fair_value_spread`: `clamp(market_price + spread * 0.5, 0.01, 0.99)`
- `yes_count` / `no_count`: `yes / (yes + no)` if both > 0 else 0.5

### What to change

**File:** `src/pms/controller/forecasters/statistical.py`

Replace placeholder. Same factor reader injection pattern as
RulesForecaster (Checkpoint 5). Configurable factor weights via
constructor parameter, defaulting to equal weight.

### Acceptance criteria

1. `StatisticalForecaster.predict()` returns probability ≠ market price
2. Test: single factor → output equals that factor's probability mapping
3. Test: multiple factors with equal weight → arithmetic mean
4. Test: high agreement among factors → high confidence
5. Test: disagreement among factors → low confidence
6. Test: no factor data available → returns None
7. Gates pass

### Dependencies

Independent, but logical to land after Checkpoint 5 so both
forecasters are available for ensemble testing.

---

## Checkpoint 7: Position Exit Monitor

**Priority:** P0-8 (from F-7) | **Effort:** L (~1-2 weeks)
**Why:** No exit path exists. Colombia position at -48% with no
system response. LIVE without this is unsafe — a single position
can consume the entire bankroll.

### Design

**Layer:** Actuator (strategy-agnostic, per Invariant 5).

**New file:** `src/pms/actuator/exit_monitor.py`

```python
@dataclass(frozen=True)
class ExitRule:
    stop_loss_pct: float = 30.0
    profit_take_pct: float = 50.0
    max_holding_days: int = 7
    time_in_force: str = "IOC"

@dataclass
class PositionExitMonitor:
    rule: ExitRule = field(default_factory=ExitRule)

    def check_exits(
        self,
        positions: Sequence[Position],
        *,
        now: datetime | None = None,
    ) -> Sequence[ExitSignal]:
        ...
```

**Exit triggers (any one fires → emit ExitSignal):**

1. **Stop-loss:** `(avg_entry_price - current_price) / avg_entry_price > stop_loss_pct / 100`
   - Position mark down >30% from entry → exit
2. **Profit-take:** `(current_price - avg_entry_price) / avg_entry_price > profit_take_pct / 100`
   - Position mark up >50% from entry → exit
3. **Time-decay:** `(now - last_fill_at).days > max_holding_days`
   - Position held >7 days with no significant movement → exit

**ExitSignal output:**

```python
@dataclass(frozen=True)
class ExitSignal:
    market_id: str
    token_id: str
    side: str          # opposite of position side
    shares: float      # full position size
    reason: str        # "stop_loss" | "profit_take" | "time_decay"
    trigger_value: float  # the % move or days held that triggered
```

**Integration:** Runner loop calls `exit_monitor.check_exits(portfolio.open_positions)`
each tick. ExitSignals are converted to `TradeDecision` objects and
fed to the normal actuator execution path. The Actuator doesn't know
these are exits — they're just opposing orders.

**Configuration:** Parameters in `RiskSettings` (new fields):

```python
stop_loss_pct: float | None = None      # None = disabled
profit_take_pct: float | None = None
max_holding_days: int | None = None
exit_time_in_force: str = "IOC"
```

**Order type:** IOC at current best price. If no fill, the exit
signal fires again next tick.

### What to change

1. **New file:** `src/pms/actuator/exit_monitor.py` — `ExitRule`, `ExitSignal`, `PositionExitMonitor`
2. **File:** `src/pms/config.py` — add exit fields to `RiskSettings`
3. **File:** `config.live-soak.yaml` — add exit config
4. **File:** Runner loop — call `check_exits()` each tick, convert signals to decisions
5. **File:** `src/pms/core/models.py` — `ExitSignal` model if not in actuator

### What NOT to change

- `RiskManager.check()` — exit decisions go through the same risk checks
- `Executor` — executes exit orders like any other order
- Strategy layer — remains unaware of exit logic (Invariant 5)

### Acceptance criteria

1. Stop-loss: position at -31% from entry → ExitSignal emitted
2. Stop-loss: position at -29% → no signal
3. Profit-take: position at +51% → ExitSignal emitted
4. Profit-take: position at +49% → no signal
5. Time-decay: position held 8 days → ExitSignal emitted
6. Time-decay: position held 6 days → no signal
7. ExitSignal converts to valid `TradeDecision` with opposing side
8. Disabled trigger (None) → never fires
9. Multiple triggers on same position → only one ExitSignal (priority: stop_loss > profit_take > time_decay)
10. Exit order goes through `RiskManager.check()` — not bypassed
11. Config values in `config.live-soak.yaml`: `stop_loss_pct: 30`, `profit_take_pct: 50`, `max_holding_days: 7`
12. Gates pass

### Dependencies

Depends on Checkpoint 2 (CLOB staleness fix) — exit triggers rely
on accurate `current_price`. With stale marks, stop-loss could
misfire.

---

## Checkpoint Dependency Graph

```text
CP1 (CalibrationSpec)     ─── independent
CP2 (CLOB staleness)      ─── independent
CP3 (KellySizer)          ─── independent
CP4 (Beta-Binomial conf)  ─── independent
CP5 (RulesForecaster)     ─── after CP1 (calibration pipeline)
CP6 (StatisticalForec.)   ─── after CP5 (logical sequence)
CP7 (Exit Monitor)        ─── after CP2 (accurate marks)
```

Parallelizable cohorts:
- **Cohort A:** CP1 + CP2 + CP3 + CP4 (all independent, can run in parallel)
- **Cohort B:** CP5 → CP6 (sequential, both forecasters)
- **Cohort C:** CP7 (after CP2)

---

## Execution Timeline

```text
Week 1:
  Day 1:    CP1 (CalibrationSpec, ~1h) + CP4 (confidence fix, ~0.5d)
  Day 1-2:  CP2 (CLOB staleness, ~0.5-1d)
  Day 2-3:  CP3 (KellySizer wiring, ~1-2d)

Week 2:
  Day 4-8:  CP5 (RulesForecaster, ~3-5d)

Week 3:
  Day 9-13: CP6 (StatisticalForecaster, ~3-5d)
            CP7 start (Exit Monitor, overlaps)

Week 4:
  Day 14-18: CP7 continue + integration testing
             Paper soak restart with all fixes active
```

**Total estimated effort:** 3-4 weeks
**Critical path:** CP5 → CP6 (forecaster implementations)

---

## Additional Finding: P1-3 Status Update

During this audit, `RiskManager.check_auto_halt()` at
`risk.py:120-197` was found to already implement all 6 halt triggers
specified in P1-3:

- Credential failure (401/403) — line 131
- Drawdown circuit breaker — line 140
- 5 consecutive losses — line 152
- Slippage spike (>100bps avg/10) — line 162
- Rate limit (3x 429 in 10min) — line 173
- Order without fill (30min) — line 183

**Recommendation:** Mark P1-3 as ✅ DONE in the parent spec.

---

## Success Criteria (All Checkpoints Complete)

After all 7 checkpoints land:

1. CalibrationSpec active — extreme probability decisions < 5%
2. Position marks accurate — CLOB-vs-Gamma divergence < 50bps when book < 60s old
3. Kelly sizing active — position size proportional to edge
4. Posterior confidence computed from evidence, not forecaster self-report
5. RulesForecaster returns non-market-price predictions for markets with factor data
6. StatisticalForecaster returns factor-weighted probability estimates
7. Exit logic protects against unbounded losses on held positions
8. All gates pass: pytest + mypy strict
9. System ready for meaningful 30-day paper soak restart
