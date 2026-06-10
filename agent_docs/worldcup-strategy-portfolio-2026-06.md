# World Cup 2026 Strategy Portfolio (2026-06)

**Status:** active R&D portfolio, window 2026-06-11 → 2026-07-19.
**Owner:** Stometa.
**Scope:** five ranked strategy candidates for the World Cup 2026
trading window, with per-candidate thesis, factor composition, risk
caps, failure modes, and pre-registered kill criteria. All
`file:line` references were verified in the 2026-06-10 R&D session
and spot-checked again before this doc landed.

**Related documents:**
- `agent_docs/strategy-iteration-sop.md` — the SOP every candidate
  here runs through (channel decision → factor check → install →
  backtest → soak → LIVE → retro).
- `agent_docs/strategy-authoring-guide.md` — Channel A/B recipes.

---

## Hard deadline — soak math

The paper GO gate requires `min_soak_days=30`
(`scripts/paper_report.py:176`). The window ends **2026-07-19**, so
**every candidate must be installed and soaking by 2026-06-19** to
produce an in-window GO. The window is 38 days total — there is no
slack for sequential rollout. Install all surviving candidates in
parallel on day 1; the runner builds one ControllerPipeline per
active strategy and hot-reloads new versions without restart
(`strategy-authoring-guide.md:385-397`).

**Action item for 2026-06-11: start the paper runner and subscribe
WC token ids immediately — every un-recorded match day is
unrecoverable backtest coverage** (backtests replay only your own
recorded outer ring, Gap G2).

---

## Known controller bugs affecting this portfolio

Two confirmed controller bugs change how every candidate below must
be configured. Both were re-verified against HEAD on 2026-06-10.

### Bug A — calibration feedback never wired; the extreme clamp never unlocks

With `CalibrationSpec(enabled=True)`, the pipeline runs
`ExtremeProbClamp`, which **rejects outright** (forecast → `None`,
decision dropped) any probability outside
`[extreme_clamp_low, extreme_clamp_high]` — default `[0.08, 0.92]`
(`src/pms/strategies/projections.py:60-61`) — until
`resolved_sample_count >= min_resolved_for_extreme` (default 500,
`projections.py:62`; rejection path
`src/pms/controller/pipeline.py:1176-1193`,
`src/pms/controller/calibrators/extreme_clamp.py:16-22`).

The unlock count comes from `calibrator.sample_count(model_id)`
(`pipeline.py:1202-1206`), which only grows via
`NetcalCalibrator.add_samples`
(`src/pms/controller/calibrators/netcal.py:13`) — and `add_samples`
**has no callers anywhere in `src/`**. The eval → calibrator
feedback edge was never wired, so `resolved_sample_count` is
permanently 0 and the clamp never unlocks.

**Consequence:** Rank 1 and Rank 2 are tail strategies — their
entire edge lives below 0.10 and above 0.90, exactly the band the
clamp rejects. They **MUST** either set
`CalibrationSpec(enabled=True, extreme_clamp_low=0.001,
extreme_clamp_high=0.999)` or run `enabled=False` until the
feedback edge is fixed. (`enabled=False` bypasses all
pre-calibrators, `pipeline.py:1169-1170`.)

### Bug B — full spread double-counted as entry cost

`_decision_cost_edges` charges the **full** spread as an entry
cost: `spread_edge = (spread_bps / 10000) · price`
(`src/pms/controller/pipeline.py:1260`). For strategies that price
decisions at best ask, crossing the spread is already paid in the
fill price — charging full `spread_bps` on top double-counts it
(half-spread vs. mid is the defensible cost). Until fixed, this
**raises the effective emission bar for every candidate** in this
portfolio: real edges near the `min_edge=0.02` threshold will be
suppressed, and observed decision counts will undershoot thesis
expectations. Account for this before killing a candidate for "no
decisions".

---

## Platform constraints every candidate respects

- Only 2 factors compute live today: `orderbook_imbalance`
  (`src/pms/factors/definitions/orderbook_imbalance.py:24-49`) and
  `favorite_longshot_bias`
  (`src/pms/factors/definitions/favorite_longshot_bias.py:21-45`).
  Everything else starves on missing `external_signal` keys
  (`src/pms/sensor/adapters/market_data.py:697-726`).
- A new factor may only consume what the live signal carries:
  `signal.orderbook`, `yes_price`, plus
  `external_signal.{last_trade_price, fee_rate_bps,
  book_received_at, raw_event_type, token ids}`. The
  `OuterRingReader` seam exposes only `read_latest_book_snapshot`
  (`src/pms/factors/base.py:27-36`) — **this rules out any
  momentum/velocity factor needing time series**; that would be a
  protocol extension, not a "small addition".
- Market selection cannot filter by category/keyword — only venue,
  horizon, volume, price band, spread, depth, liquidity,
  accepting_orders (`src/pms/strategies/projections.py:72-81`;
  `src/pms/market_selection/selector.py:115-148`). **World Cup
  scoping = tight horizon (matches resolve in hours-days) + high
  volume floor + manual subscription seeds** via
  `POST /markets/{token_id}/subscribe`
  (`src/pms/api/app.py:370-373`), which `MarketSelector` merges
  into the subscription set
  (`src/pms/market_selection/selector.py:54-70`).
- Backtest coverage = your own recorded uptime (Gap G2) and
  Brier/P&L are NULL on replayed data (Gap G1,
  `src/pms/research/runner.py:1232-1248`).

---

## Rank 1 — `wc_flb_tail_fade_v1` (Channel A, factors computed today)

**Thesis.** Favorite-longshot bias is the most robust documented
bias in sports wagering, and a World Cup is its best-case regime:
knockout brackets and group-advancement markets generate a dense
supply of >0.90 favorites and <0.10 longshots; tournament retail
flow ("bet my team", lottery-ticket longshots) systematically
overprices the YES tail of longshots. The repo already encodes the
contrarian semantics: the factor emits a signed value only in the
tails — negative below 0.10 (fade → buy NO), positive above 0.90
(buy YES) (`favorite_longshot_bias.py:14-18,32-37`).

**Factor composition** (`FactorCompositionStep` fields per
`src/pms/strategies/projections.py:15-25`; rules math:
`prob = yes_price + Σ delta·weight`, threshold filters `|delta|`,
`src/pms/controller/forecasters/rules.py:72-92`):

| factor_id | role | weight | threshold | required | SLA |
|---|---|---|---|---|---|
| `favorite_longshot_bias` | `rule_delta` | 2.0 | 0.01 | True | 300 s |
| `orderbook_imbalance` | `rule_delta` | 0.05 | 0.50 | False (neutral fallback) | 60 s |
| `rules` | `blend_weighted` | 1.0 | — | — | — |

Forecaster: `("rules", (("threshold","0.55"),))` matching the seed
pattern (`src/pms/strategies/paper_multifactor.py:110-116`).
Example: yes=0.06 → bias=−0.04 → prob ≈ 0.06 − 0.08 = clamp(0.01)
(`rules.py:16,108-109`) → NO-side edge ≈ 4–5 c.

**Entry/exit/sizing.** Entry only when the rules edge clears
`min_edge=0.02` (`rules.py:29`); sizing via fractional Kelly 0.25
capped at `risk.max_position_per_market`
(`src/pms/controller/sizers/kelly.py:17,33-39`). No in-strategy
exit (same as `h1_flb`); holding time is bounded by
`resolution_time_max_horizon_days=4` so positions resolve
in-window. Market selection: polymarket, horizon 4 d,
`volume_min_usdc=1000`, `yes_price_min=0.02`/`max=0.98` (router
band, `src/pms/controller/router.py:40-45`).

**Critical config detail (Bug A applies).** Default calibration
rejects probabilities outside [0.08, 0.92]
(`projections.py:60-61`), and the unlock feedback is never wired —
the clamp would drop every tail decision and destroy the entire
edge. Set `CalibrationSpec(enabled=True, extreme_clamp_low=0.001,
extreme_clamp_high=0.999)` (the `h1_flb` pattern) or
`enabled=False` until ≥20 resolved samples. See "Known controller
bugs" above.

**Risk caps.** `max_position_notional_usdc=1.0`,
`max_daily_drawdown_pct=20`, `min_order_size_usdc=1.0` (PAPER
floor per `agent_docs/strategy-authoring-guide.md:641`).

**Failure modes.** (a) Uncalibrated rule shift vs `h1_flb`'s
warehouse-calibrated model — fixed shift of 2× bias may overstate
edge; (b) in-play confound: a 0.05 YES price in minute 85 is often
fair, not biased — time-to-resolution is unobservable to the
factor; (c) thin tail books → fill_or_kill/book-walk rejections
(`src/pms/research/execution.py:456-521`); (d) fee asymmetry
`notional·fee·(1−price)` (`src/pms/research/specs.py:193-194`)
penalizes the cheap side it buys.

**Validation.** Backtest over self-recorded week-1 WC data (fill
mechanics only — G1); sweep weight {1.0, 1.5, 2.0} × threshold
{0.005, 0.01, 0.02} (K=9 ≥ 7, cache gate passes,
`src/pms/research/sweep.py:28-29`). Paper: run alongside `h1_flb`
from day 1; **kill criterion: if its 7-day Brier improvement vs
baseline trails `h1_flb`'s on the same markets, it is dominated
and dies.**

**Cost:** half-day, 2-file diff
(`strategy-authoring-guide.md:93-94,548`). **Edge confidence:
high. Ratio: best in class.**

---

## Rank 2 — `h1_flb` with a soccer-sliced calibration artifact (Channel B, zero new strategy code)

**Thesis.** Same FLB edge, but calibrated: replace the
all-category calibration CSV with a soccer/sports-conditioned
slice, on the hypothesis that WC retail flow makes the longshot
tail fatter than the all-market base rate. `h1_flb` is already the
flagship live candidate, is factor-pipeline-independent (empty
`factor_composition`, reads market snapshots directly —
`src/pms/strategies/flb/projection.py:47`), and is already
selected by the canonical soak config (`config.live-soak.yaml:10`
→ `paper_soak_strategy_id: h1_flb`).

**Implementation.** No strategy code. (1) Run
`uv run python scripts/flb_data_feasibility.py` to confirm the
soccer slice yields ≥`flb_min_calibration_samples` (default 100,
`src/pms/config.py:371`) per signal bucket — **if it doesn't, this
candidate is dead, fall back to Rank 1**. (2) Export via
`scripts/export_flb_warehouse_from_dune.py`, write the sha256
provenance sidecar (`src/pms/strategies/flb/artifacts.py:41-127`),
place outside the working tree, point
`strategies.flb_calibration_path` (`config.live-soak.yaml:87`;
validated `src/pms/controller/factory.py:148-171`).

**Entry/exit/sizing (existing code).** yes<0.10 → BUY NO;
yes>0.90 → BUY YES at best ask; net edge ≥ 0.02 after 15 bps
execution cost + `0.07·(1−price)` fee
(`src/pms/strategies/flb/source.py:277-284,347-413`); KellySizer
(`src/pms/runner.py:796`); no exit, resolved markets skipped
(`source.py:215,416-417`). Risk: 1.0 USDC / 20% drawdown (flb
defaults). Bug A and Bug B both apply: tail pricing requires the
wide clamp or `enabled=False`, and best-ask entry pays the
double-counted spread (see "Known controller bugs").

**Failure modes.** Calibration regime shift (historic soccer base
rates ≠ WC-2026 flow); sample sparsity per decile bucket; 7-day
horizon spec may exclude tournament-winner futures (acceptable —
match markets are the target).

**Validation.** The whole soak pipeline already exists for this
strategy: `scripts/prepare_local_paper_soak_config.py:87-89`
selects it; gate via `paper_report.py --require-go`. Backtest adds
little (forecaster bypasses factors); paper is the evidence
source. **Kill criterion: dies at the feasibility script.**

**Cost:** data work only. **Edge confidence: highest
(calibrated). Ratio: #2 only because the data feasibility risk is
real.**

---

## Rank 3 — `wc_book_pressure_v1` (Channel A, re-parameterization of a proven config)

**Thesis.** Match-day soccer markets have order-flow that chases
events (goals, cards). Persistent signed depth imbalance precedes
the resting-book reprice; with WC volume the 1 s factor cadence
(`src/pms/config.py:417`) produces genuinely dense `factor_values`
rows instead of the sparse coverage normal markets give. This is
structurally `paper_multi_factor_v1`
(`src/pms/strategies/paper_multifactor.py:24-132`) re-tuned for a
high-event-rate regime — honest framing: a parameter fork, not a
new idea.

**Factor composition.** `orderbook_imbalance` rule_delta, weight
**0.10** (vs 0.25), threshold **0.60** (vs 0.80,
`paper_multifactor.py:16-17`), required=True, **SLA 60 s** (vs
300 — in-match books go stale in minutes); `rules` blend_weighted
1.0. Drop the starved `metaculus_prior` step entirely. Note the
pipeline also recomputes imbalance inline with the NO-token sign
flip at decision time (`src/pms/controller/pipeline.py:1620-1628`).

**Entry/exit/sizing.** Trade when |imbalance| ≥ 0.60 and shifted
prob clears min_edge; Kelly 0.25; hold to resolution; horizon
**2 days**, `volume_min_usdc=5000`, `spread_max_bps=200`,
`depth_min_usdc=500` (optional filters exist:
`projections.py:76-78`, enforced `selector.py:138-148`).

**Risk caps.** 1.0 USDC / 20% drawdown / min order 1.0.

**Failure modes.** (a) Displayed depth is spoofable — imbalance
measures resting intentions, not flow; (b) one-sided books near
resolution resolve to ±1.0 (documented break point,
`orderbook_imbalance.py:33-34`) → false maximal signal exactly
when markets are most dangerous; (c) momentum-following buys
*with* informed flow at ≥250 ms latency → adverse selection; the
backtest's book-walk + `adverse_selection_bps` model
(`execution.py:456-521`) is genuinely informative here;
(d) directionally opposite to Rank 4's fade thesis — running both
is a deliberate A/B on the overreaction question.

**Validation.** Sweep threshold {0.5, 0.6, 0.7, 0.8} × weight
{0.05, 0.10, 0.25} (K=12, cache gate OK) under
`polymarket_live_estimate` execution (`specs.py:207-223`);
fragility across the slippage grid is the kill signal
(`strategy-authoring-guide.md:498-501`). Paper: compare
decision/fill counts vs `paper_multi_factor_v1` running in
parallel. **Kill criterion: dies on grid fragility.**

**Cost:** lowest of all (copy + retune). **Edge confidence:
low-medium. Ratio: #3.**

---

## Rank 4 — `wc_goal_spike_fade_v1` (Channel A + ONE small named factor: `trade_mid_divergence`)

**Thesis.** In-play overreaction: goals trigger market orders that
print through the book while resting liquidity reprices slower.
Immediately after a spike, `last_trade_price` sits far from the
new resting mid; betting-exchange literature documents systematic
overshoot after late goals/red cards. Fade the overshoot. This is
the only candidate with a World-Cup-specific *event* edge rather
than a static bias.

**The one named factor addition.**
`src/pms/factors/definitions/trade_mid_divergence.py`:
`factor_id="trade_mid_divergence"`,
`required_inputs=("orderbook","last_trade_price")` — both present
on live signals (`market_data.py:697-726`). `compute()` =
`last_trade_price − (best_bid+best_ask)/2`; return `None` on empty
book or missing trade. Follow the §8 recipe exactly: subclass
`FactorDefinition`, register in `REGISTERED`
(`definitions/__init__.py:14-23`), piecewise tests per branch +
boundaries, `uv run mypy <file> --strict`
(`strategy-authoring-guide.md:743-775`). ~40 lines + tests. Per
Invariant 4 the factor stores the **raw signed divergence**; the
fade direction lives in the strategy weight.

**Factor composition.** `trade_mid_divergence` rule_delta,
**weight −0.5** (negative = fade), threshold 0.05, required=True,
SLA 60 s; `orderbook_imbalance` rule_delta weight 0.05 optional
confirmation; `rules` blend 1.0. (Negative weight is untested
territory in-tree — `rules.py:85-87` is sign-agnostic arithmetic,
but lock it in with a unit test before install.)

**Entry/exit/sizing.** Fires only when |last_trade − mid| ≥ 0.05;
Kelly 0.25; hold to resolution; horizon 2 d,
`volume_min_usdc=5000`, tight spread/depth filters as Rank 3.

**Risk caps.** 1.0 USDC / 20% / min order 1.0.

**Failure modes.** (a) Stale-trade artifact: `last_trade_price`
can lag the book legitimately (the factor sees no trade
timestamp) — short SLA mitigates, doesn't eliminate; (b) fading
genuinely informative flow (a red card *should* reprice) —
divergence closes by the mid moving to the trade, not back;
(c) replay warm-up: `yes_price`/last-trade start 0.0/None in
backtest (Gap G4, `replay.py:686`) → spurious divergences at
window start — discard the first slice; (d) threshold straddles
near the router price band (`router.py:40-45`).

**Validation.** Piecewise unit tests first; backtest sweep
threshold {0.03, 0.05, 0.08, 0.10} × weight {−0.3, −0.5, −0.7}
(K=12); sanity-check decision timestamps cluster on match windows;
paper from week 1 with daily decision-count review. **Kill
criterion: dies if decisions don't cluster on match windows.**

**Cost:** low-medium (one factor + one strategy). **Edge
confidence: medium. Ratio: #4.**

---

## Rank 5 (stretch, gated) — `wc_contradiction_dutch_v1` (cross-market overpricing on paired threshold markets)

**Thesis.** WC listings create paired threshold binaries ("more
than 2.5 goals" / "less than 2.5 goals"). Retail buys YES on both
sides → `yes_a + yes_b > 1` → near-arbitrage buying the
underpriced complement. Detection code already exists and is
precise: opposite-direction comparisons at an identical threshold,
subject Jaccard ≥ 0.75, `mispricing = pA + pB − 1 > 0`
(`src/pms/factors/market_relations.py:178-200`).

**Why it ranks last — the cost is honestly TWO additions, not
one.** (1) `MarketRelationService` is instantiated nowhere in
`src/` and its `FactorValueSink` protocol has no implementation
(`market_relations.py:56-64`; tests only). (2) Even wired, its
rows carry `param=<other market id>`
(`market_relations.py:157-166`), while a `FactorCompositionStep`
pins a static `param` (`projections.py:17-19`) and the snapshot
reader matches exact `(factor_id, param)` keys
(`src/pms/controller/factor_snapshot.py:80-188`) — **Channel-A
composition cannot consume dynamic-param rows.** It must be a
Channel-B module reading `market_relations` directly (template:
`src/pms/strategies/flb/`, registration pattern
`runner.py:_build_flb_module` per
`strategy-authoring-guide.md:733-739`).

**Entry/sizing.** When mispricing ≥ 0.04 (covers 2× fees +
slippage): buy NO on the overpriced leg, or buy the cheap
complement; both legs ≤ 1.0 USDC; flat sizing, no Kelly (this is
structure, not forecast).

**Failure modes.** The classic killer: resolution-rule mismatch —
markets that read as contradictory but resolve from different
sources/edge cases (extra time, voids); lexical detector false
pairs; legs filling asymmetrically (one leg rejected → naked
directional position).

**Go/no-go pre-check (do this before writing any code):** after
week 1 of recording:

```bash
psql "$DATABASE_URL" -c "SELECT question FROM markets WHERE question ~* 'more than|less than|over|under';"
```

If fewer than ~10 live contradiction pairs exist, skip
permanently. **Kill criterion: dies at this pair-frequency
pre-check.**

---

## Ranking summary (edge confidence × 1/cost)

| Rank | Candidate | Edge confidence | Cost | Notes |
|---|---|---|---|---|
| 1 | `wc_flb_tail_fade_v1` | High | ~0.5 day | Pure existing-factor composition |
| 2 | `h1_flb` soccer calibration | Highest | Data work only | Gated on feasibility script |
| 3 | `wc_book_pressure_v1` | Low-medium | Hours | Param fork of proven config |
| 4 | `wc_goal_spike_fade_v1` | Medium | 1–2 days | One ~40-line factor |
| 5 | `wc_contradiction_dutch_v1` | High-when-fires, rare | ~3 days (Channel B) | Two additions; pre-check first |

Per-candidate kill criteria are pre-registered in each section
above; the retro question per candidate is therefore already
written (see `agent_docs/strategy-iteration-sop.md`, Step 7).
