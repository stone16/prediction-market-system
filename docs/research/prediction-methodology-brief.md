# Prediction Methodology Research Brief

**Author:** @Researcher-Ciga
**Date:** 2026-05-03
**Task:** #16 — Prediction methodology research + first backtestable strategy

---

## Executive Summary

The PMS trading system has a complete execution stack (forecaster → evaluator → sizing → risk → actuator) but no validated edge. This research identifies exploitable pricing biases in prediction markets and recommends a combined FLB + anchoring-lag strategy as the first backtestable approach.

**Key finding:** The most academically validated and directly measurable bias is Favorite-Longshot Bias (FLB). Combined with LLM-based news-speed trading against anchoring lag, this provides a structural edge that can be backtested against historical Polymarket resolution data.

---

## 1. Documented Prediction Market Biases

### 1.1 Favorite-Longshot Bias (FLB)

The most robust anomaly in prediction and betting markets. Longshots (low-probability events) are systematically overpriced; favorites (high-probability events) are underpriced.

- **Source:** Snowberg & Wolfers (2010), "Explaining the Favorite-Longshot Bias," *Journal of Political Economy*
- **Mechanism:** Probability misperception consistent with Kahneman & Tversky's prospect theory — traders overweight small probabilities
- **Magnitude:** Losing 5% on favorites versus 40% on longshots is common
- **Correction methods:** Goto, Power, Shin transformations convert market odds to true probabilities
- **Exploitability:** Direct — identify contracts priced <10% or >90% and take contrarian side

### 1.2 Anchoring and Self-Reinforcing Prices

Traders treat existing market odds as fixed probabilities and fail to update with new information.

- **Source:** Gelman & Rothschild (Brexit/2016 US election analysis)
- **Mechanism:** Markets become "too stable to represent current circumstances accurately"
- **Exploitability:** LLM processes news faster than order book updates — trading window during anchoring lag (hours to days)

### 1.3 Echo Chamber / Opinion Diversity Collapse

When trader populations become homogenous, prediction markets degrade into echo chambers.

- **Source:** Koleman Strumpf (2016 election analysis)
- **Mechanism:** Unwillingness to believe minority outcomes; circulating identical information produces stagnant prices
- **Surowiecki prerequisites:** Diversity, independence, decentralization must hold for markets to aggregate well
- **Exploitability:** Political markets on Polymarket with skewed user bases

### 1.4 Temporal Degradation

Prediction quality degrades for events further from resolution.

- **Source:** Page & Clemen
- **Mechanism:** Prices drift toward 50% for distant events because traders resist locking up capital
- **Exploitability:** Long-duration contracts underpriced vs. base rates — structural mispricing in weeks-to-months timeframe

### 1.5 Hedging Distortions

When participants use prediction contracts as insurance, prices diverge from true probability.

- **Relevance:** Political markets on Polymarket where partisan hedging is common
- **Exploitability:** Moderate — requires identifying specific hedging-driven mispricings

### 1.6 Compositional / Conditional Mispricing

Multi-outcome markets show probability sum inconsistencies.

- **Example:** Polymarket "Largest Company end of June" — NVIDIA at 69%, Alphabet at 29% (sums to 98%)
- **Exploitability:** Continuous — exploit probability sum gaps in multi-outcome markets

---

## 2. LLM Forecasting Advantages

### 2.1 Parity with Superforecasters

- **Source:** Alur et al. (2025), "AIA Forecaster," arXiv:2511.07678
- **Finding:** LLM system achieves performance "equal to human superforecasters" on ForecastBench
- **Key insight:** Combining LLM system with market consensus outperforms either alone — LLMs provide additive, non-redundant information

### 2.2 Bayesian Market-LLM Fusion (MixMCP)

- **Source:** "Forecasting Future Language: Context Design for Mention Markets," arXiv:2602.21229
- **Approach:** Treat market-implied probabilities as Bayesian priors; instruct LLM to update using textual evidence
- **Finding:** MixMCP "yields more robust predictions than either the market or the LLM alone" with better calibration
- **Actionable:** Use market prices as priors, LLM analysis as the update signal

### 2.3 Calibration Over Accuracy

- **Source:** TruthTensor evaluation framework, arXiv:2601.13545
- **Finding:** Models with similar accuracy "diverge markedly in calibration, drift response, and risk-sensitivity"
- **Implication:** Model selection for trading must optimize calibration, not just accuracy

### 2.4 LLM Limitations

- **Source:** Prophet Arena, arXiv:2510.17638
- **Bottlenecks:** Inaccurate event recall, misunderstanding of data sources, slower near-resolution
- **Structural edge window:** Medium-horizon questions (weeks to months) where textual reasoning matters; lose to markets on imminent-resolution events

---

## 3. Market Structure: Polymarket vs. Kalshi

### Category Efficiency Hierarchy (least to most efficient)

1. **Long-tail geopolitical/cultural** — lowest liquidity, widest spreads, most mispricing
2. **Political nomination/election sub-markets** — high volume but partisan hedging distorts prices
3. **Sports** — deep liquidity but efficient; edges are thin and short-lived
4. **Crypto micro-markets** (5-min BTC) — house-edge structure (1.98x/2.02x) makes these negative-EV
5. **Major macroeconomic events** (Fed, oil) — tightest spreads, hardest to beat

### When Mispricings Persist

- Rapid news cycles when markets lag
- Long-duration contracts where capital-lockup costs depress prices toward 50%
- Multi-outcome markets where probability sums drift from 100%
- Echo-chamber effects in politically skewed markets

---

## 4. Exploitable Bias Hypotheses

| # | Hypothesis | Bias | Edge Source | Timeframe | Priority |
|---|-----------|------|-----------|-----------|----------|
| H1 | Fade overpriced longshots + underpriced favorites | FLB | Systematic mispricing <10% and >90% (binary-side semantics below) | All | **P0** |
| H2 | Trade against anchoring lag | Anchoring | LLM processes news faster than markets | Hours-days | **P0** |
| H3 | Fade 50%-drift on distant events | Temporal degradation | Long-duration underpriced vs. base rates | Weeks-months | P1 |
| H4 | Exploit multi-outcome sum gaps | Compositional | Probability sums deviate from 100% | Continuous | P1 |
| H5 | Fade echo-chamber political prices | Opinion diversity | Homogenous trader bases misprice minority | Event-driven | P2 |

---

## 5. Recommended First Strategy: FLB + Anchoring Lag (H1+H2)

### Why H1+H2 First

- **FLB is the most academically validated** — persistent, directly measurable, no complex reasoning required
- **Anchoring lag provides LLM-specific edge** — information-speed advantage during market lag
- **Combined system** addresses both structural bias (FLB) and informational advantage (anchoring)

### Strategy Definition

**Leg 1 — FLB Contrarian (binary-side semantics):**

Polymarket uses binary YES/NO contracts. "Longshot overpriced" and "favorite underpriced" must be translated to explicit sides:

- **Longshot overpriced (YES <10%):** FLB says market overprices the low-probability YES side. The exploit is to **buy NO** (or equivalently, short YES exposure). If YES resolves to 0 (the likely outcome per FLB), NO pays out at ~90¢+.
- **Favorite underpriced (YES >90%):** FLB says market underprices the high-probability YES side. The exploit is to **buy YES**. If YES resolves to 1 (the likely outcome per FLB), YES pays out at ~90¢+ with positive expected value.

Entry criteria:
- Buy NO when market YES price <10% (longshot bucket)
- Buy YES when market YES price >90% (favorite bucket)

- Position sizing: Kelly criterion with 0.25× fractional cap (already implemented in PR #42)
- Exit: Hold to resolution (binary outcome)
- Expected edge: 5-40% on longshot contracts per literature (to be calibrated on Polymarket data)
- **Data risk:** FLB magnitude from horse racing may not transfer directly. First backtest metric = "FLB magnitude by probability decile on Polymarket resolution data."

**Leg 2 — Anchoring Lag:**
- Entry: When LLM posterior diverges >15% from market price after news event
- Use market price as Bayesian prior, LLM analysis as update signal (MixMCP approach)
- Position sizing: Kelly with market-implied odds as prior
- Exit: Hold until market converges to LLM posterior OR resolution
- Expected edge: Hours-to-days window before market catches up

### Backtest Approach

1. **Data:** Scrape historical Polymarket resolution data and prices (Dune Analytics on-chain data)
2. **FLB measurement:** Bucket contracts into probability deciles, compare market price at entry to resolution outcome
3. **Anchoring simulation:** Replay historical news timelines against historical market prices
4. **Target:** Positive expected value across ≥100 resolved contracts before going live
5. **Gate metrics:** Brier score < 0.20, hit rate > 45%, avg edge > 5bps, Sharpe > 0

### Integration with Existing PMS Stack

The existing `BacktestSpec` infrastructure (`pms/research/specs.py`) supports:
- `StrategyVersionKey` — version-controlled strategy definitions
- `ExecutionModel` — polymarket_paper() and polymarket_live_estimate() presets
- `BacktestDataset` — coverage ranges, quality gaps, market universe filters
- `RiskPolicy` — maps to existing `RiskSettings`

**First ENG task (after this research):** Create a BacktestSpec for H1+H2 against historical Polymarket data, run through the existing sweep/replay/comparison pipeline.

---

## 6. Key Sources

| Source | Finding | Relevance |
|--------|---------|-----------|
| Snowberg & Wolfers (2010), JPE | FLB is probability misperception | H1 foundation |
| Alur et al. (2025), arXiv:2511.07678 | LLM = superforecasters; market+LLM > either alone | Edge validation |
| arXiv:2602.21229 (MixMCP) | Bayesian market-LLM fusion outperforms both | H2 foundation |
| arXiv:2601.13545 (TruthTensor) | Calibration > accuracy for trading | Model selection |
| arXiv:2510.17638 (Prophet Arena) | LLMs best at medium-horizon, not near-resolution | Timeframe selection |
| Page & Clemen | Temporal degradation toward 50% | H3 foundation |
| Strumpf (2016) | Echo chamber collapse in political markets | H5 foundation |
