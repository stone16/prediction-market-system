# H2 Anchoring Lag — Math Spec

Status: **Ready for paper-safe ENG implementation**  
Author: @Researcher-Ciga  
Date: 2026-05-03

## 1. Core Mechanism

Market prices can anchor on prior beliefs and update slowly after news. The H2
hypothesis is that a deterministic LLM posterior can process new information
before the order book fully reprices. The gap between the LLM posterior and the
market YES price is the exploitable signal.

## 2. Factor Definition

`anchoring_lag_divergence` is a signed scalar factor:

```
P_market = market YES price
P_llm    = LLM posterior probability after processing news
delta    = P_llm - P_market
```

The signal decays linearly after the triggering news:

```
T_max           = 24 hours
delta_effective = delta * max(0, 1 - (now - news_timestamp) / T_max)
```

Semantics:

- `delta_effective > 0` means the market appears to underreact to positive news;
  the actionable side is buy YES.
- `delta_effective < 0` means the market appears to underreact to negative news;
  the actionable side is buy NO.
- After `T_max`, the signal is zero and no action should be emitted.

Factor catalog contract:

| Property | Value |
|----------|-------|
| factor_id | `anchoring_lag_divergence` |
| required_inputs | `yes_price`, `external_signal.llm_posterior`, `external_signal.news_timestamp` |
| output_type | `scalar` |
| direction | `neutral` with signed semantics in `value` |

## 3. Entry Criteria

| Condition | Threshold |
|-----------|-----------|
| Divergence magnitude | `abs(delta_effective) > 0.15` |
| Within decay window | `now - news_timestamp < 24h` |
| LLM confidence | `confidence > 0.60` |
| Market still open | `resolves_at > now` |

The first implementation is intentionally paper-safe: it consumes prepared LLM
posterior/news observations and does not fetch news or call an LLM provider from
inside the strategy plugin.

## 4. Exit Criteria

These are strategy-level requirements for the later production loop/backtest:

| Condition | Threshold |
|-----------|-----------|
| Price convergence | `abs(P_market - P_llm) < 0.05` |
| Market resolved | `resolves_at <= now` |
| Max hold time | `now - entry_time > 7 days` |

## 5. Position Sizing

Use the existing Kelly sizing path with the selected side's effective
probability:

- Buy YES: `prob = P_market + delta_effective`
- Buy NO: `prob = 1 - (P_market + delta_effective)`

If the sizer returns `0.0`, suppress the trade intent. H2 must not bypass the
existing RiskManager or actuator path.

## 6. Strategy Plugin Structure

The H2 plugin mirrors the H1 FLB plugin shape:

| Component | Module | Responsibility |
|-----------|--------|----------------|
| Source | `pms.strategies.anchoring.source.LiveAnchoringSource` | Convert prepared LLM/news observations into strategy observations |
| Controller | `pms.strategies.anchoring.controller.AnchoringController` | Propose candidates |
| Agent | `pms.strategies.anchoring.agent.AnchoringAgent` | Judge candidates and build typed intents |
| Evaluator | `pms.strategies.anchoring.evaluator.AnchoringEvidenceEvaluator` | Gate confidence, divergence, evidence, and edge |

## 7. Backtest Compatibility

Task #27 owns backtesting. First-run backtests may use a fixed noise schedule:

```
noise_std(t) = 0.20 * (1 - t / T_total) + 0.02 * (t / T_total)
```

A later backtest can use a Beta noise model derived from LLM calibration error:

```
N_eff = 1 / Brier - 2
alpha = outcome * N_eff + 1
beta  = (1 - outcome) * N_eff + 1
P_llm ~ Beta(alpha, beta)
```

Do not build H2 live-capital enablement or H1+H2 backtest replay in the initial
implementation PR.
