# LLM Forecaster — Real Implementation Design

**Date:** 2026-05-02
**Author:** Stometa (with assistant)
**Status:** Approved through Section 6, awaiting spec review.
**Branch:** `feat/llm-forecaster-real`

## 1. Problem statement

The Controller layer's `LLMForecaster`
(`src/pms/controller/forecasters/llm.py`) is a pre-S5 stub: when
`enabled=True`, `predict()` returns a hardcoded
`(yes_price, 0.0, "pre-s5-neutral", "neutral")` tuple without
calling any LLM. The `_client / _prompt / _parse_response` helpers
exist but are never invoked.

The seeded `default` strategy
(`strategies.active_version_id = dac2c73055ba…`) requires the `llm`
factor as `runtime_probability` with `required: true`. The same
config also requires `metaculus_prior` (no Metaculus integration)
and `subset_pricing_violation` (cross-market analysis the runner
does not currently produce). With required factors missing,
`ControllerPipeline` produces zero `TradeDecision`s — confirmed
empirically: in a fresh PAPER soak with WS healthy and 10,674+
`price_changes` rows ingested, `decisions_total = 0`.

`LLMSettings` also lacks a `base_url` field, blocking proxy and
self-hosted / OpenAI-compatible endpoints.

## 2. Goal

Make the LLM forecaster a real provider-switchable component, and
relax the seeded `default` strategy's required-factor set so that
PAPER produces decisions with the LLM forecaster as the only
runtime probability source.

The Polymarket LIVE smoke (a single $5–10 IOC order) is downstream
of this and out of scope here. This work unblocks the LIVE smoke
but does not perform it.

## 3. Non-goals

- Not implementing a Metaculus integration. `metaculus_prior` will
  remain in the strategy composition but as `required: false`.
- Not implementing `subset_pricing_violation` factor logic.
  Same — relaxed to `required: false`.
- Not adding rate-limiting or backpressure beyond the per-instance
  TTL cache (T1).
- Not adding integration tests that hit real LLM endpoints. Unit
  tests inject fake clients.
- Not changing the `default` strategy beyond two `required` flags.
  The strategy version is created anew (per Invariant 3
  immutability) but the underlying composition is preserved.
- Not adding cost telemetry. Out of scope; can be a follow-up.

## 4. Constraints

- Project gates: `uv run pytest -q` baseline must remain green
  (currently 874 passed / 161 skipped → expected ≥890 passed
  after this work). `uv run mypy src/ tests/ --strict` must be
  clean.
- Architecture invariants (full text in
  `agent_docs/architecture-invariants.md`):
  - **Invariant 3** — strategy versions are immutable. The
    `default` strategy gets a NEW `strategy_version_id` and
    `strategies.active_version_id` is updated to point at it.
  - **Invariant 5** — Sensor and Actuator stay strategy-agnostic.
    Touching only Controller-side code preserves this.
  - **Invariant 8** — Onion-concentric storage. Controller reads
    middle ring, writes inner ring. The strategy-version write
    lands in the inner ring (`strategy_versions`); no outer-ring
    rows are touched.
- Project conventions:
  - Frozen dataclasses for entities. `LLMForecastResult` already
    follows this.
  - `float` at entity boundary, `Decimal` for calculation
    internals. No money math in this work, so `float` throughout.
  - Frozen dataclass mutation via `dataclasses.replace`. Not
    relevant here — we only mutate JSON config in a transient
    Python dict.
  - No `Co-Authored-By` lines in commit messages.
  - Atomic commits. Six commits, one concern each (see §10).
  - Feature branch only — `feat/llm-forecaster-real`.

## 5. Architecture

```
ControllerPipeline (existing)
  └─ LLMForecaster (rewritten)
       ├─ LLMSettings (extended schema)
       ├─ _client() — provider-dispatch, lazy SDK import
       │    ├─ provider="anthropic" → AsyncAnthropic
       │    └─ provider="openai"    → AsyncOpenAI
       ├─ _predict_async() — async hot path
       │    ├─ cache hit → return cached
       │    ├─ cache miss → _call() → _parse() → cache put
       │    └─ on TimeoutError | LLMTransientError | LLMParseError → return None
       ├─ predict() — sync escape-hatch (returns None when enabled, for back-compat)
       └─ forecast() — async public surface, calls _predict_async()
```

Cache lives on the forecaster instance. One forecaster per
`ControllerPipeline`, one pipeline per strategy
(`controller/factory.py:102`). Cache lifetime matches strategy
lifetime — desired.

## 6. Detailed design

### 6.1 `LLMSettings` (in `src/pms/config.py`)

```python
class LLMSettings(BaseModel):
    enabled: bool = False
    provider: Literal["anthropic", "openai"] | None = None
    api_key: str | None = None
    base_url: str | None = None
    model: str = "claude-sonnet-4-6"
    timeout_s: float = 5.0
    cache_ttl_s: float = 30.0
    max_tokens: int = 256
```

Validation via a pydantic `model_validator(mode="after")`:

- If `enabled=False`: no further validation. Preserves "default
  off" semantics so unrelated tests can construct `LLMSettings()`
  freely.
- If `enabled=True`:
  - `provider` MUST be `"anthropic"` or `"openai"`.
  - `api_key` MUST be a non-empty string.
  - If `provider="openai"`: `base_url` MUST be a non-empty
    string. (Refusing implicit `api.openai.com` default avoids
    leaking API keys to the wrong endpoint by accident.)
  - If `provider="anthropic"`: `base_url` MAY be `None`; SDK
    defaults to Anthropic's official URL.

Env var surface (existing `pydantic-settings` machinery, no new
plumbing):

```bash
PMS_LLM__ENABLED=true
PMS_LLM__PROVIDER=anthropic
PMS_LLM__API_KEY=sk-ant-...
PMS_LLM__BASE_URL=https://gateway.example/v1
PMS_LLM__MODEL=claude-sonnet-4-6
PMS_LLM__TIMEOUT_S=5.0
PMS_LLM__CACHE_TTL_S=30.0
PMS_LLM__MAX_TOKENS=256
```

The `model` default bumps from `claude-3-5-sonnet-latest` to
`claude-sonnet-4-6` per the system prompt's current model family
guidance. Existing tests that pin `claude-test` still work.

### 6.2 `LLMForecaster` (in
`src/pms/controller/forecasters/llm.py`)

Public surface preserved:

- `predict(signal) -> LLMForecastResult | None` — sync. When
  `enabled=False`: `None`. When `enabled=True`: also `None` (sync
  callers cannot drive the async path without an event loop).
  Existing tests that mock `predict()` still work because mocks
  bypass this body entirely.
- `forecast(signal) -> float` — async. Runs the real path via
  `_predict_async()`.

New / changed internals:

- `_predict_async(signal)` — the real path:

  ```python
  if not self.config or not self.config.enabled:
      return None
  cached = await self._cache_get(signal.market_id)
  if cached is not None:
      return cached
  client = self._client()
  if client is None:
      return None  # F1 strict — no client wired = no decision
  try:
      raw = await asyncio.wait_for(
          self._call(client, signal),
          timeout=self.config.timeout_s,
      )
      result = self._parse(raw, signal)
  except (TimeoutError, LLMTransientError, LLMParseError):
      return None  # F1 strict
  await self._cache_put(signal.market_id, result)
  return result
  ```

- `_client()` — provider dispatch with lazy SDK import:

  ```python
  if self.client is not None:
      return cast(_LLMClient, self.client)
  if self.config.provider == "anthropic":
      from anthropic import AsyncAnthropic  # type: ignore[import-not-found]
      kwargs = {"api_key": self.config.api_key}
      if self.config.base_url:
          kwargs["base_url"] = self.config.base_url
      self.client = AsyncAnthropic(**kwargs)
  elif self.config.provider == "openai":
      from openai import AsyncOpenAI  # type: ignore[import-not-found]
      self.client = AsyncOpenAI(
          api_key=self.config.api_key,
          base_url=self.config.base_url,
      )
  else:
      return None
  return cast(_LLMClient, self.client)
  ```

- `_call(client, signal)` — split into `_call_anthropic` and
  `_call_openai`, each taking the typed client and returning a
  raw string body. Anthropic uses `await client.messages.create(
  model=..., max_tokens=..., messages=[{"role":"user",
  "content": prompt}])`. OpenAI uses
  `await client.chat.completions.create(model=...,
  messages=[{"role":"user","content":prompt}],
  max_tokens=..., response_format={"type":"json_object"})`.

- `_parse(raw, signal)` — preserved from the existing
  `_parse_response`; converts the raw text into
  `LLMForecastResult(prob_estimate, confidence, rationale,
  model_id)`. Returns the `LLMForecastResult` value class
  (existing tuple subclass).

- `_prompt(signal)` — preserved from existing `_prompt`. Sends:
  market_title, market_id, venue, yes_price, top-5 orderbook,
  external_signal. Asks for JSON-only response with keys
  `prob_estimate`, `confidence`, `rationale`. Adds an explicit
  instruction line:
  `"Respond with a JSON object only. No prose. Keys:
  prob_estimate (0..1 float), confidence (0..1 float),
  rationale (one short sentence)."`

- New exceptions, both subclasses of `RuntimeError` so callers
  catching broad `Exception` still work:
  - `LLMTransientError` — wraps SDK transient errors (rate limit,
    network, 5xx).
  - `LLMParseError` — raised when JSON parse fails or required
    keys missing.

Per-instance state added:

```python
_cache: dict[str, tuple[float, LLMForecastResult]] = field(
    default_factory=dict
)
_cache_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
```

Cache helpers as designed in §6.3.

### 6.3 Cache (T1 — per-market TTL)

```python
async def _cache_get(self, market_id: str) -> LLMForecastResult | None:
    if not self.config or self.config.cache_ttl_s <= 0:
        return None
    async with self._cache_lock:
        entry = self._cache.get(market_id)
        if entry is None:
            return None
        ts, result = entry
        if time.monotonic() - ts > self.config.cache_ttl_s:
            del self._cache[market_id]
            return None
        return result

async def _cache_put(self, market_id: str, result: LLMForecastResult) -> None:
    if not self.config or self.config.cache_ttl_s <= 0:
        return
    async with self._cache_lock:
        self._cache[market_id] = (time.monotonic(), result)
        if len(self._cache) > 1000:
            oldest = min(self._cache, key=lambda k: self._cache[k][0])
            del self._cache[oldest]
```

Decisions:

- Key is `market_id` only (not `market_id + price`). Including
  price defeats the cache.
- `time.monotonic()` not `time.time()` — clock jumps don't poison
  cache.
- Single lock per forecaster, not per-market. Contention is
  trivial at ~0.5 sig/sec/market.
- Size cap = 1000 entries. With ~100 subscribed markets, plenty
  of headroom; bounds memory. Drop oldest on overflow.
- `cache_ttl_s <= 0` disables cache. Useful for unit tests that
  want to verify every call hits the client.
- No in-flight deduplication. Concurrent burst on cold cache is
  bounded by signal rate per market; acceptable cost.

### 6.4 Failure handling (F1 — strict)

A failed LLM call returns `None` from `_predict_async()`. The
controller's `required: true` gate then prevents decision
emission for that signal. This is the conservative default for
LIVE money: a sick model blocks trades rather than letting a
silent fallback chase the market price.

Failure cases caught:

- `asyncio.TimeoutError` — request exceeded `timeout_s`.
- `LLMTransientError` — wraps SDK transient errors. Caught at
  `_call_*` boundary; raised inside `_predict_async`'s try block.
- `LLMParseError` — JSON parse / schema validation failure.
- Any other `Exception` from the SDK is NOT caught. Crashes
  surface in logs. This forces operators to fix bugs rather than
  silently downgrade to neutral.

### 6.5 Strategy config relaxation

One-off Python helper at
`scripts/relax_default_strategy_required_factors.py`:

```
1. Connect to DATABASE_URL via psycopg.
2. SELECT (active_version_id, config_json) for
   strategies.strategy_id = 'default'.
3. Mutate config_json in-memory:
     for factor in config["config"]["factor_composition"]:
         if factor["factor_id"] in {
             "metaculus_prior", "subset_pricing_violation"
         }:
             factor["required"] = False
4. If mutation produced no change: print "already relaxed",
   exit 0 (idempotent).
5. Compute new strategy_version_id by importing the same hashing
   helper the seed code uses (to be located — likely
   src/pms/strategies/aggregate.py or src/pms/storage/seed.py).
6. INSERT new row in strategy_versions(strategy_version_id,
   strategy_id, config_json, created_at) — preserving any other
   columns the schema requires.
7. UPDATE strategies SET active_version_id = <new_id> WHERE
   strategy_id = 'default'.
8. Print: old_id, new_id, list of factor_ids that were flipped.
9. Exit 0.
```

Reversibility: old `strategy_version_id` is printed and never
deleted. To revert:
`UPDATE strategies SET active_version_id = '<old_id>' WHERE
strategy_id = 'default';`

Safety:

- Wrapped in a single transaction. INSERT + UPDATE must both
  succeed or neither does.
- Refuses to run if DATABASE_URL is missing or unreachable.
- Idempotent on re-run.
- Asserts that the loaded `factor_composition` includes both
  `metaculus_prior` and `subset_pricing_violation` before
  mutation; raises if either is unexpectedly absent (defends
  against future seed changes).

### 6.6 Tests

#### 6.6.1 Migrated tests (5)

In `tests/unit/test_controller_cp05.py`:

| Test | Old → New |
|---|---|
| `test_llm_forecaster_returns_neutral_tuple_without_calling_client` | New: assert `predict()` returns `None` when enabled (sync escape-hatch behaviour under option `a`). Rename to `test_llm_forecaster_predict_sync_returns_none_when_enabled`. |
| `test_llm_forecaster_returns_none_when_disabled_and_neutral_when_enabled` | New: assert `None` for both disabled and enabled (sync). |
| `test_llm_forecaster_forecast_uses_neutral_probability_and_default_config` | Preserved — default config has `enabled=False`, so `forecast()` returns `signal.yes_price`. |
| `test_llm_forecaster_client_paths_cover_injected_missing_and_cached_factory` | Rewrite to test new provider dispatch. Cover: client injection short-circuit, missing SDK import → `None`, cached client return. |
| `test_controller_cp01.py:104` mock of `predict` returning `(0.65, 0.9, "test-llm")` | If `ControllerPipeline` calls `predict()`, the mock works as-is. **If it calls `forecast()`, update the mock target to `forecast` returning a coroutine.** Verified during impl. |

#### 6.6.2 New tests (16 new)

`LLMSettings` validation lives in a new file
`tests/unit/test_config_llm_settings.py` (no dedicated config
test home exists today; existing settings checks are scattered
across area-specific test files):

1. `test_llm_settings_requires_provider_when_enabled`
2. `test_llm_settings_requires_api_key_when_enabled`
3. `test_llm_settings_openai_requires_base_url`
4. `test_llm_settings_anthropic_optional_base_url`
5. `test_llm_settings_disabled_skips_validation`

`LLMForecaster._predict_async` lives in
`tests/unit/test_controller_cp05.py` (joining the existing LLM
forecaster tests; no new file unless §6.6.1 migrations push the
file past project conventions, in which case a sibling
`test_llm_forecaster_async.py` is acceptable):

6. `test_predict_async_returns_none_when_disabled`
7. `test_predict_async_anthropic_provider_calls_client_and_caches`
8. `test_predict_async_openai_provider_calls_client_and_caches`
9. `test_predict_async_cache_hit_skips_client`
10. `test_predict_async_cache_expiry_recalls_client`
11. `test_predict_async_cache_size_cap_evicts_oldest`
12. `test_predict_async_timeout_returns_none`
13. `test_predict_async_transient_error_returns_none`
14. `test_predict_async_malformed_response_returns_none`

`forecast()` integration:

15. `test_forecast_returns_yes_price_on_predict_failure`
16. `test_forecast_returns_predicted_value_on_success`

Mocking strategy: inject a fake async client via the existing
`client` field on `LLMForecaster`. No real HTTP calls. Pattern
matches existing
`test_llm_forecaster_client_paths_cover_injected_missing_and_cached_factory`.

#### 6.6.3 Strategy relaxation test

`tests/integration/test_relax_default_strategy_required_factors.py`,
gated on `PMS_RUN_INTEGRATION=1` and
`PMS_TEST_DATABASE_URL` (existing pattern, see
`tests/integration/test_schema_apply_outer.py`).

Asserts:

- Before run: active version's config has `required: true` for
  both target factors.
- After run: active version's config has `required: false` for
  both. New `strategy_version_id` differs from old.
- Re-run is idempotent: no-op exit, active version unchanged.
- Old version row still exists (reversibility).

## 7. Dependency changes

`pyproject.toml`:

```toml
[project.optional-dependencies]
llm = [
    "anthropic>=0.40.0",
    "openai>=1.50.0",
]
```

After change: `uv sync --extra llm` installs both. The runner
process does not need the LLM extra to start — `_client()` lazy-
imports inside the dispatch path, mirroring the existing
`importlib.import_module("anthropic")` pattern.

`uv.lock` regenerates automatically on `uv sync`. Commit the
updated `uv.lock`.

## 8. Failure modes considered

| Mode | Behaviour |
|---|---|
| LLM API down or network failure | Timeout fires (5s default). `None` returned. Decision blocked for that signal. Other signals continue (cache hit-through). |
| Rate limit (429) | SDK raises a transient error. Wrapped in `LLMTransientError`. `None` returned. |
| Malformed JSON | `LLMParseError`. `None` returned. |
| Wrong base_url (404) | Behaves like an unparseable response or a 4xx. Caught → `None`. |
| API key revoked mid-run | First call after revocation raises auth error. Currently NOT caught (lets it propagate). Operators see the failure clearly in logs. **Open question — see §11.** |
| Cache eviction during burst | Bounded loss; cache fills back during normal operation. |
| `enabled=False` in config | `_predict_async` short-circuits at the first check. Zero LLM calls. |
| Multiple concurrent calls for same market_id, cold cache | All proceed independently. Up to N redundant calls during the very first second of a market's first cache fill. Acceptable. |
| Process restart | Cache is in-memory and resets. First N seconds of post-restart calls all miss. Acceptable — soak warm-up. |
| `client` field manually injected (existing test pattern) | Provider dispatch is skipped. Test client used directly. Preserved. |

## 9. Verification gates

Before opening PR:

1. `uv sync --extra live --extra llm` (LIVE smoke still needs the
   live SDK; this work also adds the llm extra).
2. `uv run pytest -q` — must report ≥890 passed (current
   baseline 874 + ~16 new − 0 net deletions; rounded up).
3. `uv run mypy src/ tests/ --strict` — clean across all source
   files. Source file count grows by 0 (in-place edits) or by 1
   if exception types end up in a small adjacent module.
4. `PMS_RUN_INTEGRATION=1 uv run pytest tests/integration/test_relax_default_strategy_required_factors.py -q` — green when run against a Postgres instance with the seed strategy applied.
5. Manual: re-run PAPER soak with this branch, `PMS_LLM__*`
   exports set, and the relaxation script applied. Within 5
   minutes, expect `decisions_total > 0`.

## 10. Branch & commit plan

Branch: `feat/llm-forecaster-real`.

Commits, atomic and ordered:

| # | Prefix | Concern |
|---|---|---|
| 1 | `chore:` | Add `openai` to llm extra in `pyproject.toml`, regenerate `uv.lock`. |
| 2 | `feat:` | `LLMSettings` schema additions + validation + new tests. |
| 3 | `feat:` | `LLMForecaster` async path + provider dispatch + cache + new exception types + new tests. |
| 4 | `test:` | Migrate the 5 existing LLM forecaster tests to new assertions. Update `test_controller_cp01.py` mock target if needed (verified during impl). |
| 5 | `feat:` | `scripts/relax_default_strategy_required_factors.py` + integration test. |
| 6 | `docs:` | This spec doc + any cross-reference updates. (May land first as a separate commit on the branch.) |

PR opens after all six land locally and gates §9 are green. PR
description references this spec.

## 11. Open questions resolved during implementation

These are NOT design decisions for the user. Resolved by the
implementing agent during impl by reading the relevant code:

1. Does `ControllerPipeline` call `predict()` (sync) or
   `forecast()` (async)? Determines whether the test_controller
   _cp01 mock target needs updating. **Path forward:** read
   `src/pms/controller/pipeline.py`. If `predict()`: no pipeline
   change. If `forecast()`: trivial mock swap, one line.
2. Where is the `strategy_version_id` hashing scheme implemented?
   Likely `src/pms/strategies/aggregate.py` or
   `src/pms/storage/seed.py`. **Path forward:** grep for the
   string `sha256` and `strategy_version_id` in those modules.
   The relaxation script imports and reuses the helper.
3. Should auth errors be caught and downgraded to `None`, or
   surface as crashes? **Path forward:** look at how Anthropic
   SDK 0.92.0 raises auth errors (likely
   `anthropic.AuthenticationError`). If it's clearly distinct
   from network errors, surface it (config bug, operator should
   fix). Otherwise wrap in `LLMTransientError`. Decision logged
   in the implementation commit.

## 12. Alternatives rejected

| Option | Why rejected |
|---|---|
| Sync clients wrapped in `asyncio.to_thread` | Thread overhead; async-native is what the rest of the runner uses (`asyncpg`, `httpx.AsyncClient`). |
| Pure-SQL strategy relaxation via `jsonb_set` | Matching the existing hashing scheme in pure SQL is hard; Python helper is cleaner. |
| Alembic migration for strategy relaxation | Migrations are for schema changes, not application data. |
| Delete `metaculus_prior` and `subset_pricing_violation` from `factor_composition` entirely | Bigger config delta; preserves design intent if Metaculus is wired up later. |
| Materiality-threshold cache (T3) | More logic for marginal benefit at this scale. T1 (TTL) is sufficient. |
| Lenient on-failure (F2) | Footgun in LIVE — JSON parse bug would silently approve trades on `yes_price`. F1 strict is the right LIVE default. |
| `provider="openai"` defaults to `api.openai.com` if `base_url` unset | Risk of leaking API keys to wrong endpoint by accident. Make `base_url` mandatory for OpenAI provider so the operator must opt in. |

## 13. Acceptance criteria

- All gates in §9 green.
- After running the relaxation script against a Postgres dev DB
  with the seed strategy, `strategies.active_version_id` for
  `default` points at a new version whose `factor_composition`
  has `metaculus_prior.required = false` and
  `subset_pricing_violation.required = false`.
- With `PMS_LLM__ENABLED=true`, `PMS_LLM__PROVIDER=anthropic`,
  `PMS_LLM__API_KEY=<valid>` exported, restarting the runner in
  PAPER mode against the relaxed strategy version produces
  `decisions_total > 0` within 5 minutes.
- The PR is opened from `feat/llm-forecaster-real` to `main` with
  six atomic commits, no `Co-Authored-By` lines.

## 14. Out of scope (follow-up work)

- Polymarket LIVE smoke (separate operator runbook,
  `docs/operations/live-polymarket-runbook.md`).
- Metaculus integration to actually populate `metaculus_prior`.
- `subset_pricing_violation` factor implementation.
- Cost telemetry for LLM calls.
- In-flight deduplication for cold-cache concurrent calls.
- Real-LLM integration tests (gated on
  `PMS_RUN_LLM_INTEGRATION=1`).
- Promotion of the relaxed strategy from a dev-DB tweak to a
  seeded production-strategy variant.
