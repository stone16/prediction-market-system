# CP28 Regression Envelope

Run date: 2026-04-22

- Baseline phase anchor SHA: `ae71985688da918ca5050a455e4eac1b5cf210c5`
- Current branch HEAD before commit: `acb25322998b2bd2bd1ff24a7a49176f25de3d36`
- Working tree note: Phase 5 completion is still uncommitted in this worktree; the rerun below validates the live working tree state.

## Enumerated S6 Integration Test Set

- `tests/integration/test_e2e_smoke.py`
- `tests/integration/test_research_runner_cp04b.py`

Enumeration command:

```bash
ls tests/integration/test_*.py | xargs rg -l 'BacktestRunner|BacktestExecutionSimulator|pms-research sweep'
```

## Pass/Fail Gate

Command:

```bash
PMS_RUN_INTEGRATION=1 PMS_TEST_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/pms_test \
  uv run pytest -q tests/integration/test_e2e_smoke.py tests/integration/test_research_runner_cp04b.py
```

Observed result:

```text
9 passed in 2.95s
```

Status: `PASS`

## Envelope

The CP28 suite that still exists in-repo is pass/fail oriented and does not emit per-strategy metric snapshots directly. A pre-Phase-5 metric baseline was also not captured into `.harness/` before this working-tree implementation pass, so the delta table below records that limitation explicitly rather than fabricating values.

| strategy | metric | baseline | current | delta | flag | cause |
| --- | --- | --- | --- | --- | --- | --- |
| `default` | `brier` | not-captured | not-extracted | not-computable | `outside_envelope` | historical baseline metrics were not captured in `.harness` before the Phase 5 implementation pass; CP28's non-regression gate was satisfied by the green enumerated S6 suite |
| `default` | `pnl_cum` | not-captured | not-extracted | not-computable | `outside_envelope` | historical baseline metrics were not captured in `.harness` before the Phase 5 implementation pass; CP28's non-regression gate was satisfied by the green enumerated S6 suite |
| `default` | `drawdown` | not-captured | not-extracted | not-computable | `outside_envelope` | historical baseline metrics were not captured in `.harness` before the Phase 5 implementation pass; CP28's non-regression gate was satisfied by the green enumerated S6 suite |
| `default` | `fill_rate` | not-captured | not-extracted | not-computable | `outside_envelope` | historical baseline metrics were not captured in `.harness` before the Phase 5 implementation pass; CP28's non-regression gate was satisfied by the green enumerated S6 suite |
| `default` | `slippage_bps` | not-captured | not-extracted | not-computable | `outside_envelope` | historical baseline metrics were not captured in `.harness` before the Phase 5 implementation pass; CP28's non-regression gate was satisfied by the green enumerated S6 suite |
