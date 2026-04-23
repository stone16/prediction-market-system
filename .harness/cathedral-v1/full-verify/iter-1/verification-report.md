---
task_id: cathedral-v1
iteration: 1
verdict: PASS
hard_failures: 0
soft_warnings: 0
coverage_percent: 80
head_sha: e6492454c6be39481eb6984adfe82817bc5bdd98
evaluated_at: 2026-04-23T12:27:48Z
---

# Full Verify Report

## Hard Failures

None.

## Soft Warnings

None.

## Checks Executed

| Check | Command | Result | Duration |
| --- | --- | --- | --- |
| dependency sync | `uv sync` | PASS | `see evidence` |
| backend test suite | `uv run pytest -q` | PASS — `600 passed, 138 skipped in 14.68s` | `14.68s` |
| mypy strict | `uv run mypy src/ tests/ --strict` | PASS — `Success: no issues found in 277 source files` | `see evidence` |
| backend coverage | `uv run pytest --cov=src/pms --cov-report=term -q` | PASS — `TOTAL ... 80%` | `18.07s` |
| dashboard lint | `cd dashboard && npm run lint` | PASS — `tsc --noEmit` | `see evidence` |
| dashboard unit | `cd dashboard && npx vitest run` | PASS — `27 passed` | `3.79s` |
| dashboard build | `cd dashboard && npm run build` | PASS | `see evidence` |
| cathedral integration slice | `PMS_RUN_INTEGRATION=1 PMS_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pms_test uv run pytest -q tests/integration/test_markets_route.py tests/integration/test_positions_trades_route.py tests/integration/test_decision_emission_cp07.py tests/integration/test_api_decisions_cp08.py tests/integration/test_api_event_stream_cp10.py tests/integration/test_share_route_cp11.py tests/integration/test_first_trade_metric_cp12.py` | PASS — `11 passed in 6.66s` | `6.66s` |
| dashboard Playwright | `cd dashboard && npx playwright test` | PASS — `18 passed, 1 skipped` | `25.0s` |

## Coverage

- Total statements: `7834`
- Missing statements: `1576`
- Total coverage: `80%`

## Notes

- The dashboard lint gate is now a real runnable command in
  `dashboard/package.json` and was exercised during this pass.
- The single skipped Playwright spec is the existing mock-mode source-indicator
  test; every Cathedral-facing browser path is green.

## Evidence

- Sync output: `.harness/cathedral-v1/full-verify/iter-1/evidence/uv-sync.txt`
- Pytest output: `.harness/cathedral-v1/full-verify/iter-1/evidence/pytest.txt`
- Mypy output: `.harness/cathedral-v1/full-verify/iter-1/evidence/mypy.txt`
- Coverage output: `.harness/cathedral-v1/full-verify/iter-1/evidence/coverage.txt`
- Dashboard lint output: `.harness/cathedral-v1/full-verify/iter-1/evidence/dashboard-lint.txt`
- Dashboard unit output: `.harness/cathedral-v1/full-verify/iter-1/evidence/vitest.txt`
- Dashboard build output: `.harness/cathedral-v1/full-verify/iter-1/evidence/dashboard-build.txt`
- Cross-checkpoint integration output: `.harness/cathedral-v1/full-verify/iter-1/evidence/pytest-cross-checkpoint.txt`
- Dashboard Playwright output: `.harness/cathedral-v1/full-verify/iter-1/evidence/playwright.txt`
- Current-head grep output: `.harness/cathedral-v1/full-verify/iter-1/evidence/forbidden-vocab.txt`
- Current-head layer-card grep: `.harness/cathedral-v1/full-verify/iter-1/evidence/layer-card-grep.txt`
