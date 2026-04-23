---
task_id: cathedral-v1
project_type: python+node
---

# Full-Verify Discovery

## Detected Check Commands

| Name | Command | Source |
| --- | --- | --- |
| dependency sync | `uv sync` | AGENTS.md canonical gates |
| backend test | `uv run pytest -q` | AGENTS.md canonical gates |
| backend typecheck | `uv run mypy src/ tests/ --strict` | AGENTS.md canonical gates |
| backend coverage | `uv run pytest --cov=src/pms --cov-report=term -q` | full-verify coverage measurement |
| dashboard lint | `cd dashboard && npm run lint` | `dashboard/package.json` `scripts.lint` |
| dashboard unit | `cd dashboard && npx vitest run` | `dashboard/package.json` `scripts.test` adapted for non-watch CI execution |
| dashboard build | `cd dashboard && npm run build` | `dashboard/package.json` `scripts.build` |
| dashboard e2e | `cd dashboard && npx playwright test` | `dashboard/package.json` `scripts["test:e2e"]` |
| cathedral integration slice | `PMS_RUN_INTEGRATION=1 PMS_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pms_test uv run pytest -q tests/integration/test_markets_route.py tests/integration/test_positions_trades_route.py tests/integration/test_decision_emission_cp07.py tests/integration/test_api_decisions_cp08.py tests/integration/test_api_event_stream_cp10.py tests/integration/test_share_route_cp11.py tests/integration/test_first_trade_metric_cp12.py` | closeout branch-level seam verification |

## Test Frameworks

- Backend: `pytest`
- Frontend unit: `vitest`
- Frontend browser: `playwright`

## Coverage Tool

- `pytest-cov` / `coverage.py`
