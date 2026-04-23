# E2E Context

Task: `cathedral-v1`  
Current HEAD: `e6492454c6be39481eb6984adfe82817bc5bdd98`  
E2E baseline SHA: `01eb94dafd2d8640f597adfcc920ca23df94145d`

Checkpoint status before E2E:

- `01` PASS
- `02` PASS
- `03` PASS
- `04` PASS
- `05` PASS
- `06` PASS
- `07` PASS
- `08` PASS
- `09` PASS
- `10` PASS
- `11` PASS
- `12` PASS

Fresh current-head evidence gathered for E2E:

- Cross-checkpoint integration slice:
  - `PMS_RUN_INTEGRATION=1 PMS_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pms_test uv run pytest -q tests/integration/test_markets_route.py tests/integration/test_positions_trades_route.py tests/integration/test_decision_emission_cp07.py tests/integration/test_api_decisions_cp08.py tests/integration/test_api_event_stream_cp10.py tests/integration/test_share_route_cp11.py tests/integration/test_first_trade_metric_cp12.py`
  - Result: `11 passed in 6.66s`
- Dashboard Playwright suite:
  - `cd dashboard && PMS_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pms_test npx playwright test`
  - Result: `18 passed, 1 skipped (25.0s)`
- Current-head Cathedral grep checks:
  - Forbidden runtime architecture vocabulary grep is empty.
  - `layer-card` / `grid-four` grep is empty.

Primary cross-checkpoint flows the evaluator should trace:

1. `MarketDiscoverySensor`
   -> `markets` / `tokens`
   -> `GET /markets`
   -> dashboard `/markets`
2. `Opportunity` + `TradeDecision`
   -> durable `decisions` row
   -> `POST /decisions/{id}/accept`
   -> `PgDedupStore`
   -> `fills`
   -> `/positions` + `/trades`
3. `Runner.event_bus`
   -> `GET /stream/events`
   -> `EventLogDrawer`
   -> replay from `Last-Event-ID`
4. `strategies` share metadata
   -> `GET /share/{strategy_id}`
   -> public share page
5. `decisions` + `fill_payloads` + `fills`
   -> `/metrics`
   -> `pms.ui.first_trade_time_seconds`
   -> Today hero / cathedral happy path
