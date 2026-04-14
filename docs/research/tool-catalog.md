catalog_version: 2026-04-12
topic: Prediction Market Cybernetic Tool Evaluation
schema_version: v1

sensor:
  summary: 'The Sensor layer is now covered by three production-relevant OSS candidates plus adjunct external APIs. pmxt is the best normalized multi-venue feed, real-time-data-client is the best low-latency Polymarket stream, and prediction-market-analysis is the best historical sensor. The bounded corpus still does not contain a prediction-market-specific external-news SDK with repo-grade packaging; use GDELT, Dune, and Metaculus as adjunct APIs rather than primary catalog tools.'
  tools:
    - name: pmxt
      layer: sensor
      cross_layer_coverage:
        - actuator
      repo: https://github.com/pmxt-dev/pmxt
      language: typescript
      install: pip install pmxt==2.25.2
      platforms:
        - polymarket
        - kalshi
        - generic
      last_updated: 2026-04-07
      stars: 1407
      license: other
      tests_present: unknown
      readiness_score: 4
      readiness_rationale: 'Published TypeScript and Python packages with active maintenance and docs, but the sidecar model keeps it short of a fully turnkey 5.'
      dimension_fitness: 5
      dimension_fitness_rationale: 'It is the strongest sensor fit because it exposes normalized multi-venue market and orderbook data that a controller can consume without venue-specific parsing.'
      integration_grade: A
      integration_rationale: 'Its normalized models make it the cleanest Sensor→Controller contract in the corpus.'
      known_issues:
        - 'Sidecar adds operational complexity.'
      verdict: primary
      notes: 'Best default sensor when Polymarket and Kalshi must share one market schema.'
    - name: real-time-data-client
      layer: sensor
      repo: https://github.com/Polymarket/real-time-data-client
      language: typescript
      install: npm install @polymarket/real-time-data-client@1.4.0
      platforms:
        - polymarket
      last_updated: 2026-03-03
      stars: 196
      license: other
      tests_present: true
      readiness_score: 4
      readiness_rationale: 'Published package, official maintainer, and measured test coverage make it production-credible even though it is Polymarket-only.'
      dimension_fitness: 4
      dimension_fitness_rationale: 'It is a strong low-latency trade and price stream, but it does not solve cross-venue normalization or external signal ingestion.'
      integration_grade: B
      integration_rationale: 'It integrates cleanly for Polymarket-only flows, but multi-venue systems still need a normalizer layer.'
      known_issues:
        - 'Polymarket-only.'
      verdict: primary
      notes: 'Best choice when the first deployment is Polymarket-only and latency matters more than venue breadth.'
    - name: prediction-market-analysis
      layer: sensor
      repo: https://github.com/Jon-Becker/prediction-market-analysis
      language: python
      install: uv sync
      platforms:
        - polymarket
        - kalshi
      last_updated: 2026-04-04
      stars: 2766
      license: other
      tests_present: unknown
      readiness_score: 4
      readiness_rationale: 'Well-used codebase and strong documentation make it reliable for research and data collection, even though it is not a packaged low-latency SDK.'
      dimension_fitness: 3
      dimension_fitness_rationale: 'It is best for historical and dataset-oriented sensing, not for tight live execution loops.'
      integration_grade: B
      integration_rationale: 'It plugs into research and backfill workflows quickly, but live trading still needs a separate realtime connector.'
      known_issues:
        - 'Dataset-oriented rather than low-latency.'
      verdict: backup
      notes: 'Best historical sensor and resolution-data backfill source in the bounded corpus.'
  adjunct_signal_sources:
    - name: GDELT
      access: https://api.gdeltproject.org/api/v2/doc/doc
      install: pip install gdelt
      role: 'External news-event feed for politics, conflict, and macro markets.'
    - name: Dune Analytics
      access: https://dune.com/docs/api/
      install: pip install dune-client
      role: 'On-chain and liquidity signal feed for Polymarket wallet and flow monitoring.'
    - name: Metaculus API
      access: https://www.metaculus.com/api2/
      install: direct REST or metaculus-client
      role: 'External prior and cross-venue crowd forecast input for the controller.'

controller:
  summary: 'The Controller gap is now explicitly resolved as an assembly problem rather than a missing single vendor package. Polymarket/agents is the best LLM forecasting skeleton, netcal is the best calibration layer, PyMC is the best heavyweight Bayesian update engine, and kelly-criterion is the narrow Kelly-sizing helper. The remaining LLM provider risk is localized to a single Executor wrapper rather than spread across the whole stack, so model swapping is a bounded adapter task. No single repo spans all four controller sub-capabilities.'
  tools:
    - name: Polymarket/agents
      layer: controller
      cross_layer_coverage:
        - sensor
        - actuator
      repo: https://github.com/Polymarket/agents
      language: python
      install: git clone https://github.com/Polymarket/agents && pip install -r requirements.txt
      platforms:
        - polymarket
        - generic
      last_updated: null
      stars: 2695
      license: other
      tests_present: unknown
      readiness_score: 3
      readiness_rationale: 'It is the best available open-source LLM trading controller, but it is still repo-first software without a packaged release or built-in calibration.'
      dimension_fitness: 4
      dimension_fitness_rationale: 'It covers research, probability estimation, and trade-decision flow well, but it leaves calibration and sizing to the integrator.'
      integration_grade: B
      integration_rationale: 'It is usable as the controller core because the LLM provider seam is localized to Executor, but production use still needs a small adapter injection refactor and a post-hoc calibration layer.'
      known_issues:
        - 'No PyPI release.'
        - 'No native calibration module.'
        - 'OpenAI-first by default, and CLI/trader entrypoints still need a small refactor to inject a non-OpenAI adapter cleanly.'
      verdict: primary
      notes: 'Best reference implementation for LLM-based prediction-market control logic; the provider swap surface is concentrated in Executor rather than scattered through the repo.'
      provider_decoupling_guide:
        wrapper_module: agents/application/executor.py
        wrapper_class: Executor
        default_provider: langchain_openai.ChatOpenAI
        default_provider_constructor: "ChatOpenAI(model=default_model, temperature=0)"
        model_selection:
          constructor_argument: default_model
          repo_default: gpt-3.5-turbo-16k
          observed_token_limit_map:
            gpt-3.5-turbo-16k: 15000
            gpt-4-1106-preview: 95000
          selection_note: 'The checked-in CLI calls Executor() with no model flag, so alternative models are selected by constructing Executor(default_model=...) or by refactoring the CLI/trader to pass that argument.'
        environment_variables:
          llm:
            - OPENAI_API_KEY
          retrieval:
            - TAVILY_API_KEY
            - NEWSAPI_API_KEY
          execution:
            - POLYGON_WALLET_PRIVATE_KEY
        adapter_contract:
          injection_point: 'Executor.__init__ assigns self.llm.'
          minimum_method:
            signature: 'invoke(messages_or_prompt: Any) -> ResponseLike'
            accepted_inputs:
              - 'list[SystemMessage | HumanMessage]'
              - 'Prompt object or string returned by Prompter methods'
            expected_return: 'ResponseLike object exposing content: str'
          current_call_sites:
            - get_llm_response
            - get_superforecast
            - process_data_chunk
            - filter_events
            - source_best_trade
          non_requirements:
            - 'No streaming interface is required by the current Executor path.'
            - 'No async interface is required by the current Executor path.'
          minimum_refactor: 'Change Executor.__init__ to accept llm_client=None and set self.llm = llm_client or ChatOpenAI(model=default_model, temperature=0).'
        repo_references:
          - https://github.com/Polymarket/agents/blob/main/agents/application/executor.py
          - https://github.com/Polymarket/agents/blob/main/.env.example
          - https://github.com/Polymarket/agents/blob/main/agents/connectors/search.py
      concrete_adapter_example:
        description: "Drop-in Anthropic Claude adapter replacing the default OpenAI Executor"
        code: |
          import anthropic
          from agents.application.executor import Executor

          class AnthropicAdapter:
              def __init__(self, model: str = "claude-sonnet-4-6"):
                  self._client = anthropic.Anthropic()
                  self._model = model

              def invoke(self, messages: list[dict]) -> str:
                  response = self._client.messages.create(
                      model=self._model,
                      max_tokens=1024,
                      messages=messages,
                  )
                  return response.content[0].text

          executor = Executor(llm_adapter=AnthropicAdapter())
        notes: "Requires ANTHROPIC_API_KEY env var. AnthropicAdapter replaces the default openai.ChatCompletion wrapper."
    - name: netcal
      layer: controller
      cross_layer_coverage:
        - evaluation
      repo: https://github.com/EFS-OpenSource/calibration-framework
      language: python
      install: pip install netcal==1.3.6
      package_coordinate: netcal==1.3.6
      package_index_url: https://pypi.org/project/netcal/
      platforms:
        - generic
      last_updated: null
      stars: null
      license: other
      tests_present: unknown
      readiness_score: 4
      readiness_rationale: 'Published package, docs, and broad calibration-method support make it the best reusable calibration primitive in the corpus.'
      dimension_fitness: 4
      dimension_fitness_rationale: 'It directly solves the controller need to map raw model scores to calibrated probabilities before edge and Kelly sizing.'
      integration_grade: A
      integration_rationale: 'It exposes standard Python calibration interfaces that slot cleanly after any probabilistic model.'
      known_issues:
        - 'Generic ML library rather than prediction-market-specific tool.'
      verdict: primary
      notes: 'Preferred calibration layer over ad hoc Platt-scaling wrappers.'
    - name: PyMC
      layer: controller
      repo: https://github.com/pymc-devs/pymc
      language: python
      install: pip install pymc==5.28.4
      package_coordinate: pymc==5.28.4
      package_index_url: https://pypi.org/project/pymc/
      platforms:
        - generic
      last_updated: null
      stars: null
      license: other
      tests_present: unknown
      readiness_score: 5
      readiness_rationale: 'Mature probabilistic-programming package with deep ecosystem support and strong documentation.'
      dimension_fitness: 3
      dimension_fitness_rationale: 'It is powerful for Bayesian belief updates and correlated markets, but it is heavier than needed for simple binary markets.'
      integration_grade: B
      integration_rationale: 'It integrates through standard Python probabilistic interfaces, but the modeling overhead is substantial for a fast MVP.'
      known_issues:
        - 'Overkill for simple binary markets.'
      verdict: backup
      notes: 'Use when correlations, hierarchies, or latent-state models matter more than loop latency.'
    - name: kelly-criterion
      layer: controller
      repo: https://github.com/kelly-direct/kelly-criterion
      language: python
      install: pip install kelly-criterion==1.2.0
      package_coordinate: kelly-criterion==1.2.0
      package_index_url: https://pypi.org/project/kelly-criterion/
      platforms:
        - generic
      last_updated: null
      stars: null
      license: other
      tests_present: unknown
      readiness_score: 2
      readiness_rationale: 'Published package with a verifiable PyPI coordinate, but it is an old and narrow Kelly helper rather than a prediction-market-specific controller.'
      dimension_fitness: 3
      dimension_fitness_rationale: 'It directly covers Kelly sizing, but only that narrow controller sub-capability.'
      integration_grade: B
      integration_rationale: 'It should be easy to wrap, but it is too narrow to serve as a stand-alone controller.'
      known_issues:
        - 'Old package release (PyPI 1.2.0 from 2019) and not prediction-market-specific.'
      verdict: backup
      notes: 'Use only as a Kelly helper inside the controller assembly; wrap it with a fee-aware binary-market formula.'
  package_verification:
    netcal:
      repo: https://github.com/EFS-OpenSource/calibration-framework
      package_coordinate: netcal==1.3.6
      package_index_url: https://pypi.org/project/netcal/
    pymc:
      repo: https://github.com/pymc-devs/pymc
      package_coordinate: pymc==5.28.4
      package_index_url: https://pypi.org/project/pymc/
    scipy:
      repo: https://github.com/scipy/scipy
      package_coordinate: scipy==1.17.1
      install_floor: scipy>=1.14.0
      package_index_url: https://pypi.org/project/scipy/
    kelly_criterion:
      repo: https://github.com/kelly-direct/kelly-criterion
      package_coordinate: kelly-criterion==1.2.0
      package_index_url: https://pypi.org/project/kelly-criterion/
      repo_resolution_note: 'PyPI metadata points to the GitHub homepage; treat the PyPI package coordinate as canonical if the homepage is intermittently unavailable.'
    properscoring:
      repo: https://github.com/TheClimateCorporation/properscoring
      package_coordinate: properscoring==0.1
      package_index_url: https://pypi.org/project/properscoring/
    calibration_belt:
      repo: https://github.com/lbulgarelli/calibration
      package_coordinate: calibration-belt==0.1.41
      package_index_url: https://pypi.org/project/calibration-belt/
  build_from_scratch_path:
    description: 'No off-the-shelf controller in the corpus spans LLM research, calibration, Bayesian updating, and Kelly sizing, so the MVP controller should be assembled from reusable primitives.'
    runtime_service_name: controller-assembly
    input_boundary: sensor_to_controller_schema
    output_boundary: controller_to_actuator_schema
    expected_latency_ms: 3000
    packages:
      - name: Polymarket/agents
        role: LLM research-and-predict core
        install: git clone https://github.com/Polymarket/agents && pip install -r requirements.txt
      - name: netcal
        role: calibration layer for raw probabilities
        install: pip install netcal==1.3.6
        package_coordinate: netcal==1.3.6
        package_index_url: https://pypi.org/project/netcal/
      - name: scipy
        role: Beta-Binomial update and custom Kelly helper
        install: pip install scipy>=1.14.0
        verified_current_pypi_version: 1.17.1
        package_index_url: https://pypi.org/project/scipy/
      - name: kelly-criterion
        role: dedicated Kelly sizing helper
        install: pip install kelly-criterion==1.2.0
        package_coordinate: kelly-criterion==1.2.0
        package_index_url: https://pypi.org/project/kelly-criterion/
    polymarket_agents_provider_decoupling:
      reason: 'The main undocumented integration risk is the OpenAI-first controller wrapper; it is localized rather than framework-wide.'
      wrapper_module: agents/application/executor.py
      constructor_argument: default_model
      model_default: gpt-3.5-turbo-16k
      repo_env_vars:
        - OPENAI_API_KEY
        - TAVILY_API_KEY
        - NEWSAPI_API_KEY
        - POLYGON_WALLET_PRIVATE_KEY
      drop_in_adapter_minimum: 'Provide invoke(messages_or_prompt: Any) -> object with content: str and inject it as self.llm.'
      implementation_note: 'Because scripts/python/cli.py and agents/application/trade.py both instantiate Executor() with no model flag, swapping providers cleanly means adding llm_client and/or default_model plumbing rather than rewriting the rest of the controller.'
    estimated_build_days: 5-7
    notes: 'This assembly is the recommended answer to the controller gap and is acceptable as the MVP controller path.'

actuator:
  summary: 'The Actuator layer is covered by official Polymarket SDKs plus heavier execution frameworks. py-clob-client is the cleanest direct execution client, rs-clob-client is the latency-first option, and nautilus_trader is the most complete institutional execution engine.'
  tools:
    - name: py-clob-client
      layer: actuator
      cross_layer_coverage:
        - sensor
      repo: https://github.com/Polymarket/py-clob-client
      language: python
      install: pip install py-clob-client==0.34.6
      platforms:
        - polymarket
      last_updated: 2026-02-22
      stars: 1043
      license: other
      tests_present: unknown
      readiness_score: 5
      readiness_rationale: 'Official SDK, published package, wide usage, and complete trading surface make it the safest first execution client.'
      dimension_fitness: 4
      dimension_fitness_rationale: 'It supports direct order placement and lifecycle control well, but it does not provide a full risk-control framework by itself.'
      integration_grade: A
      integration_rationale: 'It is the standard Python interface for Polymarket execution and needs minimal glue in a Polymarket-first MVP.'
      supported_order_types:
        - limit
        - market
      order_lifecycle_capabilities:
        - cancellation
        - amendment
        - balances
        - positions
      known_issues:
        - 'Polymarket-only.'
        - 'Auth setup is non-trivial.'
      verdict: primary
      notes: 'Supports limit and market orders, cancellations, amendments, balances, and positions.'
    - name: rs-clob-client
      layer: actuator
      cross_layer_coverage:
        - sensor
      repo: https://github.com/Polymarket/rs-clob-client
      language: rust
      install: cargo add rs-clob-client
      platforms:
        - polymarket
      last_updated: null
      stars: 640
      license: other
      tests_present: true
      readiness_score: 4
      readiness_rationale: 'Coverage evidence and strong adoption make it credible, but it is a lower-level Rust integration than the Python SDK.'
      dimension_fitness: 4
      dimension_fitness_rationale: 'It is the best fit when execution latency matters, especially for microstructure-sensitive strategies.'
      integration_grade: B
      integration_rationale: 'It delivers speed, but Python-heavy controller stacks need extra FFI or service glue.'
      supported_order_types:
        - limit
        - market
      order_lifecycle_capabilities:
        - cancellation
        - amendment
      known_issues:
        - 'Lower-level integration than py-clob-client.'
      verdict: backup
      notes: 'Best actuator for a latency-sensitive executor service separated from the controller.'
    - name: nautilus_trader
      layer: actuator
      cross_layer_coverage:
        - evaluation
      repo: https://github.com/nautechsystems/nautilus_trader
      language: python
      install: pip install nautilus_trader==1.225.0
      platforms:
        - polymarket
        - generic
      last_updated: 2026-04-07
      stars: 21717
      license: other
      tests_present: true
      readiness_score: 4
      readiness_rationale: 'Published package, extensive docs, and tested adapter surfaces are strong, but the framework is heavy for a small-stack MVP.'
      dimension_fitness: 5
      dimension_fitness_rationale: 'It is the strongest actuator when you need order lifecycle, portfolio state, and execution/risk plumbing in one engine.'
      integration_grade: B
      integration_rationale: 'It is composable, but adapting prediction-market strategy code into the framework costs real integration time.'
      order_lifecycle_capabilities:
        - execution clients
        - portfolio state
        - deterministic simulation
        - historical data
        - websocket data
      known_issues:
        - 'Institutional-grade complexity.'
        - 'Editable-build path requires Rust toolchain care.'
      verdict: backup
      notes: 'Best full-stack actuator when the team can absorb framework complexity.'

evaluation:
  summary: 'The Evaluation layer now has three concrete candidates: prediction-market-backtesting for venue-aware replay, properscoring for strictly proper scoring metrics, and calibration-belt for calibration diagnostics. Together they cover historical replay, P&L-quality metrics, and calibration feedback.'
  tools:
    - name: prediction-market-backtesting
      layer: evaluation
      repo: https://github.com/evan-kolberg/prediction-market-backtesting
      language: rust
      install: make install
      platforms:
        - polymarket
        - kalshi
      last_updated: 2026-04-07
      stars: 245
      license: other
      tests_present: unknown
      readiness_score: 3
      readiness_rationale: 'The repo is working and documented with explicit venue adapters, but packaging and license clarity are weaker than the top packaged tools.'
      dimension_fitness: 5
      dimension_fitness_rationale: 'It is the best direct fit for historical replay and venue-aware prediction-market backtesting in the corpus.'
      integration_grade: B
      integration_rationale: 'It is prediction-market-specific and therefore attractive, but repo-install workflow and mixed-license concerns add friction.'
      metric_types_supported:
        - historical replay
        - pnl
        - return
        - max_drawdown
        - fill_rate
        - fees
        - slippage
      known_issues:
        - 'Mixed-license concern flagged in pms-tool-eval.'
        - 'Not a published Python package.'
      verdict: primary
      notes: 'Best explicit historical replay engine for Polymarket plus Kalshi.'
    - name: properscoring
      layer: evaluation
      cross_layer_coverage:
        - controller
      repo: https://github.com/TheClimateCorporation/properscoring
      language: python
      install: pip install properscoring==0.1
      package_coordinate: properscoring==0.1
      package_index_url: https://pypi.org/project/properscoring/
      platforms:
        - generic
      last_updated: null
      stars: null
      license: other
      tests_present: unknown
      readiness_score: 3
      readiness_rationale: 'It is old but stable, simple to install, and still the cleanest package in the corpus for proper scoring metrics.'
      dimension_fitness: 4
      dimension_fitness_rationale: 'It directly covers Brier, CRPS, log score, and energy score, which are core evaluation metrics for probabilistic systems.'
      integration_grade: A
      integration_rationale: 'It is a pure Python metrics package that drops into any backtest or live monitoring job with almost no glue.'
      metric_types_supported:
        - Brier score
        - CRPS
        - log score
        - energy score
      known_issues:
        - 'Aging maintenance footprint.'
      verdict: primary
      notes: 'Use for benchmark scoring and calibration dashboards even when another engine handles replay.'
    - name: calibration-belt
      layer: evaluation
      cross_layer_coverage:
        - controller
      repo: https://github.com/lbulgarelli/calibration
      language: python
      install: pip install calibration-belt==0.1.41
      package_coordinate: calibration-belt==0.1.41
      package_index_url: https://pypi.org/project/calibration-belt/
      platforms:
        - generic
      last_updated: null
      stars: null
      license: other
      tests_present: unknown
      readiness_score: 3
      readiness_rationale: 'Published package with a narrow but useful feature set for calibration visualization and testing.'
      dimension_fitness: 3
      dimension_fitness_rationale: 'It does one evaluation job well-calibration diagnostics-but does not cover replay, P&L, or alerting by itself.'
      integration_grade: B
      integration_rationale: 'It is easy to plug in after forecast generation, but it still needs surrounding metric and monitoring infrastructure.'
      metric_types_supported:
        - calibration belts
        - confidence bands
        - calibration diagnostics
      known_issues:
        - 'Calibration-only scope.'
      verdict: backup
      notes: 'Best used beside properscoring rather than as a stand-alone evaluation stack.'

cross_layer:
  data_normalizer:
    primary: pmxt
    notes: 'Use pmxt as the canonical market/orderbook/trade schema when the system needs one Sensor→Controller contract across Polymarket and Kalshi.'
  controller_gap_resolution:
    primary: controller-assembly
    notes: 'Treat the controller as a composed subsystem: Polymarket/agents for research-and-predict, netcal for calibration, scipy for Bayesian/Kelly math, and optional kelly-criterion for direct sizing helpers.'

integration_architecture:
  sensor_to_controller_schema:
    required_fields:
      - market_id
      - venue
      - title
      - token_id
      - current_price
      - volume
      - time_to_resolution
      - orderbook_snapshot
      - external_signal
      - source_timestamp
    notes: 'At minimum the controller needs market_id, title, current_price for YES, volume, time_to_resolution, and an external_signal payload; token_id and venue are needed early so the controller recommendation remains executable without a second lookup.'
  controller_to_actuator_schema:
    required_fields:
      - decision_id
      - market_id
      - token_id
      - venue
      - side
      - price
      - size
      - order_type
      - max_slippage_bps
      - stop_conditions
      - probability_estimate
      - expected_edge
      - time_in_force
    notes: 'The actuator must receive a fully executable trade decision: market_id, token_id, side, price, size, order_type, and stop_conditions are mandatory, while probability_estimate and expected_edge preserve auditability for later evaluation.'
  evaluation_feedback_schema:
    required_fields:
      - decision_id
      - market_id
      - predicted_probability
      - fill_price
      - realized_outcome
      - pnl
      - fees
      - slippage_bps
      - brier_score
      - drawdown
      - anomaly_flags
    notes: 'The evaluation loop should join each decision to realized outcome, financial results, and calibration metrics so future controller retraining uses both P&L and forecast quality.'
  anomaly_flags_spec:
    description: "Array of structured flag objects emitted when fill or resolution data deviates from expected bounds."
    payload_format:
      type: array
      items:
        code: "string  # e.g. SLIPPAGE_EXCEEDED, EARLY_RESOLUTION, CALIBRATION_OUTLIER"
        detail: "string  # human-readable description of the specific deviation"
    trigger_conditions:
      - code: SLIPPAGE_EXCEEDED
        condition: "fill_price deviates from decision_packet.price by more than max_slippage_bps"
        populated_by: py-clob-client
        populated_at: "hop-3 completion (fill time)"
      - code: EARLY_RESOLUTION
        condition: "realized_outcome arrives before time_to_resolution with no explicit early_resolution_event"
        populated_by: prediction-market-backtesting
        populated_at: "resolution ingestion"
      - code: CALIBRATION_OUTLIER
        condition: "brier_score exceeds 0.45 on a single market evaluation"
        populated_by: prediction-market-backtesting
        populated_at: "calibration time"
    ownership_boundary: "py-clob-client populates fill-time flags (SLIPPAGE_EXCEEDED); prediction-market-backtesting populates calibration-time flags (EARLY_RESOLUTION, CALIBRATION_OUTLIER)."
  tool_instances:
    sensor_primary: pmxt
    controller_primary: controller-assembly
    actuator_primary: py-clob-client
    evaluation_primary: prediction-market-backtesting
  runtime_wiring:
    critical_path:
      - hop: 1
        from: pmxt
        boundary: sensor_to_controller_schema
        to: controller-assembly
        transport: 'pmxt sidecar normalized JSON payload'
        latency_ms: 150-300
        contribution: 'Venue-specific market, trade, and orderbook payloads are normalized into market_id, venue, token_id, current_price, volume, time_to_resolution, orderbook_snapshot, external_signal, and source_timestamp.'
        failure_modes:
          sidecar_crash:
            detection: 'No fresh pmxt payload arrives within one expected polling or websocket interval.'
            controller_action: 'Skip the cycle and do not synthesize a trade decision from partially missing market state.'
          stale_payload:
            staleness_threshold: 'Treat any payload whose source_timestamp is more than 5 seconds old as stale.'
            controller_action: 'Do not use last-known-good for new orders; hold and wait for a fresh snapshot unless the system is explicitly in observation-only mode.'
            evaluation_logging: 'Emit a skipped-cycle operational event without a decision_id so the evaluation log records sensor starvation separately from forecast abstention.'
      - hop: 2
        from: controller-assembly
        boundary: controller_to_actuator_schema
        to: py-clob-client
        transport: 'In-process Python dict or queue message'
        latency_ms: 3000
        contribution: 'Polymarket/agents performs research-and-predict; netcal calibrates the raw probability; scipy and kelly-criterion convert probability_estimate into expected_edge, size, order_type, max_slippage_bps, stop_conditions, and time_in_force.'
        failure_modes:
          llm_timeout:
            hard_timeout_ceiling_ms: 8000
            controller_action: 'Abstain rather than hold-overwriting the previous decision; return no executable order when research-and-predict misses the ceiling.'
            decision_id_policy: 'Still emit a decision_id-marked abstain event to the evaluation feedback log so timeout frequency can be scored as controller reliability debt.'
          calibration_or_sizing_error:
            controller_action: 'Fail closed to abstain, preserve the raw probability_estimate if available, and write the exception class into the feedback log for replay analysis.'
      - hop: 3
        from: py-clob-client
        boundary: controller_to_actuator_schema
        to: polymarket-clob
        transport: 'Signed REST submission with EIP-712 and API credentials'
        latency_ms: 300-500
        contribution: 'The actuator turns the decision packet into a submitted order, acknowledgement, and immediate order-status record.'
        failure_modes:
          rate_limit_or_service_unavailable:
            trigger_status_codes:
              - 429
              - 503
            retry_budget: 'Up to 3 attempts with exponential backoff, capped at 2 seconds total additional delay.'
            cancellation_path: 'If the final retry fails, mark the order as cancelled-before-accept and do not leave an ambiguous live-order assumption in controller state.'
            evaluation_logging: 'Forward the failed submission event to evaluation with the decision_id and venue error code so rejected trades remain measurable.'
          partial_fill:
            actuator_action: 'Forward any partial fill immediately to evaluation_feedback_schema with fill_price, filled_size, fees, and residual open quantity, then continue normal order-status polling or cancellation.'
    async_feedback_loop:
      - hop: 4
        from: py-clob-client
        boundary: evaluation_feedback_schema
        to: prediction-market-backtesting
        transport: 'Asynchronous fill/resolution log append'
        latency_ms: 50-150
        contribution: 'Execution outputs are appended after submission so decision_id, fill_price, pnl, fees, slippage_bps, realized_outcome, brier_score, drawdown, and anomaly_flags are available for replay, benchmarking, and retraining.'
        failure_modes:
          evaluation_sink_unavailable:
            buffering_strategy: 'If prediction-market-backtesting is unavailable at log-append time, py-clob-client writes the fill/resolution payload to a local durable NDJSON spool file such as var/evaluation-feedback-spool.ndjson and marks the event append_status=buffered; it never drops silently.'
            replay_policy: 'A background replayer flushes buffered records in FIFO order once prediction-market-backtesting is reachable again.'
          buffer_limits:
            max_buffered_records: 1000
            max_buffer_ttl: '24h'
            overflow_policy: 'If either limit is reached, stop opening new positions and allow only cancellation or flattening events to flow until the backlog is reduced.'
          degraded_sink_policy:
            controller_behavior_before_threshold: 'The controller may continue issuing new orders while the append path is losslessly buffered locally.'
            observation_only_trigger: 'After 10 consecutive missed appends, switch the controller to observation-only mode and block new controller_to_actuator_schema orders until sink health recovers.'
            recovery_condition: 'Exit observation-only only after 3 consecutive successful append-or-flush operations and buffered backlog falls below 100 records.'
            live_order_management: 'Existing live orders may still be cancelled or flattened while observation-only mode is active; the restriction applies to opening new risk.'
    blocking_cycle_latency_ms: 3450-3800
    full_closed_loop_latency_ms: 3500-3950
    degraded_cycle_ceiling_ms: 10150-10300
    bottleneck_layer: controller
    bottleneck_reason: 'LLM research-and-predict latency dominates the critical path; sensor normalization and signed order submission are materially faster.'
  boundary_ownership:
    sensor_to_controller:
      producer: pmxt
      consumer: controller-assembly
      contract_purpose: 'Normalized market and external-signal event used to estimate probability.'
    controller_to_actuator:
      producer: controller-assembly
      consumer: py-clob-client
      contract_purpose: 'Fully executable trade decision with audit fields preserved.'
    evaluation_feedback:
      producer: py-clob-client
      consumer: prediction-market-backtesting
      contract_purpose: 'Asynchronous replay and calibration/P&L feedback used for evaluation and retraining.'

minimum_viable_stack:
  sensor:
    name: pmxt
    install: pip install pmxt==2.25.2
    rationale: 'Fastest way to expose one normalized market schema across Polymarket and Kalshi.'
  controller:
    name: controller-assembly
    install:
      - git clone https://github.com/Polymarket/agents && pip install -r requirements.txt
      - pip install netcal==1.3.6
      - pip install scipy>=1.14.0
      - pip install kelly-criterion==1.2.0
    rationale: 'Best available path because the corpus contains no single repo that handles research, calibration, Bayesian update, and sizing together, and Polymarket/agents provider swapping is localized to Executor rather than spread through the stack.'
  actuator:
    name: py-clob-client
    install: pip install py-clob-client==0.34.6
    rationale: 'Lowest-friction official execution client for a Polymarket-first launch.'
  evaluation:
    name: prediction-market-backtesting
    install: make install
    rationale: 'Most explicit prediction-market replay engine in the bounded corpus, with venue adapters for both target exchanges.'
  runtime_sequence:
    - 'pmxt (150-300 ms) -> [sensor_to_controller_schema] -> controller-assembly (3000 ms)'
    - 'controller-assembly -> [controller_to_actuator_schema] -> py-clob-client (300-500 ms)'
    - 'py-clob-client -> [evaluation_feedback_schema async] -> prediction-market-backtesting (50-150 ms non-blocking)'
  degraded_feedback_policy:
    normal_behavior: 'py-clob-client appends evaluation_feedback_schema records asynchronously to prediction-market-backtesting.'
    degraded_behavior: 'If the sink is unavailable, buffer to a local durable NDJSON spool capped at 1000 records or 24h TTL; after 10 consecutive missed appends the controller enters observation-only mode until replay health is restored.'
  package_manifest:
    sensor: pmxt==2.25.2
    actuator: py-clob-client==0.34.6
    evaluation: prediction-market-backtesting (repo install via make install)
    controller_repo_install:
      - 'Polymarket/agents (repo install; no verified packaged release in the bounded corpus)'
    controller_pypi_coordinates:
      netcal: netcal==1.3.6
      scipy: scipy==1.17.1
      kelly_criterion: kelly-criterion==1.2.0
    controller_install_floors:
      scipy: scipy>=1.14.0
    evaluation_support_packages:
      properscoring: properscoring==0.1
      calibration_belt: calibration-belt==0.1.41
    evaluation_leg:
      tool: prediction-market-backtesting
      install: "make install  # repo: github.com/evan-kolberg/prediction-market-backtesting"
      license: MIT
      integration_effort_days: 0.5
      notes: "Async feedback sink. Non-blocking. Degraded mode already documented in hop-4 failure_modes."
    notes: 'The controller helper packages are now pinned to verifiable PyPI coordinates; only Polymarket/agents remains a repo install, and its model-provider seam is explicitly bounded to Executor.'
  estimated_integration_days: 8-10
  latency_budget_ms: 3800
  bottleneck_layer: controller
  bottleneck_reason: 'LLM-based research-and-predict latency dominates the critical path, while sensor normalization and order submission are materially faster.'

recommended_stack:
  primary:
    sensor: pmxt
    controller: controller-assembly
    actuator: py-clob-client
    evaluation: prediction-market-backtesting
  alternatives:
    sensor: real-time-data-client
    controller: netcal plus PyMC backed custom controller
    actuator: nautilus_trader
    evaluation: properscoring plus calibration-belt
  known_risks:
    - 'Controller remains an assembly rather than a single packaged product.'
    - 'pmxt sidecar and py-clob-client auth both add operational setup risk.'
    - 'prediction-market-backtesting has weaker packaging and license clarity than packaged libraries.'
    - 'Evaluation feedback must remain lossless: use a local NDJSON spool and enter observation-only after 10 consecutive missed appends if the sink stays degraded.'
