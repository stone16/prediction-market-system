1. 功能完备性
1(a) 目前功能上是否已经 Ready？

Paper soak：基本 ready。真钱 live：不 ready。

功能层面已经有完整的四层架构：Sensor、Controller、Actuator、Evaluator；Polymarket 是 primary venue，Kalshi 只保留 enum/stub，没有 v1 adapter。README 写明支持 backtest、paper、gated Polymarket live 三种模式，但 live 是 fail-closed，必须 live_trading_enabled=true、credentials 校验通过、operator_approval_mode=every_order、paper-soak GO report、operator rehearsal PASS report、合规/退出条件 attestation、venue reconciliation、fresh market-data preflight 全部通过才会提交。

代码里 live 链路不是空壳：Runner.start() 在 LIVE 下会做 live readiness validation、Postgres boot、factor/catalog 初始化、portfolio reconciliation、venue account reconciliation；actuator loop 会把 accepted decisions 交给 ActuatorExecutor，订单/成交持久化失败会 hard halt。

但它还没有达到“我给你 key，你就放心跑 production”的标准。几个硬点：

LIVE 默认不是无条件打开，而是多层 gate。 PMSSettings 默认 mode=backtest、live_trading_enabled=false，live validation 要求 approved secret source、Polymarket credential 字段、IOC/FOK、strict_factor_gates=true、quote_source=dual/venue_direct、account reconciliation、paper soak GO、operator rehearsal PASS、operator/compliance attestation，而且 live 下 GTC 会被拒绝，因为没有 durable live open-order ledger 来 reserve resting exposure。
Polymarket adapter 已经比较认真，但仍是 gated live path。 它有 SDK client、pre-submit quote guard、every-order file approval、direct/snapshot/dual quote provider、account reconciler、timeout/submission unknown handling；但 account reconciler 明确把 venue open orders 当作 mismatch，因为系统还没有 durable live open-order ledger。
真实 venue 集成证据还不够。 现有 integration test 验证了 runner → actuator → order/fill persistence 的路径，但 Polymarket client、quote provider、first-order gate 都是 mock/fake；这证明结构链路通，不证明真实 Polymarket API、签名、余额、撮合、异常响应在 production 上一定无问题。
部署边界现在已拆开：fly.toml 仍然只跑 pms-paper-soak，默认 config 指向 config.live-soak.yaml；已新增 fly.live.toml.example 作为 env-only live-capital 模板，使用 secret_source=fly，要求独立 app、/secure volume、Fly secrets、preflight artifact、operator approval/rehearsal 和 venue reconciliation。它还没被真实部署验证，但不会再让 live-capital 误用 paper-soak Fly config。
1(b) 如果提供 credential，是否能在 production 顺利跑起来且没有任何问题？

不能这样保证；我会明确判定为“credentials alone: no”。

要顺利启动 live，至少还要满足这些条件：

PMS_MODE=live、PMS_LIVE_TRADING_ENABLED=true、PMS_LIVE_ACCOUNT_RECONCILIATION_REQUIRED=true。
Polymarket 6 个字段齐全：private_key、api_key、api_secret、api_passphrase、signature_type、funder_address；新 API deposit-wallet 凭据使用 `signature_type=3` / `POLY_1271`，现有 proxy/Safe 钱包才继续用 `1`/`2`。
live secret 必须来自允许的 secret source；local file 模式还要求 secret file 权限私密。
time_in_force 不能用当前默认 GTC；runbook 推荐 IOC。
每个 live order 都需要 operator approval JSON，且字段必须和 preview 精确匹配。
Postgres、Alembic migration、schema check、market data ingestion 都要正常。
如果 API 绑定到非 loopback 地址，还必须配置非 placeholder 的
`PMS_API_TOKEN`，否则 LIVE validation 和 `pms-api` startup 都会 fail closed。
venue account reconciliation 必须通过；未处理的 submission_unknown incident、stale/missing two-sided book snapshots、schema drift、runtime dependency missing、operator approval path/first-order audit path 不合格都会阻止 live preflight/start。

换句话说，它设计上就是不让你“给 key 就跑”。这是好事，但也说明还没到 turnkey production。

1(c) 需要额外注意什么？

最重要的 production 注意事项：

风险点	我看到的状态	建议
Open-order ledger	live 下 GTC 被禁；reconciler 对 venue open orders fail closed；`pms-live preflight` 也会拒绝 PMS DB 里仍有 remaining notional 的 live/unmatched/partial Polymarket order	初期强制 IOC/FOK；要做 GTC/挂单策略，先实现 durable open-order ledger、cancel/reconcile/reserve exposure
Submission unknown	timeout/transport failure 会进入 PolymarketSubmissionUnknownError，runner 会暂停 live，需要人工 reconcile；`filled`/`open` 结论必须带 venue_order_id，否则 storage/API/CLI 都拒绝写入	先用极小订单测全流程；把 pms-live reconcile-submission-unknown 纳入 runbook
Daily max loss	已补 risk.max_daily_loss_usdc，并接入 auto-halt；LIVE validation 现在要求它是有限正数	保持 live template 的 $20 cap；实盘后每日复核
Operator approval	已要求 operator_approval_mode=every_order；runtime validation 拒绝 first_order 作为最终 LIVE	前期真钱保持每单审批；放宽前必须有 retro/PR
Approval/audit filesystem	Runtime validation 和 pms-live preflight 都会拒绝缺失、不可写、权限过宽的 approval/audit parent；两者都会拒绝 stale approval file	上线前用专用 `/secure/pms` 路径并 `chmod 700`，先跑 preflight，再由 operator 在 preview 后写 approval JSON
Live cash budget	Runner 和 pms-live preflight 现在都用 `risk.max_total_exposure` 作为 launch cash baseline；account snapshot 会先同步 CLOB balance/allowance cache，再校验 venue pUSD 余额和 allowance；余额或 allowance 低于该预算、SDK 响应无法解析出 pUSD/collateral 余额和 allowance，或解析结果是 NaN/Infinity，都会在 reconciliation 阶段 fail closed	部署前让账户余额和 allowance 覆盖 live template 的 exposure cap，不要依赖默认 `$1000` hardcoded portfolio
Dashboard 数据真实性	production dashboard 已 fail-closed，不能无 PMS_API_BASE_URL 回退 mock	部署时仍要确认 dashboard 指向真实 API
Backtest CLI 文档不一致	README 已改为 runner/API backtest 或 pms-research，不再指向不存在的 pms-backtest	后续新增 CLI 时保持 pyproject/README 同步
合规/地域限制	prediction market 的法律环境还在快速变化；2026 年 5 月仍有美国州层面的禁令/诉讼新闻	上线前把 jurisdiction、税务、venue ToS、KYC/geo-restriction 当成 P0，不是 P3

原先的 daily max loss、TODO_DECISION、Brier-vs-baseline、Docker optional extra、dashboard mock fallback、README pms-backtest 脚注、submission_unknown 无 venue_order_id 可结案、preflight/Runner cash budget 不一致、runtime approval/audit path 父目录不可用或权限过宽、Fly live-capital 部署模板缺失、LIVE strategy evidence metadata 可缺失、首笔实盘后没有机器可校验 reconciliation artifact、runtime bridge 概率/价格语义混用、非 loopback LIVE API 缺少 concrete `PMS_API_TOKEN` 这些问题已经关闭；post-live artifact 现在还会绑定 credentialed preflight artifact，并验证 preflight 早于该 live order 的提交时间；直接手工构造 `PolymarketActuator` 的 true LIVE 路径也不能绕过 credentialed preflight artifact gate。剩下的 live blocker 不是“再写一行配置”，而是真实 paper soak、credentialed venue preflight、operator/compliance sign-off、极小 live order 演练和实盘后人工复核。

2. 策略与维护
2(a) 在功能可用前提下，现在策略表现如何？

目前不能证明策略有正期望。它有策略框架和 hypothesis，但没有足够证据显示能稳定盈利。

你现在的核心显性策略是 H1 favorite-longshot bias，也就是：低 YES 价格的 longshot 视为 overpriced，买 NO；高 YES 价格的 favorite 视为 underpriced，买 YES。LiveFlbSource 的逻辑确实按 <10% 和 >90% 分桶，并生成 BUY NO / BUY YES 信号；当 `strategies.flb_calibration_path` 指向 warehouse-calibrated CSV artifact 时，它会用 `signal_name,probability_estimate,sample_count,source_label` 中的概率替代 `limit_price + min_expected_edge`，并在样本不足或净 edge 低于门槛时 fail closed。净 edge 现在会先扣 `strategies.flb_entry_execution_cost_bps` 和 `strategies.flb_fee_rate` 估计的入场执行成本 / fee，且低于门槛会在 sizing 前被压掉。没有配置该 artifact 时，旧的 2% placeholder 逻辑仍只适合 paper plumbing。LIVE runtime/preflight 现在会拒绝只写 `metadata.live_allowed=true` 的策略；要进入真钱路径，策略投影必须带非 placeholder 的 `alpha_source`、`edge_model_source`、`calibration_source`、`evidence_source`，并启用 calibration。但这些字段只能防止误启动，不能替代真实 paper/live 业绩证明。

FLB feasibility 脚本本身也很诚实：Gamma closed markets 的 lastTradePrice 只是 resolution 前最后成交价代理，不是固定 entry horizon 的 timestamped snapshot；脚本是 feasibility check，不是 strategy P&L backtest，不包含 fees、slippage、execution timing。它要求 extreme buckets 至少 100 个 resolved contract observations 才过 sample gate。

所以现在的策略状态更准确地说是：

“可用于 paper 验证的 FLB hypothesis implementation”，不是“已验证的盈利策略”。

另外，你说“套利”。从代码看，系统更偏 single-leg forecasting / mispricing trading，而不是严格意义的无风险套利。ExecutionPlanner 是 single-leg planner；agent bridge 对 basket intents 目前标记为 unsupported；runtime bridge 还 disabled by default；现在它要求 `TradeIntent.probability_estimate` 与 planner 使用的 `expected_price` 一致，且入队的 `TradeDecision.prob_estimate` 来自显式概率字段，避免把价格语义误写成概率证据。

如果你要做真正的 prediction-market arbitrage，下一步应该是多腿组合：同一事件 YES/NO sum mispricing、互斥结果 sum >/< 1、跨市场/跨 venue spread、resolution-equivalence mapping。这需要 basket execution、leg-level risk、partial-fill unwind、atomic-ish hedging，不是现在的主路径。

2(b) 后期维护是否足够轻量？

工程结构是可维护的，但 production trading 维护不会轻。

好的部分：模块边界清楚，Controller/Actuator/Risk/Sensor/Evaluator 分层明确；RiskManager 覆盖 exposure、drawdown、slippage、open positions、credential failure、rate limit、stale orders 等 auto-halt trigger；LLM forecaster 有 cache 和日预算；paper report 能从 /status、/trades、/positions、/metrics 拉指标，并且 daily P&L 必须由 /metrics 的 `pnl_series` 累计序列推导，缺失或格式错误会成为 risk event；`max_drawdown_pct` 也由同一个窗口化 P&L 证据按 `risk.max_total_exposure` 推导，`sharpe_ratio` 则由同窗口 daily P&L 计算并作为正值 GO gate；如果 `/status.controller.diagnostics_total > 0`，`/status.controller.diagnostic_counts` 必须用非空 reason code 精确覆盖所有 rejection reason，否则 `risk_events` gate 会失败。

但真钱 production 的维护项会不少：

Polymarket API / SDK / websocket schema 变化。
市场 discovery 质量、token mapping、orderbook freshness。
Venue balance / open order / submission unknown reconciliation。
策略 drift：FLB 是否仍存在、在哪些 category/horizon 存在。
LLM provider 成本、latency、failure、calibration。
Dashboard、alerting、runbook、incident review。
法务/合规/税务/地区访问限制。

所以我的判断是：代码维护性中等偏好；交易运营维护中等偏重。 它不适合“放云上然后忘了它”，至少前 2–4 周需要每天看 paper/live report 和 incident logs。

2(c) 策略优化空间

按优先级排：

把 FLB 的 placeholder edge 换成真实校准模型。 Runtime 已能从 `strategies.flb_calibration_path` 加载 warehouse-calibrated H1 signal probabilities，并拒绝样本不足或扣除入场执行成本 / fee 后净 edge 不达标的 signal；credentialed preflight 现在也会把该 CSV 内容纳入 fingerprint，避免 preflight 后替换模型。`scripts/flb_data_feasibility.py --source warehouse-csv --calibration-csv ...` 已能从严格 warehouse resolution export 生成 runtime artifact。下一步是用真实数据产出并版本化该 artifact，最好继续按 category、time-to-resolution、liquidity、volume、spread、price bucket 估计 conditional actual payout rate。不要把未配置 artifact 的 fixed limit + 2% 路径当 production alpha。
把“套利”和“预测交易”分开。
预测交易：做概率校准、Brier improvement、edge after costs。
套利交易：做组合约束、全腿执行、partial fill unwind、最坏情形 payout 检查。
把 execution costs 放进策略入口，而不是只在 planner/risk 后面挡。 FLB source 现在会把 `flb_entry_execution_cost_bps` 和 `flb_fee_rate` 从 gross edge 中扣掉并用净 edge 过门槛；下一步仍要把这些数从 paper/live telemetry 校准出来，并继续拆分 spread、slippage、latency、queue position、失败率，而不是长期用静态 buffer。
扩展 baseline comparison。 当前已经有 decision-time market-implied baseline 和 Brier improvement gate；每笔 decision 的 `decision_evidence` 现在也保存 market-implied、mid-quote、last-trade 三类 baseline 概率，并且当上游 signal 提供 `category_prior_baseline_prob_estimate` 时会把它作为 decision-time category-prior baseline 一起保存。`pms.controller.baselines.CategoryPriorBaselineEstimator` 已能从历史 resolved observation 按 signal 时间戳做无前视过滤、按 category 样本不足时回退 global prior，并用 Laplace smoothing 生成可注入 `MarketSignal.external_signal` 的 baseline 概率；`controller.category_prior_observations_path` 现在会在 Runner 启动时加载真实 historical resolution CSV export，schema 为 `market_id,category,yes_payout,no_payout,resolved_at`，并拒绝 price-like payout rows，避免把未结算价格当 outcome。credentialed preflight 现在也会把该 CSV 内容纳入 fingerprint，避免 preflight 后替换 baseline artifact。EvalRecord 会把这些 baseline 的 probability / Brier score 写入 JSONB maps，metrics 会按 baseline source 汇总 Brier improvement。paper report 会显示二级 baseline 覆盖率，并在已有 `decision_evidence` 但缺少 market-implied 或 mid-quote baseline 时记 risk event；同时它会把 `/metrics` 的 secondary baseline 分项 Brier / improvement 纳入 `Secondary Baseline Brier` 表和最终 GO gate，所有可用 baseline source 都必须有正的 Brier improvement。下一步是生成并版本化真实 warehouse export artifact，把路径接到 soak/live config，而不是手工固定一个常数。策略如果打不过市场基线，就不要上真钱。
做 paper-vs-backtest diff。 同样策略在同样市场上的 simulated fill、rejection、slippage、PnL 应该可对齐；`scripts/paper_backtest_execution_diff.py` 现在能读取严格 paper/backtest execution CSV export，比较 matched decision ids、fill/rejection rate、平均 slippage 与总 PnL，并在 `--require-pass` 下对薄样本、不匹配或阈值超标返回非零。LIVE validation/preflight 现在要求 `live_paper_backtest_diff_path` 指向通过的 diff JSON，并把 artifact 内容纳入 preflight fingerprint。偏差大或样本太薄都说明 execution model 不可信。
引入 correlation/category caps。 例如同一政治事件、同一体育联赛、同一宏观主题，不能只按 market_id 看 exposure。
把 LLM 降级为 feature，而不是核心 alpha。 LLMForecaster 的工程封装还行，但 LLM 输出必须经过 calibration、drift monitoring 和 cost/latency gating。
2(d) 是否符合商业和业务逻辑？

系统设计逻辑符合：fail-closed、小仓位 paper soak、every-order approval、risk envelope、reconciliation，这些都很对。盈利逻辑尚未证明。

如果你的商业目标是“先把 prediction-market research pipeline 做成可持续迭代的平台”，这个 repo 方向是合理的。
如果目标是“马上靠套利稳定赚钱”，目前还不够。严格套利需要多腿、确定性 payoff、执行原子性/对冲、手续费后净边际、partial-fill 风险处理；当前代码更偏概率交易系统。

3. 模拟与回测
3(a) 目前 backtest 和 paper trading 如何实现？

Paper trading： PaperActuator 从 live signals / orderbooks 拿当前盘口，按 BUY 用 asks、SELL 用 bids，考虑 slippage-adjusted limit，走深度计算 VWAP；深度不足就抛 InsufficientLiquidityError，成功则生成 matched order state，不发真实订单。这个 paper fill 比“直接按 mid price fill”靠谱很多。

普通 backtest actuator： BacktestActuator 从 fixture 加载 orderbook snapshot，然后调用 BacktestExecutionSimulator。

research backtest： 这块更完整。BacktestExecutionSimulator 支持 latency、staleness、IOC/FOK/GTC、partial fills、open order ledger、TTL cancel、price invalidation、replay engine lookup；BacktestRunner 会从 Postgres replay market universe，按 strategy version 运行 controller pipeline，记录 opportunity/decision/fill、Brier、slippage、PnL、drawdown，并在 session end cancel open orders。

research CLI： pms-research 支持 sweep 和 worker，能 enqueue parameter sweep、跑 queued backtest、生成 report。

原先 README run modes 表里的 `uv run pms-backtest` footgun 已修：当前 README 指向 `PMS_MODE=backtest PMS_AUTO_START=1 uv run pms-api`，研究回测走 `pms-research`。

3(b) 怎么把模拟/回测做得更好？

我会按这个顺序改：

执行统一后的 paper-soak gate。 README、runbook、config 注释、paper report 已统一到 30-day GO gate；接下来不是改文案，而是用真实 live-data paper 产出 artefact：至少 N 笔 simulated fills；Brier 优于 market-implied baseline；净 edge after spread/fee/slippage > 0；无 unresolved incident。
保存 decision-time 的完整盘口快照。 每一笔 decision 会保存：book hash、top N levels、book age、spread、quote source、latency estimate、selected factor snapshot hash、market-implied/mid-quote/last-trade baseline 概率，以及上游提供时的 category-prior baseline 概率。否则回测和 live/paper 对不上。
用 paper telemetry 校准 execution model。 `ExecutionModel.polymarket_live_estimate()` 仍然是静态估计，但现在有 `ExecutionModel.from_observed_telemetry(...)` 可从非空、有限的 paper/live slippage、latency 与 adverse-selection bps 样本构造 `telemetry_calibrated` profile；`scripts/execution_model_from_telemetry.py` 现在能把严格 telemetry CSV 产成可嵌入 backtest spec 的 execution-model JSON，并支持 `--require-adverse-selection` 与 `--min-samples` 防止薄样本误过关。LIVE validation/preflight 现在还会要求 `live_execution_model_path` 指向这个 `telemetry_calibrated` JSON，并拒绝没有正数 `adverse_selection_bps`、缺少 telemetry sample contract、`require_adverse_selection` 未开启或声明样本下限低于 10 的 profile。上线前必须用真实样本重建 profile，而不是长期使用静态估计。
加入 queue-position / adverse selection 模型。 walk-book 现在有 `displayed_depth_fill_ratio`，可以把盘口显示深度按队列位置/撤单证据做确定性 haircut；也有 `adverse_selection_bps`，会在 limit eligibility 和 slippage 之前把价格向不利方向漂移，避免 tight limit 在回测中被乐观成交。下一层仍需要从真实 paper/live fills 导出 queue-position、撤单和延迟后 quote drift 样本。
做 walk-forward / out-of-sample。 不要用全历史调一个 FLB threshold 后再宣称有效。现在 research runner 会按 `exec_config.chunk_days` 自动写入时间切片 `strategy_run_slices`，也会按 `external_signal.category`/`market_category` 和 `volume_24h` buckets 写入 category/liquidity slices；evaluation report 会把每个 slice 的 Brier、PnL、fill rate、drawdown、slippage、opportunity/decision/fill counts 写入 `benchmark_rows`。少于 20 个 decision samples 的 slice 会产生 promotion warning，避免一个 category 或 liquidity regime 的样本太少却被当作稳定 out-of-sample 证据。
把 feasibility 与 PnL backtest 分层。 FLB feasibility 脚本已经说明它不是 PnL backtest。下一层应是固定 entry horizon，例如 resolution 前 24h、72h、7d，使用 timestamped price snapshot，再跑 cost-aware execution。
增加 no-trade / market-implied baseline。 market-implied baseline 已进入 EvalRecord 和 paper GO gate；decision-time evidence 也开始保留 mid-quote / last-trade / category-prior baseline，EvalRecord / metrics 已能做 secondary baseline 的成对 Brier 对比，paper report 已把 secondary baseline 分项结果纳入 `Secondary Baseline Brier` 表和最终 GO gate。category-prior estimator 已能从 historical resolution CSV export 做 runtime loading，并按 decision timestamp 计算无前视、平滑后的 prior；下一步是生成真实 warehouse export artifact 并在 paper soak/live config 中显式配置。策略必须证明比“直接相信市场价格”更好。否则就只是承担流动性/信息风险。
我建议的 go-live 顺序

P0：不上真钱前必须完成/确认

保持 live Docker/部署边界清楚：当前 Fly 是 paper-soak；真正 live-capital 需要 config.live.yaml + credentialed preflight。
强制 IOC/FOK；GTC 等 open-order ledger 做完再说。
保留 risk.max_daily_loss_usdc、risk-group cap、live exit metrics 和 Brier-vs-baseline gate。
完成 30 天 paper soak，并让 `scripts/paper_report.py --require-go` 产出 GO artefact。
paper report 必须显示并通过：Brier vs baseline、market-implied/mid-quote/category-prior baseline coverage，所有 baseline source label 具体、非 placeholder、以小写字母开头且为 lowercase snake_case，所有 baseline coverage row 使用同一个 reported-decision denominator，所有有 covered decision evidence 的 secondary baseline 都有 positive Brier improvement、net edge after spread/fee/slippage、fill rate、由 /metrics `pnl_series` 支撑的 daily P&L、positive Sharpe、rejection reasons、unresolved incidents、risk events。
用极小 live order 做 every-order approval + dual quote guard + venue submit + fill persistence + `pms-live reconcile-live-order` 产物演练。
写清楚 emergency rollback：停止 runner、撤/查 venue open orders、rotate credentials、reconcile DB。现在还可以用 `pms-live record-emergency-stop` 把手动 emergency stop 的完成状态写入 `live_emergency_audit_path`，并要求 operator id、原因、runner stopped、credential rotated、runtime secrets removed、venue open orders reviewed、DB reconciled 和 paper/backtest restart mode 都明确记录；任何写在 credentialed preflight 之后的 emergency audit 记录都会让旧 preflight 在下次 LIVE 启动时失效，必须重新跑 `pms-live preflight`。

P1：真钱小规模后再扩

把 FLB edge 从 fixed placeholder 换成 warehouse-calibrated decile/category/horizon model。
给 arbitrage 单独建 basket/multi-leg execution path。
增加 correlation/category exposure caps。
持续保持 dashboard hard gate：production 禁止 mock data fallback。
建 daily review：纸面/真钱都必须每天看 report，直到策略稳定。
最终判断

这个 repo 的工程方向是对的，尤其是 fail-closed、every-order approval、risk gate、submission_unknown 处理、paper/backtest simulator 这些地方，明显是按真钱系统思路在搭。
但从“能不能 production 盈利”看，现在最大短板不是代码能不能跑，而是：

策略 edge 还没被严格证明；execution model 还没用真实 paper/live 数据校准；live open-order ledger 还没闭环；真实 venue submit/fill/reconcile 还没用 credentialed preflight 和极小真钱订单证明。

所以我的建议是：现在不要直接上真钱 production；先把它作为 live-data paper-soak 系统运行，并把 paper soak 变成 go/no-go gate。 过了这个 gate，再用最小 notional、IOC-only、每单人工审批扩大到 very small live。
