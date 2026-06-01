from __future__ import annotations

import importlib.machinery
import importlib.util
import json
import os
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, cast

import pytest
from pydantic import SecretStr

from pms.actuator.adapters.polymarket import (
    FileFirstLiveOrderGate,
    LiveOrderPreview,
    PolymarketActuator,
    PolymarketOrderResult,
    PolymarketSubmissionUnknownError,
)
from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import (
    ControllerSettings,
    DiscordSettings,
    LLMSettings,
    PMSSettings,
    PolymarketSettings,
    RiskSettings,
    SecretSource,
    validate_live_mode_ready,
)
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.factor_snapshot import FactorSnapshot
from pms.controller.forecasters.llm import LLMForecaster
from pms.controller.pipeline import ControllerPipeline
from pms.core.enums import MarketStatus, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import (
    LiveTradingDisabledError,
    MarketSignal,
    OrderState,
    Portfolio,
    TradeDecision,
)
from pms.factors.composition import evaluate_branch_probabilities
from pms.live_preflight_artifact import (
    live_preflight_readiness_reports_fingerprint,
    live_preflight_settings_fingerprint,
)
from pms.runner import ActuatorWorkItem, Runner
from pms.storage.dedup_store import InMemoryDedupStore
from pms.storage.feedback_store import FeedbackStore
from pms.strategies.defaults import DEFAULT_STRATEGY_COMPOSITION
from pms.strategies.projections import (
    ActiveStrategy,
    CalibrationSpec,
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from tests.support.fake_stores import InMemoryFeedbackStore
from tests.support.live_paths import (
    make_live_preflight_artifact_path,
    make_live_report_paths,
)


FIXTURE_PAPER_SOAK_GO_REPORT = "tests/fixtures/paper_soak_go_report.md"
FIXTURE_OPERATOR_REHEARSAL_REPORT = (
    "tests/fixtures/operator_rehearsal_pass_report.md"
)
PAPER_SOAK_GO_REPORT, OPERATOR_REHEARSAL_REPORT = make_live_report_paths(
    prefix="pms-live-blockers-reports-"
)
_LIVE_PATH_ROOT = Path(tempfile.mkdtemp(prefix="pms-live-blockers-"))
_LIVE_PATH_ROOT.chmod(0o700)


def _replace_report_provenance_field(
    report_path: str,
    *,
    field_name: str,
    value: str,
) -> None:
    path = Path(report_path)
    replaced = False
    lines: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"| {field_name} |"):
            lines.append(f"| {field_name} | {value} |")
            replaced = True
        else:
            lines.append(line)
    assert replaced
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _copy_report_with_output_path(source_path: str, target_path: Path) -> None:
    source_text = Path(source_path).read_text(encoding="utf-8")
    source_output_line = next(
        line for line in source_text.splitlines() if line.startswith("| output_path |")
    )
    target_text = source_text.replace(
        source_output_line,
        f"| output_path | {target_path} |",
    )
    target_path.write_text(target_text, encoding="utf-8")


def _remove_markdown_section(report_text: str, heading: str) -> str:
    lines: list[str] = []
    skipping = False
    for raw_line in report_text.splitlines():
        if raw_line.strip() == heading:
            skipping = True
            continue
        if skipping and raw_line.startswith("## "):
            skipping = False
        if not skipping:
            lines.append(raw_line)
    return "\n".join(lines) + "\n"


def _signal(
    *,
    yes_price: float = 0.10,
    external_signal: dict[str, Any] | None = None,
) -> MarketSignal:
    return MarketSignal(
        market_id="m-live-blocker",
        token_id="t-yes",
        venue="polymarket",
        title="Will live blocker tests pass?",
        yes_price=yes_price,
        volume_24h=10_000.0,
        resolves_at=datetime(2026, 5, 1, tzinfo=UTC),
        orderbook={
            "bids": [{"price": yes_price - 0.01, "size": 100.0}],
            "asks": [{"price": yes_price + 0.01, "size": 100.0}],
        },
        external_signal=external_signal or {},
        fetched_at=datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _active_strategy(
    *,
    composition: Sequence[FactorCompositionStep] = DEFAULT_STRATEGY_COMPOSITION,
) -> ActiveStrategy:
    return ActiveStrategy(
        strategy_id="default",
        strategy_version_id="default-live-blockers",
        config=StrategyConfig(
            strategy_id="default",
            factor_composition=tuple(composition),
            metadata=(
                ("owner", "system"),
                ("live_allowed", "true"),
                ("alpha_source", "warehouse_flb_decile_model_v1"),
                ("edge_model_source", "paper_soak_net_edge_model_v1"),
                ("calibration_source", "paper_soak_eval_records_v1"),
                ("evidence_source", "paper_soak_go_report_v1"),
            ),
        ),
        risk=RiskParams(
            max_position_notional_usdc=1_000.0,
            max_daily_drawdown_pct=0.0,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl")),
        forecaster=ForecasterSpec(
            forecasters=(("rules", ()), ("stats", ()), ("llm", ())),
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=30,
            volume_min_usdc=0.0,
        ),
    )


class ConstantForecaster:
    def __init__(self, probability: float, rationale: str = "constant") -> None:
        self.probability = probability
        self.rationale = rationale

    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return self.probability, 0.0, self.rationale

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return self.probability


class FixedSizer:
    def size(self, *, prob: float, market_price: float, portfolio: Portfolio) -> float:
        del prob, market_price, portfolio
        return 10.0


@dataclass(frozen=True)
class SnapshotReader:
    snapshot_value: FactorSnapshot

    async def snapshot(
        self,
        *,
        market_id: str,
        as_of: datetime,
        required: Sequence[FactorCompositionStep],
        strategy_id: str,
        strategy_version_id: str,
    ) -> FactorSnapshot:
        del market_id, as_of, required, strategy_id, strategy_version_id
        return self.snapshot_value


@pytest.mark.asyncio
async def test_default_strategy_does_not_trade_when_required_raw_factors_missing() -> None:
    pipeline = ControllerPipeline(
        strategy=_active_strategy(),
        factor_reader=SnapshotReader(
            FactorSnapshot(
                values={},
                missing_factors=(
                    ("fair_value_spread", ""),
                    ("metaculus_prior", ""),
                    ("yes_count", ""),
                    ("no_count", ""),
                ),
                snapshot_hash="missing-raw",
            )
        ),
        forecasters=(ConstantForecaster(0.10), ConstantForecaster(0.10)),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        settings=PMSSettings(
            mode=RunMode.LIVE,
            controller=ControllerSettings(min_volume=0.0),
            risk=RiskSettings(min_order_usdc=1.0),
        ),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    assert pipeline.last_diagnostic is not None
    assert pipeline.last_diagnostic.code == "missing_required_factors"


@pytest.mark.asyncio
async def test_strategy_does_not_trade_when_required_raw_factor_is_stale() -> None:
    pipeline = ControllerPipeline(
        strategy=_active_strategy(),
        factor_reader=SnapshotReader(
            FactorSnapshot(
                values={("metaculus_prior", ""): 0.8},
                missing_factors=(),
                stale_factors=(("metaculus_prior", ""),),
                snapshot_hash="stale-raw",
            )
        ),
        forecasters=(ConstantForecaster(0.8),),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        settings=PMSSettings(
            mode=RunMode.LIVE,
            controller=ControllerSettings(min_volume=0.0),
            risk=RiskSettings(min_order_usdc=1.0),
        ),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    assert pipeline.last_diagnostic is not None
    assert pipeline.last_diagnostic.code == "stale_required_factors"


@pytest.mark.asyncio
async def test_on_signal_emits_diagnostic_when_composition_resolution_fails() -> None:
    """A composition that cannot resolve a probability (e.g. a weighted step
    whose factor is absent and has no threshold gate, raising inside
    apply_composition) must surface as an ``error``-severity diagnostic. Without
    it, a broken strategy config looks identical to an idle controller — unsafe
    for real-money operation.
    """
    strategy = _active_strategy(
        composition=(
            FactorCompositionStep(
                factor_id="missing_weighted_factor",
                role="weighted",
                param="",
                weight=1.0,
                threshold=None,
            ),
        )
    )
    pipeline = ControllerPipeline(
        strategy=strategy,
        factor_reader=SnapshotReader(
            FactorSnapshot(
                values={},
                missing_factors=(("missing_weighted_factor", ""),),
                snapshot_hash="composition-unresolvable",
            )
        ),
        forecasters=(ConstantForecaster(0.5),),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        settings=PMSSettings(
            mode=RunMode.PAPER,
            controller=ControllerSettings(min_volume=0.0, strict_factor_gates=False),
            risk=RiskSettings(min_order_usdc=1.0),
        ),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    diagnostic = pipeline.last_diagnostic
    assert diagnostic is not None, (
        "a composition resolution failure must surface as a diagnostic, not a "
        "silent drop — it may indicate a broken strategy config"
    )
    assert diagnostic.code == "composition_resolution_failed"
    assert diagnostic.severity == "error"


@pytest.mark.asyncio
async def test_on_signal_emits_diagnostic_when_calibration_clamp_rejects() -> None:
    """When the extreme-probability clamp rejects a forecast (too few resolved
    samples to trust a near-0/near-1 estimate), the drop must surface as a
    diagnostic so the operator sees calibration is gating order flow."""
    strategy = ActiveStrategy(
        strategy_id="clamp-probe",
        strategy_version_id="clamp-probe-v1",
        config=StrategyConfig(
            strategy_id="clamp-probe",
            factor_composition=(),
            metadata=(("owner", "system"), ("live_allowed", "false")),
        ),
        risk=RiskParams(
            max_position_notional_usdc=1_000.0,
            max_daily_drawdown_pct=0.0,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl")),
        forecaster=ForecasterSpec(forecasters=(("rules", ()),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=30,
            volume_min_usdc=0.0,
        ),
        calibration=CalibrationSpec(
            enabled=True,
            shrinkage_factor=1.0,  # no shrinkage, so 0.99 stays extreme
            shrinkage_bias=0.0,
            extreme_clamp_low=0.08,
            extreme_clamp_high=0.92,
            min_resolved_for_extreme=500,  # 0 resolved samples << 500 -> reject
        ),
    )
    pipeline = ControllerPipeline(
        strategy=strategy,
        forecasters=(ConstantForecaster(0.99),),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        settings=PMSSettings(
            mode=RunMode.PAPER,
            controller=ControllerSettings(min_volume=0.0),
            risk=RiskSettings(min_order_usdc=1.0),
        ),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    diagnostic = pipeline.last_diagnostic
    assert diagnostic is not None, (
        "an extreme-probability clamp rejection must surface as a diagnostic"
    )
    assert diagnostic.code == "calibration_clamp_rejected"
    assert diagnostic.severity == "info"


def test_disabled_llm_forecaster_does_not_emit_neutral_runtime_factor() -> None:
    result = LLMForecaster(config=LLMSettings(enabled=False)).predict(_signal())

    assert result is None


def test_posterior_branch_missing_all_inputs_does_not_emit_statistical_probability() -> None:
    branch_probabilities = evaluate_branch_probabilities(
        (
            FactorCompositionStep(
                factor_id="metaculus_prior",
                role="posterior_prior",
                param="",
                weight=2.0,
                threshold=None,
            ),
            FactorCompositionStep(
                factor_id="yes_count",
                role="posterior_success",
                param="",
                weight=1.0,
                threshold=None,
            ),
            FactorCompositionStep(
                factor_id="no_count",
                role="posterior_failure",
                param="",
                weight=1.0,
                threshold=None,
            ),
        ),
        {("yes_price", ""): 0.10},
    )

    assert "statistical" not in branch_probabilities


def _live_settings(
    *,
    tif: str = "IOC",
    quote_source: Literal["postgres_snapshot", "venue_direct", "dual"] = "dual",
    secret_source: SecretSource | None = "fly",
    operator_attested: bool = True,
    operator_approval_mode: Literal["first_order", "every_order"] = "every_order",
    live_emergency_audit_path: str | None = None,
    live_first_order_audit_path: str | None = None,
    first_live_order_approval_path: str | None = None,
    live_paper_soak_report_path: str = PAPER_SOAK_GO_REPORT,
    live_operator_rehearsal_report_path: str = OPERATOR_REHEARSAL_REPORT,
    live_readiness_report_max_age_s: float = 7 * 24 * 60 * 60,
    local_secret_file: str | None = None,
    api_host: str = "127.0.0.1",
    api_token: str | None = "live-api-token",
) -> PMSSettings:
    attested_at = datetime.now(tz=UTC)
    first_order_audit_path = (
        str(_LIVE_PATH_ROOT / "first-order-audit.jsonl")
        if live_first_order_audit_path is None
        else live_first_order_audit_path
    )
    emergency_audit_path = (
        str(_LIVE_PATH_ROOT / "live-emergency-audit.jsonl")
        if live_emergency_audit_path is None
        else live_emergency_audit_path
    )
    approval_path = (
        str(_LIVE_PATH_ROOT / "first-order.json")
        if first_live_order_approval_path is None
        else first_live_order_approval_path
    )
    return PMSSettings(
        mode=RunMode.LIVE,
        live_trading_enabled=True,
        secret_source=secret_source,
        local_secret_file=local_secret_file,
        api_host=api_host,
        api_token=api_token,
        auto_migrate_default_v2=False,
        live_emergency_audit_path=emergency_audit_path,
        live_first_order_audit_path=first_order_audit_path,
        live_preflight_artifact_path=str(
            _LIVE_PATH_ROOT / "credentialed-preflight.json"
        ),
        live_exit_criteria_ratified_by=(
            "operator" if operator_attested else None
        ),
        live_exit_criteria_ratified_at=attested_at if operator_attested else None,
        live_compliance_reviewed_by="counsel" if operator_attested else None,
        live_compliance_reviewed_at=attested_at if operator_attested else None,
        live_compliance_jurisdiction=(
            "US-operator-approved" if operator_attested else None
        ),
        live_paper_soak_report_path=live_paper_soak_report_path,
        live_operator_rehearsal_report_path=live_operator_rehearsal_report_path,
        live_readiness_report_max_age_s=live_readiness_report_max_age_s,
        discord=DiscordSettings(
            webhook_url=SecretStr("https://discord.example/webhooks/live/unit"),
            alert_dir=str(_LIVE_PATH_ROOT / "discord-alerts"),
        ),
        risk=RiskSettings(
            max_position_per_market=1_000.0,
            max_total_exposure=10_000.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
            max_exposure_per_risk_group=5_000.0,
            max_quantity_shares=500.0,
            min_order_usdc=1.0,
        ),
        controller=ControllerSettings(
            time_in_force=tif,
            min_volume=0.0,
            quote_source=quote_source,
        ),
        polymarket=PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0x1111111111111111111111111111111111111111",
            operator_approval_mode=operator_approval_mode,
            first_live_order_approval_path=approval_path,
        ),
    )


def _decision(
    *,
    decision_id: str = "d-live-blocker",
    time_in_force: TimeInForce = TimeInForce.IOC,
    side: Literal["BUY", "SELL"] = Side.BUY.value,
    action: Literal["BUY", "SELL"] | None = Side.BUY.value,
    intent_key: str | None = None,
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id="m-live-blocker",
        token_id="t-yes",
        venue="polymarket",
        side=side,
        notional_usdc=10.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["unit-test"],
        prob_estimate=0.7,
        expected_edge=0.2,
        time_in_force=time_in_force,
        opportunity_id=f"op-{decision_id}",
        strategy_id="default",
        strategy_version_id="default-v1",
        action=action,
        limit_price=0.4,
        outcome="YES",
        intent_key=intent_key,
    )


def test_live_mode_rejects_gtc_until_open_order_ledger_exists() -> None:
    with pytest.raises(LiveTradingDisabledError, match="LIVE GTC disabled"):
        validate_live_mode_ready(_live_settings(tif="GTC"))


def test_live_mode_rejects_unsupported_time_in_force_values() -> None:
    with pytest.raises(LiveTradingDisabledError, match="LIVE time_in_force"):
        validate_live_mode_ready(_live_settings(tif="DAY"))


def test_live_mode_ready_rejects_live_trading_enabled_outside_live_mode() -> None:
    settings = _live_settings()
    settings.mode = RunMode.PAPER

    with pytest.raises(LiveTradingDisabledError, match="mode=live"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_exposed_api_without_api_token() -> None:
    with pytest.raises(LiveTradingDisabledError, match="PMS_API_TOKEN"):
        validate_live_mode_ready(_live_settings(api_host="0.0.0.0", api_token=None))


def test_live_mode_rejects_loopback_api_without_api_token() -> None:
    with pytest.raises(LiveTradingDisabledError, match="PMS_API_TOKEN"):
        validate_live_mode_ready(_live_settings(api_host="127.0.0.1", api_token=None))


def test_live_mode_rejects_discord_alert_dir_inside_working_tree() -> None:
    settings = _live_settings()
    settings.discord = DiscordSettings(
        webhook_url=SecretStr("https://discord.example/webhooks/a/b"),
        alert_dir=".alerts",
    )

    with pytest.raises(LiveTradingDisabledError, match="discord.alert_dir"):
        validate_live_mode_ready(settings)


def test_live_mode_accepts_private_discord_alert_dir_outside_working_tree() -> None:
    settings = _live_settings()
    settings.discord = DiscordSettings(
        webhook_url=SecretStr("https://discord.example/webhooks/a/b"),
        alert_dir=str(_LIVE_PATH_ROOT / "discord-alerts"),
    )

    validate_live_mode_ready(settings)


def test_live_mode_requires_discord_webhook_for_operator_alerting() -> None:
    settings = _live_settings()
    settings.discord = DiscordSettings(
        webhook_url=None,
        alert_dir=str(_LIVE_PATH_ROOT / "discord-alerts"),
    )

    with pytest.raises(LiveTradingDisabledError, match="discord.webhook_url"):
        validate_live_mode_ready(settings)


@pytest.mark.parametrize(
    ("path_name", "label"),
    (
        ("operator_approval_path", "operator approval"),
        ("first_order_audit_path", "first-order audit"),
        ("emergency_audit_path", "emergency audit"),
        ("preflight_artifact_path", "preflight artifact"),
    ),
)
def test_live_mode_rejects_discord_alert_dir_reusing_launch_control_path(
    path_name: str,
    label: str,
) -> None:
    settings = _live_settings()
    approval_path = settings.polymarket.first_live_order_approval_path
    assert approval_path is not None
    candidates = {
        "operator_approval_path": approval_path,
        "operator_approval_sidecar": f"{approval_path}.meta.json",
        "first_order_audit_path": settings.live_first_order_audit_path,
        "emergency_audit_path": settings.live_emergency_audit_path,
        "preflight_artifact_path": str(_LIVE_PATH_ROOT / "credentialed-preflight.json"),
    }
    if path_name == "preflight_artifact_path":
        settings.live_preflight_artifact_path = candidates[path_name]
    settings.discord = DiscordSettings(
        webhook_url=SecretStr("https://discord.example/webhooks/a/b"),
        alert_dir=candidates[path_name],
    )

    with pytest.raises(LiveTradingDisabledError, match=f"discord.alert_dir.*{label}"):
        validate_live_mode_ready(settings)


@pytest.mark.parametrize(
    ("path_name", "label"),
    (
        ("operator_approval_path", "operator approval"),
        ("first_order_audit_path", "first-order audit"),
        ("emergency_audit_path", "emergency audit"),
        ("preflight_artifact_path", "preflight artifact"),
    ),
)
def test_live_mode_rejects_discord_alert_dir_containing_launch_control_path(
    path_name: str,
    label: str,
) -> None:
    alert_dir = _LIVE_PATH_ROOT / f"shared-alert-launch-control-{path_name}"
    alert_dir.mkdir(mode=0o700, exist_ok=True)
    settings = _live_settings()
    if path_name == "operator_approval_path":
        settings.polymarket.first_live_order_approval_path = str(
            alert_dir / "first-order.json"
        )
    elif path_name == "first_order_audit_path":
        settings.live_first_order_audit_path = str(
            alert_dir / "first-order-audit.jsonl"
        )
    elif path_name == "emergency_audit_path":
        settings.live_emergency_audit_path = str(
            alert_dir / "live-emergency-audit.jsonl"
        )
    else:
        settings.live_preflight_artifact_path = str(
            alert_dir / "credentialed-preflight.json"
        )
    settings.discord = DiscordSettings(
        webhook_url=SecretStr("https://discord.example/webhooks/a/b"),
        alert_dir=str(alert_dir),
    )

    with pytest.raises(LiveTradingDisabledError, match=f"discord.alert_dir.*{label}"):
        validate_live_mode_ready(settings)


def test_live_preflight_settings_fingerprint_binds_discord_webhook_secret() -> None:
    settings = _live_settings()
    alert_dir = str(_LIVE_PATH_ROOT / "discord-alerts-fingerprint")
    settings.discord = DiscordSettings(
        webhook_url=SecretStr("https://discord.example/webhooks/a/b"),
        alert_dir=alert_dir,
    )
    changed = settings.model_copy(deep=True)
    changed.discord = DiscordSettings(
        webhook_url=SecretStr("https://discord.example/webhooks/a/c"),
        alert_dir=alert_dir,
    )

    assert live_preflight_settings_fingerprint(settings) != (
        live_preflight_settings_fingerprint(changed)
    )


def test_live_mode_rejects_placeholder_api_token() -> None:
    with pytest.raises(LiveTradingDisabledError, match="api_token.*placeholder"):
        validate_live_mode_ready(
            _live_settings(
                api_host="0.0.0.0",
                api_token="__FILL_IN_API_TOKEN__",
            )
        )


def test_live_mode_rejects_overwide_preflight_artifact_freshness_window() -> None:
    settings = _live_settings()
    settings.live_preflight_artifact_max_age_s = (60 * 60) + 1

    with pytest.raises(
        LiveTradingDisabledError,
        match="credentialed preflight artifact freshness window",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_overwide_readiness_report_freshness_window() -> None:
    settings = _live_settings(
        live_readiness_report_max_age_s=(7 * 24 * 60 * 60) + 1
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="readiness report freshness window",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_requires_configured_preflight_artifact_path() -> None:
    settings = _live_settings()
    settings.live_preflight_artifact_path = None

    with pytest.raises(
        LiveTradingDisabledError,
        match="live_preflight_artifact_path",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_postgres_snapshot_quote_source() -> None:
    with pytest.raises(LiveTradingDisabledError, match="LIVE quote_source"):
        validate_live_mode_ready(_live_settings(quote_source="postgres_snapshot"))


def test_live_mode_rejects_relaxed_strict_factor_gates() -> None:
    settings = _live_settings()
    settings.controller.strict_factor_gates = False

    with pytest.raises(LiveTradingDisabledError, match="strict_factor_gates"):
        validate_live_mode_ready(settings)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("max_position_per_market", float("nan")),
        ("max_total_exposure", 0.0),
        ("max_daily_loss_usdc", None),
        ("max_daily_loss_usdc", float("inf")),
        ("min_order_usdc", -1.0),
        ("max_drawdown_pct", 0.0),
        ("max_exposure_per_risk_group", float("-inf")),
        ("max_quantity_shares", -1.0),
        ("max_open_positions", 0),
        ("slippage_threshold_bps", float("nan")),
        ("slippage_threshold_bps", -1.0),
    ],
)
def test_live_mode_rejects_invalid_risk_envelope(
    field_name: str,
    value: object,
) -> None:
    settings = _live_settings()
    setattr(settings.risk, field_name, value)

    with pytest.raises(LiveTradingDisabledError, match=field_name):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_min_order_above_position_cap() -> None:
    settings = _live_settings()
    settings.risk.min_order_usdc = settings.risk.max_position_per_market + 1.0

    with pytest.raises(LiveTradingDisabledError, match="min_order_usdc"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_position_cap_above_total_exposure_cap() -> None:
    settings = _live_settings()
    settings.risk.max_position_per_market = settings.risk.max_total_exposure + 1.0

    with pytest.raises(LiveTradingDisabledError, match="max_position_per_market"):
        validate_live_mode_ready(settings)


def test_live_mode_requires_risk_group_exposure_cap() -> None:
    settings = _live_settings()
    settings.risk.max_exposure_per_risk_group = None

    with pytest.raises(LiveTradingDisabledError, match="max_exposure_per_risk_group"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_risk_group_cap_above_total_exposure_cap() -> None:
    settings = _live_settings()
    settings.risk.max_exposure_per_risk_group = settings.risk.max_total_exposure + 1.0

    with pytest.raises(LiveTradingDisabledError, match="max_exposure_per_risk_group"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_risk_group_cap_below_min_order() -> None:
    settings = _live_settings()
    settings.risk.max_exposure_per_risk_group = settings.risk.min_order_usdc / 2.0

    with pytest.raises(LiveTradingDisabledError, match="max_exposure_per_risk_group"):
        validate_live_mode_ready(settings)


def test_live_mode_requires_drawdown_cap() -> None:
    settings = _live_settings()
    settings.risk.max_drawdown_pct = None

    with pytest.raises(LiveTradingDisabledError, match="max_drawdown_pct"):
        validate_live_mode_ready(settings)


def test_live_mode_requires_max_open_positions_cap() -> None:
    settings = _live_settings()
    settings.risk.max_open_positions = None

    with pytest.raises(LiveTradingDisabledError, match="max_open_positions"):
        validate_live_mode_ready(settings)


def test_live_mode_requires_max_quantity_shares_cap() -> None:
    settings = _live_settings()
    settings.risk.max_quantity_shares = None

    with pytest.raises(LiveTradingDisabledError, match="max_quantity_shares"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_enabled_agent_strategy_runtime() -> None:
    settings = _live_settings()
    settings.agent_strategy_runtime_enabled = True

    with pytest.raises(LiveTradingDisabledError, match="agent strategy runtime"):
        validate_live_mode_ready(settings)


def test_live_mode_requires_approved_secret_source_marker() -> None:
    with pytest.raises(LiveTradingDisabledError, match="PMS_SECRET_SOURCE=local_file"):
        validate_live_mode_ready(_live_settings(secret_source=None))


def test_live_mode_rejects_local_secret_file_with_fly_secret_source(
    tmp_path: Path,
) -> None:
    secret_dir = tmp_path / "secure-secrets"
    secret_dir.mkdir(mode=0o700)
    secret_path = secret_dir / "polymarket.local-secrets.yaml"
    secret_path.write_text("polymarket: {}\n", encoding="utf-8")
    secret_path.chmod(0o600)
    settings = _live_settings(
        secret_source="fly",
        local_secret_file=str(secret_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="local_secret_file"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_local_secret_file_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_path = tmp_path / "polymarket.local-secrets.yaml"
    secret_path.write_text("polymarket: {}\n", encoding="utf-8")
    secret_path.chmod(0o600)
    monkeypatch.chdir(tmp_path)

    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(secret_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_placeholder_local_secret_file_path(tmp_path: Path) -> None:
    secret_dir = tmp_path / "secure-secrets"
    secret_dir.mkdir(mode=0o700)
    secret_path = secret_dir / "__FILL_IN_POLYMARKET_SECRET_FILE__.yaml"
    secret_path.write_text("polymarket: {}\n", encoding="utf-8")
    secret_path.chmod(0o600)
    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(secret_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="local secret file path"):
        validate_live_mode_ready(settings)


@pytest.mark.parametrize(
    ("collision_name", "expected_match"),
    (
        ("operator_approval_path", "operator approval path.*local secret file"),
        ("first_order_audit_path", "first-order audit path.*local secret file"),
        ("emergency_audit_path", "emergency audit path.*local secret file"),
        ("preflight_artifact_path", "preflight artifact path.*local secret file"),
    ),
)
def test_live_mode_rejects_launch_control_path_reusing_local_secret_file(
    tmp_path: Path,
    collision_name: str,
    expected_match: str,
) -> None:
    secret_dir = tmp_path / "secure-secrets"
    secret_dir.mkdir(mode=0o700)
    secret_path = secret_dir / "polymarket.local-secrets.yaml"
    secret_path.write_text("polymarket: {}\n", encoding="utf-8")
    secret_path.chmod(0o600)
    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(secret_path),
    )
    if collision_name == "operator_approval_path":
        settings.polymarket.first_live_order_approval_path = str(secret_path)
    elif collision_name == "first_order_audit_path":
        settings.live_first_order_audit_path = str(secret_path)
    elif collision_name == "emergency_audit_path":
        settings.live_emergency_audit_path = str(secret_path)
    elif collision_name == "preflight_artifact_path":
        settings.live_preflight_artifact_path = str(secret_path)
    else:
        raise AssertionError(f"unhandled collision case: {collision_name}")

    with pytest.raises(LiveTradingDisabledError, match=expected_match):
        validate_live_mode_ready(
            settings,
            allow_pending_operator_approval=True,
        )


def test_live_mode_rejects_local_secret_file_inside_repo_from_subdirectory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    workdir = repo_root / "dashboard"
    workdir.mkdir()
    secret_path = repo_root / "secrets" / "polymarket.local-secrets.yaml"
    secret_path.parent.mkdir()
    secret_path.write_text("polymarket: {}\n", encoding="utf-8")
    secret_path.chmod(0o600)
    monkeypatch.chdir(workdir)

    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(secret_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_repo_symlink_to_external_local_secret_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    outside_secret = tmp_path / "outside" / "polymarket.local-secrets.yaml"
    outside_secret.parent.mkdir()
    outside_secret.write_text("polymarket: {}\n", encoding="utf-8")
    outside_secret.chmod(0o600)
    symlink_path = repo_root / "polymarket.local-secrets.yaml"
    symlink_path.symlink_to(outside_secret)
    monkeypatch.chdir(repo_root)

    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(symlink_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_local_secret_file_inside_repo_when_started_elsewhere(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    secret_path = repo_root / "secrets" / "polymarket.local-secrets.yaml"
    secret_path.parent.mkdir()
    secret_path.write_text("polymarket: {}\n", encoding="utf-8")
    secret_path.chmod(0o600)
    launcher_dir = tmp_path / "launcher"
    launcher_dir.mkdir()
    monkeypatch.chdir(launcher_dir)

    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(secret_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_local_secret_file_in_permissive_parent(
    tmp_path: Path,
) -> None:
    secret_dir = tmp_path / "shared-secrets"
    secret_dir.mkdir(mode=0o700)
    secret_dir.chmod(0o777)
    secret_path = secret_dir / "polymarket.local-secrets.yaml"
    secret_path.write_text("polymarket: {}\n", encoding="utf-8")
    secret_path.chmod(0o600)
    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(secret_path),
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="parent directory"):
            validate_live_mode_ready(settings)
    finally:
        secret_dir.chmod(0o700)


def test_live_mode_rejects_local_secret_file_in_symlink_parent(
    tmp_path: Path,
) -> None:
    secret_dir = tmp_path / "target-secrets"
    secret_dir.mkdir(mode=0o700)
    secret_path = secret_dir / "polymarket.local-secrets.yaml"
    secret_path.write_text("polymarket: {}\n", encoding="utf-8")
    secret_path.chmod(0o600)
    symlink_parent = tmp_path / "linked-secrets"
    symlink_parent.symlink_to(secret_dir, target_is_directory=True)
    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(symlink_parent / secret_path.name),
    )

    with pytest.raises(LiveTradingDisabledError, match="parent path is not a directory"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_local_secret_file_symlink_outside_repo(
    tmp_path: Path,
) -> None:
    target_dir = tmp_path / "target-secrets"
    target_dir.mkdir(mode=0o700)
    target_secret = target_dir / "polymarket.local-secrets.yaml"
    target_secret.write_text("polymarket: {}\n", encoding="utf-8")
    target_secret.chmod(0o600)
    link_dir = tmp_path / "link-secrets"
    link_dir.mkdir(mode=0o700)
    symlink_path = link_dir / "polymarket.local-secrets.yaml"
    symlink_path.symlink_to(target_secret)
    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(symlink_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="regular file"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_hardlinked_local_secret_file_outside_repo(
    tmp_path: Path,
) -> None:
    target_dir = tmp_path / "target-secrets"
    target_dir.mkdir(mode=0o700)
    target_secret = target_dir / "polymarket.local-secrets.yaml"
    target_secret.write_text("polymarket: {}\n", encoding="utf-8")
    target_secret.chmod(0o600)
    link_dir = tmp_path / "link-secrets"
    link_dir.mkdir(mode=0o700)
    hardlink_path = link_dir / "polymarket.local-secrets.yaml"
    os.link(target_secret, hardlink_path)
    settings = _live_settings(
        secret_source="local_file",
        local_secret_file=str(hardlink_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="single-link"):
        validate_live_mode_ready(settings)


def test_live_mode_requires_exit_criteria_and_compliance_attestations() -> None:
    with pytest.raises(LiveTradingDisabledError, match="operator readiness"):
        validate_live_mode_ready(_live_settings(operator_attested=False))


def test_live_mode_requires_every_order_operator_approval() -> None:
    with pytest.raises(LiveTradingDisabledError, match="every_order"):
        validate_live_mode_ready(
            _live_settings(operator_approval_mode="first_order")
        )


def test_live_mode_rejects_overwide_operator_approval_freshness_window() -> None:
    settings = _live_settings()
    settings.polymarket.operator_approval_max_age_s = (5 * 60) + 1

    with pytest.raises(
        LiveTradingDisabledError,
        match="operator approval freshness window",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_requires_operator_approval_path() -> None:
    settings = _live_settings()
    settings.polymarket.first_live_order_approval_path = None

    with pytest.raises(LiveTradingDisabledError, match="approval path"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_missing_operator_approval_parent(tmp_path: Path) -> None:
    settings = _live_settings(
        first_live_order_approval_path=str(tmp_path / "missing" / "first-order.json")
    )

    with pytest.raises(LiveTradingDisabledError, match="approval parent directory"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_approval_path_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    approval_dir = repo_root / "secure"
    approval_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    settings = _live_settings(
        first_live_order_approval_path=str(approval_dir / "first-order.json")
    )

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_non_owner_writable_operator_approval_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    approval_dir.chmod(0o500)
    settings = _live_settings(
        first_live_order_approval_path=str(approval_dir / "first-order.json")
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="owner-writable"):
            validate_live_mode_ready(settings)
    finally:
        approval_dir.chmod(0o700)


def test_live_mode_rejects_permissive_operator_approval_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "permissive"
    approval_dir.mkdir(mode=0o700)
    approval_dir.chmod(0o755)
    settings = _live_settings(
        first_live_order_approval_path=str(approval_dir / "first-order.json")
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="too permissive"):
            validate_live_mode_ready(settings)
    finally:
        approval_dir.chmod(0o700)


def test_live_mode_rejects_symlink_operator_approval_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    symlink_parent = tmp_path / "approval-parent-link"
    symlink_parent.symlink_to(approval_dir, target_is_directory=True)
    settings = _live_settings(
        first_live_order_approval_path=str(symlink_parent / "first-order.json")
    )

    with pytest.raises(LiveTradingDisabledError, match="not a directory"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_stale_operator_approval_file(tmp_path: Path) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    approval_path = approval_dir / "first-order.json"
    approval_path.write_text('{"approved": true}\n', encoding="utf-8")
    settings = _live_settings(first_live_order_approval_path=str(approval_path))

    with pytest.raises(LiveTradingDisabledError, match="stale approval file"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_stale_operator_approval_sidecar(tmp_path: Path) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    approval_path = approval_dir / "first-order.json"
    sidecar_path = Path(str(approval_path) + ".meta.json")
    sidecar_path.write_text('{"approver_id": "operator-alice"}\n', encoding="utf-8")
    settings = _live_settings(first_live_order_approval_path=str(approval_path))

    with pytest.raises(LiveTradingDisabledError, match="stale approval sidecar"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_approval_symlink(tmp_path: Path) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    target_path = tmp_path / "target-approval.json"
    target_path.write_text('{"approved": true}\n', encoding="utf-8")
    approval_path = approval_dir / "first-order.json"
    approval_path.symlink_to(target_path)
    settings = _live_settings(first_live_order_approval_path=str(approval_path))

    with pytest.raises(LiveTradingDisabledError, match="regular file"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_missing_polymarket_live_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_find_spec(
        name: str,
        package: str | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        del package
        if name == "py_clob_client_v2":
            return None
        return importlib.machinery.ModuleSpec(name, loader=None)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    with pytest.raises(LiveTradingDisabledError) as exc_info:
        validate_live_mode_ready(_live_settings())

    message = str(exc_info.value)
    assert "LIVE Polymarket dependency missing" in message
    assert "py_clob_client_v2" in message
    assert "uv sync --extra live" in message


def test_live_mode_skips_polymarket_sdk_check_for_injected_non_sdk_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # require_live_mode=False is the signal an injected client (a test double
    # or replay harness) does not drive the real Polymarket SDK, so the
    # py_clob_client_v2 runtime dependency must not be required. This keeps the
    # actuator's mock-client integration paths runnable while every production
    # caller (default require_live_mode=True) stays gated by the assertion
    # above and by live preflight's independent find_spec check.
    def fake_find_spec(
        name: str,
        package: str | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        del package
        if name == "py_clob_client_v2":
            return None
        return importlib.machinery.ModuleSpec(name, loader=None)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    credentials = validate_live_mode_ready(
        _live_settings(),
        require_live_mode=False,
    )

    assert credentials.api_key == "api-key"


def test_live_mode_rejects_enabled_llm_when_provider_sdk_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_find_spec(
        name: str,
        package: str | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        del package
        if name == "anthropic":
            return None
        return importlib.machinery.ModuleSpec(name, loader=None)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    settings = _live_settings()
    settings.llm.enabled = True
    settings.llm.provider = "anthropic"
    settings.llm.api_key = "sk-test"

    with pytest.raises(LiveTradingDisabledError) as exc_info:
        validate_live_mode_ready(settings)

    message = str(exc_info.value)
    assert "LIVE LLM dependency missing" in message
    assert "anthropic" in message
    assert "uv sync --extra llm" in message


def test_live_mode_rejects_unfilled_operator_readiness_placeholders() -> None:
    settings = _live_settings()
    settings.live_exit_criteria_ratified_by = "__FILL_IN_OPERATOR_ID__"
    settings.live_compliance_reviewed_by = "<reviewer-id>"
    settings.live_compliance_jurisdiction = "__FILL_IN_JURISDICTION__"

    with pytest.raises(LiveTradingDisabledError, match="placeholder"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_placeholder_polymarket_credentials() -> None:
    settings = _live_settings()
    settings.polymarket.private_key = "<paste private key>"
    settings.polymarket.api_secret = "__FILL_IN_POLYMARKET_API_SECRET__"

    with pytest.raises(
        LiveTradingDisabledError,
        match="Placeholder Polymarket credential fields: private_key, api_secret",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_unsupported_polymarket_signature_type() -> None:
    settings = _live_settings()
    settings.polymarket.signature_type = 99

    with pytest.raises(
        LiveTradingDisabledError,
        match="Invalid Polymarket signature_type: 99; expected one of 0, 1, 2, 3",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_accepts_polymarket_deposit_wallet_signature_type() -> None:
    settings = _live_settings()
    settings.polymarket.signature_type = 3

    credentials = validate_live_mode_ready(settings)

    assert credentials.signature_type == 3


def test_live_mode_rejects_malformed_polymarket_funder_address() -> None:
    settings = _live_settings()
    settings.polymarket.funder_address = "0xabc"

    with pytest.raises(
        LiveTradingDisabledError,
        match="Invalid Polymarket funder_address: expected 0x-prefixed 40 hex characters",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_future_operator_readiness_timestamps() -> None:
    settings = _live_settings()
    future = datetime.now(tz=UTC) + timedelta(days=1)
    settings.live_exit_criteria_ratified_at = future
    settings.live_compliance_reviewed_at = future

    with pytest.raises(LiveTradingDisabledError, match="future"):
        validate_live_mode_ready(settings)


def test_live_mode_requires_passing_paper_soak_go_report() -> None:
    settings = _live_settings()
    settings.live_paper_soak_report_path = None

    with pytest.raises(LiveTradingDisabledError, match="paper soak"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_test_fixture_paper_soak_go_report() -> None:
    settings = _live_settings(
        live_paper_soak_report_path=FIXTURE_PAPER_SOAK_GO_REPORT
    )

    with pytest.raises(LiveTradingDisabledError, match="test fixture"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    report_dir = repo_root / "secure-reports"
    report_dir.mkdir(mode=0o700)
    report_path = report_dir / "paper-soak-go-report.md"
    _copy_report_with_output_path(PAPER_SOAK_GO_REPORT, report_path)
    monkeypatch.chdir(repo_root)
    settings = _live_settings(live_paper_soak_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_symlink_paper_soak_go_report() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-report-symlink-"
    )
    report_path = Path(paper_report_path)
    symlink_path = report_path.parent / "paper-soak-go-link.md"
    symlink_path.symlink_to(report_path)
    settings = _live_settings(
        live_paper_soak_report_path=str(symlink_path),
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="regular file"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_hardlinked_paper_soak_go_report() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-report-hardlink-"
    )
    report_path = Path(paper_report_path)
    hardlink_path = report_path.parent / "paper-soak-go-hardlink.md"
    os.link(report_path, hardlink_path)
    settings = _live_settings(
        live_paper_soak_report_path=str(hardlink_path),
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="single-link"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_permissive_paper_soak_report_parent() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-report-permissive-parent-"
    )
    parent = Path(paper_report_path).parent
    parent.chmod(0o755)
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="parent directory"):
            validate_live_mode_ready(settings)
    finally:
        parent.chmod(0o700)


def test_live_mode_rejects_symlink_paper_soak_report_parent(
    tmp_path: Path,
) -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-report-symlink-parent-"
    )
    report_path = Path(paper_report_path)
    symlink_parent = tmp_path / "paper-report-parent-link"
    symlink_parent.symlink_to(report_path.parent, target_is_directory=True)
    settings = _live_settings(
        live_paper_soak_report_path=str(symlink_parent / report_path.name),
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="not a directory"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_symlink_operator_rehearsal_report_parent(
    tmp_path: Path,
) -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-rehearsal-report-symlink-parent-"
    )
    report_path = Path(rehearsal_report_path)
    symlink_parent = tmp_path / "rehearsal-report-parent-link"
    symlink_parent.symlink_to(report_path.parent, target_is_directory=True)
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=str(symlink_parent / report_path.name),
    )

    with pytest.raises(LiveTradingDisabledError, match="not a directory"):
        validate_live_mode_ready(settings)


def test_live_mode_opens_readiness_reports_with_no_follow_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-readiness-no-follow-"
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )
    expected_paths = {Path(paper_report_path), Path(rehearsal_report_path)}
    observed: list[tuple[Path, int]] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        observed.append((Path(os.fsdecode(os.fspath(path_arg))), flags))
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", recording_open)

    validate_live_mode_ready(settings)

    observed_by_path = {path: flags for path, flags in observed}
    assert expected_paths <= set(observed_by_path)
    assert all(observed_by_path[path] & no_follow_flag for path in expected_paths)


def test_live_mode_rejects_dry_run_paper_soak_report(tmp_path: Path) -> None:
    report_path = tmp_path / "paper-soak-dry-run.md"
    report_text = Path(PAPER_SOAK_GO_REPORT).read_text(encoding="utf-8")
    report_path.write_text(
        report_text.replace(
            "| artifact_mode | persisted |",
            "| artifact_mode | dry_run |",
        ).replace(
            f"| output_path | {PAPER_SOAK_GO_REPORT} |",
            "| output_path | stdout |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(live_paper_soak_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="persisted"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_report_missing_generated_at(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "paper-soak-missing-generated-at.md"
    report_text = Path(PAPER_SOAK_GO_REPORT).read_text(encoding="utf-8")
    report_path.write_text(
        "\n".join(
            line
            for line in report_text.replace(
                f"| output_path | {PAPER_SOAK_GO_REPORT} |",
                f"| output_path | {report_path} |",
            ).splitlines()
            if not line.startswith("| generated_at |")
        ),
        encoding="utf-8",
    )
    settings = _live_settings(live_paper_soak_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="generated_at"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_report_output_path_mismatch(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "paper-soak-go.md"
    other_path = tmp_path / "other-paper-soak-go.md"
    report_text = Path(PAPER_SOAK_GO_REPORT).read_text(encoding="utf-8")
    report_path.write_text(
        report_text.replace(
            f"| output_path | {PAPER_SOAK_GO_REPORT} |",
            f"| output_path | {other_path} |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(live_paper_soak_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="output_path"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_report_duplicate_provenance_field() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-duplicate-paper-provenance-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| output_path |",
            "| output_path | stdout |\n| output_path |",
            1,
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="duplicate provenance"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_provenance_row_with_extra_cells() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-provenance-extra-cell-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            next(
                line
                for line in report_path.read_text(encoding="utf-8").splitlines()
                if line.startswith("| generated_at |")
            ),
            "| generated_at | 2026-05-25T00:00:00+00:00 | TODO: hidden extra cell |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="malformed provenance row: generated_at",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_stale_paper_soak_report_generated_at() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-stale-paper-report-"
    )
    now = datetime.now(tz=UTC)
    stale_generated_at = now - timedelta(hours=2, seconds=1)
    fresh_generated_at = now - timedelta(seconds=10)
    attested_at = now - timedelta(seconds=1)
    _replace_report_provenance_field(
        paper_report_path,
        field_name="generated_at",
        value=stale_generated_at.isoformat(),
    )
    _replace_report_provenance_field(
        rehearsal_report_path,
        field_name="generated_at",
        value=fresh_generated_at.isoformat(),
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
        live_readiness_report_max_age_s=60 * 60,
    )
    settings.live_exit_criteria_ratified_at = attested_at
    settings.live_compliance_reviewed_at = attested_at

    with pytest.raises(LiveTradingDisabledError, match="paper soak GO report is stale"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_no_go_paper_soak_report(tmp_path: Path) -> None:
    report_path = tmp_path / "paper-soak-no-go.md"
    report_path.write_text(
        "\n".join(
            [
                "# Paper Daily Report - 2026-05-30",
                "",
                "## Go/No-Go Gate",
                "",
                "**Decision:** NO-GO",
                "",
                "| Check | Status | Detail |",
                "|---|---|---|",
                "| brier_improvement | FAIL | -0.01 < 0.0 |",
            ]
        ),
        encoding="utf-8",
    )
    settings = _live_settings()
    settings.live_paper_soak_report_path = str(report_path)

    with pytest.raises(LiveTradingDisabledError, match="GO decision"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_missing_gate_rows(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "paper-soak-forged-go.md"
    report_path.write_text(
        "\n".join(
            [
                "# Paper Daily Report - 2026-05-30",
                "",
                "## Go/No-Go Gate",
                "",
                "**Decision:** GO",
                "",
                "| Check | Status | Detail |",
                "|---|---|---|",
                "| soak_days | PASS | 30 >= 30 |",
                "| decisions_accepted | PASS | 30 >= 30 |",
                "| fills | PASS | 50 >= 50 |",
            ]
        ),
        encoding="utf-8",
    )
    settings = _live_settings()
    settings.live_paper_soak_report_path = str(report_path)

    with pytest.raises(LiveTradingDisabledError, match="missing required gate checks"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_without_baseline_evidence_section() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-missing-baseline-evidence-"
    )
    report_path = Path(paper_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_path.write_text(
        _remove_markdown_section(report_text, "## Baseline Evidence Coverage"),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="Baseline Evidence Coverage"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_without_secondary_baseline_section() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-missing-secondary-baseline-"
    )
    report_path = Path(paper_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_path.write_text(
        _remove_markdown_section(report_text, "## Secondary Baseline Brier"),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="Secondary Baseline Brier"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_incomplete_mid_quote_baseline() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-incomplete-mid-quote-baseline-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| mid_quote | 50 / 50 | 100.0% |",
            "| mid_quote | 49 / 50 | 98.0% |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="mid_quote coverage"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_duplicate_baseline_coverage_rows() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-duplicate-baseline-coverage-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| mid_quote | 50 / 50 | 100.0% |",
            "\n".join(
                [
                    "| mid_quote | 49 / 50 | 98.0% |",
                    "| mid_quote | 50 / 50 | 100.0% |",
                ]
            ),
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="duplicate baseline coverage rows"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_contradictory_baseline_coverage_percent() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-contradictory-baseline-coverage-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| mid_quote | 50 / 50 | 100.0% |",
            "| mid_quote | 50 / 50 | 0.0% |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="mid_quote coverage percentage"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_malformed_baseline_coverage_percent() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-malformed-baseline-coverage-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| category_prior | 50 / 50 | 100.0% |",
            "| category_prior | 50 / 50 | complete |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="category_prior coverage percentage"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_baseline_coverage_denominator_mismatch() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-baseline-denominator-mismatch-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| last_trade | 40 / 50 | 80.0% |",
            "| last_trade | 40 / 40 | 100.0% |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="last_trade coverage total differs from reported decision set",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_baseline_coverage_denominator_mismatch_before_required_rows() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-baseline-denominator-mismatch-before-required-"
    )
    report_path = Path(paper_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_text = report_text.replace("| last_trade | 40 / 50 | 80.0% |\n", "")
    report_text = report_text.replace(
        "| market_implied | 50 / 50 | 100.0% |",
        "\n".join(
            [
                "| last_trade | 40 / 40 | 100.0% |",
                "| market_implied | 50 / 50 | 100.0% |",
            ]
        ),
    )
    report_path.write_text(report_text, encoding="utf-8")
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="last_trade coverage total differs from reported decision set",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_blank_baseline_source_label() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-blank-baseline-source-"
    )
    report_path = Path(paper_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_text = report_text.replace(
        "| last_trade | 40 / 50 | 80.0% |",
        "|  | 40 / 50 | 80.0% |",
    )
    report_text = report_text.replace(
        "| last_trade | 0.2400 | 0.0300 |",
        "|  | 0.2400 | 0.0300 |",
    )
    report_path.write_text(report_text, encoding="utf-8")
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="invalid baseline source label"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_placeholder_baseline_source_label() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-fake-baseline-source-"
    )
    report_path = Path(paper_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_text = report_text.replace(
        "| last_trade | 40 / 50 | 80.0% |",
        "| TODO_BASELINE | 40 / 50 | 80.0% |",
    )
    report_text = report_text.replace(
        "| last_trade | 0.2400 | 0.0300 |",
        "| TODO_BASELINE | 0.2400 | 0.0300 |",
    )
    report_path.write_text(report_text, encoding="utf-8")
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="invalid baseline source label"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_prose_baseline_source_label() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-prose-baseline-source-"
    )
    report_path = Path(paper_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_text = report_text.replace(
        "| last_trade | 40 / 50 | 80.0% |",
        "| last trade baseline | 40 / 50 | 80.0% |",
    )
    report_text = report_text.replace(
        "| last_trade | 0.2400 | 0.0300 |",
        "| last trade baseline | 0.2400 | 0.0300 |",
    )
    report_path.write_text(report_text, encoding="utf-8")
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="invalid baseline source label"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_digit_prefixed_baseline_source_label() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-digit-baseline-source-"
    )
    report_path = Path(paper_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_text = report_text.replace(
        "| last_trade | 40 / 50 | 80.0% |",
        "| 7day_baseline | 40 / 50 | 80.0% |",
    )
    report_text = report_text.replace(
        "| last_trade | 0.2400 | 0.0300 |",
        "| 7day_baseline | 0.2400 | 0.0300 |",
    )
    report_path.write_text(report_text, encoding="utf-8")
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="invalid baseline source label"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_secondary_baseline_without_coverage_evidence() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-secondary-without-coverage-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| last_trade | 40 / 50 | 80.0% |\n",
            "",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="secondary baseline rows without coverage evidence: last_trade",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_secondary_baseline_with_zero_covered_decisions() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-secondary-zero-coverage-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| last_trade | 40 / 50 | 80.0% |",
            "| last_trade | 0 / 50 | 0.0% |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="last_trade coverage has no decision evidence",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_optional_baseline_coverage_with_zero_total_decisions() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-optional-zero-total-coverage-"
    )
    report_path = Path(paper_report_path)
    report_text = report_path.read_text(encoding="utf-8").replace(
        "| last_trade | 40 / 50 | 80.0% |",
        "| last_trade | 0 / 0 | 0.0% |",
    )
    report_path.write_text(
        report_text.replace("| last_trade | 0.2400 | 0.0300 |\n", ""),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="last_trade coverage has no decision evidence",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_covered_baseline_without_secondary_brier_score() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-covered-without-secondary-score-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| last_trade | 0.2400 | 0.0300 |\n",
            "",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="coverage evidence without secondary baseline rows: last_trade",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_incomplete_category_prior_baseline() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-incomplete-category-prior-baseline-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| category_prior | 50 / 50 | 100.0% |",
            "| category_prior | 0 / 50 | 0.0% |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="category_prior coverage"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_non_positive_secondary_brier() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-nonpositive-secondary-brier-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| mid_quote | 0.2200 | 0.0400 |",
            "| mid_quote | 0.2200 | 0.0000 |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="mid_quote improvement"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_impossible_secondary_baseline_brier() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-impossible-secondary-brier-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| mid_quote | 0.2200 | 0.0400 |",
            "| mid_quote | 1.5000 | 0.0400 |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="mid_quote baseline Brier"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_impossible_secondary_brier_improvement() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-impossible-secondary-improvement-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| mid_quote | 0.2200 | 0.0400 |",
            "| mid_quote | 0.2200 | 0.2500 |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="mid_quote improvement exceeds baseline"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_non_positive_category_prior_brier() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-nonpositive-category-prior-brier-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| category_prior | 0.2100 | 0.0200 |",
            "| category_prior | 0.2100 | -0.0100 |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="category_prior improvement"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_non_positive_emitted_baseline_brier() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-nonpositive-emitted-baseline-brier-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| last_trade | 0.2400 | 0.0300 |",
            "| last_trade | 0.2400 | -0.0100 |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="last_trade improvement"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_duplicate_secondary_baseline_rows() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-duplicate-secondary-baseline-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| last_trade | 0.2400 | 0.0300 |",
            "\n".join(
                [
                    "| last_trade | 0.2400 | -0.0100 |",
                    "| last_trade | 0.2400 | 0.0300 |",
                ]
            ),
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="duplicate secondary baseline rows"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_extra_non_pass_gate_row() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-extra-paper-row-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| risk_events | PASS | 0 risk event(s) |",
            "\n".join(
                [
                    "| risk_events | PASS | 0 risk event(s) |",
                    "| manual_operator_review | WARN | review was not completed |",
                ]
            ),
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="non-PASS gate rows"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_with_duplicate_gate_row() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-duplicate-paper-row-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| fills | PASS | 50 >= 50 |",
            "\n".join(
                [
                    "| fills | PASS | 50 >= 50 |",
                    "| fills | PASS | duplicated fill evidence |",
                ]
            ),
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="duplicate gate rows: fills"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_blank_required_detail() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-blank-paper-row-detail-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| brier_improvement | PASS | 0.05 >= 0.0 |",
            "| brier_improvement | PASS |  |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="invalid PASS details: brier_improvement",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_pass_row_with_failing_detail() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-contradictory-paper-row-detail-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| soak_days | PASS | 30 >= 30 |",
            "| soak_days | PASS | 4 < 30 |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="soak_days detail below LIVE threshold",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_placeholder_required_detail() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-row-detail-marker-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| average_net_edge_bps | PASS | 20.0 >= 0.0 |",
            "| average_net_edge_bps | PASS | TODO: fill after paper review |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="invalid PASS details: average_net_edge_bps contains placeholder",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_placeholder_after_escaped_pipe_detail() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-row-detail-escaped-marker-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| average_net_edge_bps | PASS | 20.0 >= 0.0 |",
            "| average_net_edge_bps | PASS | 20.0 >= 0.0 \\| TODO: fill after paper review |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="invalid PASS details: average_net_edge_bps contains placeholder",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_report_strategy_evidence_mismatch() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-strategy-evidence-mismatch-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| strategy_evidence | PASS | default@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 |",
            "| strategy_evidence | PASS | other@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="strategy_evidence.*Summary Strategy",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_strategy_mismatch_after_escaped_pipe() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-strategy-escaped-mismatch-"
    )
    report_path = Path(paper_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_text = report_text.replace(
        "| Strategy | default@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 | - |",
        "| Strategy | default\\|paper@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 | - |",
    )
    report_text = report_text.replace(
        "| strategy_evidence | PASS | default@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 |",
        "| strategy_evidence | PASS | default\\|other@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 |",
    )
    report_path.write_text(report_text, encoding="utf-8")
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="strategy_evidence must match Summary Strategy",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_summary_strategy_row_with_extra_cells() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-summary-extra-cell-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| Strategy | default@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 | - |",
            "| Strategy | default@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 | - | TODO: hidden extra cell |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="malformed Summary Strategy row",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_gate_row_with_extra_cells() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-row-extra-cell-"
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| strategy_evidence | PASS | default@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 |",
            "| strategy_evidence | PASS | default@4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25 | TODO: hidden extra cell |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="malformed gate row: strategy_evidence",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_go_decision_outside_gate_section(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "paper-soak-decision-outside-gate.md"
    fixture_text = Path(PAPER_SOAK_GO_REPORT).read_text(encoding="utf-8")
    report_text = fixture_text.replace(
        f"| output_path | {PAPER_SOAK_GO_REPORT} |",
        f"| output_path | {report_path} |",
    ).replace(
        "\n**Decision:** GO\n",
        "\n",
        1,
    )
    report_path.write_text(
        "\n".join(
            [
                report_text.rstrip(),
                "",
                "## Appendix",
                "",
                "**Decision:** GO",
                "",
            ]
        ),
        encoding="utf-8",
    )
    settings = _live_settings(live_paper_soak_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="GO decision"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_future_dated_paper_soak_go_report() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-future-paper-report-"
    )
    report_path = Path(paper_report_path)
    future_date = datetime.now(tz=UTC).date() + timedelta(days=1)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "# Paper Daily Report - 2026-05-25",
            f"# Paper Daily Report - {future_date.isoformat()}",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="report date"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_paper_soak_report_date_after_generated_at() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-paper-report-date-after-generated-"
    )
    now = datetime.now(tz=UTC)
    generated_at = now - timedelta(days=1)
    report_date = now.date()
    _replace_report_provenance_field(
        paper_report_path,
        field_name="generated_at",
        value=generated_at.isoformat(),
    )
    _replace_report_provenance_field(
        rehearsal_report_path,
        field_name="generated_at",
        value=generated_at.isoformat(),
    )
    report_path = Path(paper_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "# Paper Daily Report - 2026-05-25",
            f"# Paper Daily Report - {report_date.isoformat()}",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )
    settings.live_exit_criteria_ratified_at = now
    settings.live_compliance_reviewed_at = now

    with pytest.raises(LiveTradingDisabledError, match="generated_at"):
        validate_live_mode_ready(settings)


def test_live_mode_requires_passing_operator_rehearsal_report() -> None:
    approval_dir = _LIVE_PATH_ROOT / "missing-rehearsal"
    approval_dir.mkdir(mode=0o700, exist_ok=True)
    attested_at = datetime.now(tz=UTC)
    settings = PMSSettings(
        mode=RunMode.LIVE,
        secret_source="fly",
        live_trading_enabled=True,
        api_token="live-api-token",
        auto_migrate_default_v2=False,
        live_emergency_audit_path=str(
            approval_dir / "live-emergency-audit.jsonl"
        ),
        live_first_order_audit_path=str(approval_dir / "first-order-audit.jsonl"),
        live_preflight_artifact_path=str(
            approval_dir / "credentialed-preflight.json"
        ),
        live_exit_criteria_ratified_by="operator",
        live_exit_criteria_ratified_at=attested_at,
        live_compliance_reviewed_by="counsel",
        live_compliance_reviewed_at=attested_at,
        live_compliance_jurisdiction="US-operator-approved",
        live_paper_soak_report_path=PAPER_SOAK_GO_REPORT,
        risk=RiskSettings(
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
            max_exposure_per_risk_group=500.0,
            max_quantity_shares=500.0,
        ),
        controller=ControllerSettings(
            time_in_force="IOC",
            min_volume=0.0,
            quote_source="dual",
        ),
        discord=DiscordSettings(
            webhook_url=SecretStr("https://discord.example/webhooks/live/rehearsal"),
            alert_dir=str(approval_dir / "discord-alerts"),
        ),
        polymarket=PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0x1111111111111111111111111111111111111111",
            operator_approval_mode="every_order",
            first_live_order_approval_path=str(approval_dir / "first-order.json"),
        ),
    )

    with pytest.raises(LiveTradingDisabledError, match="rehearsal"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_test_fixture_operator_rehearsal_report() -> None:
    settings = _live_settings(
        live_operator_rehearsal_report_path=FIXTURE_OPERATOR_REHEARSAL_REPORT
    )

    with pytest.raises(LiveTradingDisabledError, match="test fixture"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    report_dir = repo_root / "secure-reports"
    report_dir.mkdir(mode=0o700)
    report_path = report_dir / "operator-rehearsal-report.md"
    _copy_report_with_output_path(OPERATOR_REHEARSAL_REPORT, report_path)
    monkeypatch.chdir(repo_root)
    settings = _live_settings(live_operator_rehearsal_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_symlink_operator_rehearsal_report() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-rehearsal-report-symlink-"
    )
    report_path = Path(rehearsal_report_path)
    symlink_path = report_path.parent / "operator-rehearsal-link.md"
    symlink_path.symlink_to(report_path)
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=str(symlink_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="regular file"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_hardlinked_operator_rehearsal_report() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-rehearsal-report-hardlink-"
    )
    report_path = Path(rehearsal_report_path)
    hardlink_path = report_path.parent / "operator-rehearsal-hardlink.md"
    os.link(report_path, hardlink_path)
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=str(hardlink_path),
    )

    with pytest.raises(LiveTradingDisabledError, match="single-link"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_permissive_operator_rehearsal_report_parent() -> None:
    paper_report_path, _ = make_live_report_paths(
        prefix="pms-live-rehearsal-report-private-paper-"
    )
    _, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-rehearsal-report-permissive-parent-"
    )
    parent = Path(rehearsal_report_path).parent
    parent.chmod(0o755)
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="parent directory"):
            validate_live_mode_ready(settings)
    finally:
        parent.chmod(0o700)


def test_live_mode_rejects_operator_rehearsal_report_missing_provenance(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "operator-rehearsal-forged-pass.md"
    report_path.write_text(
        "\n".join(
            [
                "# Operator Approval Rehearsal - 2026-05-25",
                "",
                "## Operator Approval Rehearsal",
                "",
                "**Decision:** PASS",
                "",
                "| Check | Status | Detail |",
                "|---|---|---|",
                "| approval_denied | PASS | observed before approval file existed |",
                "| approval_matched | PASS | approval JSON matched preview |",
                "| approval_consumed | PASS | approval JSON and sidecar were unlinked |",
                (
                    "| strict_sidecar_provenance | PASS | strict gate required "
                    "sidecar approver_id, timestamp, and approval hash |"
                ),
                (
                    "| fresh_approval_required | PASS | every-order mode denied "
                    "the next submit after approval consume |"
                ),
                (
                    "| unexpected_events | PASS | events=['approval_denied', "
                    "'approval_matched', 'approval_consumed', 'approval_denied'] |"
                ),
                "| operator_id | PASS | rehearsal-operator |",
            ]
        ),
        encoding="utf-8",
    )
    settings = _live_settings(live_operator_rehearsal_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="persisted"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_output_path_mismatch(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "operator-rehearsal-pass.md"
    other_path = tmp_path / "other-operator-rehearsal-pass.md"
    report_text = Path(OPERATOR_REHEARSAL_REPORT).read_text(encoding="utf-8")
    report_path.write_text(
        report_text.replace(
            f"| output_path | {OPERATOR_REHEARSAL_REPORT} |",
            f"| output_path | {other_path} |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(live_operator_rehearsal_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="output_path"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_duplicate_provenance_field() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-duplicate-rehearsal-provenance-"
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| artifact_mode |",
            "| artifact_mode | dry_run |\n| artifact_mode |",
            1,
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="duplicate provenance"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_stale_operator_rehearsal_report_generated_at() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-stale-rehearsal-report-"
    )
    now = datetime.now(tz=UTC)
    stale_generated_at = now - timedelta(hours=2, seconds=1)
    fresh_generated_at = now - timedelta(seconds=10)
    attested_at = now - timedelta(seconds=1)
    _replace_report_provenance_field(
        paper_report_path,
        field_name="generated_at",
        value=fresh_generated_at.isoformat(),
    )
    _replace_report_provenance_field(
        rehearsal_report_path,
        field_name="generated_at",
        value=stale_generated_at.isoformat(),
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
        live_readiness_report_max_age_s=60 * 60,
    )
    settings.live_exit_criteria_ratified_at = attested_at
    settings.live_compliance_reviewed_at = attested_at

    with pytest.raises(
        LiveTradingDisabledError,
        match="operator rehearsal report is stale",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_date_after_generated_at() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-rehearsal-date-after-generated-"
    )
    now = datetime.now(tz=UTC)
    generated_at = now - timedelta(days=1)
    report_date = now.date()
    _replace_report_provenance_field(
        paper_report_path,
        field_name="generated_at",
        value=generated_at.isoformat(),
    )
    _replace_report_provenance_field(
        rehearsal_report_path,
        field_name="generated_at",
        value=generated_at.isoformat(),
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "# Operator Approval Rehearsal - 2026-05-25",
            f"# Operator Approval Rehearsal - {report_date.isoformat()}",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )
    settings.live_exit_criteria_ratified_at = now
    settings.live_compliance_reviewed_at = now

    with pytest.raises(LiveTradingDisabledError, match="generated_at"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_missing_generated_at(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "operator-rehearsal-missing-generated-at.md"
    report_text = Path(OPERATOR_REHEARSAL_REPORT).read_text(encoding="utf-8")
    report_path.write_text(
        "\n".join(
            line
            for line in report_text.replace(
                f"| output_path | {OPERATOR_REHEARSAL_REPORT} |",
                f"| output_path | {report_path} |",
            ).splitlines()
            if not line.startswith("| generated_at |")
        ),
        encoding="utf-8",
    )
    settings = _live_settings(live_operator_rehearsal_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="generated_at"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_signoff_before_evidence_reports() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-stale-signoff-"
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )
    settings.live_exit_criteria_ratified_at = datetime(2026, 5, 24, tzinfo=UTC)
    settings.live_compliance_reviewed_at = datetime(2026, 5, 24, tzinfo=UTC)

    with pytest.raises(LiveTradingDisabledError, match="predates LIVE evidence"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_missing_fresh_approval_check() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-missing-fresh-approval-"
    )
    report_path = Path(rehearsal_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_path.write_text(
        "\n".join(
            line
            for line in report_text.splitlines()
            if not line.startswith("| fresh_approval_required |")
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="fresh_approval_required"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_missing_strict_sidecar_check() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-missing-strict-sidecar-"
    )
    report_path = Path(rehearsal_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_path.write_text(
        "\n".join(
            line
            for line in report_text.splitlines()
            if not line.startswith("| strict_sidecar_provenance |")
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="strict_sidecar_provenance"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_missing_unexpected_events_check() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-missing-unexpected-events-"
    )
    report_path = Path(rehearsal_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_path.write_text(
        "\n".join(
            line
            for line in report_text.splitlines()
            if not line.startswith("| unexpected_events |")
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="unexpected_events"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_missing_operator_id_check() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-missing-operator-id-"
    )
    report_path = Path(rehearsal_report_path)
    report_text = report_path.read_text(encoding="utf-8")
    report_path.write_text(
        "\n".join(
            line
            for line in report_text.splitlines()
            if not line.startswith("| operator_id |")
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="operator_id"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_report_blank_operator_id_detail() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-blank-operator-id-"
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| operator_id | PASS | rehearsal-operator |",
            "| operator_id | PASS |  |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="operator_id"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_pass_row_with_weak_detail() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-weak-rehearsal-row-detail-"
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| approval_denied | PASS | observed before approval file existed |",
            "| approval_denied | PASS | operator confirmed denial |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="approval_denied detail does not prove gate denial before approval file",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_unexpected_events_without_sequence() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-weak-unexpected-events-"
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            (
                "| unexpected_events | PASS | events=['approval_denied', "
                "'approval_matched', 'approval_consumed', 'approval_denied'] |"
            ),
            "| unexpected_events | PASS | observed |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="unexpected_events"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_placeholder_required_detail() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-rehearsal-row-detail-marker-"
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            (
                "| strict_sidecar_provenance | PASS | strict gate required "
                "sidecar approver_id, timestamp, and approval hash |"
            ),
            "| strict_sidecar_provenance | PASS | TODO: rerun sidecar rehearsal |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="strict_sidecar_provenance contains placeholder",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_placeholder_after_escaped_pipe_detail() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-rehearsal-row-detail-escaped-marker-"
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            (
                "| strict_sidecar_provenance | PASS | strict gate required "
                "sidecar approver_id, timestamp, and approval hash |"
            ),
            (
                "| strict_sidecar_provenance | PASS | strict gate required "
                "sidecar approver_id, timestamp, and approval hash "
                "\\| TODO: rerun sidecar rehearsal |"
            ),
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="strict_sidecar_provenance contains placeholder",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_gate_row_with_extra_cells() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-rehearsal-row-extra-cell-"
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| operator_id | PASS | rehearsal-operator |",
            "| operator_id | PASS | rehearsal-operator | TODO: hidden extra cell |",
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="malformed gate row: operator_id",
    ):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_with_extra_non_pass_gate_row() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-extra-rehearsal-row-"
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| operator_id | PASS | rehearsal-operator |",
            "\n".join(
                [
                    "| operator_id | PASS | rehearsal-operator |",
                    "| second_operator_dry_run | WARN | backup approval not rehearsed |",
                ]
            ),
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="non-PASS gate rows"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_with_duplicate_gate_row() -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-duplicate-rehearsal-row-"
    )
    report_path = Path(rehearsal_report_path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace(
            "| operator_id | PASS | rehearsal-operator |",
            "\n".join(
                [
                    "| operator_id | PASS | rehearsal-operator |",
                    "| operator_id | PASS | duplicate-operator |",
                ]
            ),
        ),
        encoding="utf-8",
    )
    settings = _live_settings(
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
    )

    with pytest.raises(LiveTradingDisabledError, match="duplicate gate rows: operator_id"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_rows_outside_section(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "operator-rehearsal-rows-outside-section.md"
    report_path.write_text(
        "\n".join(
            [
                "# Operator Approval Rehearsal - 2026-05-25",
                "",
                "## Report Provenance",
                "",
                "| Field | Value |",
                "|---|---|",
                "| generated_by | scripts/rehearse_first_order.py |",
                "| artifact_mode | persisted |",
                f"| output_path | {report_path} |",
                "",
                "## Operator Approval Rehearsal",
                "",
                "**Decision:** PASS",
                "",
                "| Check | Status | Detail |",
                "|---|---|---|",
                "",
                "## Appendix",
                "",
                "| Check | Status | Detail |",
                "|---|---|---|",
                "| approval_denied | PASS | observed |",
                "| approval_matched | PASS | observed |",
                "| approval_consumed | PASS | observed |",
                "| fresh_approval_required | PASS | observed |",
            ]
        ),
        encoding="utf-8",
    )
    settings = _live_settings(live_operator_rehearsal_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="approval_denied"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_operator_rehearsal_pass_decision_outside_section(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "operator-rehearsal-pass-outside-section.md"
    fixture_text = Path(OPERATOR_REHEARSAL_REPORT).read_text(encoding="utf-8")
    report_text = fixture_text.replace(
        f"| output_path | {OPERATOR_REHEARSAL_REPORT} |",
        f"| output_path | {report_path} |",
    ).replace(
        "\n**Decision:** PASS\n",
        "\n",
        1,
    )
    report_path.write_text(
        "\n".join(
            [
                report_text.rstrip(),
                "",
                "## Appendix",
                "",
                "**Decision:** PASS",
                "",
            ]
        ),
        encoding="utf-8",
    )
    settings = _live_settings(live_operator_rehearsal_report_path=str(report_path))

    with pytest.raises(LiveTradingDisabledError, match="PASS decision"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_failed_operator_rehearsal_report(tmp_path: Path) -> None:
    report_path = tmp_path / "operator-rehearsal-failed.md"
    report_path.write_text(
        "\n".join(
            [
                "# Operator Approval Rehearsal - 2026-05-25",
                "",
                "## Operator Approval Rehearsal",
                "",
                "**Decision:** FAIL",
                "",
                "| Check | Status | Detail |",
                "|---|---|---|",
                "| approval_denied | PASS | observed |",
                "| approval_matched | FAIL | not observed |",
                "| approval_consumed | FAIL | not observed |",
                "| primary_operator | PASS | primary |",
                "| backup_operator | PASS | backup |",
            ]
        ),
        encoding="utf-8",
    )
    settings = _live_settings()
    settings.live_operator_rehearsal_report_path = str(report_path)

    with pytest.raises(LiveTradingDisabledError, match="PASS decision"):
        validate_live_mode_ready(settings)


def test_live_mode_requires_distinct_first_order_and_emergency_audit_paths() -> None:
    shared_path = ".data/shared-live-audit.jsonl"

    with pytest.raises(LiveTradingDisabledError, match="first-order audit"):
        validate_live_mode_ready(
            _live_settings(
                live_emergency_audit_path=shared_path,
                live_first_order_audit_path=shared_path,
            )
        )


@pytest.mark.parametrize(
    ("collision_name", "expected_match"),
    (
        (
            "first_order_audit_preflight",
            "first-order audit path.*preflight artifact path",
        ),
        (
            "emergency_audit_preflight",
            "emergency audit path.*preflight artifact path",
        ),
    ),
)
def test_live_mode_rejects_audit_path_reusing_preflight_artifact_path(
    collision_name: str,
    expected_match: str,
) -> None:
    settings = _live_settings()
    shared_path = str(_LIVE_PATH_ROOT / f"{collision_name}.jsonl")
    settings.live_preflight_artifact_path = shared_path
    if collision_name == "first_order_audit_preflight":
        settings.live_first_order_audit_path = shared_path
    elif collision_name == "emergency_audit_preflight":
        settings.live_emergency_audit_path = shared_path
    else:
        raise AssertionError(f"unhandled collision case: {collision_name}")

    with pytest.raises(LiveTradingDisabledError, match=expected_match):
        validate_live_mode_ready(settings)


@pytest.mark.parametrize(
    ("collision_name", "expected_match"),
    (
        (
            "approval_first_order_audit",
            "operator approval path.*first-order audit path",
        ),
        (
            "approval_emergency_audit",
            "operator approval path.*emergency audit path",
        ),
        (
            "approval_preflight_artifact",
            "operator approval path.*preflight artifact path",
        ),
        (
            "sidecar_first_order_audit",
            "operator approval sidecar path.*first-order audit path",
        ),
        (
            "sidecar_emergency_audit",
            "operator approval sidecar path.*emergency audit path",
        ),
        (
            "sidecar_preflight_artifact",
            "operator approval sidecar path.*preflight artifact path",
        ),
    ),
)
def test_live_mode_rejects_operator_approval_path_reusing_launch_control_path(
    collision_name: str,
    expected_match: str,
) -> None:
    settings = _live_settings()
    approval_base = str(_LIVE_PATH_ROOT / f"{collision_name}-approval.json")

    if collision_name == "approval_first_order_audit":
        settings.polymarket.first_live_order_approval_path = (
            settings.live_first_order_audit_path
        )
    elif collision_name == "approval_emergency_audit":
        settings.polymarket.first_live_order_approval_path = (
            settings.live_emergency_audit_path
        )
    elif collision_name == "approval_preflight_artifact":
        shared_path = str(_LIVE_PATH_ROOT / "credentialed-preflight.json")
        settings.live_preflight_artifact_path = shared_path
        settings.polymarket.first_live_order_approval_path = shared_path
    elif collision_name == "sidecar_first_order_audit":
        settings.polymarket.first_live_order_approval_path = approval_base
        settings.live_first_order_audit_path = f"{approval_base}.meta.json"
    elif collision_name == "sidecar_emergency_audit":
        settings.polymarket.first_live_order_approval_path = approval_base
        settings.live_emergency_audit_path = f"{approval_base}.meta.json"
    elif collision_name == "sidecar_preflight_artifact":
        settings.polymarket.first_live_order_approval_path = approval_base
        settings.live_preflight_artifact_path = f"{approval_base}.meta.json"
    else:
        raise AssertionError(f"unhandled collision case: {collision_name}")

    with pytest.raises(LiveTradingDisabledError, match=expected_match):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_missing_emergency_audit_parent(tmp_path: Path) -> None:
    settings = _live_settings(
        live_emergency_audit_path=str(
            tmp_path / "missing" / "live-emergency-audit.jsonl"
        )
    )

    with pytest.raises(LiveTradingDisabledError, match="emergency audit parent"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_emergency_audit_path_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    audit_dir = repo_root / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    settings = _live_settings(
        live_emergency_audit_path=str(audit_dir / "live-emergency-audit.jsonl")
    )

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_non_owner_writable_emergency_audit_parent(
    tmp_path: Path,
) -> None:
    audit_dir = tmp_path / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    audit_dir.chmod(0o500)
    settings = _live_settings(
        live_emergency_audit_path=str(audit_dir / "live-emergency-audit.jsonl")
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="owner-writable"):
            validate_live_mode_ready(settings)
    finally:
        audit_dir.chmod(0o700)


def test_live_mode_rejects_permissive_emergency_audit_parent(
    tmp_path: Path,
) -> None:
    audit_dir = tmp_path / "permissive-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    audit_dir.chmod(0o755)
    settings = _live_settings(
        live_emergency_audit_path=str(audit_dir / "live-emergency-audit.jsonl")
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="too permissive"):
            validate_live_mode_ready(settings)
    finally:
        audit_dir.chmod(0o700)


def test_live_mode_rejects_symlink_emergency_audit_parent(tmp_path: Path) -> None:
    audit_dir = tmp_path / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    symlink_parent = tmp_path / "emergency-audit-parent-link"
    symlink_parent.symlink_to(audit_dir, target_is_directory=True)
    settings = _live_settings(
        live_emergency_audit_path=str(
            symlink_parent / "live-emergency-audit.jsonl"
        )
    )

    with pytest.raises(LiveTradingDisabledError, match="not a directory"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_symlink_emergency_audit_path(tmp_path: Path) -> None:
    audit_dir = tmp_path / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    target_path = audit_dir / "target-emergency-audit.jsonl"
    target_path.write_text("existing audit\n", encoding="utf-8")
    audit_path = audit_dir / "live-emergency-audit.jsonl"
    audit_path.symlink_to(target_path)
    settings = _live_settings(live_emergency_audit_path=str(audit_path))

    with pytest.raises(LiveTradingDisabledError, match="regular file"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_hardlinked_emergency_audit_path(tmp_path: Path) -> None:
    audit_dir = tmp_path / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    target_path = audit_dir / "target-emergency-audit.jsonl"
    target_path.write_text("existing audit\n", encoding="utf-8")
    audit_path = audit_dir / "live-emergency-audit.jsonl"
    os.link(target_path, audit_path)
    settings = _live_settings(live_emergency_audit_path=str(audit_path))

    with pytest.raises(LiveTradingDisabledError, match="single-link"):
        validate_live_mode_ready(settings)


def test_live_runner_constructed_before_start_does_not_mark_adapter_preflight_validated() -> None:
    runner = Runner(config=_live_settings())

    adapter = runner.actuator_executor.adapter

    assert isinstance(adapter, PolymarketActuator)
    assert adapter.live_preflight_validated is False


@pytest.mark.asyncio
async def test_live_runner_rejects_missing_credentialed_preflight_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = Runner(config=_live_settings())

    async def fail_if_database_boots() -> None:
        raise AssertionError("LIVE runner must reject before database bootstrap")

    monkeypatch.setattr(runner, "ensure_pg_pool", fail_if_database_boots)

    with pytest.raises(LiveTradingDisabledError, match="preflight artifact"):
        await runner.start()


@pytest.mark.asyncio
async def test_live_runner_rejects_preflight_artifact_output_path_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _live_settings()
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-output-mismatch-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["output_path"] = str(artifact_path.parent / "copied-preflight.json")
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    runner = Runner(config=settings)

    async def fail_if_database_boots() -> None:
        raise AssertionError("LIVE runner must reject before database bootstrap")

    monkeypatch.setattr(runner, "ensure_pg_pool", fail_if_database_boots)

    with pytest.raises(LiveTradingDisabledError, match="output_path"):
        await runner.start()


@pytest.mark.asyncio
async def test_live_runner_rejects_preflight_artifact_before_operator_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _live_settings()
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-before-readiness-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["generated_at"] = "2026-05-24T23:59:59+00:00"
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    runner = Runner(config=settings)

    async def fail_if_database_boots() -> None:
        raise AssertionError("LIVE runner must reject before database bootstrap")

    monkeypatch.setattr(runner, "ensure_pg_pool", fail_if_database_boots)

    with pytest.raises(LiveTradingDisabledError, match="predates LIVE readiness"):
        await runner.start()


@pytest.mark.asyncio
async def test_live_runner_rejects_stale_preflight_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _live_settings()
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-stale-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    stale_generated_at = datetime.now(tz=UTC) - timedelta(hours=2, seconds=1)
    artifact["generated_at"] = stale_generated_at.isoformat()
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    readiness_report_generated_at = stale_generated_at - timedelta(seconds=60)
    operator_attested_at = stale_generated_at - timedelta(seconds=30)
    _replace_report_provenance_field(
        cast(str, settings.live_paper_soak_report_path),
        field_name="generated_at",
        value=readiness_report_generated_at.isoformat(),
    )
    _replace_report_provenance_field(
        cast(str, settings.live_operator_rehearsal_report_path),
        field_name="generated_at",
        value=readiness_report_generated_at.isoformat(),
    )
    settings.live_exit_criteria_ratified_at = operator_attested_at
    settings.live_compliance_reviewed_at = operator_attested_at
    artifact["settings_fingerprint"] = live_preflight_settings_fingerprint(settings)
    artifact["readiness_reports_fingerprint"] = (
        live_preflight_readiness_reports_fingerprint(settings)
    )
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    runner = Runner(config=settings)

    async def fail_if_database_boots() -> None:
        raise AssertionError("LIVE runner must reject before database bootstrap")

    monkeypatch.setattr(runner, "ensure_pg_pool", fail_if_database_boots)

    with pytest.raises(
        LiveTradingDisabledError,
        match="stale|predates readiness reports",
    ):
        await runner.start()


@pytest.mark.asyncio
async def test_live_runner_rejects_preflight_artifact_with_database_url_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _live_settings()
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-database-override-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["database_url_override_used"] = True
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    runner = Runner(config=settings)

    async def fail_if_database_boots() -> None:
        raise AssertionError("LIVE runner must reject before database bootstrap")

    monkeypatch.setattr(runner, "ensure_pg_pool", fail_if_database_boots)

    with pytest.raises(LiveTradingDisabledError, match="database-url override"):
        await runner.start()


@pytest.mark.asyncio
async def test_live_runner_rejects_preflight_artifact_with_extra_failed_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _live_settings()
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-extra-failed-check-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["result"]["checks"].append(
        {
            "name": "operator_shadow_check",
            "ok": False,
            "detail": "operator proof contradicted",
        }
    )
    artifact["result"]["ok"] = True
    artifact["final_go_no_go_valid"] = True
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    runner = Runner(config=settings)

    async def fail_if_database_boots() -> None:
        raise AssertionError("LIVE runner must reject before database bootstrap")

    monkeypatch.setattr(runner, "ensure_pg_pool", fail_if_database_boots)

    with pytest.raises(LiveTradingDisabledError, match="operator_shadow_check"):
        await runner.start()


@pytest.mark.asyncio
async def test_live_runner_rejects_preflight_artifact_settings_fingerprint_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _live_settings()
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-settings-mismatch-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["settings_fingerprint"] = "not-current-live-settings"
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    runner = Runner(config=settings)

    async def fail_if_database_boots() -> None:
        raise AssertionError("LIVE runner must reject before database bootstrap")

    monkeypatch.setattr(runner, "ensure_pg_pool", fail_if_database_boots)

    with pytest.raises(LiveTradingDisabledError, match="settings fingerprint"):
        await runner.start()


@pytest.mark.asyncio
async def test_live_runner_rejects_preflight_artifact_active_strategy_fingerprint_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _live_settings()
    settings.live_preflight_artifact_path = make_live_preflight_artifact_path(
        prefix="pms-live-preflight-active-strategy-startup-mismatch-",
        settings=settings,
    )
    runner = Runner(config=settings)

    async def fake_ensure_pg_pool() -> None:
        runner._pg_pool = cast(Any, object())  # noqa: SLF001

    async def reject_active_strategy_artifact(
        settings_arg: PMSSettings,
        registry: object,
    ) -> None:
        assert settings_arg is settings
        assert registry is runner.strategy_registry
        raise LiveTradingDisabledError(
            "LIVE credentialed preflight active strategies fingerprint mismatch"
        )

    async def fail_if_factor_catalog_boots(_pool: object) -> None:
        raise AssertionError(
            "LIVE runner must reject stale active-strategy preflight "
            "before factor catalog"
        )

    monkeypatch.setattr(runner, "ensure_pg_pool", fake_ensure_pg_pool)
    monkeypatch.setattr(
        "pms.runner.require_live_preflight_active_strategies_artifact",
        reject_active_strategy_artifact,
        raising=False,
    )
    monkeypatch.setattr("pms.runner.ensure_factor_catalog", fail_if_factor_catalog_boots)

    with pytest.raises(
        LiveTradingDisabledError,
        match="active strategies fingerprint mismatch",
    ):
        await runner.start()


def test_live_mode_rejects_missing_first_order_audit_parent(tmp_path: Path) -> None:
    settings = _live_settings(
        live_first_order_audit_path=str(tmp_path / "missing" / "first-order-audit.jsonl")
    )

    with pytest.raises(LiveTradingDisabledError, match="first-order audit parent"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_first_order_audit_path_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    audit_dir = repo_root / "secure-first-order-audit"
    audit_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    settings = _live_settings(
        live_first_order_audit_path=str(audit_dir / "first-order-audit.jsonl")
    )

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_non_owner_writable_first_order_audit_parent(
    tmp_path: Path,
) -> None:
    audit_dir = tmp_path / "secure-audit"
    audit_dir.mkdir(mode=0o700)
    audit_dir.chmod(0o500)
    settings = _live_settings(
        live_first_order_audit_path=str(audit_dir / "first-order-audit.jsonl")
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="owner-writable"):
            validate_live_mode_ready(settings)
    finally:
        audit_dir.chmod(0o700)


def test_live_mode_rejects_permissive_first_order_audit_parent(
    tmp_path: Path,
) -> None:
    audit_dir = tmp_path / "permissive-audit"
    audit_dir.mkdir(mode=0o700)
    audit_dir.chmod(0o755)
    settings = _live_settings(
        live_first_order_audit_path=str(audit_dir / "first-order-audit.jsonl")
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="too permissive"):
            validate_live_mode_ready(settings)
    finally:
        audit_dir.chmod(0o700)


def test_live_mode_rejects_symlink_first_order_audit_parent(tmp_path: Path) -> None:
    audit_dir = tmp_path / "secure-first-order-audit"
    audit_dir.mkdir(mode=0o700)
    symlink_parent = tmp_path / "first-order-audit-parent-link"
    symlink_parent.symlink_to(audit_dir, target_is_directory=True)
    settings = _live_settings(
        live_first_order_audit_path=str(symlink_parent / "first-order-audit.jsonl")
    )

    with pytest.raises(LiveTradingDisabledError, match="not a directory"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_symlink_first_order_audit_path(tmp_path: Path) -> None:
    audit_dir = tmp_path / "secure-first-order-audit"
    audit_dir.mkdir(mode=0o700)
    target_path = audit_dir / "target-first-order-audit.jsonl"
    target_path.write_text("existing audit\n", encoding="utf-8")
    audit_path = audit_dir / "first-order-audit.jsonl"
    audit_path.symlink_to(target_path)
    settings = _live_settings(live_first_order_audit_path=str(audit_path))

    with pytest.raises(LiveTradingDisabledError, match="regular file"):
        validate_live_mode_ready(settings)


def test_live_mode_rejects_hardlinked_first_order_audit_path(tmp_path: Path) -> None:
    audit_dir = tmp_path / "secure-first-order-audit"
    audit_dir.mkdir(mode=0o700)
    target_path = audit_dir / "target-first-order-audit.jsonl"
    target_path.write_text("existing audit\n", encoding="utf-8")
    audit_path = audit_dir / "first-order-audit.jsonl"
    os.link(target_path, audit_path)
    settings = _live_settings(live_first_order_audit_path=str(audit_path))

    with pytest.raises(LiveTradingDisabledError, match="single-link"):
        validate_live_mode_ready(settings)


def test_polymarket_operator_approval_mode_rejects_unknown_values() -> None:
    invalid_mode: Any = "market_bucket"

    with pytest.raises(ValueError, match="operator_approval_mode"):
        PolymarketSettings(operator_approval_mode=invalid_mode)


def test_trade_decision_rejects_action_side_mismatch() -> None:
    with pytest.raises(ValueError, match="side/action mismatch"):
        _decision(side=Side.SELL.value, action=Side.BUY.value)


@dataclass
class AllowFirstOrderGate:
    async def approve_first_order(self, preview: LiveOrderPreview) -> bool:
        del preview
        return True

    async def consume(self, preview: LiveOrderPreview) -> None:
        del preview


def _strict_file_gate(
    settings: PMSSettings,
    decision: TradeDecision,
) -> FileFirstLiveOrderGate:
    approval_path = _write_operator_approval(settings, decision)
    return FileFirstLiveOrderGate(
        approval_path,
        require_approver_sidecar=True,
        approval_max_age_s=settings.polymarket.operator_approval_max_age_s,
    )


def _write_operator_approval(
    settings: PMSSettings,
    decision: TradeDecision,
) -> Path:
    assert decision.limit_price is not None
    approval_path = Path(cast(str, settings.polymarket.first_live_order_approval_path))
    approval_payload: dict[str, object] = {
        "approved": True,
        "max_notional_usdc": decision.notional_usdc,
        "venue": decision.venue,
        "market_id": decision.market_id,
        "token_id": decision.token_id,
        "side": decision.side,
        "outcome": decision.outcome,
        "limit_price": decision.limit_price,
        "max_slippage_bps": decision.max_slippage_bps,
    }
    approval_path.write_text(json.dumps(approval_payload), encoding="utf-8")
    _sidecar_path(approval_path).write_text(
        json.dumps(
            {
                "approver_id": "test-operator",
                "approval_sha256": _approval_payload_hash(approval_payload),
                "ts": datetime.now(UTC).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    return approval_path


def _approval_payload_hash(payload: dict[str, object]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def _sidecar_path(approval_path: Path) -> Path:
    return Path(str(approval_path) + ".meta.json")


@dataclass
class RecordingClient:
    submitted: list[object]

    async def submit_order(
        self,
        order: object,
        credentials: object,
    ) -> PolymarketOrderResult:
        del credentials
        self.submitted.append(order)
        return PolymarketOrderResult(
            order_id="pm-live-blocker",
            status=OrderStatus.MATCHED.value,
            raw_status="matched",
            filled_notional_usdc=10.0,
            remaining_notional_usdc=0.0,
            fill_price=0.4,
            filled_quantity=25.0,
        )


@pytest.mark.asyncio
async def test_polymarket_actuator_rejects_live_gtc_decision_before_submission() -> None:
    client = RecordingClient(submitted=[])
    actuator = PolymarketActuator(
        _live_settings(),
        client=client,
        operator_gate=AllowFirstOrderGate(),
        live_preflight_validated=True,
    )

    with pytest.raises(LiveTradingDisabledError, match="LIVE order time_in_force"):
        await actuator.execute(
            _decision(time_in_force=TimeInForce.GTC),
            _portfolio(),
        )

    assert client.submitted == []


@pytest.mark.asyncio
async def test_polymarket_actuator_requires_pre_submit_quote_guard() -> None:
    client = RecordingClient(submitted=[])
    approval_root = Path(tempfile.mkdtemp(prefix="pms-live-blockers-quote-approval-"))
    approval_root.chmod(0o700)
    settings = _live_settings(
        first_live_order_approval_path=str(approval_root / "first-order.json")
    )
    settings.live_preflight_artifact_path = make_live_preflight_artifact_path(
        prefix="pms-live-blockers-quote-guard-preflight-",
        settings=settings,
    )
    decision = _decision()
    gate = _strict_file_gate(settings, decision)
    actuator = PolymarketActuator(
        settings,
        client=client,
        operator_gate=gate,
        live_preflight_validated=True,
    )

    try:
        with pytest.raises(LiveTradingDisabledError, match="pre-submit quote guard"):
            await actuator.execute(decision, _portfolio())
    finally:
        gate.path.unlink(missing_ok=True)
        _sidecar_path(gate.path).unlink(missing_ok=True)

    assert client.submitted == []


@dataclass
class UnknownSubmissionAdapter:
    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        del decision, portfolio
        raise PolymarketSubmissionUnknownError("timeout")


@pytest.mark.asyncio
async def test_executor_attaches_submission_unknown_order_state_before_reraising() -> None:
    decision = _decision()
    executor = ActuatorExecutor(
        adapter=UnknownSubmissionAdapter(),
        risk=RiskManager(
            RiskSettings(max_position_per_market=1_000.0, max_total_exposure=10_000.0)
        ),
        feedback=ActuatorFeedback(cast(FeedbackStore, InMemoryFeedbackStore())),
    )

    with pytest.raises(PolymarketSubmissionUnknownError) as exc_info:
        await executor.execute(decision, _portfolio())

    order_state = exc_info.value.order_state
    assert order_state is not None
    assert order_state.decision_id == decision.decision_id
    assert order_state.raw_status == "submission_unknown"


@dataclass
class RecordingOrderStore:
    inserted: list[OrderState]

    async def insert(self, order: OrderState) -> None:
        self.inserted.append(order)


@dataclass
class RaisingUnknownExecutor:
    order_state: OrderState

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio,
        *,
        dedup_acquired: bool = False,
    ) -> OrderState:
        del decision, portfolio, dedup_acquired
        error = PolymarketSubmissionUnknownError("timeout")
        error.order_state = self.order_state
        raise error


def _submission_unknown_order(decision: TradeDecision) -> OrderState:
    now = datetime(2026, 4, 26, tzinfo=UTC)
    return OrderState(
        order_id=f"unknown-{decision.decision_id}",
        decision_id=decision.decision_id,
        status=OrderStatus.INVALID.value,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_notional_usdc=decision.notional_usdc,
        filled_notional_usdc=0.0,
        remaining_notional_usdc=decision.notional_usdc,
        fill_price=None,
        submitted_at=now,
        last_updated_at=now,
        raw_status="submission_unknown",
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        filled_quantity=0.0,
    )


def _mark_controller_done(runner: Runner) -> None:
    import asyncio

    runner._controller_task = asyncio.create_task(asyncio.sleep(0))  # noqa: SLF001


@pytest.mark.asyncio
async def test_runner_persists_submission_unknown_and_suspends_live_orders() -> None:
    decision = _decision()
    order_state = _submission_unknown_order(decision)
    runner = Runner(config=_live_settings())
    runner.actuator_executor = cast(Any, RaisingUnknownExecutor(order_state))
    order_store = RecordingOrderStore(inserted=[])
    runner.order_store = cast(Any, order_store)
    _mark_controller_done(runner)

    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(decision=decision, signal=None)
    )

    await runner._actuator_loop()  # noqa: SLF001

    assert runner.state.orders == [order_state]
    assert order_store.inserted == [order_state]
    assert runner.live_trading_suspended is True


@pytest.mark.asyncio
async def test_in_memory_dedup_blocks_same_economic_intent_key() -> None:
    store = InMemoryDedupStore()
    first = _decision(decision_id="d-intent-1", intent_key="intent:same")
    second = _decision(decision_id="d-intent-2", intent_key="intent:same")

    assert await store.acquire(first) is True
    assert await store.acquire(second) is False
