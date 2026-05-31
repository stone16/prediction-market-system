from __future__ import annotations

import importlib.machinery
import importlib.util
import json
import os
import stat
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import asyncpg
import pytest
from pydantic import SecretStr

from pms.actuator.adapters.polymarket import PolymarketVenueAccountReconciler
from pms.config import (
    ControllerSettings,
    DiscordSettings,
    PMSSettings,
    PolymarketSettings,
    RiskSettings,
    StrategyRuntimeSettings,
)
from pms.core.enums import RunMode
from pms.core.models import (
    LiveTradingDisabledError,
    Portfolio,
    Position,
    ReconciliationReport,
    VenueAccountSnapshot,
)
from pms import live_cli
import pms.live_preflight as live_preflight_module
import pms.live_preflight_artifact as live_preflight_artifact_module
from pms.live_cli import build_parser
from pms.live_preflight import (
    LivePreflightCheck,
    LivePreflightResult,
    live_preflight_active_strategies_fingerprint,
    run_live_preflight,
)
from pms.storage.schema_check import EXPECTED_SCHEMA_HEAD
from pms.storage.live_reconciliation import LiveOrderReconciliationRecord
from pms.strategies.aggregate import Strategy
from pms.strategies.paper_canary import build_paper_canary_strategy
from pms.strategies.projections import (
    ActiveStrategy,
    CalibrationSpec,
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.strategies.versioning import (
    compute_strategy_version_id,
    serialize_strategy_config_json,
)
from tests.support.live_paths import make_live_preflight_artifact_path, make_live_report_paths


_FINAL_PREFLIGHT_CHECK_NAMES: tuple[str, ...] = (
    "live_config",
    "runtime_dependencies",
    "operator_approval",
    "emergency_audit",
    "first_order_audit",
    "database_connection",
    "schema_current",
    "market_data_freshness",
    "submission_unknown",
    "live_open_orders",
    "active_strategies",
    "venue_reconciliation",
)
_VALID_ACTIVE_STRATEGIES_FINGERPRINT = "a" * 64
_STALE_ACTIVE_STRATEGIES_FINGERPRINT = "b" * 64


def _live_strategy(
    *,
    forecasters: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = (("rules", ()),),
) -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id="default",
            factor_composition=(),
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
            max_position_notional_usdc=50.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        forecaster=ForecasterSpec(forecasters=forecasters),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=30,
            volume_min_usdc=500.0,
        ),
    )


def _live_active_strategy() -> ActiveStrategy:
    strategy = _live_strategy()
    return ActiveStrategy(
        strategy_id=strategy.config.strategy_id,
        strategy_version_id=_strategy_version_id(strategy),
        config=strategy.config,
        risk=strategy.risk,
        eval_spec=strategy.eval_spec,
        forecaster=strategy.forecaster,
        market_selection=strategy.market_selection,
        calibration=CalibrationSpec(enabled=True),
    )


def _strategy_version_id(strategy: Strategy, *, calibrated: bool = True) -> str:
    return compute_strategy_version_id(
        strategy.config,
        strategy.risk,
        strategy.eval_spec,
        strategy.forecaster,
        strategy.market_selection,
        CalibrationSpec(enabled=True) if calibrated else strategy.calibration,
    )


def _strategy_config_json(strategy: Strategy, *, calibrated: bool = True) -> str:
    return serialize_strategy_config_json(
        strategy.config,
        strategy.risk,
        strategy.eval_spec,
        strategy.forecaster,
        strategy.market_selection,
        CalibrationSpec(enabled=True) if calibrated else strategy.calibration,
    )


def _active_strategy_row(
    strategy: Strategy,
    *,
    strategy_version_id: str | None = None,
    calibrated: bool = True,
) -> dict[str, object]:
    config_json = json.loads(_strategy_config_json(strategy, calibrated=calibrated))
    return {
        "strategy_id": strategy.config.strategy_id,
        "strategy_version_id": strategy_version_id or _strategy_version_id(
            strategy,
            calibrated=calibrated,
        ),
        "config_json": json.dumps(config_json, sort_keys=True),
    }


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


def _insert_report_summary_strategy(report_path: str, strategy_label: str) -> None:
    path = Path(report_path)
    lines = path.read_text(encoding="utf-8").splitlines()
    in_summary = False
    replaced = False
    updated_lines: list[str] = []
    for line in lines:
        if line.startswith("## "):
            in_summary = line == "## Summary"
        if in_summary and line.startswith("| Strategy |"):
            updated_lines.append(f"| Strategy | {strategy_label} | - |")
            replaced = True
        elif line.startswith("| strategy_evidence |"):
            updated_lines.append(f"| strategy_evidence | PASS | {strategy_label} |")
        else:
            updated_lines.append(line)
    if replaced:
        path.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")
        return

    try:
        gate_index = lines.index("## Go/No-Go Gate")
    except ValueError as exc:
        raise AssertionError("fixture report must include Go/No-Go Gate") from exc
    summary = [
        "## Summary",
        "",
        "| Metric | Value | Gate |",
        "|---|---:|---|",
        f"| Strategy | {strategy_label} | - |",
        "",
    ]
    path.write_text(
        "\n".join([*lines[:gate_index], *summary, *lines[gate_index:]]) + "\n",
        encoding="utf-8",
    )


class _Acquire:
    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection

    def __await__(self) -> Any:
        async def _result() -> "_Connection":
            return self.connection

        return _result().__await__()

    async def __aenter__(self) -> "_Connection":
        return self.connection

    async def __aexit__(self, *_: object) -> None:
        return None


class _Connection:
    def __init__(
        self,
        *,
        unresolved_submission_unknown: int = 0,
        latest_book_snapshot_age_s: float | None = 30.0,
        latest_usable_book_snapshot_age_s: float | None = 30.0,
        missing_market_risk_metadata_count: int = 0,
        active_strategy_rows: tuple[dict[str, object], ...] | None = None,
    ) -> None:
        self.unresolved_submission_unknown = unresolved_submission_unknown
        self.latest_book_snapshot_age_s = latest_book_snapshot_age_s
        self.latest_usable_book_snapshot_age_s = latest_usable_book_snapshot_age_s
        self.missing_market_risk_metadata_count = missing_market_risk_metadata_count
        self.active_strategy_rows = (
            active_strategy_rows
            if active_strategy_rows is not None
            else (_active_strategy_row(_live_strategy()),)
        )
        self.fetchval_calls: list[str] = []

    async def fetchval(self, query: str, *args: object) -> object:
        del args
        self.fetchval_calls.append(query)
        if "alembic_version" in query:
            return EXPECTED_SCHEMA_HEAD
        if "outcome = 'submission_unknown'" in query:
            return self.unresolved_submission_unknown
        if "missing_market_risk_metadata" in query:
            return self.missing_market_risk_metadata_count
        if "usable_book_snapshots" in query:
            return self.latest_usable_book_snapshot_age_s
        if "book_snapshots" in query:
            return self.latest_book_snapshot_age_s
        return None

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        del args
        if "strategy_versions AS versions" in query:
            return list(self.active_strategy_rows)
        return []

    async def execute(self, query: str, *args: object) -> str:
        del query, args
        return "CREATE TABLE"


class _FailingConnection(_Connection):
    def __init__(
        self,
        *,
        message: str,
        fail_fetchval_contains: str | None = None,
        fail_fetch_contains: str | None = None,
    ) -> None:
        super().__init__()
        self.message = message
        self.fail_fetchval_contains = fail_fetchval_contains
        self.fail_fetch_contains = fail_fetch_contains

    async def fetchval(self, query: str, *args: object) -> object:
        if (
            self.fail_fetchval_contains is not None
            and self.fail_fetchval_contains in query
        ):
            raise OSError(self.message)
        return await super().fetchval(query, *args)

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        if self.fail_fetch_contains is not None and self.fail_fetch_contains in query:
            raise OSError(self.message)
        return await super().fetch(query, *args)


class _LiveOpenOrderConnection(_Connection):
    def __init__(self, *, live_open_order_count: int) -> None:
        super().__init__()
        self.live_open_order_count = live_open_order_count

    async def fetchval(self, query: str, *args: object) -> object:
        if "FROM orders" in query and "remaining_notional_usdc" in query:
            return self.live_open_order_count
        return await super().fetchval(query, *args)


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection
        self.release_calls = 0

    def acquire(self) -> _Acquire:
        return _Acquire(self.connection)

    async def release(self, connection: _Connection) -> None:
        assert connection is self.connection
        self.release_calls += 1


@dataclass(frozen=True)
class _MatchingVenueReconciler:
    async def snapshot(self, credentials: object) -> VenueAccountSnapshot:
        del credentials
        return VenueAccountSnapshot(balances={"USDC": 50.0}, open_orders=(), positions=())

    async def compare(
        self,
        db_portfolio: Portfolio,
        venue_snapshot: VenueAccountSnapshot,
    ) -> ReconciliationReport:
        del db_portfolio, venue_snapshot
        return ReconciliationReport(ok=True, mismatches=())


@dataclass(frozen=True)
class _MismatchingVenueReconciler(_MatchingVenueReconciler):
    mismatches: tuple[str, ...] = ("venue has open orders",)

    async def compare(
        self,
        db_portfolio: Portfolio,
        venue_snapshot: VenueAccountSnapshot,
    ) -> ReconciliationReport:
        del db_portfolio, venue_snapshot
        return ReconciliationReport(ok=False, mismatches=self.mismatches)


@dataclass(frozen=True)
class _RecordingVenueReconciler(_MatchingVenueReconciler):
    portfolios: list[Portfolio]

    async def compare(
        self,
        db_portfolio: Portfolio,
        venue_snapshot: VenueAccountSnapshot,
    ) -> ReconciliationReport:
        del venue_snapshot
        self.portfolios.append(db_portfolio)
        return ReconciliationReport(ok=True, mismatches=())


@dataclass(frozen=True)
class _FailingVenueReconciler(_MatchingVenueReconciler):
    message: str

    async def snapshot(self, credentials: object) -> VenueAccountSnapshot:
        del credentials
        raise ConnectionError(self.message)


@pytest.fixture(autouse=True)
def stub_runtime_dependency_specs(monkeypatch: pytest.MonkeyPatch) -> None:
    original_find_spec = importlib.util.find_spec

    def fake_find_spec(
        name: str,
        package: str | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if name in {"py_clob_client_v2", "anthropic", "openai"}:
            return importlib.machinery.ModuleSpec(name, loader=None)
        return original_find_spec(name, package)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)


def _settings(
    *,
    approval_path: Path,
    mode: RunMode = RunMode.LIVE,
    live_emergency_audit_path: Path | None = None,
) -> PMSSettings:
    attested_at = datetime(2026, 5, 25, tzinfo=UTC)
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-preflight-reports-"
    )
    default_live_strategy = _live_strategy()
    _insert_report_summary_strategy(
        paper_report_path,
        f"{default_live_strategy.config.strategy_id}@"
        f"{_strategy_version_id(default_live_strategy)}",
    )
    emergency_audit_path = (
        approval_path.parent / "live-emergency-audit.jsonl"
        if live_emergency_audit_path is None
        else live_emergency_audit_path
    )
    flb_calibration_path = Path(paper_report_path).parent / "flb-calibration.csv"
    category_prior_path = Path(paper_report_path).parent / "category-prior.csv"
    execution_model_path = Path(paper_report_path).parent / "execution-model.json"
    paper_backtest_diff_path = (
        Path(paper_report_path).parent / "paper-backtest-execution-diff.json"
    )
    _write_valid_flb_calibration_csv(flb_calibration_path)
    _write_valid_category_prior_csv(category_prior_path)
    _write_valid_execution_model_json(execution_model_path)
    _write_valid_paper_backtest_diff_json(paper_backtest_diff_path)
    return PMSSettings(
        mode=mode,
        secret_source="fly",
        live_trading_enabled=True,
        api_token="live-api-token",
        live_exit_criteria_ratified_by="operator",
        live_exit_criteria_ratified_at=attested_at,
        live_compliance_reviewed_by="counsel",
        live_compliance_reviewed_at=attested_at,
        live_compliance_jurisdiction="US-operator-approved",
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
        live_execution_model_path=str(execution_model_path),
        live_paper_backtest_diff_path=str(paper_backtest_diff_path),
        live_emergency_audit_path=str(emergency_audit_path),
        live_first_order_audit_path=str(
            approval_path.parent / "first-order-audit.jsonl"
        ),
        live_preflight_artifact_path=str(
            approval_path.parent / "credentialed-preflight.json"
        ),
        risk=RiskSettings(
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
            max_exposure_per_risk_group=250.0,
            max_quantity_shares=500.0,
        ),
        strategies=StrategyRuntimeSettings(
            flb_calibration_path=str(flb_calibration_path),
        ),
        controller=ControllerSettings(
            time_in_force="IOC",
            quote_source="dual",
            category_prior_observations_path=str(category_prior_path),
        ),
        discord=DiscordSettings(
            webhook_url=SecretStr("https://discord.example/webhooks/preflight/unit"),
            alert_dir=str(approval_path.parent / "discord-alerts"),
        ),
        polymarket=PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0x1111111111111111111111111111111111111111",
            first_live_order_approval_path=str(approval_path),
            operator_approval_mode="every_order",
        ),
    )


def _write_valid_paper_backtest_diff_json(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "generated_by": "scripts/paper_backtest_execution_diff.py",
                "artifact_mode": "paper_backtest_execution_diff",
                "generated_at": datetime(2026, 5, 25, tzinfo=UTC).isoformat(),
                "final_go_no_go_valid": True,
                "thresholds": {
                    "min_matched_decisions": 10,
                    "max_fill_rate_delta": 0.05,
                    "max_rejection_rate_delta": 0.05,
                    "max_avg_slippage_bps_delta": 5.0,
                    "max_total_pnl_delta": 1.0,
                },
                "metrics": {
                    "paper_decision_count": 10,
                    "backtest_decision_count": 10,
                    "matched_decision_count": 10,
                    "paper_fill_rate": 0.5,
                    "backtest_fill_rate": 0.5,
                    "fill_rate_delta_abs": 0.0,
                    "paper_rejection_rate": 0.5,
                    "backtest_rejection_rate": 0.5,
                    "rejection_rate_delta_abs": 0.0,
                    "paper_avg_slippage_bps": 3.0,
                    "backtest_avg_slippage_bps": 3.0,
                    "avg_slippage_bps_delta_abs": 0.0,
                    "paper_total_pnl": 1.2,
                    "backtest_total_pnl": 1.2,
                    "total_pnl_delta_abs": 0.0,
                },
                "paper_only_decision_ids": [],
                "backtest_only_decision_ids": [],
                "status_mismatches": [],
                "failures": [],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_valid_execution_model_json(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "generated_by": "scripts/execution_model_from_telemetry.py",
                "artifact_mode": "telemetry_execution_model",
                "generated_at": datetime.now(tz=UTC).isoformat(),
                "fee_rate": 0.04,
                "slippage_bps": 6.0,
                "latency_ms": 500.0,
                "staleness_ms": 120_000.0,
                "fill_policy": "immediate_or_cancel",
                "displayed_depth_fill_ratio": 0.75,
                "adverse_selection_bps": 9.0,
                "order_ttl_ms": 60_000,
                "price_invalidation_streak": 10,
                "replay_window_ms": 86_400_000,
                "calibration_source": "telemetry_calibrated",
                "min_samples": 10,
                "telemetry_sample_count": 10,
                "adverse_selection_sample_count": 10,
                "require_adverse_selection": True,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_valid_flb_calibration_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            (
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
                "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1",
            )
        ),
        encoding="utf-8",
    )


def _write_valid_category_prior_csv(path: Path) -> None:
    rows = ["market_id,category,yes_payout,no_payout,resolved_at"]
    for index in range(1, 121):
        category = "politics" if index % 2 == 0 else "sports"
        yes_payout, no_payout = ("1", "0") if index % 3 == 0 else ("0", "1")
        rows.append(
            f"m-{index},{category},{yes_payout},{no_payout},2026-05-{(index % 20) + 1:02d}T12:00:00Z"
        )
    path.write_text(
        "\n".join(rows),
        encoding="utf-8",
    )


def _final_preflight_result() -> LivePreflightResult:
    return LivePreflightResult(
        tuple(
            LivePreflightCheck(name, True, f"{name} passed")
            for name in _FINAL_PREFLIGHT_CHECK_NAMES
        ),
        active_strategies_fingerprint=_VALID_ACTIVE_STRATEGIES_FINGERPRINT,
    )


def _configure_live_preflight_artifact_path(
    settings: PMSSettings,
    output_path: Path,
) -> None:
    settings.live_preflight_artifact_path = str(
        output_path.expanduser().resolve(strict=False)
    )


class _CliFakePool:
    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


def _live_order_record() -> LiveOrderReconciliationRecord:
    submitted_at = datetime(2026, 5, 26, 10, 0, tzinfo=UTC)
    filled_at = datetime(2026, 5, 26, 10, 0, 2, tzinfo=UTC)
    return LiveOrderReconciliationRecord(
        decision_id="decision-1",
        decision_status="filled",
        order_id="venue-order-1",
        order_status="matched",
        order_raw_status="matched",
        market_id="market-1",
        token_id="token-yes",
        venue="polymarket",
        strategy_id="default",
        strategy_version_id="default-v1",
        requested_notional_usdc=10.0,
        filled_notional_usdc=10.0,
        remaining_notional_usdc=0.0,
        filled_quantity=25.0,
        fill_price=0.4,
        submitted_at=submitted_at,
        last_updated_at=filled_at,
        time_in_force="IOC",
        action="BUY",
        outcome="YES",
        intent_key="intent-1",
        pre_submit_quote_fingerprint="a" * 64,
        pre_submit_quote_hash="quote-hash-1",
        pre_submit_quote_source="dual",
        fill_id="fill-1",
        fill_status="matched",
        fill_notional_usdc=10.0,
        fill_quantity=25.0,
        filled_at=filled_at,
    )


def _credentialed_preflight_reference_for_test(
    settings: PMSSettings,
    *,
    prefix: str,
) -> dict[str, object]:
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix=prefix,
            settings=settings,
        )
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    return {
        "path": str(artifact_path.resolve(strict=False)),
        "sha256": sha256(artifact_path.read_bytes()).hexdigest(),
        "generated_at": artifact["generated_at"],
        "artifact_mode": artifact["artifact_mode"],
        "final_go_no_go_valid": artifact["final_go_no_go_valid"],
    }


def test_pms_live_cli_parses_preflight_command() -> None:
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            "config.live.yaml",
            "--database-url",
            "postgresql://localhost/pms_live",
            "--skip-venue",
            "--json",
            "--output",
            "docs/live/preflight.json",
        ]
    )

    assert args.command == "preflight"
    assert args.config == "config.live.yaml"
    assert args.database_url == "postgresql://localhost/pms_live"
    assert args.skip_venue is True
    assert args.json is True
    assert args.output == "docs/live/preflight.json"


def test_pms_live_cli_parses_live_order_reconcile_command() -> None:
    args = build_parser().parse_args(
        [
            "reconcile-live-order",
            "--config",
            "config.live.yaml",
            "--decision-id",
            "decision-1",
            "--reconciled-by",
            "operator",
            "--output",
            "/secure/pms/first-live-order-reconciliation.json",
        ]
    )

    assert args.command == "reconcile-live-order"
    assert args.config == "config.live.yaml"
    assert args.decision_id == "decision-1"
    assert args.reconciled_by == "operator"
    assert args.output == "/secure/pms/first-live-order-reconciliation.json"


def test_emergency_stop_audit_path_loader_rejects_symlink_config_file(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target-config.yaml"
    target_path.write_text(
        "live_emergency_audit_path: /secure/pms/live-emergency-audit.jsonl\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.symlink_to(target_path)

    with pytest.raises(ValueError, match="Config file cannot be read safely"):
        live_cli._load_emergency_stop_audit_path(str(config_path))


def test_emergency_stop_audit_path_loader_rejects_duplicate_config_key(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "live_emergency_audit_path: /secure/pms/forged-audit.jsonl",
                "live_emergency_audit_path: /secure/pms/live-emergency-audit.jsonl",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="duplicate YAML key: live_emergency_audit_path",
    ):
        live_cli._load_emergency_stop_audit_path(str(config_path))


def test_emergency_stop_audit_path_loader_opens_config_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "live_emergency_audit_path: /secure/pms/live-emergency-audit.jsonl\n",
        encoding="utf-8",
    )
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

    audit_path = live_cli._load_emergency_stop_audit_path(str(config_path))

    observed_by_path = {path: flags for path, flags in observed}
    assert audit_path == Path("/secure/pms/live-emergency-audit.jsonl")
    assert observed_by_path[config_path] & no_follow_flag


def test_emergency_stop_audit_path_loader_rejects_hardlink_swap_during_config_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "live_emergency_audit_path: /secure/pms/live-emergency-audit.jsonl\n",
        encoding="utf-8",
    )
    replacement_source = tmp_path / "replacement-config.yaml"
    replacement_source.write_text(
        "live_emergency_audit_path: /secure/pms/replacement-audit.jsonl\n",
        encoding="utf-8",
    )
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == config_path and not swapped:
            swapped = True
            config_path.unlink()
            os.link(replacement_source, config_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="Config file cannot be read safely"):
        live_cli._load_emergency_stop_audit_path(str(config_path))

    assert swapped is True


def test_pms_live_preflight_output_records_readiness_reports_fingerprint(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "credentialed-preflight.json"
    _configure_live_preflight_artifact_path(settings, output_path)

    final_go_no_go_valid = live_cli._write_preflight_artifact(
        _final_preflight_result(),
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert final_go_no_go_valid is True
    assert isinstance(artifact["readiness_reports_fingerprint"], str)
    assert artifact["readiness_reports_fingerprint"].strip() != ""


def test_pms_live_preflight_output_preserves_existing_artifact_when_truncate_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "credentialed-preflight.json"
    original_artifact_text = (
        '{"artifact_mode":"credentialed_preflight","final_go_no_go_valid":true}\n'
    )
    output_path.write_text(original_artifact_text, encoding="utf-8")
    output_path.chmod(0o600)
    _configure_live_preflight_artifact_path(settings, output_path)
    real_ftruncate = os.ftruncate

    def truncate_then_fail(fd: int, length: int) -> None:
        real_ftruncate(fd, length)
        raise OSError("simulated preflight artifact truncate failure")

    monkeypatch.setattr(os, "ftruncate", truncate_then_fail)

    with pytest.raises(OSError, match="simulated preflight artifact truncate failure"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert output_path.read_text(encoding="utf-8") == original_artifact_text


def test_pms_live_preflight_output_does_not_publish_new_artifact_when_truncate_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "credentialed-preflight.json"
    _configure_live_preflight_artifact_path(settings, output_path)
    real_ftruncate = os.ftruncate

    def truncate_then_fail(fd: int, length: int) -> None:
        real_ftruncate(fd, length)
        raise OSError("simulated preflight artifact truncate failure")

    monkeypatch.setattr(os, "ftruncate", truncate_then_fail)

    with pytest.raises(OSError, match="simulated preflight artifact truncate failure"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert not output_path.exists()


def test_pms_live_preflight_artifact_rejects_non_finite_result_payload(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "credentialed-preflight.json"
    result = LivePreflightResult(
        (
            LivePreflightCheck(
                "live_config",
                True,
                cast(str, float("nan")),
            ),
        ),
    )

    with pytest.raises(ValueError, match="result.checks\\[0\\].detail"):
        live_cli._write_preflight_artifact(
            result,
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert not output_path.exists()


def test_pms_live_preflight_artifact_records_absolute_output_path_for_relative_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = Path("../secure/credentialed-preflight.json")
    expected_output_path = secure_dir / "credentialed-preflight.json"
    _configure_live_preflight_artifact_path(settings, output_path)

    final_go_no_go_valid = live_cli._write_preflight_artifact(
        _final_preflight_result(),
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    artifact = json.loads(expected_output_path.read_text(encoding="utf-8"))
    assert final_go_no_go_valid is True
    assert artifact["output_path"] == str(expected_output_path)


def test_pms_live_preflight_output_rejects_final_artifact_path_mismatch(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    configured_dir = tmp_path / "configured-preflight"
    configured_dir.mkdir(mode=0o700)
    output_path = output_dir / "credentialed-preflight.json"
    configured_path = configured_dir / "credentialed-preflight.json"
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_preflight_artifact_path = str(configured_path)

    with pytest.raises(ValueError, match="live_preflight_artifact_path"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert not output_path.exists()


def test_pms_live_preflight_output_rejects_final_artifact_without_configured_path(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "credentialed-preflight.json"
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_preflight_artifact_path = None

    with pytest.raises(ValueError, match="live_preflight_artifact_path"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert not output_path.exists()


def test_pms_live_preflight_output_rejects_incomplete_artifact_at_configured_preflight_path(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    output_path = approval_dir / "credentialed-preflight.json"
    original_artifact_text = (
        '{"artifact_mode":"credentialed_preflight","final_go_no_go_valid":true}\n'
    )
    output_path.write_text(original_artifact_text, encoding="utf-8")
    settings = _settings(approval_path=approval_dir / "first-order.json")
    _configure_live_preflight_artifact_path(settings, output_path)

    with pytest.raises(ValueError, match="credentialed preflight artifact"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=True,
            database_url_override_used=False,
        )

    assert output_path.read_text(encoding="utf-8") == original_artifact_text


def test_pms_live_preflight_output_rejects_paper_soak_report_path(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_path = Path(cast(str, settings.live_paper_soak_report_path))
    original_report_text = output_path.read_text(encoding="utf-8")
    _configure_live_preflight_artifact_path(settings, output_path)

    with pytest.raises(ValueError, match="paper soak GO report"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert output_path.read_text(encoding="utf-8") == original_report_text


def test_pms_live_preflight_output_rejects_config_file_path(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    config_path = secure_dir / "config.live.yaml"
    original_config_text = "mode: live\n"
    config_path.write_text(original_config_text, encoding="utf-8")
    settings = _settings(approval_path=secure_dir / "first-order.json")
    settings.live_preflight_artifact_path = str(config_path)

    with pytest.raises(ValueError, match="config file"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=config_path,
            config_path=str(config_path),
            skip_venue=False,
            database_url_override_used=False,
        )

    assert config_path.read_text(encoding="utf-8") == original_config_text


@pytest.mark.asyncio
async def test_pms_live_preflight_cli_rejects_env_config_file_output_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    config_path = secure_dir / "config.live.yaml"
    configured_preflight_path = secure_dir / "credentialed-preflight.json"
    original_config_text = (
        f"live_preflight_artifact_path: {json.dumps(str(configured_preflight_path))}\n"
    )
    config_path.write_text(original_config_text, encoding="utf-8")
    monkeypatch.setenv("PMS_CONFIG_PATH", str(config_path))

    async def fake_run_live_preflight(
        settings: PMSSettings,
        *,
        skip_venue: bool,
    ) -> LivePreflightResult:
        assert settings.live_preflight_artifact_path == str(configured_preflight_path)
        assert skip_venue is False
        return LivePreflightResult(
            (
                LivePreflightCheck(
                    "venue_reconciliation",
                    False,
                    "venue reconciliation not run",
                ),
            ),
            active_strategies_fingerprint=_VALID_ACTIVE_STRATEGIES_FINGERPRINT,
        )

    monkeypatch.setattr(live_cli, "run_live_preflight", fake_run_live_preflight)
    args = build_parser().parse_args(
        [
            "preflight",
            "--json",
            "--output",
            str(config_path),
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["ok"] is False
    assert payload["checks"][-1]["name"] == "artifact_write"
    assert "config file" in payload["checks"][-1]["detail"]
    assert config_path.read_text(encoding="utf-8") == original_config_text


def test_pms_live_preflight_output_rejects_missing_config_file_path(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    config_path = secure_dir / "config.live.yaml"
    settings = _settings(approval_path=secure_dir / "first-order.json")
    settings.live_preflight_artifact_path = str(config_path)

    with pytest.raises(ValueError, match="config file"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=config_path,
            config_path=str(config_path),
            skip_venue=False,
            database_url_override_used=False,
        )

    assert not config_path.exists()


def test_pms_live_preflight_output_rejects_local_secret_file_path(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    secret_path = secure_dir / "polymarket.local-secrets.yaml"
    original_secret_text = "polymarket:\n  api_key: live-api-key\n"
    secret_path.write_text(original_secret_text, encoding="utf-8")
    settings = _settings(approval_path=secure_dir / "first-order.json")
    settings.secret_source = "local_file"
    settings.local_secret_file = str(secret_path)
    settings.live_preflight_artifact_path = str(secret_path)

    with pytest.raises(ValueError, match="local secret file"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=secret_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert secret_path.read_text(encoding="utf-8") == original_secret_text


def test_live_preflight_readiness_fingerprint_rejects_symlink_report(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    paper_report_path = Path(cast(str, settings.live_paper_soak_report_path))
    symlink_path = paper_report_path.parent / "paper-soak-go-link.md"
    symlink_path.symlink_to(paper_report_path)
    settings.live_paper_soak_report_path = str(symlink_path)

    with pytest.raises(LiveTradingDisabledError, match="regular file"):
        live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)


def test_live_preflight_readiness_fingerprint_rejects_hardlinked_report(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    paper_report_path = Path(cast(str, settings.live_paper_soak_report_path))
    hardlink_path = paper_report_path.parent / "paper-soak-go-hardlink.md"
    os.link(paper_report_path, hardlink_path)
    settings.live_paper_soak_report_path = str(hardlink_path)

    with pytest.raises(LiveTradingDisabledError, match="single-link"):
        live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)


def test_live_preflight_redacts_api_token_from_live_errors(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.api_token = "super-secret-api-token"

    redacted = live_preflight_module.redact_live_error(
        "startup failed with PMS_API_TOKEN=super-secret-api-token",
        settings,
    )

    assert "super-secret-api-token" not in redacted
    assert "<redacted-polymarket-credential>" in redacted


def test_live_preflight_readiness_fingerprint_binds_execution_model_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    before = live_preflight_module.live_preflight_readiness_reports_fingerprint(
        settings
    )

    model_path = Path(cast(str, settings.live_execution_model_path))
    payload = json.loads(model_path.read_text(encoding="utf-8"))
    payload["slippage_bps"] = 12.0
    model_path.write_text(json.dumps(payload), encoding="utf-8")

    after = live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)

    assert after != before


def test_live_preflight_readiness_fingerprint_binds_paper_backtest_diff_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    before = live_preflight_module.live_preflight_readiness_reports_fingerprint(
        settings
    )

    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    payload["metrics"]["total_pnl_delta_abs"] = 0.5
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    after = live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)

    assert after != before


def test_live_preflight_readiness_fingerprint_binds_category_prior_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    before = live_preflight_module.live_preflight_readiness_reports_fingerprint(
        settings
    )

    prior_path = Path(cast(str, settings.controller.category_prior_observations_path))
    prior_path.write_text(
        prior_path.read_text(encoding="utf-8").replace(
            "m-1,sports,0,1,",
            "m-1,sports,1,0,",
            1,
        ),
        encoding="utf-8",
    )

    after = live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)

    assert after != before


def test_live_preflight_readiness_fingerprint_binds_flb_calibration_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    before = live_preflight_module.live_preflight_readiness_reports_fingerprint(
        settings
    )

    calibration_path = Path(cast(str, settings.strategies.flb_calibration_path))
    calibration_path.write_text(
        calibration_path.read_text(encoding="utf-8").replace(
            "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
            "longshot_yes_overpriced_buy_no,0.98,150,warehouse-flb-v1",
            1,
        ),
        encoding="utf-8",
    )

    after = live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)

    assert after != before


@pytest.mark.parametrize(
    ("artifact_name", "expected_detail"),
    [
        ("execution_model", "execution-model artifact"),
        ("paper_backtest_diff", "paper-vs-backtest execution diff artifact"),
        ("category_prior", "category-prior artifact"),
        ("flb_calibration", "FLB calibration artifact"),
    ],
)
def test_live_preflight_readiness_fingerprint_rejects_strategy_artifact_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_name: str,
    expected_detail: str,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    artifact_dir = repo_root / "secure-artifacts"
    artifact_dir.mkdir(mode=0o700)

    if artifact_name == "execution_model":
        source_path = Path(cast(str, settings.live_execution_model_path))
        target_path = artifact_dir / source_path.name
        target_path.write_bytes(source_path.read_bytes())
        settings.live_execution_model_path = str(target_path)
    elif artifact_name == "paper_backtest_diff":
        source_path = Path(cast(str, settings.live_paper_backtest_diff_path))
        target_path = artifact_dir / source_path.name
        target_path.write_bytes(source_path.read_bytes())
        settings.live_paper_backtest_diff_path = str(target_path)
    elif artifact_name == "category_prior":
        source_path = Path(cast(str, settings.controller.category_prior_observations_path))
        target_path = artifact_dir / source_path.name
        target_path.write_bytes(source_path.read_bytes())
        settings.controller.category_prior_observations_path = str(target_path)
    elif artifact_name == "flb_calibration":
        source_path = Path(cast(str, settings.strategies.flb_calibration_path))
        target_path = artifact_dir / source_path.name
        target_path.write_bytes(source_path.read_bytes())
        settings.strategies.flb_calibration_path = str(target_path)
    else:
        raise AssertionError(f"unknown artifact_name: {artifact_name}")
    monkeypatch.chdir(repo_root)

    with pytest.raises(LiveTradingDisabledError) as exc_info:
        live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)

    detail = str(exc_info.value)
    assert expected_detail in detail
    assert "working tree" in detail


def test_live_preflight_readiness_fingerprint_rejects_permissive_paper_report_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    paper_report_path = Path(cast(str, settings.live_paper_soak_report_path))
    paper_report_path.parent.chmod(0o755)

    try:
        with pytest.raises(LiveTradingDisabledError, match="parent directory"):
            live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)
    finally:
        paper_report_path.parent.chmod(0o700)


def test_live_preflight_readiness_fingerprint_rejects_symlink_paper_report_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    paper_report_path = Path(cast(str, settings.live_paper_soak_report_path))
    symlink_parent = tmp_path / "paper-report-parent-link"
    symlink_parent.symlink_to(paper_report_path.parent, target_is_directory=True)
    settings.live_paper_soak_report_path = str(symlink_parent / paper_report_path.name)

    with pytest.raises(LiveTradingDisabledError, match="not a directory"):
        live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)


def test_live_preflight_readiness_fingerprint_rejects_permissive_rehearsal_report_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    private_paper_report_path, _ = make_live_report_paths(
        prefix="pms-live-preflight-private-paper-parent-"
    )
    _, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-preflight-permissive-rehearsal-parent-"
    )
    rehearsal_parent = Path(rehearsal_report_path).parent
    rehearsal_parent.chmod(0o755)
    settings.live_paper_soak_report_path = private_paper_report_path
    settings.live_operator_rehearsal_report_path = rehearsal_report_path

    try:
        with pytest.raises(LiveTradingDisabledError, match="parent directory"):
            live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)
    finally:
        rehearsal_parent.chmod(0o700)


def test_live_preflight_readiness_fingerprint_rejects_symlink_rehearsal_report_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    private_paper_report_path, _ = make_live_report_paths(
        prefix="pms-live-preflight-private-paper-symlink-parent-"
    )
    _, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-preflight-rehearsal-symlink-parent-"
    )
    rehearsal_path = Path(rehearsal_report_path)
    symlink_parent = tmp_path / "rehearsal-report-parent-link"
    symlink_parent.symlink_to(rehearsal_path.parent, target_is_directory=True)
    settings.live_paper_soak_report_path = private_paper_report_path
    settings.live_operator_rehearsal_report_path = str(
        symlink_parent / rehearsal_path.name
    )

    with pytest.raises(LiveTradingDisabledError, match="not a directory"):
        live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)


def test_live_preflight_readiness_fingerprint_opens_reports_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    expected_paths = {
        Path(cast(str, settings.live_paper_soak_report_path)),
        Path(cast(str, settings.live_operator_rehearsal_report_path)),
        Path(cast(str, settings.live_execution_model_path)),
        Path(cast(str, settings.live_paper_backtest_diff_path)),
        Path(cast(str, settings.controller.category_prior_observations_path)),
        Path(cast(str, settings.strategies.flb_calibration_path)),
    }
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

    live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)

    observed_by_path = {path: flags for path, flags in observed}
    assert set(observed_by_path) == expected_paths
    assert all(flags & no_follow_flag for flags in observed_by_path.values())


def test_pms_live_preflight_output_marks_duplicate_checks_as_not_final(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "duplicate-checks-preflight.json"
    result = LivePreflightResult(
        (
            *_final_preflight_result().checks,
            LivePreflightCheck(
                "venue_reconciliation",
                True,
                "duplicate venue reconciliation row",
            ),
        ),
        active_strategies_fingerprint=_VALID_ACTIVE_STRATEGIES_FINGERPRINT,
    )

    final_go_no_go_valid = live_cli._write_preflight_artifact(
        result,
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert final_go_no_go_valid is False
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["readiness_reports_fingerprint"] is None


def test_pms_live_preflight_output_marks_unknown_checks_as_not_final(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "unknown-checks-preflight.json"
    result = LivePreflightResult(
        (
            *_final_preflight_result().checks,
            LivePreflightCheck(
                "operator_shadow_check",
                True,
                "unknown operator shadow row",
            ),
        ),
        active_strategies_fingerprint=_VALID_ACTIVE_STRATEGIES_FINGERPRINT,
    )

    final_go_no_go_valid = live_cli._write_preflight_artifact(
        result,
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert final_go_no_go_valid is False
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["readiness_reports_fingerprint"] is None


def test_pms_live_preflight_output_marks_empty_check_detail_as_not_final(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "empty-detail-preflight.json"
    checks = [
        LivePreflightCheck(check.name, check.ok, check.detail)
        for check in _final_preflight_result().checks
    ]
    checks[-1] = LivePreflightCheck(
        checks[-1].name,
        checks[-1].ok,
        "",
    )
    result = LivePreflightResult(
        tuple(checks),
        active_strategies_fingerprint=_VALID_ACTIVE_STRATEGIES_FINGERPRINT,
    )

    final_go_no_go_valid = live_cli._write_preflight_artifact(
        result,
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert final_go_no_go_valid is False
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["readiness_reports_fingerprint"] is None


def test_pms_live_preflight_output_marks_placeholder_check_detail_as_not_final(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "detail-marker-preflight.json"
    checks = [
        LivePreflightCheck(check.name, check.ok, check.detail)
        for check in _final_preflight_result().checks
    ]
    checks[-1] = LivePreflightCheck(
        checks[-1].name,
        checks[-1].ok,
        "TODO: confirm venue reconciliation",
    )
    result = LivePreflightResult(
        tuple(checks),
        active_strategies_fingerprint=_VALID_ACTIVE_STRATEGIES_FINGERPRINT,
    )

    final_go_no_go_valid = live_cli._write_preflight_artifact(
        result,
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert final_go_no_go_valid is False
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["readiness_reports_fingerprint"] is None


def test_pms_live_preflight_output_marks_placeholder_active_strategy_fingerprint_as_not_final(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "active-strategy-marker-preflight.json"
    result = LivePreflightResult(
        _final_preflight_result().checks,
        active_strategies_fingerprint="TODO: compute active strategies fingerprint",
    )

    final_go_no_go_valid = live_cli._write_preflight_artifact(
        result,
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert final_go_no_go_valid is False
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["readiness_reports_fingerprint"] is None


def test_pms_live_preflight_output_marks_non_hash_active_strategy_fingerprint_as_not_final(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "non-hash-active-strategy-preflight.json"
    result = LivePreflightResult(
        _final_preflight_result().checks,
        active_strategies_fingerprint="active-strategy-fingerprint",
    )

    final_go_no_go_valid = live_cli._write_preflight_artifact(
        result,
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert final_go_no_go_valid is False
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["readiness_reports_fingerprint"] is None


def test_pms_live_preflight_output_rejects_final_artifact_permissive_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "permissive-preflight"
    output_dir.mkdir(mode=0o755)
    output_path = output_dir / "credentialed-preflight.json"
    _configure_live_preflight_artifact_path(settings, output_path)

    try:
        with pytest.raises(
            LiveTradingDisabledError,
            match="preflight artifact parent.*too permissive",
        ):
            live_cli._write_preflight_artifact(
                _final_preflight_result(),
                settings=settings,
                output_path=output_path,
                config_path="config.live.yaml",
                skip_venue=False,
                database_url_override_used=False,
            )
    finally:
        output_dir.chmod(0o700)
    assert not output_path.exists()


def test_pms_live_preflight_output_rejects_final_artifact_missing_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_path = tmp_path / "missing-preflight" / "credentialed-preflight.json"
    _configure_live_preflight_artifact_path(settings, output_path)

    with pytest.raises(
        LiveTradingDisabledError,
        match="preflight artifact parent does not exist",
    ):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )
    assert not output_path.exists()


def test_pms_live_preflight_output_rejects_placeholder_artifact_path(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "__FILL_IN_PREFLIGHT_ARTIFACT__.json"
    _configure_live_preflight_artifact_path(settings, output_path)

    with pytest.raises(LiveTradingDisabledError, match="path contains placeholder"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )
    assert not output_path.exists()


def test_pms_live_preflight_output_rejects_final_artifact_symlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    target_dir = tmp_path / "artifact-target"
    target_dir.mkdir(mode=0o700)
    target_path = target_dir / "target.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    output_path = output_dir / "credentialed-preflight.json"
    output_path.symlink_to(target_path)
    _configure_live_preflight_artifact_path(settings, output_path)

    with pytest.raises(LiveTradingDisabledError, match="regular file"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"


def test_pms_live_preflight_output_rejects_final_artifact_hardlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    target_dir = tmp_path / "artifact-target"
    target_dir.mkdir(mode=0o700)
    target_path = target_dir / "target.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    output_path = output_dir / "credentialed-preflight.json"
    os.link(target_path, output_path)
    _configure_live_preflight_artifact_path(settings, output_path)

    with pytest.raises(LiveTradingDisabledError, match="single-link"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"


def test_pms_live_preflight_output_hardlink_swap_during_atomic_publish_keeps_linked_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    target_dir = tmp_path / "artifact-target"
    target_dir.mkdir(mode=0o700)
    target_path = target_dir / "target.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    output_path = output_dir / "credentialed-preflight.json"
    output_path.write_text("old single-link output\n", encoding="utf-8")
    _configure_live_preflight_artifact_path(settings, output_path)
    real_replace = os.replace
    swapped = False

    def swapping_replace(
        src: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        dst: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    ) -> None:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(dst)))
        if observed_path == output_path and not swapped:
            swapped = True
            output_path.unlink()
            os.link(target_path, output_path)
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", swapping_replace)

    live_cli._write_preflight_artifact(
        _final_preflight_result(),
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    assert swapped is True
    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"
    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert artifact["artifact_mode"] == "credentialed_preflight"


def test_pms_live_preflight_output_rejects_incomplete_artifact_symlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    target_path = tmp_path / "target-incomplete-preflight.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    output_path = output_dir / "incomplete-preflight.json"
    output_path.symlink_to(target_path)

    with pytest.raises(OSError, match="regular file"):
        live_cli._write_preflight_artifact(
            LivePreflightResult(
                (
                    LivePreflightCheck(
                        "live_config",
                        False,
                        "config failed before final go/no-go",
                    ),
                )
            ),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"


def test_pms_live_preflight_output_creates_incomplete_artifact_parent_private(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "missing-incomplete-preflight"
    output_path = output_dir / "incomplete-preflight.json"

    final_go_no_go_valid = live_cli._write_preflight_artifact(
        LivePreflightResult(
            (
                LivePreflightCheck(
                    "live_config",
                    False,
                    "config failed before final go/no-go",
                ),
            )
        ),
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    assert final_go_no_go_valid is False
    assert output_path.exists()
    assert stat.S_IMODE(output_dir.stat().st_mode) == 0o700


def test_pms_live_preflight_output_rejects_incomplete_artifact_permissive_parent(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "permissive-incomplete-preflight"
    output_dir.mkdir(mode=0o700)
    output_dir.chmod(0o755)
    output_path = output_dir / "incomplete-preflight.json"

    try:
        with pytest.raises(OSError, match="preflight artifact parent"):
            live_cli._write_preflight_artifact(
                LivePreflightResult(
                    (
                        LivePreflightCheck(
                            "live_config",
                            False,
                            "config failed before final go/no-go",
                        ),
                    )
                ),
                settings=settings,
                output_path=output_path,
                config_path="config.live.yaml",
                skip_venue=False,
                database_url_override_used=False,
            )
    finally:
        output_dir.chmod(0o700)

    assert not output_path.exists()


def test_pms_live_preflight_output_rejects_incomplete_artifact_hardlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    target_path = tmp_path / "target-incomplete-preflight.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    output_path = output_dir / "incomplete-preflight.json"
    os.link(target_path, output_path)

    with pytest.raises(OSError, match="single-link"):
        live_cli._write_preflight_artifact(
            LivePreflightResult(
                (
                    LivePreflightCheck(
                        "live_config",
                        False,
                        "config failed before final go/no-go",
                    ),
                )
            ),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"


def test_pms_live_preflight_output_opens_temp_artifact_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    output_dir = tmp_path / "preflight"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "credentialed-preflight.json"
    _configure_live_preflight_artifact_path(settings, output_path)
    observed_write_flags: list[int] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        if flags & os.O_WRONLY:
            observed_write_flags.append(flags)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", recording_open)

    live_cli._write_preflight_artifact(
        _final_preflight_result(),
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )

    assert len(observed_write_flags) == 1
    assert observed_write_flags[0] & no_follow_flag


@pytest.mark.asyncio
async def test_pms_live_preflight_cli_reports_artifact_write_failure_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_path = tmp_path / "missing-preflight" / "credentialed-preflight.json"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "mode: live\n"
        f"live_preflight_artifact_path: {json.dumps(str(output_path))}\n",
        encoding="utf-8",
    )

    async def fake_run_live_preflight(
        settings: PMSSettings,
        *,
        skip_venue: bool,
    ) -> LivePreflightResult:
        del settings
        assert skip_venue is False
        return _final_preflight_result()

    monkeypatch.setattr(live_cli, "run_live_preflight", fake_run_live_preflight)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--json",
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["ok"] is False
    assert payload["checks"][-1] == {
        "name": "artifact_write",
        "ok": False,
        "detail": (
            "LIVE credentialed preflight artifact parent does not exist: "
            f"{output_path.parent}"
        ),
    }
    assert not output_path.exists()


@pytest.mark.asyncio
async def test_pms_live_preflight_output_marks_database_url_override_as_not_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "preflight" / "credentialed-preflight.json"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text("mode: live\n", encoding="utf-8")
    observed_skip_venue: list[bool] = []

    async def fake_run_live_preflight(
        settings: PMSSettings,
        *,
        skip_venue: bool,
    ) -> LivePreflightResult:
        assert settings.database.dsn == "postgresql://user:secret@db.example/pms_live"
        observed_skip_venue.append(skip_venue)
        return LivePreflightResult(
            (
                LivePreflightCheck(
                    "live_config",
                    True,
                    "LIVE config validates",
                ),
                LivePreflightCheck(
                    "venue_reconciliation",
                    True,
                    "venue account snapshot reconciles",
                ),
            )
        )

    monkeypatch.setattr(live_cli, "run_live_preflight", fake_run_live_preflight)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--database-url",
            "postgresql://user:secret@db.example/pms_live",
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert observed_skip_venue == [False]
    assert artifact["generated_by"] == "pms-live preflight"
    assert "generated_at" in artifact
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["skip_venue"] is False
    assert artifact["database_url_override_used"] is True
    assert artifact["config_path"] == str(config_path)
    assert artifact["output_path"] == str(output_path)
    assert "settings_fingerprint" in artifact
    assert artifact["result"]["ok"] is True
    assert "secret" not in output_path.read_text(encoding="utf-8")
    assert "db.example" not in output_path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_pms_live_preflight_output_marks_skip_venue_as_not_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "preflight-debug.json"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text("mode: live\n", encoding="utf-8")

    async def fake_run_live_preflight(
        settings: PMSSettings,
        *,
        skip_venue: bool,
    ) -> LivePreflightResult:
        del settings
        assert skip_venue is True
        return LivePreflightResult(
            (
                LivePreflightCheck(
                    "venue_reconciliation",
                    False,
                    "skipped by operator flag",
                ),
            )
        )

    monkeypatch.setattr(live_cli, "run_live_preflight", fake_run_live_preflight)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--skip-venue",
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["skip_venue"] is True
    assert artifact["result"]["ok"] is False


@pytest.mark.asyncio
async def test_pms_live_preflight_output_marks_missing_required_checks_as_not_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "preflight-missing-checks.json"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text("mode: live\n", encoding="utf-8")

    async def fake_run_live_preflight(
        settings: PMSSettings,
        *,
        skip_venue: bool,
    ) -> LivePreflightResult:
        del settings
        assert skip_venue is False
        return LivePreflightResult(
            (
                LivePreflightCheck(
                    "live_config",
                    True,
                    "LIVE config validates",
                ),
            )
        )

    monkeypatch.setattr(live_cli, "run_live_preflight", fake_run_live_preflight)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["result"]["ok"] is True


@pytest.mark.asyncio
async def test_pms_live_preflight_output_records_active_strategy_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "preflight-active-strategies.json"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text("mode: live\n", encoding="utf-8")

    async def fake_run_live_preflight(
        settings: PMSSettings,
        *,
        skip_venue: bool,
    ) -> LivePreflightResult:
        del settings
        assert skip_venue is False
        return LivePreflightResult(
            (
                LivePreflightCheck(
                    "active_strategies",
                    True,
                    "1 active strategy version validates for LIVE: default@v1",
                ),
            ),
            active_strategies_fingerprint=_VALID_ACTIVE_STRATEGIES_FINGERPRINT,
        )

    monkeypatch.setattr(live_cli, "run_live_preflight", fake_run_live_preflight)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert (
        artifact["active_strategies_fingerprint"]
        == _VALID_ACTIVE_STRATEGIES_FINGERPRINT
    )
    assert artifact["result"]["checks"] == [
        {
            "name": "active_strategies",
            "ok": True,
            "detail": "1 active strategy version validates for LIVE: default@v1",
        }
    ]


@pytest.mark.asyncio
async def test_pms_live_preflight_output_requires_active_strategy_fingerprint_for_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "preflight-missing-active-fingerprint.json"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text("mode: live\n", encoding="utf-8")
    final_check_names = (
        "live_config",
        "runtime_dependencies",
        "operator_approval",
        "emergency_audit",
        "first_order_audit",
        "database_connection",
        "schema_current",
        "market_data_freshness",
        "submission_unknown",
        "live_open_orders",
        "active_strategies",
        "venue_reconciliation",
    )

    async def fake_run_live_preflight(
        settings: PMSSettings,
        *,
        skip_venue: bool,
    ) -> LivePreflightResult:
        del settings
        assert skip_venue is False
        return LivePreflightResult(
            tuple(
                LivePreflightCheck(
                    name,
                    True,
                    f"{name} passed",
                )
                for name in final_check_names
            ),
            active_strategies_fingerprint=None,
        )

    monkeypatch.setattr(live_cli, "run_live_preflight", fake_run_live_preflight)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert artifact["artifact_mode"] == "incomplete_preflight"
    assert artifact["final_go_no_go_valid"] is False
    assert artifact["result"]["ok"] is True
    assert artifact["active_strategies_fingerprint"] is None


@pytest.mark.asyncio
async def test_pms_live_preflight_reports_config_load_failure_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    missing_secret_path = tmp_path / "missing-polymarket-secrets.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {missing_secret_path}",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("preflight must not connect after config load failure")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--json",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload == {
        "ok": False,
        "checks": [
            {
                "name": "config_load",
                "ok": False,
                "detail": f"Local secret file does not exist: {missing_secret_path}",
            }
        ],
    }


@pytest.mark.asyncio
async def test_pms_live_preflight_redacts_malformed_local_secret_value_from_config_load_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir(mode=0o700)
    secret_dir.chmod(0o700)
    secret_path = secret_dir / "polymarket.local-secrets.yaml"
    credential_values = (
        "private-key-secret",
        "api-key-secret",
        "api-secret-secret",
        "passphrase-secret",
        "0x1111111111111111111111111111111111111111",
    )
    secret_path.write_text(
        "\n".join(
            [
                "polymarket:",
                "  private_key:",
                f"    raw: {credential_values[0]}",
                f"  api_key: {credential_values[1]}",
                f"  api_secret: {credential_values[2]}",
                f"  api_passphrase: {credential_values[3]}",
                "  signature_type: 1",
                f"  funder_address: '{credential_values[4]}'",
            ]
        ),
        encoding="utf-8",
    )
    secret_path.chmod(0o600)
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
                "live_trading_enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("preflight must not connect after config load failure")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--json",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload["checks"][0]["name"] == "config_load"
    assert payload["checks"][0]["ok"] is False
    detail = payload["checks"][0]["detail"]
    assert "private_key" in detail
    for credential_value in credential_values:
        assert credential_value not in detail


@pytest.mark.asyncio
async def test_pms_live_preflight_rejects_placeholder_local_secret_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir(mode=0o700)
    secret_dir.chmod(0o700)
    secret_path = secret_dir / "polymarket.local-secrets.yaml"
    placeholder_private_key = "__FILL_IN_PRIVATE_KEY__"
    secret_path.write_text(
        "\n".join(
            [
                "polymarket:",
                f"  private_key: {placeholder_private_key}",
                "  api_key: api-key-secret",
                "  api_secret: api-secret-secret",
                "  api_passphrase: passphrase-secret",
                "  signature_type: 1",
                "  funder_address: '0x1111111111111111111111111111111111111111'",
            ]
        ),
        encoding="utf-8",
    )
    secret_path.chmod(0o600)
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
                "live_trading_enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("preflight must not connect after config load failure")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--json",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload["checks"][0]["name"] == "config_load"
    assert payload["checks"][0]["ok"] is False
    detail = payload["checks"][0]["detail"]
    assert "placeholder" in detail
    assert "private_key" in detail
    assert placeholder_private_key not in detail


@pytest.mark.asyncio
async def test_pms_live_preflight_redacts_malformed_local_secret_yaml_from_config_load_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir(mode=0o700)
    secret_dir.chmod(0o700)
    secret_path = secret_dir / "polymarket.local-secrets.yaml"
    secret_path.write_text(
        "polymarket:\n  private_key: [private-key-secret\n",
        encoding="utf-8",
    )
    secret_path.chmod(0o600)
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
                "live_trading_enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("preflight must not connect after config load failure")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--json",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload["checks"][0]["name"] == "config_load"
    assert payload["checks"][0]["ok"] is False
    detail = payload["checks"][0]["detail"]
    assert "Local secret file is not valid YAML" in detail
    assert "private-key-secret" not in detail


@pytest.mark.asyncio
async def test_pms_live_preflight_redacts_discord_webhook_from_config_load_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text("mode: live\n", encoding="utf-8")
    webhook_secret = "super-secret-webhook-token"
    monkeypatch.setenv(
        "PMS_DISCORD__WEBHOOK_URL",
        f"http://discord.com/api/webhooks/{webhook_secret}",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("preflight must not connect after config load failure")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "preflight",
            "--config",
            str(config_path),
            "--json",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload["checks"][0]["name"] == "config_load"
    detail = payload["checks"][0]["detail"]
    assert "<redacted-discord-webhook-url>" in detail
    assert webhook_secret not in detail
    assert "discord.com/api/webhooks" not in detail


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_uses_config_database_dsn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  dsn: postgresql://configured.example/pms_live",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls: list[str] = []
    reconcile_calls: list[dict[str, object]] = []
    call_order: list[str] = []

    class FakePool:
        async def close(self) -> None:
            return None

    fake_pool = FakePool()

    async def fake_create_pool(
        *,
        dsn: str,
        min_size: int,
        max_size: int,
    ) -> FakePool:
        assert min_size == 1
        assert max_size == 1
        create_pool_calls.append(dsn)
        return fake_pool

    async def fake_ensure_schema_current(pool: object) -> None:
        assert pool is fake_pool
        call_order.append("schema")

    class FakeSubmissionUnknownReconciliationStore:
        def __init__(self, pool: object) -> None:
            assert pool is fake_pool

        async def reconcile_submission_unknown(
            self,
            *,
            decision_id: str,
            venue_order_id: str | None,
            status: str,
            reconciled_by: str,
            note: str | None,
        ) -> bool:
            call_order.append("reconcile")
            reconcile_calls.append(
                {
                    "decision_id": decision_id,
                    "venue_order_id": venue_order_id,
                    "status": status,
                    "reconciled_by": reconciled_by,
                    "note": note,
                }
            )
            return True

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(
        live_cli,
        "ensure_schema_current",
        fake_ensure_schema_current,
        raising=False,
    )
    monkeypatch.setattr(
        live_cli,
        "SubmissionUnknownReconciliationStore",
        FakeSubmissionUnknownReconciliationStore,
    )
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            "decision-1",
            "--venue-order-id",
            "venue-order-1",
            "--status",
            "filled",
            "--reconciled-by",
            "operator",
            "--note",
            "matched venue fill",
        ]
    )

    exit_code = await live_cli._main_async(args)

    assert exit_code == 0
    assert create_pool_calls == ["postgresql://configured.example/pms_live"]
    assert call_order == ["schema", "reconcile"]
    assert reconcile_calls == [
        {
            "decision_id": "decision-1",
            "venue_order_id": "venue-order-1",
            "status": "filled",
            "reconciled_by": "operator",
            "note": "matched venue fill",
        }
    ]


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_reports_no_update_with_actionable_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  dsn: postgresql://configured.example/pms_live",
            ]
        ),
        encoding="utf-8",
    )
    close_calls = 0
    call_order: list[str] = []

    class FakePool:
        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    fake_pool = FakePool()

    async def fake_create_pool(
        *,
        dsn: str,
        min_size: int,
        max_size: int,
    ) -> FakePool:
        assert dsn == "postgresql://configured.example/pms_live"
        assert min_size == 1
        assert max_size == 1
        return fake_pool

    async def fake_ensure_schema_current(pool: object) -> None:
        assert pool is fake_pool
        call_order.append("schema")

    class FakeSubmissionUnknownReconciliationStore:
        def __init__(self, pool: object) -> None:
            assert pool is fake_pool

        async def reconcile_submission_unknown(self, **_: object) -> bool:
            call_order.append("reconcile")
            return False

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(live_cli, "ensure_schema_current", fake_ensure_schema_current)
    monkeypatch.setattr(
        live_cli,
        "SubmissionUnknownReconciliationStore",
        FakeSubmissionUnknownReconciliationStore,
    )
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            "decision-1",
            "--venue-order-id",
            "venue-order-1",
            "--status",
            "filled",
            "--reconciled-by",
            "operator",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert close_calls == 1
    assert call_order == ["schema", "reconcile"]
    assert payload == {
        "decision_id": "decision-1",
        "error": (
            "submission_unknown incident was not updated; verify the decision is "
            "still submission_unknown and has not already been reconciled: decision-1"
        ),
        "status": "filled",
        "updated": False,
    }


@pytest.mark.asyncio
async def test_reconcile_live_order_writes_post_live_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    output_dir = tmp_path / "artifacts"
    output_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    output_path = Path("../artifacts/first-live-order-reconciliation.json")
    expected_output_path = output_dir / "first-live-order-reconciliation.json"
    base_settings = _settings(approval_path=approval_dir / "first-order.json")
    settings = base_settings.model_copy(
        update={
            "database": base_settings.database.model_copy(
                update={"dsn": "postgresql://operator:secret@db.example/pms_live"}
            ),
            "risk": base_settings.risk.model_copy(
                update={
                    "max_total_exposure": 100.0,
                    "max_exposure_per_risk_group": 50.0,
                }
            ),
        }
    )
    settings.live_preflight_artifact_path = make_live_preflight_artifact_path(
        prefix="pms-live-order-reconcile-preflight-",
        settings=settings,
    )
    fake_pool = _CliFakePool()
    create_pool_calls: list[str] = []
    call_order: list[str] = []
    compared_portfolios: list[Portfolio] = []
    submitted_at = datetime.now(tz=UTC)
    filled_at = submitted_at + timedelta(seconds=2)
    record = replace(
        _live_order_record(),
        submitted_at=submitted_at,
        last_updated_at=filled_at,
        filled_at=filled_at,
    )

    def fake_load_settings(config_path: str | None) -> PMSSettings:
        assert config_path == "config.live.yaml"
        return settings

    async def fake_create_pool(
        *,
        dsn: str,
        min_size: int,
        max_size: int,
    ) -> _CliFakePool:
        assert min_size == 1
        assert max_size == 1
        create_pool_calls.append(dsn)
        return fake_pool

    async def fake_ensure_schema_current(pool: object) -> None:
        assert pool is fake_pool
        call_order.append("schema")

    class FakeLiveOrderReconciliationStore:
        def __init__(self, pool: object) -> None:
            assert pool is fake_pool

        async def load_live_order_record(
            self,
            *,
            decision_id: str,
        ) -> LiveOrderReconciliationRecord:
            assert decision_id == "decision-1"
            call_order.append("record")
            return record

    class FakeFillStore:
        def __init__(self, pool: object) -> None:
            assert pool is fake_pool

        async def read_positions(self) -> list[Position]:
            call_order.append("positions")
            return [
                Position(
                    market_id="market-1",
                    token_id="token-yes",
                    venue="polymarket",
                    side="BUY",
                    shares_held=25.0,
                    avg_entry_price=0.4,
                    unrealized_pnl=0.0,
                    locked_usdc=10.0,
                    risk_group_id="rg-1",
                )
            ]

    class FakeVenueReconciler:
        async def snapshot(self, credentials: object) -> VenueAccountSnapshot:
            assert credentials == settings.polymarket.credentials()
            call_order.append("snapshot")
            return VenueAccountSnapshot(
                balances={"USDC": 90.0},
                open_orders=(),
                positions=(),
            )

        async def compare(
            self,
            portfolio: Portfolio,
            venue_snapshot: VenueAccountSnapshot,
        ) -> ReconciliationReport:
            assert venue_snapshot.balances == {"USDC": 90.0}
            call_order.append("compare")
            compared_portfolios.append(portfolio)
            return ReconciliationReport(ok=True, mismatches=())

    monkeypatch.setattr(live_cli, "_load_cli_settings", fake_load_settings)
    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(live_cli, "ensure_schema_current", fake_ensure_schema_current)
    monkeypatch.setattr(
        live_cli,
        "LiveOrderReconciliationStore",
        FakeLiveOrderReconciliationStore,
    )
    monkeypatch.setattr(live_cli, "FillStore", FakeFillStore)
    monkeypatch.setattr(live_cli, "PolymarketVenueAccountReconciler", FakeVenueReconciler)
    args = build_parser().parse_args(
        [
            "reconcile-live-order",
            "--config",
            "config.live.yaml",
            "--decision-id",
            "decision-1",
            "--reconciled-by",
            "operator",
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    cli_payload = json.loads(capsys.readouterr().out)
    artifact_text = expected_output_path.read_text(encoding="utf-8")
    artifact = json.loads(artifact_text)
    assert exit_code == 0
    assert fake_pool.close_calls == 1
    assert create_pool_calls == ["postgresql://operator:secret@db.example/pms_live"]
    assert call_order == ["schema", "record", "positions", "snapshot", "compare"]
    assert compared_portfolios[0].locked_usdc == 10.0
    assert compared_portfolios[0].free_usdc == 90.0
    assert cli_payload == {
        "artifact_mode": "post_live_order_reconciliation",
        "decision_id": "decision-1",
        "final_post_live_valid": True,
        "output_path": str(expected_output_path),
        "reconciled": True,
    }
    assert artifact["generated_by"] == "pms-live reconcile-live-order"
    assert artifact["artifact_mode"] == "post_live_order_reconciliation"
    assert artifact["final_post_live_valid"] is True
    assert artifact["decision_id"] == "decision-1"
    assert artifact["reconciled_by"] == "operator"
    assert artifact["output_path"] == str(expected_output_path)
    assert artifact["database_url_override_used"] is False
    assert artifact["order"]["order_id"] == "venue-order-1"
    assert artifact["order"]["pre_submit_quote_hash"] == "quote-hash-1"
    assert artifact["fill"]["fill_id"] == "fill-1"
    assert artifact["venue_reconciliation"] == {"ok": True, "mismatches": []}
    assert artifact["portfolio"]["open_positions_count"] == 1
    assert artifact["credentialed_preflight_artifact"]["path"] == str(
        Path(settings.live_preflight_artifact_path).resolve(strict=False)
    )
    assert isinstance(artifact["credentialed_preflight_artifact"]["sha256"], str)
    assert artifact["credentialed_preflight_artifact"]["artifact_mode"] == (
        "credentialed_preflight"
    )
    assert (
        artifact["credentialed_preflight_artifact"]["final_go_no_go_valid"] is True
    )
    assert isinstance(
        artifact["credentialed_preflight_artifact"]["generated_at"],
        str,
    )
    assert "settings_fingerprint" in artifact
    assert "secret" not in artifact_text
    assert "db.example" not in artifact_text


def test_reconcile_live_order_artifact_records_absolute_output_path_for_relative_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = Path("../secure/first-live-order-reconciliation.json")
    expected_output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )

    live_cli._write_live_order_reconciliation_artifact(
        _live_order_record(),
        report=ReconciliationReport(ok=True, mismatches=()),
        settings=settings,
        output_path=output_path,
        config_path="config.live.yaml",
        reconciled_by="operator",
        database_url_override_used=False,
        portfolio=Portfolio(
            total_usdc=100.0,
            free_usdc=90.0,
            locked_usdc=10.0,
            open_positions=[],
        ),
        final_post_live_valid=True,
        credentialed_preflight_artifact=credentialed_preflight_artifact,
    )

    artifact = json.loads(expected_output_path.read_text(encoding="utf-8"))
    assert artifact["output_path"] == str(expected_output_path)


def test_reconcile_live_order_artifact_rejects_output_inside_discord_alert_dir(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    alert_dir = Path(settings.discord.alert_dir)
    alert_dir.mkdir(mode=0o700)
    output_path = alert_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )

    with pytest.raises(ValueError, match="discord alert directory"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_requires_preflight_reference_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"

    with pytest.raises(ValueError, match="credentialed_preflight_artifact"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=None,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_incomplete_preflight_reference_metadata(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-order-reconcile-incomplete-preflight-reference-",
            settings=settings,
        )
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    credentialed_preflight_artifact = {
        "path": str(artifact_path.resolve(strict=False)),
        "sha256": sha256(artifact_path.read_bytes()).hexdigest(),
    }

    with pytest.raises(
        ValueError,
        match="credentialed_preflight_artifact.*generated_at",
    ):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_requires_configured_preflight_path_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-missing-configured-preflight-",
    )
    settings.live_preflight_artifact_path = None

    with pytest.raises(ValueError, match="configured live_preflight_artifact_path"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


@pytest.mark.parametrize(
    ("credentialed_preflight_artifact", "expected_error"),
    (
        (
            {
                "path": "__FILL_IN_PREFLIGHT_ARTIFACT__.json",
                "sha256": "a" * 64,
            },
            "path contains placeholder",
        ),
        (
            {
                "path": "/secure/pms/credentialed-preflight.json",
                "sha256": "not-a-sha256-digest",
            },
            "sha256",
        ),
    ),
)
def test_reconcile_live_order_artifact_rejects_malformed_preflight_reference(
    tmp_path: Path,
    credentialed_preflight_artifact: dict[str, str],
    expected_error: str,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"

    with pytest.raises(ValueError, match=expected_error):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_preflight_reference_sha_mismatch(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-sha-mismatch-",
    )
    credentialed_preflight_artifact["sha256"] = "b" * 64

    with pytest.raises(ValueError, match="sha256.*match"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_preflight_reference_path_mismatch(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-configured-preflight-",
    )
    other_artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-order-reconcile-other-preflight-",
            settings=settings,
        )
    )
    credentialed_preflight_artifact = {
        "path": str(other_artifact_path.resolve(strict=False)),
        "sha256": sha256(other_artifact_path.read_bytes()).hexdigest(),
    }

    with pytest.raises(ValueError, match="configured live_preflight_artifact_path"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_preflight_artifact_output_path(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-output-preflight-",
    )
    output_path = Path(cast(str, settings.live_preflight_artifact_path))
    original_artifact_text = output_path.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="credentialed preflight artifact"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert output_path.read_text(encoding="utf-8") == original_artifact_text


def test_reconcile_live_order_artifact_rejects_config_file_output_path(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    config_path = secure_dir / "config.live.yaml"
    original_config_text = "mode: live\n"
    config_path.write_text(original_config_text, encoding="utf-8")
    settings = _settings(approval_path=secure_dir / "first-order.json")
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-config-output-",
    )

    with pytest.raises(ValueError, match="config file"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=config_path,
            config_path=str(config_path),
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert config_path.read_text(encoding="utf-8") == original_config_text


def test_reconcile_live_order_artifact_rejects_missing_config_file_output_path(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    config_path = secure_dir / "config.live.yaml"
    settings = _settings(approval_path=secure_dir / "first-order.json")
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-missing-config-output-",
    )

    with pytest.raises(ValueError, match="config file"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=config_path,
            config_path=str(config_path),
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not config_path.exists()


def test_reconcile_live_order_artifact_rejects_failed_venue_reconciliation_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )

    with pytest.raises(ValueError, match="venue reconciliation"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(
                ok=False,
                mismatches=("venue has unreconciled open orders",),
            ),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_database_url_override_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )

    with pytest.raises(ValueError, match="database-url override"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=True,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_open_order_evidence_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(
        _live_order_record(),
        decision_status="partially_filled",
        order_status="partial",
        remaining_notional_usdc=5.0,
    )

    with pytest.raises(ValueError, match="open order evidence"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_non_filled_decision_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), decision_status="submitted")

    with pytest.raises(ValueError, match="decision status"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_non_filled_fill_status_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), fill_status="cancelled")

    with pytest.raises(ValueError, match="fill status"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_non_filled_order_status_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(
        _live_order_record(),
        order_status="cancelled",
        order_raw_status="cancelled",
    )

    with pytest.raises(ValueError, match="order status"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_inconsistent_notional_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(
        _live_order_record(),
        requested_notional_usdc=10.0,
        filled_notional_usdc=10.0,
        remaining_notional_usdc=2.0,
    )

    with pytest.raises(ValueError, match="notional accounting"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_notional_above_live_risk_cap_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(
        _live_order_record(),
        requested_notional_usdc=settings.risk.max_position_per_market + 1.0,
        filled_notional_usdc=settings.risk.max_position_per_market + 1.0,
        remaining_notional_usdc=0.0,
        fill_notional_usdc=settings.risk.max_position_per_market + 1.0,
        filled_quantity=(settings.risk.max_position_per_market + 1.0) / 0.4,
        fill_quantity=(settings.risk.max_position_per_market + 1.0) / 0.4,
        fill_price=0.4,
    )

    with pytest.raises(ValueError, match="max_position_per_market"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_notional_below_min_order_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    requested_notional = settings.risk.min_order_usdc / 2.0
    record = replace(
        _live_order_record(),
        requested_notional_usdc=requested_notional,
        filled_notional_usdc=requested_notional,
        remaining_notional_usdc=0.0,
        fill_notional_usdc=requested_notional,
        filled_quantity=requested_notional / 0.4,
        fill_quantity=requested_notional / 0.4,
        fill_price=0.4,
    )

    with pytest.raises(ValueError, match="min_order_usdc"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_inconsistent_fill_arithmetic_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(
        _live_order_record(),
        filled_notional_usdc=10.0,
        fill_notional_usdc=10.0,
        filled_quantity=20.0,
        fill_quantity=20.0,
        fill_price=0.4,
    )

    with pytest.raises(ValueError, match="fill arithmetic"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_negative_fill_price_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(
        _live_order_record(),
        filled_notional_usdc=10.0,
        fill_notional_usdc=10.0,
        filled_quantity=-25.0,
        fill_quantity=-25.0,
        fill_price=-0.4,
    )

    with pytest.raises(ValueError, match="fill_price"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_negative_filled_notional_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(
        _live_order_record(),
        requested_notional_usdc=10.0,
        filled_notional_usdc=-10.0,
        remaining_notional_usdc=20.0,
        fill_notional_usdc=-10.0,
        filled_quantity=-25.0,
        fill_quantity=-25.0,
        fill_price=0.4,
    )

    with pytest.raises(ValueError, match="filled_notional_usdc"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_negative_remaining_notional_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(
        _live_order_record(),
        requested_notional_usdc=10.0,
        filled_notional_usdc=11.0,
        remaining_notional_usdc=-1.0,
        fill_notional_usdc=11.0,
        filled_quantity=27.5,
        fill_quantity=27.5,
        fill_price=0.4,
    )

    with pytest.raises(ValueError, match="remaining_notional_usdc"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_mismatched_fill_record_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(
        _live_order_record(),
        filled_notional_usdc=10.0,
        fill_notional_usdc=9.0,
    )

    with pytest.raises(ValueError, match="fill record"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_impossible_chronology_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    submitted_at = datetime(2026, 5, 26, 10, 0, tzinfo=UTC)
    record = replace(
        _live_order_record(),
        submitted_at=submitted_at,
        last_updated_at=submitted_at + timedelta(seconds=1),
        filled_at=submitted_at - timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="chronology"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_missing_quote_source_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), pre_submit_quote_source=None)

    with pytest.raises(ValueError, match="pre-submit quote source"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_missing_quote_hash_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), pre_submit_quote_hash="")

    with pytest.raises(ValueError, match="pre-submit quote hash"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_malformed_quote_fingerprint_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), pre_submit_quote_fingerprint="TODO")

    with pytest.raises(ValueError, match="pre-submit quote fingerprint"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_gtc_order_evidence_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), time_in_force="GTC")

    with pytest.raises(ValueError, match="IOC/FOK"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_non_polymarket_venue_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), venue="kalshi")

    with pytest.raises(ValueError, match="polymarket"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_placeholder_decision_id_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), decision_id="__FILL_IN_DECISION_ID__")

    with pytest.raises(ValueError, match="decision_id"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_missing_outcome_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), outcome=None)

    with pytest.raises(ValueError, match="outcome"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_invalid_action_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )
    record = replace(_live_order_record(), action="HOLD")

    with pytest.raises(ValueError, match="action"):
        live_cli._write_live_order_reconciliation_artifact(
            record,
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_non_finite_portfolio_when_final(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"
    credentialed_preflight_artifact = _credentialed_preflight_reference_for_test(
        settings,
        prefix="pms-live-order-reconcile-preflight-",
    )

    with pytest.raises(ValueError, match="portfolio.free_usdc"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=float("nan"),
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=credentialed_preflight_artifact,
        )

    assert not output_path.exists()


def test_reconcile_live_order_artifact_rejects_forged_reconciled_by(
    tmp_path: Path,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=secure_dir / "first-order.json")
    output_path = secure_dir / "first-live-order-reconciliation.json"

    with pytest.raises(ValueError, match="reconciled_by"):
        live_cli._write_live_order_reconciliation_artifact(
            _live_order_record(),
            report=ReconciliationReport(ok=True, mismatches=()),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            reconciled_by="operator|forged",
            database_url_override_used=False,
            portfolio=Portfolio(
                total_usdc=100.0,
                free_usdc=90.0,
                locked_usdc=10.0,
                open_positions=[],
            ),
            final_post_live_valid=True,
            credentialed_preflight_artifact=None,
        )

    assert not output_path.exists()


@pytest.mark.asyncio
async def test_reconcile_live_order_requires_credentialed_preflight_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    output_dir = tmp_path / "artifacts"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "first-live-order-reconciliation.json"
    settings = _settings(approval_path=approval_dir / "first-order.json")
    create_pool_called = False

    def fake_load_settings(config_path: str | None) -> PMSSettings:
        assert config_path == "config.live.yaml"
        return settings

    async def fail_if_database_opens(
        *,
        dsn: str,
        min_size: int,
        max_size: int,
    ) -> _CliFakePool:
        nonlocal create_pool_called
        del dsn, min_size, max_size
        create_pool_called = True
        raise RuntimeError("database opened before preflight artifact validation")

    monkeypatch.setattr(live_cli, "_load_cli_settings", fake_load_settings)
    monkeypatch.setattr(asyncpg, "create_pool", fail_if_database_opens)
    args = build_parser().parse_args(
        [
            "reconcile-live-order",
            "--config",
            "config.live.yaml",
            "--decision-id",
            "decision-1",
            "--reconciled-by",
            "operator",
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["decision_id"] == "decision-1"
    assert payload["reconciled"] is False
    assert "preflight artifact" in payload["error"]
    assert create_pool_called is False
    assert not output_path.exists()


@pytest.mark.asyncio
async def test_reconcile_live_order_rejects_preflight_generated_after_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    output_dir = tmp_path / "artifacts"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "first-live-order-reconciliation.json"
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_preflight_artifact_path = make_live_preflight_artifact_path(
        prefix="pms-live-order-postdated-preflight-",
        settings=settings,
    )
    fake_pool = _CliFakePool()
    call_order: list[str] = []
    submitted_at = datetime.now(tz=UTC) - timedelta(minutes=5)
    record = replace(
        _live_order_record(),
        submitted_at=submitted_at,
        last_updated_at=submitted_at + timedelta(seconds=2),
        filled_at=submitted_at + timedelta(seconds=2),
    )

    def fake_load_settings(config_path: str | None) -> PMSSettings:
        assert config_path == "config.live.yaml"
        return settings

    async def fake_create_pool(
        *,
        dsn: str,
        min_size: int,
        max_size: int,
    ) -> _CliFakePool:
        del dsn, min_size, max_size
        return fake_pool

    async def fake_ensure_schema_current(pool: object) -> None:
        assert pool is fake_pool
        call_order.append("schema")

    class FakeLiveOrderReconciliationStore:
        def __init__(self, pool: object) -> None:
            assert pool is fake_pool

        async def load_live_order_record(
            self,
            *,
            decision_id: str,
        ) -> LiveOrderReconciliationRecord:
            assert decision_id == "decision-1"
            call_order.append("record")
            return record

    monkeypatch.setattr(live_cli, "_load_cli_settings", fake_load_settings)
    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(live_cli, "ensure_schema_current", fake_ensure_schema_current)
    monkeypatch.setattr(
        live_cli,
        "LiveOrderReconciliationStore",
        FakeLiveOrderReconciliationStore,
    )
    args = build_parser().parse_args(
        [
            "reconcile-live-order",
            "--config",
            "config.live.yaml",
            "--decision-id",
            "decision-1",
            "--reconciled-by",
            "operator",
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert fake_pool.close_calls == 1
    assert call_order == ["schema", "record"]
    assert payload["decision_id"] == "decision-1"
    assert payload["reconciled"] is False
    assert "postdates live order submission" in payload["error"]
    assert not output_path.exists()


@pytest.mark.asyncio
async def test_reconcile_live_order_fails_without_persisted_order_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    output_dir = tmp_path / "artifacts"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "first-live-order-reconciliation.json"
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_preflight_artifact_path = make_live_preflight_artifact_path(
        prefix="pms-live-order-reconcile-missing-evidence-preflight-",
        settings=settings,
    )
    fake_pool = _CliFakePool()

    def fake_load_settings(config_path: str | None) -> PMSSettings:
        assert config_path == "config.live.yaml"
        return settings

    async def fake_create_pool(
        *,
        dsn: str,
        min_size: int,
        max_size: int,
    ) -> _CliFakePool:
        del dsn, min_size, max_size
        return fake_pool

    async def fake_ensure_schema_current(pool: object) -> None:
        assert pool is fake_pool

    class FakeLiveOrderReconciliationStore:
        def __init__(self, pool: object) -> None:
            assert pool is fake_pool

        async def load_live_order_record(
            self,
            *,
            decision_id: str,
        ) -> None:
            assert decision_id == "decision-1"
            return None

    monkeypatch.setattr(live_cli, "_load_cli_settings", fake_load_settings)
    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(live_cli, "ensure_schema_current", fake_ensure_schema_current)
    monkeypatch.setattr(
        live_cli,
        "LiveOrderReconciliationStore",
        FakeLiveOrderReconciliationStore,
    )
    args = build_parser().parse_args(
        [
            "reconcile-live-order",
            "--config",
            "config.live.yaml",
            "--decision-id",
            "decision-1",
            "--reconciled-by",
            "operator",
            "--output",
            str(output_path),
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert fake_pool.close_calls == 1
    assert payload == {
        "decision_id": "decision-1",
        "error": "live order reconciliation evidence not found: decision-1",
        "reconciled": False,
    }
    assert not output_path.exists()


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_reports_config_load_failure_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    missing_secret_path = tmp_path / "missing-polymarket-secrets.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {missing_secret_path}",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("reconcile must not connect after config load failure")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            "decision-1",
            "--venue-order-id",
            "venue-order-1",
            "--status",
            "filled",
            "--reconciled-by",
            "operator",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload == {
        "decision_id": "decision-1",
        "error": f"Local secret file does not exist: {missing_secret_path}",
        "status": "filled",
        "updated": False,
    }


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_reports_schema_failure_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  dsn: postgresql://configured.example/pms_live",
            ]
        ),
        encoding="utf-8",
    )
    close_calls = 0
    reconcile_calls = 0

    class FakePool:
        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    fake_pool = FakePool()

    async def fake_create_pool(
        *,
        dsn: str,
        min_size: int,
        max_size: int,
    ) -> FakePool:
        del dsn, min_size, max_size
        return fake_pool

    async def fake_ensure_schema_current(pool: object) -> None:
        assert pool is fake_pool
        raise RuntimeError("schema out of date")

    class FakeSubmissionUnknownReconciliationStore:
        def __init__(self, pool: object) -> None:
            assert pool is fake_pool

        async def reconcile_submission_unknown(self, **_: object) -> bool:
            nonlocal reconcile_calls
            reconcile_calls += 1
            return True

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(
        live_cli,
        "ensure_schema_current",
        fake_ensure_schema_current,
    )
    monkeypatch.setattr(
        live_cli,
        "SubmissionUnknownReconciliationStore",
        FakeSubmissionUnknownReconciliationStore,
    )
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            "decision-1",
            "--venue-order-id",
            "venue-order-1",
            "--status",
            "filled",
            "--reconciled-by",
            "operator",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert close_calls == 1
    assert reconcile_calls == 0
    assert payload == {
        "decision_id": "decision-1",
        "error": "schema out of date",
        "status": "filled",
        "updated": False,
    }


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_redacts_live_credentials_from_schema_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    credential_values = (
        "private-key-secret",
        "api-key-secret",
        "api-secret-secret",
        "passphrase-secret",
        "0x2222222222222222222222222222222222222222",
    )
    secret_dsn = "postgresql://admin:supersecret@db.internal.example.com:5432/pms_live"
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(
        approval_path=approval_dir / "first-order.json",
    ).model_copy(
        update={
            "polymarket": PolymarketSettings(
                private_key=credential_values[0],
                api_key=credential_values[1],
                api_secret=credential_values[2],
                api_passphrase=credential_values[3],
                signature_type=1,
                funder_address=credential_values[4],
                first_live_order_approval_path=str(
                    approval_dir / "first-order.json"
                ),
                operator_approval_mode="every_order",
            )
        }
    )
    close_calls = 0

    class FakePool:
        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    fake_pool = FakePool()

    def fake_load_settings(config_path: str | None) -> PMSSettings:
        assert config_path == "config.live.yaml"
        return settings

    async def fake_create_pool(
        *,
        dsn: str,
        min_size: int,
        max_size: int,
    ) -> FakePool:
        assert dsn == settings.database.dsn
        assert min_size == 1
        assert max_size == 1
        return fake_pool

    async def fake_ensure_schema_current(pool: object) -> None:
        assert pool is fake_pool
        raise RuntimeError(
            "schema check failed "
            f"{credential_values[0]} {credential_values[1]} "
            f"{credential_values[2]} {credential_values[3]} {credential_values[4]} "
            f"{secret_dsn} password=keyword-secret"
        )

    monkeypatch.setattr(live_cli, "_load_cli_settings", fake_load_settings)
    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(live_cli, "ensure_schema_current", fake_ensure_schema_current)
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            "config.live.yaml",
            "--decision-id",
            "decision-1",
            "--venue-order-id",
            "venue-order-1",
            "--status",
            "filled",
            "--reconciled-by",
            "operator",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    detail = payload["error"]
    assert exit_code == 1
    assert close_calls == 1
    assert payload["decision_id"] == "decision-1"
    assert payload["status"] == "filled"
    assert payload["updated"] is False
    assert "<redacted-polymarket-credential>" in detail
    assert "<redacted-database-url>" in detail
    assert "password=<redacted>" in detail
    for credential in credential_values:
        assert credential not in detail
    assert "supersecret" not in detail
    assert "keyword-secret" not in detail
    assert "admin" not in detail


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_rejects_open_without_venue_order_id_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  dsn: postgresql://configured.example/pms_live",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("invalid operator input should not connect to database")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            "decision-1",
            "--status",
            "open",
            "--reconciled-by",
            "operator",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload == {
        "decision_id": "decision-1",
        "error": "venue_order_id is required when status is filled or open",
        "status": "open",
        "updated": False,
    }


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_rejects_blank_reconciled_by_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  dsn: postgresql://configured.example/pms_live",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("blank operator input should not connect to database")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            "decision-1",
            "--venue-order-id",
            "venue-order-1",
            "--status",
            "filled",
            "--reconciled-by",
            " ",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload == {
        "decision_id": "decision-1",
        "error": "reconciled_by is required",
        "status": "filled",
        "updated": False,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("decision_id", "expected_error"),
    [
        (" ", "decision_id is required"),
        ("__FILL_IN_DECISION_ID__", "decision_id must not contain a placeholder"),
    ],
)
async def test_reconcile_submission_unknown_rejects_bad_decision_id_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    decision_id: str,
    expected_error: str,
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  dsn: postgresql://configured.example/pms_live",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("bad decision id should not connect to database")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            decision_id,
            "--venue-order-id",
            "venue-order-1",
            "--status",
            "filled",
            "--reconciled-by",
            "operator",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload == {
        "decision_id": decision_id,
        "error": expected_error,
        "status": "filled",
        "updated": False,
    }


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_rejects_placeholder_reconciled_by_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  dsn: postgresql://configured.example/pms_live",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError(
            "placeholder operator input should not connect to database"
        )

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            "decision-1",
            "--venue-order-id",
            "venue-order-1",
            "--status",
            "filled",
            "--reconciled-by",
            "__FILL_IN_OPERATOR_ID__",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload == {
        "decision_id": "decision-1",
        "error": "reconciled_by must not contain a placeholder",
        "status": "filled",
        "updated": False,
    }


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_rejects_forged_reconciled_by_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  dsn: postgresql://configured.example/pms_live",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("forged operator input should not connect to database")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            "decision-1",
            "--venue-order-id",
            "venue-order-1",
            "--status",
            "filled",
            "--reconciled-by",
            "operator|forged",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload == {
        "decision_id": "decision-1",
        "error": "reconciled_by must not contain delimiters or newlines",
        "status": "filled",
        "updated": False,
    }


@pytest.mark.asyncio
async def test_reconcile_submission_unknown_rejects_placeholder_venue_order_id_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  dsn: postgresql://configured.example/pms_live",
            ]
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        nonlocal create_pool_calls
        create_pool_calls += 1
        raise AssertionError("placeholder venue input should not connect to database")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--config",
            str(config_path),
            "--decision-id",
            "decision-1",
            "--venue-order-id",
            "__FILL_IN_VENUE_ORDER_ID__",
            "--status",
            "filled",
            "--reconciled-by",
            "operator",
        ]
    )

    exit_code = await live_cli._main_async(args)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert create_pool_calls == 0
    assert payload == {
        "decision_id": "decision-1",
        "error": "venue_order_id must not contain a placeholder",
        "status": "filled",
        "updated": False,
    }


@pytest.mark.asyncio
async def test_live_preflight_passes_with_live_config_schema_incidents_and_venue(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    pool = _Pool(_Connection())

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(asyncpg.Pool, pool),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    assert result.ok is True
    assert {check.name for check in result.checks} == {
        "live_config",
        "runtime_dependencies",
        "operator_approval",
        "emergency_audit",
        "first_order_audit",
        "database_connection",
        "schema_current",
        "market_data_freshness",
        "submission_unknown",
        "live_open_orders",
        "active_strategies",
        "venue_reconciliation",
    }
    assert all(check.ok for check in result.checks)
    assert pool.release_calls == 1
    audit_check = result.require_check("first_order_audit")
    assert "first-order-audit.jsonl" in audit_check.detail
    assert "distinct from live_emergency_audit_path" in audit_check.detail
    market_data_check = result.require_check("market_data_freshness")
    assert "latest book snapshot age" in market_data_check.detail
    assert "latest usable book snapshot age" in market_data_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_without_flb_calibration_artifact_path(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.strategies.flb_calibration_path = None

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "FLB calibration artifact path is required" in live_config.detail


@pytest.mark.parametrize(
    ("artifact_name", "expected_detail"),
    [
        ("execution_model", "execution-model artifact"),
        ("paper_backtest_diff", "paper-vs-backtest execution diff artifact"),
        ("category_prior", "category-prior artifact"),
        ("flb_calibration", "FLB calibration artifact"),
    ],
)
@pytest.mark.asyncio
async def test_live_preflight_rejects_strategy_artifact_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_name: str,
    expected_detail: str,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    artifact_dir = repo_root / "secure-artifacts"
    artifact_dir.mkdir(mode=0o700)

    if artifact_name == "execution_model":
        source_path = Path(cast(str, settings.live_execution_model_path))
        target_path = artifact_dir / source_path.name
        target_path.write_bytes(source_path.read_bytes())
        settings.live_execution_model_path = str(target_path)
    elif artifact_name == "paper_backtest_diff":
        source_path = Path(cast(str, settings.live_paper_backtest_diff_path))
        target_path = artifact_dir / source_path.name
        target_path.write_bytes(source_path.read_bytes())
        settings.live_paper_backtest_diff_path = str(target_path)
    elif artifact_name == "category_prior":
        source_path = Path(cast(str, settings.controller.category_prior_observations_path))
        target_path = artifact_dir / source_path.name
        target_path.write_bytes(source_path.read_bytes())
        settings.controller.category_prior_observations_path = str(target_path)
    elif artifact_name == "flb_calibration":
        source_path = Path(cast(str, settings.strategies.flb_calibration_path))
        target_path = artifact_dir / source_path.name
        target_path.write_bytes(source_path.read_bytes())
        settings.strategies.flb_calibration_path = str(target_path)
    else:
        raise AssertionError(f"unknown artifact_name: {artifact_name}")
    monkeypatch.chdir(repo_root)

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert expected_detail in live_config.detail
    assert "working tree" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_with_malformed_flb_calibration_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    model_path = Path(cast(str, settings.strategies.flb_calibration_path))
    model_path.write_text(
        "\n".join(
            (
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
            )
        ),
        encoding="utf-8",
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "FLB calibration artifact invalid" in live_config.detail
    assert "missing calibrated FLB signals" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_without_execution_model_artifact_path(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_execution_model_path = None

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "execution-model artifact path is required" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_with_static_execution_model_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    model_path = Path(cast(str, settings.live_execution_model_path))
    payload = json.loads(model_path.read_text(encoding="utf-8"))
    payload["calibration_source"] = "static_live_estimate"
    model_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "execution-model artifact must be telemetry_calibrated" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_without_execution_model_adverse_selection(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    model_path = Path(cast(str, settings.live_execution_model_path))
    payload = json.loads(model_path.read_text(encoding="utf-8"))
    payload["adverse_selection_bps"] = 0.0
    model_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "execution-model artifact must include positive adverse_selection_bps" in (
        live_config.detail
    )


@pytest.mark.asyncio
async def test_live_preflight_fails_when_execution_model_staleness_is_infinite(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    model_path = Path(cast(str, settings.live_execution_model_path))
    payload = json.loads(model_path.read_text(encoding="utf-8"))
    payload["staleness_ms"] = ".inf"
    model_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "execution-model artifact staleness_ms must be finite" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_execution_model_lacks_sample_contract(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    model_path = Path(cast(str, settings.live_execution_model_path))
    payload = json.loads(model_path.read_text(encoding="utf-8"))
    payload.pop("min_samples", None)
    payload.pop("telemetry_sample_count", None)
    payload.pop("adverse_selection_sample_count", None)
    payload.pop("require_adverse_selection", None)
    model_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "execution-model artifact missing telemetry sample contract" in (
        live_config.detail
    )
    assert "min_samples" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_with_thin_execution_model_sample_contract(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    model_path = Path(cast(str, settings.live_execution_model_path))
    payload = json.loads(model_path.read_text(encoding="utf-8"))
    payload["min_samples"] = 1
    payload["telemetry_sample_count"] = 1
    payload["adverse_selection_sample_count"] = 1
    payload["require_adverse_selection"] = True
    model_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "execution-model artifact min_samples must be at least 10" in (
        live_config.detail
    )


@pytest.mark.asyncio
async def test_live_preflight_fails_when_execution_model_lacks_provenance(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    model_path = Path(cast(str, settings.live_execution_model_path))
    payload = json.loads(model_path.read_text(encoding="utf-8"))
    payload.pop("generated_by", None)
    payload.pop("artifact_mode", None)
    payload.pop("generated_at", None)
    model_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "execution-model artifact generated_by is invalid" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_without_paper_backtest_diff_artifact_path(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_paper_backtest_diff_path = None

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "paper-vs-backtest execution diff artifact path is required" in (
        live_config.detail
    )


@pytest.mark.asyncio
async def test_live_preflight_fails_with_failed_paper_backtest_diff_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    payload["final_go_no_go_valid"] = False
    payload["failures"] = ["fill_rate_delta_abs 0.2 > max_fill_rate_delta 0.05"]
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert (
        "paper-vs-backtest execution diff artifact must be final GO"
        in live_config.detail
    )


@pytest.mark.asyncio
async def test_live_preflight_fails_with_thin_paper_backtest_diff_sample(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    metrics = cast(dict[str, object], payload["metrics"])
    metrics["paper_decision_count"] = 9
    metrics["backtest_decision_count"] = 9
    metrics["matched_decision_count"] = 9
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "matched_decision_count must be at least 10" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_without_paper_backtest_diff_min_matched_threshold(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    thresholds = cast(dict[str, object], payload["thresholds"])
    thresholds.pop("min_matched_decisions", None)
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "missing threshold: min_matched_decisions" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_with_weak_paper_backtest_diff_min_matched_threshold(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    thresholds = cast(dict[str, object], payload["thresholds"])
    thresholds["min_matched_decisions"] = 9
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "min_matched_decisions must be at least 10" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_matched_count_misses_declared_minimum(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    thresholds = cast(dict[str, object], payload["thresholds"])
    thresholds["min_matched_decisions"] = 11
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert (
        "matched_decision_count must be at least min_matched_decisions"
        in live_config.detail
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metric_name", "metric_value"),
    [
        ("fill_rate_delta_abs", "0.0"),
        ("avg_slippage_bps_delta_abs", float("nan")),
        ("total_pnl_delta_abs", -1.0),
        ("matched_decision_count", True),
    ],
)
async def test_live_preflight_fails_with_malformed_paper_backtest_diff_metric(
    tmp_path: Path,
    metric_name: str,
    metric_value: object,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    metrics = cast(dict[str, object], payload["metrics"])
    metrics[metric_name] = metric_value
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert f"paper-vs-backtest execution diff artifact metric {metric_name}" in (
        live_config.detail
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metric_name", "metric_value", "expected_detail"),
        [
            (
                "matched_decision_count",
                11,
                "matched_decision_count cannot exceed paper_decision_count",
            ),
        (
            "backtest_decision_count",
            1,
            "matched_decision_count cannot exceed backtest_decision_count",
        ),
        (
            "fill_rate_delta_abs",
            1.01,
            "fill_rate_delta_abs must be between 0 and 1",
        ),
        (
            "rejection_rate_delta_abs",
            1.01,
            "rejection_rate_delta_abs must be between 0 and 1",
        ),
    ],
)
async def test_live_preflight_fails_with_impossible_paper_backtest_diff_metrics(
    tmp_path: Path,
    metric_name: str,
    metric_value: object,
    expected_detail: str,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    metrics = cast(dict[str, object], payload["metrics"])
    metrics[metric_name] = metric_value
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert expected_detail in live_config.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("paper_decision_count", "backtest_decision_count", "expected_detail"),
        [
            (
                11,
                10,
                "matched_decision_count must equal paper_decision_count",
            ),
            (
                10,
                11,
                "matched_decision_count must equal backtest_decision_count",
            ),
    ],
)
async def test_live_preflight_fails_when_paper_backtest_diff_counts_hide_unmatched_ids(
    tmp_path: Path,
    paper_decision_count: int,
    backtest_decision_count: int,
    expected_detail: str,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    metrics = cast(dict[str, object], payload["metrics"])
    metrics["matched_decision_count"] = 10
    metrics["paper_decision_count"] = paper_decision_count
    metrics["backtest_decision_count"] = backtest_decision_count
    payload["paper_only_decision_ids"] = []
    payload["backtest_only_decision_ids"] = []
    payload["status_mismatches"] = []
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert expected_detail in live_config.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metric_name", "metric_value", "threshold_name", "threshold_value"),
    [
        ("fill_rate_delta_abs", 0.04, "max_fill_rate_delta", 0.03),
        ("rejection_rate_delta_abs", 0.04, "max_rejection_rate_delta", 0.03),
        ("avg_slippage_bps_delta_abs", 4.0, "max_avg_slippage_bps_delta", 3.0),
        ("total_pnl_delta_abs", 0.8, "max_total_pnl_delta", 0.7),
    ],
)
async def test_live_preflight_fails_when_paper_backtest_diff_exceeds_threshold(
    tmp_path: Path,
    metric_name: str,
    metric_value: float,
    threshold_name: str,
    threshold_value: float,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    metrics = cast(dict[str, object], payload["metrics"])
    thresholds = cast(dict[str, object], payload["thresholds"])
    metrics[metric_name] = metric_value
    thresholds[threshold_name] = threshold_value
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert f"{metric_name} exceeds {threshold_name}" in live_config.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "field_value", "expected_detail"),
    [
        (
            "paper_only_decision_ids",
            ["decision-paper-only"],
            "paper-vs-backtest execution diff artifact contains paper-only decisions",
        ),
        (
            "backtest_only_decision_ids",
            ["decision-backtest-only"],
            "paper-vs-backtest execution diff artifact contains backtest-only decisions",
        ),
        (
            "status_mismatches",
            ["status mismatch d-1: paper=filled backtest=rejected"],
            "paper-vs-backtest execution diff artifact contains status mismatches",
        ),
    ],
)
async def test_live_preflight_fails_when_paper_backtest_diff_hides_mismatch_lists(
    tmp_path: Path,
    field_name: str,
    field_value: list[str],
    expected_detail: str,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    payload[field_name] = field_value
    payload["failures"] = []
    payload["final_go_no_go_valid"] = True
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert expected_detail in live_config.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("generated_at", "expected_detail"),
    [
        ("not-a-timestamp", "paper-vs-backtest execution diff artifact generated_at"),
        (
            (datetime.now(tz=UTC) + timedelta(seconds=60)).isoformat(),
            "paper-vs-backtest execution diff artifact generated_at is in the future",
        ),
        (
            (datetime.now(tz=UTC) - timedelta(days=8)).isoformat(),
            "paper-vs-backtest execution diff artifact is stale",
        ),
    ],
)
async def test_live_preflight_fails_with_bad_paper_backtest_diff_generated_at(
    tmp_path: Path,
    generated_at: str,
    expected_detail: str,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    payload["generated_at"] = generated_at
    diff_path.write_text(json.dumps(payload), encoding="utf-8")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert expected_detail in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_without_category_prior_artifact_path(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.controller.category_prior_observations_path = None

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "category-prior artifact path is required" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_with_malformed_category_prior_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    prior_path = Path(cast(str, settings.controller.category_prior_observations_path))
    prior_path.write_text(
        "\n".join(
            (
                "market_id,category,yes_payout,resolved_at",
                "m-1,politics,1,2026-05-01T12:00:00Z",
            )
        ),
        encoding="utf-8",
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "category-prior artifact invalid" in live_config.detail
    assert "missing required columns" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_category_prior_artifact_is_too_thin(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    prior_path = Path(cast(str, settings.controller.category_prior_observations_path))
    prior_path.write_text(
        "\n".join(
            (
                "market_id,category,yes_payout,no_payout,resolved_at",
                "m-1,politics,1,0,2026-05-01T12:00:00Z",
            )
        ),
        encoding="utf-8",
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_config = result.require_check("live_config")
    assert result.ok is False
    assert live_config.ok is False
    assert "category-prior artifact has too few observations" in live_config.detail
    assert "controller.category_prior_min_global_samples" in live_config.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_emergency_audit_directory_is_missing(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(
        approval_path=approval_dir / "first-order.json",
        live_emergency_audit_path=tmp_path
        / "missing"
        / "live-emergency-audit.jsonl",
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    audit_check = result.require_check("emergency_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "parent directory does not exist" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_operator_approval_path_is_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    approval_dir = repo_root / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    settings = _settings(approval_path=approval_dir / "first-order.json")

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    approval_check = result.require_check("operator_approval")
    assert result.ok is False
    assert approval_check.ok is False
    assert "outside the working tree" in approval_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_emergency_audit_path_is_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    audit_dir = repo_root / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    settings = _settings(
        approval_path=approval_dir / "first-order.json",
        live_emergency_audit_path=audit_dir / "live-emergency-audit.jsonl",
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    audit_check = result.require_check("emergency_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "outside the working tree" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_emergency_audit_directory_is_not_owner_writable(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    audit_dir = tmp_path / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    audit_dir.chmod(0o500)
    settings = _settings(
        approval_path=approval_dir / "first-order.json",
        live_emergency_audit_path=audit_dir / "live-emergency-audit.jsonl",
    )

    try:
        result = await run_live_preflight(
            settings,
            pool=cast(asyncpg.Pool, _Pool(_Connection())),
            venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
            skip_venue=True,
        )
    finally:
        audit_dir.chmod(0o700)

    audit_check = result.require_check("emergency_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "is not owner-writable" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_emergency_audit_directory_is_symlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    audit_dir = tmp_path / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    symlink_parent = tmp_path / "emergency-audit-parent-link"
    symlink_parent.symlink_to(audit_dir, target_is_directory=True)
    settings = _settings(
        approval_path=approval_dir / "first-order.json",
        live_emergency_audit_path=symlink_parent / "live-emergency-audit.jsonl",
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    audit_check = result.require_check("emergency_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "parent path is not a directory" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_emergency_audit_path_is_symlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    audit_dir = tmp_path / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    target_path = audit_dir / "target-emergency-audit.jsonl"
    target_path.write_text("existing audit\n", encoding="utf-8")
    audit_path = audit_dir / "live-emergency-audit.jsonl"
    audit_path.symlink_to(target_path)
    settings = _settings(
        approval_path=approval_dir / "first-order.json",
        live_emergency_audit_path=audit_path,
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    audit_check = result.require_check("emergency_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "regular file" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_emergency_audit_path_is_hardlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    audit_dir = tmp_path / "secure-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    target_path = audit_dir / "target-emergency-audit.jsonl"
    target_path.write_text("existing audit\n", encoding="utf-8")
    audit_path = audit_dir / "live-emergency-audit.jsonl"
    os.link(target_path, audit_path)
    settings = _settings(
        approval_path=approval_dir / "first-order.json",
        live_emergency_audit_path=audit_path,
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    audit_check = result.require_check("emergency_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "single-link" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_first_order_audit_path_is_symlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    target_path = approval_dir / "target-first-order-audit.jsonl"
    target_path.write_text("existing audit\n", encoding="utf-8")
    audit_path = approval_dir / "first-order-audit.jsonl"
    audit_path.symlink_to(target_path)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_first_order_audit_path = str(audit_path)

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    audit_check = result.require_check("first_order_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "regular file" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_first_order_audit_path_is_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    audit_dir = repo_root / "secure-first-order-audit"
    audit_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_first_order_audit_path = str(
        audit_dir / "first-order-audit.jsonl"
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    audit_check = result.require_check("first_order_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "outside the working tree" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_first_order_audit_path_is_hardlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    target_path = approval_dir / "target-first-order-audit.jsonl"
    target_path.write_text("existing audit\n", encoding="utf-8")
    audit_path = approval_dir / "first-order-audit.jsonl"
    os.link(target_path, audit_path)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_first_order_audit_path = str(audit_path)

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    audit_check = result.require_check("first_order_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "single-link" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_reconciles_against_configured_risk_budget(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json").model_copy(
        update={
            "risk": RiskSettings(
                max_position_per_market=50.0,
                max_total_exposure=50.0,
                max_drawdown_pct=20.0,
                max_daily_loss_usdc=20.0,
                max_open_positions=5,
                max_exposure_per_risk_group=15.0,
                max_quantity_shares=500.0,
            )
        }
    )
    reconciler = _RecordingVenueReconciler(portfolios=[])

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, reconciler),
    )

    assert result.ok is True
    assert len(reconciler.portfolios) == 1
    assert reconciler.portfolios[0].total_usdc == pytest.approx(50.0)
    assert reconciler.portfolios[0].free_usdc == pytest.approx(50.0)
    assert reconciler.portfolios[0].locked_usdc == 0.0


@pytest.mark.asyncio
async def test_live_preflight_fails_when_no_active_strategy_versions_exist(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(_Connection(active_strategy_rows=())),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    strategy_check = result.require_check("active_strategies")
    assert result.ok is False
    assert strategy_check.ok is False
    assert "no active strategy versions" in strategy_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_active_strategy_is_paper_only(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
                _Pool(
                    _Connection(
                        active_strategy_rows=(
                            _active_strategy_row(build_paper_canary_strategy()),
                        )
                    )
                ),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    strategy_check = result.require_check("active_strategies")
    assert result.ok is False
    assert strategy_check.ok is False
    assert "paper_canary_v1 is PAPER-only" in strategy_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_active_strategy_lacks_explicit_live_opt_in(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    strategy = Strategy(
        config=StrategyConfig(
            strategy_id="implicit-live",
            factor_composition=(),
            metadata=(("owner", "system"),),
        ),
        risk=RiskParams(
            max_position_notional_usdc=50.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        forecaster=ForecasterSpec(forecasters=(("rules", ()),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=30,
            volume_min_usdc=500.0,
        ),
    )

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(
                _Connection(
                    active_strategy_rows=(_active_strategy_row(strategy),)
                )
            ),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    strategy_check = result.require_check("active_strategies")
    assert result.ok is False
    assert strategy_check.ok is False
    assert "metadata.live_allowed=true" in strategy_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_paper_report_names_different_strategy(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    _insert_report_summary_strategy(
        cast(str, settings.live_paper_soak_report_path),
        "other-strategy@other-version",
    )

    result = await run_live_preflight(
        settings,
        pool=cast(
            asyncpg.Pool,
            _Pool(
                _Connection(
                    active_strategy_rows=(_active_strategy_row(_live_strategy()),)
                )
            ),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    strategy_check = result.require_check("active_strategies")
    assert result.ok is False
    assert strategy_check.ok is False
    assert "paper-soak GO report strategy mismatch" in strategy_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_paper_report_strategy_row_has_extra_cells(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    active_strategy = _live_strategy()
    active_strategy_label = (
        f"{active_strategy.config.strategy_id}@"
        f"{_strategy_version_id(active_strategy)}"
    )
    _insert_report_summary_strategy(
        cast(str, settings.live_paper_soak_report_path),
        active_strategy_label,
    )
    paper_report_path = Path(cast(str, settings.live_paper_soak_report_path))
    paper_report_path.write_text(
        paper_report_path.read_text(encoding="utf-8").replace(
            f"| Strategy | {active_strategy_label} | - |",
            f"| Strategy | {active_strategy_label} | - | TODO: hidden extra cell |",
        ),
        encoding="utf-8",
    )

    result = await run_live_preflight(
        settings,
        pool=cast(
            asyncpg.Pool,
            _Pool(
                _Connection(
                    active_strategy_rows=(_active_strategy_row(active_strategy),)
                )
            ),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    strategy_check = result.require_check("active_strategies")
    assert result.ok is False
    assert strategy_check.ok is False
    assert "malformed Summary Strategy row" in strategy_check.detail


@pytest.mark.asyncio
async def test_live_preflight_accepts_escaped_pipe_inside_strategy_label(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    base_strategy = _live_strategy()
    strategy = Strategy(
        config=replace(base_strategy.config, strategy_id="default|live"),
        risk=base_strategy.risk,
        eval_spec=base_strategy.eval_spec,
        forecaster=base_strategy.forecaster,
        market_selection=base_strategy.market_selection,
    )
    active_strategy_label = f"default\\|live@{_strategy_version_id(strategy)}"
    _insert_report_summary_strategy(
        cast(str, settings.live_paper_soak_report_path),
        active_strategy_label,
    )

    result = await run_live_preflight(
        settings,
        pool=cast(
            asyncpg.Pool,
            _Pool(_Connection(active_strategy_rows=(_active_strategy_row(strategy),))),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    strategy_check = result.require_check("active_strategies")
    assert strategy_check.ok is True
    assert "default|live@" in strategy_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_active_strategy_calibration_is_disabled(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(
                _Connection(
                    active_strategy_rows=(
                        _active_strategy_row(_live_strategy(), calibrated=False),
                    )
                )
            ),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    strategy_check = result.require_check("active_strategies")
    assert result.ok is False
    assert strategy_check.ok is False
    assert "calibration.enabled=true" in strategy_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_active_strategy_contains_non_finite_number(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    row = _active_strategy_row(_live_strategy())
    config_json = json.loads(cast(str, row["config_json"]))
    config_json["risk"]["max_position_notional_usdc"] = "NaN"
    row["config_json"] = json.dumps(config_json, sort_keys=True)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(_Connection(active_strategy_rows=(row,))),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    strategy_check = result.require_check("active_strategies")
    assert result.ok is False
    assert strategy_check.ok is False
    assert "risk.max_position_notional_usdc" in strategy_check.detail
    assert "finite" in strategy_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_active_strategy_is_llm_only(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(
                _Connection(
                    active_strategy_rows=(
                        _active_strategy_row(
                            _live_strategy(forecasters=(("llm", ()),))
                        ),
                    )
                )
            ),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    strategy_check = result.require_check("active_strategies")
    assert result.ok is False
    assert strategy_check.ok is False
    assert "non-LLM forecaster" in strategy_check.detail


@pytest.mark.asyncio
async def test_live_preflight_active_strategy_artifact_rejects_strategy_switch(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-active-strategy-mismatch-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["active_strategies_fingerprint"] = _STALE_ACTIVE_STRATEGIES_FINGERPRINT
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(
        live_preflight_module,
        "require_live_preflight_active_strategies_artifact",
    )

    class _Registry:
        async def list_active_strategies(self) -> list[ActiveStrategy]:
            return [_live_active_strategy()]

    with pytest.raises(
        LiveTradingDisabledError,
        match="active strategies fingerprint mismatch",
    ):
        await validator(settings, _Registry())


def test_live_preflight_artifact_rejects_swapped_readiness_report(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-swapped-readiness-",
            settings=settings,
        )
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    paper_report_path = Path(cast(str, settings.live_paper_soak_report_path))
    paper_report_path.write_text(
        paper_report_path.read_text(encoding="utf-8").replace(
            "| fills | PASS | 50 >= 50 |",
            "| fills | PASS | 51 >= 50 |",
        ),
        encoding="utf-8",
    )
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="readiness reports fingerprint mismatch",
    ):
        validator(settings)


@pytest.mark.parametrize(
    ("artifact_name", "expected_detail"),
    [
        ("execution_model", "execution-model artifact must be telemetry_calibrated"),
        (
            "paper_backtest_diff",
            "paper-vs-backtest execution diff artifact must be final GO",
        ),
        ("category_prior", "category-prior artifact has too few observations"),
        ("flb_calibration", "FLB calibration artifact invalid"),
    ],
)
def test_live_preflight_artifact_revalidates_strategy_artifact_content(
    tmp_path: Path,
    artifact_name: str,
    expected_detail: str,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-revalidate-strategy-artifacts-",
            settings=settings,
        )
    )

    if artifact_name == "execution_model":
        model_path = Path(cast(str, settings.live_execution_model_path))
        payload = json.loads(model_path.read_text(encoding="utf-8"))
        payload["calibration_source"] = "static_live_estimate"
        model_path.write_text(json.dumps(payload), encoding="utf-8")
    elif artifact_name == "paper_backtest_diff":
        diff_path = Path(cast(str, settings.live_paper_backtest_diff_path))
        payload = json.loads(diff_path.read_text(encoding="utf-8"))
        payload["final_go_no_go_valid"] = False
        payload["failures"] = ["fill_rate_delta_abs 0.2 > max_fill_rate_delta 0.05"]
        diff_path.write_text(json.dumps(payload), encoding="utf-8")
    elif artifact_name == "category_prior":
        prior_path = Path(cast(str, settings.controller.category_prior_observations_path))
        prior_path.write_text(
            "\n".join(
                (
                    "market_id,category,yes_payout,no_payout,resolved_at",
                    "m-1,politics,1,0,2026-05-01T12:00:00Z",
                )
            )
            + "\n",
            encoding="utf-8",
        )
    elif artifact_name == "flb_calibration":
        calibration_path = Path(cast(str, settings.strategies.flb_calibration_path))
        calibration_path.write_text(
            "\n".join(
                (
                    "signal_name,probability_estimate,sample_count,source_label",
                    "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
                )
            )
            + "\n",
            encoding="utf-8",
        )
    else:
        raise AssertionError(f"unknown artifact_name: {artifact_name}")

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["readiness_reports_fingerprint"] = (
        live_preflight_module.live_preflight_readiness_reports_fingerprint(settings)
    )
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(LiveTradingDisabledError, match=expected_detail):
        validator(settings)


def test_live_preflight_artifact_rejects_preflight_before_readiness_reports(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    now = datetime.now(tz=UTC)
    report_generated_at = now - timedelta(seconds=10)
    preflight_generated_at = now - timedelta(seconds=20)
    signoff_at = now - timedelta(seconds=30)
    settings.live_exit_criteria_ratified_at = signoff_at
    settings.live_compliance_reviewed_at = signoff_at
    _replace_report_provenance_field(
        cast(str, settings.live_paper_soak_report_path),
        field_name="generated_at",
        value=report_generated_at.isoformat(),
    )
    _replace_report_provenance_field(
        cast(str, settings.live_operator_rehearsal_report_path),
        field_name="generated_at",
        value=report_generated_at.isoformat(),
    )
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-before-readiness-reports-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["generated_at"] = preflight_generated_at.isoformat()
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="predates readiness reports",
    ):
        validator(settings)


@pytest.mark.parametrize(
    "artifact_name",
    ["execution_model", "paper_backtest_diff"],
)
def test_live_preflight_artifact_rejects_preflight_before_strategy_artifacts(
    tmp_path: Path,
    artifact_name: str,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    now = datetime.now(tz=UTC)
    strategy_artifact_generated_at = now - timedelta(seconds=10)
    preflight_generated_at = now - timedelta(seconds=20)
    signoff_at = now - timedelta(seconds=30)
    settings.live_exit_criteria_ratified_at = signoff_at
    settings.live_compliance_reviewed_at = signoff_at
    if artifact_name == "execution_model":
        strategy_artifact_path = Path(cast(str, settings.live_execution_model_path))
    elif artifact_name == "paper_backtest_diff":
        strategy_artifact_path = Path(cast(str, settings.live_paper_backtest_diff_path))
    else:
        raise AssertionError(f"unknown artifact_name: {artifact_name}")
    payload = json.loads(strategy_artifact_path.read_text(encoding="utf-8"))
    payload["generated_at"] = strategy_artifact_generated_at.isoformat()
    strategy_artifact_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix=f"pms-live-preflight-before-{artifact_name}-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["generated_at"] = preflight_generated_at.isoformat()
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="predates readiness reports",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_malformed_readiness_generated_at_row(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    now = datetime.now(tz=UTC)
    report_generated_at = now - timedelta(seconds=20)
    signoff_at = now - timedelta(seconds=30)
    settings.live_exit_criteria_ratified_at = signoff_at
    settings.live_compliance_reviewed_at = signoff_at
    _replace_report_provenance_field(
        cast(str, settings.live_paper_soak_report_path),
        field_name="generated_at",
        value=f"{report_generated_at.isoformat()} | TODO: hidden extra cell",
    )
    _replace_report_provenance_field(
        cast(str, settings.live_operator_rehearsal_report_path),
        field_name="generated_at",
        value=report_generated_at.isoformat(),
    )
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-malformed-readiness-generated-at-",
            settings=settings,
        )
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="malformed generated_at row",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_preflight_before_emergency_audit_record(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    now = datetime.now(tz=UTC)
    readiness_generated_at = now - timedelta(seconds=30)
    preflight_generated_at = now - timedelta(seconds=20)
    emergency_audit_at = now - timedelta(seconds=10)
    for raw_path in (
        settings.live_execution_model_path,
        settings.live_paper_backtest_diff_path,
    ):
        payload_path = Path(cast(str, raw_path))
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        payload["generated_at"] = readiness_generated_at.isoformat()
        payload_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-before-emergency-audit-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["generated_at"] = preflight_generated_at.isoformat()
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    audit_path = Path(settings.live_emergency_audit_path)
    audit_path.write_text(
        json.dumps(
            {
                "timestamp": emergency_audit_at.isoformat(),
                "phase": "manual_emergency_stop",
                "event": "manual_emergency_stop",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="predates emergency audit",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_malformed_emergency_audit_record(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-malformed-emergency-audit-",
            settings=settings,
        )
    )
    audit_path = Path(settings.live_emergency_audit_path)
    audit_path.write_text("{not-json}\n", encoding="utf-8")
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="emergency audit record invalid",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_placeholder_artifact_path(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-placeholder-path-",
            settings=settings,
        )
    )
    placeholder_path = artifact_path.parent / "__FILL_IN_PREFLIGHT_ARTIFACT__.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["output_path"] = str(placeholder_path)
    placeholder_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifact_path.unlink()
    settings.live_preflight_artifact_path = str(placeholder_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(LiveTradingDisabledError, match="path contains placeholder"):
        validator(settings)


def test_live_preflight_artifact_rejects_permissive_parent_directory(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_dir = tmp_path / "permissive-preflight"
    artifact_dir.mkdir(mode=0o700)
    artifact_path = artifact_dir / "credentialed-preflight.json"
    _configure_live_preflight_artifact_path(settings, artifact_path)
    live_cli._write_preflight_artifact(
        _final_preflight_result(),
        settings=settings,
        output_path=artifact_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    artifact_dir.chmod(0o755)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    try:
        with pytest.raises(
            LiveTradingDisabledError,
            match="preflight artifact parent.*too permissive",
        ):
            validator(settings)
    finally:
        artifact_dir.chmod(0o700)


def test_live_preflight_artifact_rejects_symlink_parent_directory(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_dir = tmp_path / "real-preflight"
    artifact_dir.mkdir(mode=0o700)
    artifact_path = artifact_dir / "credentialed-preflight.json"
    _configure_live_preflight_artifact_path(settings, artifact_path)
    live_cli._write_preflight_artifact(
        _final_preflight_result(),
        settings=settings,
        output_path=artifact_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )
    symlink_parent = tmp_path / "preflight-parent-link"
    symlink_parent.symlink_to(artifact_dir, target_is_directory=True)
    settings.live_preflight_artifact_path = str(symlink_parent / artifact_path.name)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(LiveTradingDisabledError, match="not a directory"):
        validator(settings)


def test_live_preflight_artifact_validation_rejects_symlink_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    target_dir = tmp_path / "artifact-target"
    target_dir.mkdir(mode=0o700)
    target_path = target_dir / "target.json"
    _configure_live_preflight_artifact_path(settings, target_path)
    live_cli._write_preflight_artifact(
        _final_preflight_result(),
        settings=settings,
        output_path=target_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )
    artifact_dir = tmp_path / "preflight"
    artifact_dir.mkdir(mode=0o700)
    artifact_path = artifact_dir / "credentialed-preflight.json"
    artifact_path.symlink_to(target_path)
    artifact = json.loads(target_path.read_text(encoding="utf-8"))
    artifact["output_path"] = str(artifact_path)
    target_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(LiveTradingDisabledError, match="regular file"):
        validator(settings)


def test_live_preflight_artifact_validation_rejects_hardlinked_artifact(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    target_dir = tmp_path / "artifact-target"
    target_dir.mkdir(mode=0o700)
    target_path = target_dir / "target.json"
    _configure_live_preflight_artifact_path(settings, target_path)
    live_cli._write_preflight_artifact(
        _final_preflight_result(),
        settings=settings,
        output_path=target_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )
    artifact_dir = tmp_path / "preflight"
    artifact_dir.mkdir(mode=0o700)
    artifact_path = artifact_dir / "credentialed-preflight.json"
    os.link(target_path, artifact_path)
    artifact = json.loads(target_path.read_text(encoding="utf-8"))
    artifact["output_path"] = str(artifact_path)
    target_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(LiveTradingDisabledError, match="single-link"):
        validator(settings)


def test_live_preflight_artifact_validation_opens_artifact_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-no-follow-read-",
            settings=settings,
        )
    )
    settings.live_preflight_artifact_path = str(artifact_path)
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
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    validator(settings)

    observed_by_path = {path: flags for path, flags in observed}
    assert observed_by_path[artifact_path] & no_follow_flag


def test_live_preflight_artifact_validation_rejects_hardlink_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-hardlink-swap-",
            settings=settings,
        )
    )
    replacement_source = artifact_path.parent / "replacement-source.json"
    replacement_source.write_bytes(artifact_path.read_bytes())
    settings.live_preflight_artifact_path = str(artifact_path)
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == artifact_path and not swapped:
            swapped = True
            artifact_path.unlink()
            os.link(replacement_source, artifact_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(LiveTradingDisabledError):
        validator(settings)

    assert swapped is True


def test_live_preflight_output_rejects_artifact_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    artifact_dir = repo_root / "secure"
    artifact_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=tmp_path / "runtime" / "first-order.json")
    output_path = artifact_dir / "credentialed-preflight.json"
    monkeypatch.chdir(repo_root)
    _configure_live_preflight_artifact_path(settings, output_path)

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        live_cli._write_preflight_artifact(
            _final_preflight_result(),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )


def test_live_preflight_output_rejects_incomplete_artifact_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    artifact_dir = repo_root / "secure"
    artifact_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=tmp_path / "runtime" / "first-order.json")
    output_path = artifact_dir / "incomplete-preflight.json"
    monkeypatch.chdir(repo_root)

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        live_cli._write_preflight_artifact(
            LivePreflightResult(
                (
                    LivePreflightCheck(
                        "live_config",
                        False,
                        "config failed before final go/no-go",
                    ),
                )
            ),
            settings=settings,
            output_path=output_path,
            config_path="config.live.yaml",
            skip_venue=False,
            database_url_override_used=False,
        )

    assert not output_path.exists()


def test_live_preflight_artifact_validation_rejects_artifact_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    artifact_dir = repo_root / "secure"
    artifact_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=tmp_path / "runtime" / "first-order.json")
    scratch_dir = tmp_path / "scratch-preflight"
    scratch_dir.mkdir(mode=0o700)
    scratch_path = scratch_dir / "credentialed-preflight.json"
    artifact_path = artifact_dir / "credentialed-preflight.json"
    _configure_live_preflight_artifact_path(settings, scratch_path)
    live_cli._write_preflight_artifact(
        _final_preflight_result(),
        settings=settings,
        output_path=scratch_path,
        config_path="config.live.yaml",
        skip_venue=False,
        database_url_override_used=False,
    )
    artifact = json.loads(scratch_path.read_text(encoding="utf-8"))
    artifact["output_path"] = str(artifact_path)
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")
    monkeypatch.chdir(repo_root)

    with pytest.raises(LiveTradingDisabledError, match="outside the working tree"):
        validator(settings)


def test_live_preflight_artifact_rejects_duplicate_json_keys(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-duplicate-json-key-",
            settings=settings,
        )
    )
    artifact_text = artifact_path.read_text(encoding="utf-8")
    artifact_text = artifact_text.replace(
        '  "final_go_no_go_valid": true,\n',
        '  "final_go_no_go_valid": false,\n'
        '  "final_go_no_go_valid": true,\n',
        1,
    )
    artifact_path.write_text(artifact_text, encoding="utf-8")
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="duplicate JSON key: final_go_no_go_valid",
    ):
        validator(settings)


def test_live_preflight_category_prior_artifact_rejects_duplicate_csv_header() -> None:
    validator = getattr(
        live_preflight_artifact_module,
        "_count_category_prior_observations",
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="duplicate CSV column: category",
    ):
        validator(
            "\n".join(
                (
                    "market_id,category,category,yes_payout,no_payout,resolved_at",
                    "m-1,politics,shadowed,1,0,2026-05-01T12:00:00Z",
                )
            )
        )


def test_live_preflight_flb_calibration_artifact_rejects_duplicate_csv_header() -> None:
    validator = getattr(
        live_preflight_artifact_module,
        "_validate_flb_calibration_rows",
    )

    with pytest.raises(
        LiveTradingDisabledError,
        match="duplicate CSV column: source_label",
    ):
        validator(
            "\n".join(
                (
                    "signal_name,probability_estimate,sample_count,source_label,source_label",
                    "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1,shadowed",
                    "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1,shadowed",
                )
            ),
            min_sample_count=100,
        )


def test_live_preflight_artifact_rejects_duplicate_check_names(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-duplicate-checks-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    result = cast(dict[str, object], artifact["result"])
    checks = cast(list[object], result["checks"])
    checks.append(
        {
            "name": "venue_reconciliation",
            "ok": True,
            "detail": "duplicate venue reconciliation row",
        }
    )
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="duplicate checks: venue_reconciliation",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_malformed_passing_check_row(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-malformed-check-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    result = cast(dict[str, object], artifact["result"])
    checks = cast(list[object], result["checks"])
    checks.append({"ok": True, "detail": "nameless passing row"})
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="malformed checks: unnamed",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_unknown_check_names(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-unknown-checks-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    result = cast(dict[str, object], artifact["result"])
    checks = cast(list[object], result["checks"])
    checks.append(
        {
            "name": "operator_shadow_check",
            "ok": True,
            "detail": "unknown operator shadow row",
        }
    )
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="unknown checks: operator_shadow_check",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_empty_check_detail(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-empty-detail-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    result = cast(dict[str, object], artifact["result"])
    checks = cast(list[dict[str, object]], result["checks"])
    checks[-1]["detail"] = ""
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="malformed checks: venue_reconciliation",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_placeholder_check_detail(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-detail-marker-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    result = cast(dict[str, object], artifact["result"])
    checks = cast(list[dict[str, object]], result["checks"])
    checks[-1]["detail"] = "TODO: confirm venue reconciliation"
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="malformed checks: venue_reconciliation",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_placeholder_active_strategy_fingerprint(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-active-strategy-marker-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["active_strategies_fingerprint"] = (
        "TODO: compute active strategies fingerprint"
    )
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="active strategies fingerprint",
    ):
        validator(settings)


def test_live_preflight_artifact_rejects_non_hash_active_strategy_fingerprint(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    artifact_path = Path(
        make_live_preflight_artifact_path(
            prefix="pms-live-preflight-non-hash-active-strategy-",
            settings=settings,
        )
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["active_strategies_fingerprint"] = "active-strategy-fingerprint"
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    settings.live_preflight_artifact_path = str(artifact_path)
    validator = getattr(live_preflight_module, "require_live_preflight_artifact")

    with pytest.raises(
        LiveTradingDisabledError,
        match="active strategies fingerprint",
    ):
        validator(settings)


def test_live_preflight_active_strategies_fingerprint_includes_projection_content() -> None:
    strategy = _live_active_strategy()
    changed_strategy = replace(
        strategy,
        risk=replace(
            strategy.risk,
            max_position_notional_usdc=strategy.risk.max_position_notional_usdc + 1.0,
        ),
    )

    assert live_preflight_active_strategies_fingerprint(
        [strategy]
    ) != live_preflight_active_strategies_fingerprint([changed_strategy])


@pytest.mark.asyncio
async def test_live_preflight_returns_failed_check_when_database_connection_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    async def fake_create_pool(**_: object) -> asyncpg.Pool:
        raise OSError(
            "database unavailable for "
            "postgresql://pms_user:super-secret-pass@db.example/pms_live "
            "password=keyword-secret"
        )

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        skip_venue=True,
    )

    database_check = result.require_check("database_connection")
    assert result.ok is False
    assert database_check.ok is False
    assert "database connection failed" in database_check.detail
    assert "database unavailable" in database_check.detail
    assert "super-secret-pass" not in database_check.detail
    assert "keyword-secret" not in database_check.detail
    assert "pms_user" not in database_check.detail
    assert "<redacted-database-url>" in database_check.detail
    assert "password=<redacted>" in database_check.detail
    assert "schema_current" not in {check.name for check in result.checks}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "check_name",
        "fail_fetchval_contains",
        "fail_fetch_contains",
    ),
    [
        ("schema_current", "alembic_version", None),
        ("market_data_freshness", "book_snapshots", None),
        ("submission_unknown", "outcome = 'submission_unknown'", None),
        ("live_open_orders", "FROM orders", None),
        ("active_strategies", None, "strategy_versions AS versions"),
    ],
)
async def test_live_preflight_redacts_secrets_from_database_backed_check_failures(
    tmp_path: Path,
    check_name: str,
    fail_fetchval_contains: str | None,
    fail_fetch_contains: str | None,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    secret_dsn = "postgresql://admin:supersecret@db.internal.example.com:5432/pms_live"
    credential_values = (
        "private-key",
        "api-key",
        "api-secret",
        "passphrase",
        "0x1111111111111111111111111111111111111111",
    )
    failure_message = (
        "query failed "
        f"{secret_dsn} password=keyword-secret "
        + " ".join(credential_values)
    )

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(
                _FailingConnection(
                    message=failure_message,
                    fail_fetchval_contains=fail_fetchval_contains,
                    fail_fetch_contains=fail_fetch_contains,
                )
            ),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    check = result.require_check(check_name)
    assert result.ok is False
    assert check.ok is False
    assert "<redacted-database-url>" in check.detail
    assert "password=<redacted>" in check.detail
    assert "<redacted-polymarket-credential>" in check.detail
    for secret_value in (
        *credential_values,
        "supersecret",
        "keyword-secret",
        "admin",
    ):
        assert secret_value not in check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_live_sdk_dependency_is_missing(
    tmp_path: Path,
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
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    dependency_check = result.require_check("runtime_dependencies")
    assert result.ok is False
    assert dependency_check.ok is False
    assert "py_clob_client_v2" in dependency_check.detail
    assert "uv sync --extra live" in dependency_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_enabled_llm_sdk_dependency_is_missing(
    tmp_path: Path,
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
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.llm.enabled = True
    settings.llm.provider = "anthropic"
    settings.llm.api_key = "sk-test"

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    dependency_check = result.require_check("runtime_dependencies")
    assert result.ok is False
    assert dependency_check.ok is False
    assert "anthropic" in dependency_check.detail
    assert "uv sync --extra llm" in dependency_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_submission_unknown_is_unresolved(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    pool = _Pool(_Connection(unresolved_submission_unknown=1))

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(asyncpg.Pool, pool),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    submission_check = result.require_check("submission_unknown")
    assert result.ok is False
    assert submission_check.ok is False
    assert "pms-live reconcile-submission-unknown" in submission_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_persisted_live_open_order_exists(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    pool = _Pool(_LiveOpenOrderConnection(live_open_order_count=1))

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(asyncpg.Pool, pool),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
    )

    live_open_orders_check = result.require_check("live_open_orders")
    assert result.ok is False
    assert live_open_orders_check.ok is False
    assert "durable live open-order ledger" in live_open_orders_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_no_book_snapshots_exist(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(_Connection(latest_book_snapshot_age_s=None)),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    market_data_check = result.require_check("market_data_freshness")
    assert result.ok is False
    assert market_data_check.ok is False
    assert "no book_snapshots" in market_data_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_book_snapshots_are_stale(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(_Connection(latest_book_snapshot_age_s=600.0)),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    market_data_check = result.require_check("market_data_freshness")
    assert result.ok is False
    assert market_data_check.ok is False
    assert "600.0s exceeds" in market_data_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_recent_snapshots_lack_two_sided_depth(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(
                _Connection(
                    latest_book_snapshot_age_s=30.0,
                    latest_usable_book_snapshot_age_s=None,
                )
            ),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    market_data_check = result.require_check("market_data_freshness")
    assert result.ok is False
    assert market_data_check.ok is False
    assert "two-sided" in market_data_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_usable_book_snapshot_is_stale(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(
                _Connection(
                    latest_book_snapshot_age_s=30.0,
                    latest_usable_book_snapshot_age_s=600.0,
                )
            ),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    market_data_check = result.require_check("market_data_freshness")
    assert result.ok is False
    assert market_data_check.ok is False
    assert "latest usable book snapshot age 600.0s exceeds" in market_data_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_fresh_usable_market_lacks_risk_group_metadata(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(
            asyncpg.Pool,
            _Pool(_Connection(missing_market_risk_metadata_count=1)),
        ),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    market_data_check = result.require_check("market_data_freshness")
    assert result.ok is False
    assert market_data_check.ok is False
    assert "risk_group_id" in market_data_check.detail
    assert "1 fresh usable market" in market_data_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_venue_reconciliation_mismatches(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MismatchingVenueReconciler()),
    )

    venue_check = result.require_check("venue_reconciliation")
    assert result.ok is False
    assert venue_check.ok is False
    assert venue_check.detail == "venue has open orders"


@pytest.mark.asyncio
async def test_live_preflight_redacts_polymarket_credentials_from_venue_errors(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    secret_dsn = "postgresql://admin:supersecret@db.internal.example.com:5432/pms_live"
    credentials = ("private-key", "api-key", "api-secret", "passphrase", "0x1111111111111111111111111111111111111111")
    reconciler = _FailingVenueReconciler(
        "venue auth failed "
        f"{credentials[0]} {credentials[1]} {credentials[2]} "
        f"{credentials[3]} {credentials[4]} {secret_dsn} password=keyword-secret"
    )

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, reconciler),
    )

    venue_check = result.require_check("venue_reconciliation")
    assert result.ok is False
    assert venue_check.ok is False
    assert "<redacted-polymarket-credential>" in venue_check.detail
    assert "<redacted-database-url>" in venue_check.detail
    assert "password=<redacted>" in venue_check.detail
    for credential in credentials:
        assert credential not in venue_check.detail
    assert "supersecret" not in venue_check.detail
    assert "keyword-secret" not in venue_check.detail
    assert "admin" not in venue_check.detail


@pytest.mark.asyncio
async def test_live_preflight_redacts_polymarket_credentials_from_venue_mismatches(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    secret_dsn = "postgresql://admin:supersecret@db.internal.example.com:5432/pms_live"
    credentials = ("private-key", "api-key", "api-secret", "passphrase", "0x1111111111111111111111111111111111111111")
    reconciler = _MismatchingVenueReconciler(
        (
            "venue mismatch included "
            f"{credentials[0]} {credentials[1]} {credentials[2]}",
            (
                "venue mismatch included "
                f"{credentials[3]} {credentials[4]} {secret_dsn} "
                "password=keyword-secret"
            ),
        )
    )

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, reconciler),
    )

    venue_check = result.require_check("venue_reconciliation")
    assert result.ok is False
    assert venue_check.ok is False
    assert "<redacted-polymarket-credential>" in venue_check.detail
    assert "<redacted-database-url>" in venue_check.detail
    assert "password=<redacted>" in venue_check.detail
    for credential in credentials:
        assert credential not in venue_check.detail
    assert "supersecret" not in venue_check.detail
    assert "keyword-secret" not in venue_check.detail
    assert "admin" not in venue_check.detail


@pytest.mark.asyncio
async def test_live_preflight_is_incomplete_when_venue_reconciliation_is_skipped(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)

    result = await run_live_preflight(
        _settings(approval_path=approval_dir / "first-order.json"),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=None,
        skip_venue=True,
    )

    venue_check = result.require_check("venue_reconciliation")
    assert result.ok is False
    assert venue_check.ok is False
    assert "skipped by operator flag" in venue_check.detail
    assert "final live go/no-go" in venue_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_approval_directory_is_missing(
    tmp_path: Path,
) -> None:
    result = await run_live_preflight(
        _settings(approval_path=tmp_path / "missing" / "first-order.json"),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    approval_check = result.require_check("operator_approval")
    assert result.ok is False
    assert approval_check.ok is False
    assert "parent directory does not exist" in approval_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_approval_directory_is_not_owner_writable(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    approval_dir.chmod(0o500)
    try:
        result = await run_live_preflight(
            _settings(approval_path=approval_dir / "first-order.json"),
            pool=cast(asyncpg.Pool, _Pool(_Connection())),
            venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
            skip_venue=True,
        )
    finally:
        approval_dir.chmod(0o700)

    approval_check = result.require_check("operator_approval")
    assert result.ok is False
    assert approval_check.ok is False
    assert "is not owner-writable" in approval_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_approval_directory_is_symlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    symlink_parent = tmp_path / "approval-parent-link"
    symlink_parent.symlink_to(approval_dir, target_is_directory=True)

    result = await run_live_preflight(
        _settings(approval_path=symlink_parent / "first-order.json"),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    approval_check = result.require_check("operator_approval")
    assert result.ok is False
    assert approval_check.ok is False
    assert "parent path is not a directory" in approval_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_stale_approval_file_already_exists(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    approval_path = approval_dir / "first-order.json"
    approval_path.write_text('{"approved": true}\n', encoding="utf-8")

    result = await run_live_preflight(
        _settings(approval_path=approval_path),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    approval_check = result.require_check("operator_approval")
    assert result.ok is False
    assert approval_check.ok is False
    assert "stale approval file" in approval_check.detail
    assert str(approval_path) in approval_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_stale_approval_sidecar_already_exists(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    approval_path = approval_dir / "first-order.json"
    sidecar_path = Path(str(approval_path) + ".meta.json")
    sidecar_path.write_text('{"approver_id": "operator-alice"}\n', encoding="utf-8")

    result = await run_live_preflight(
        _settings(approval_path=approval_path),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    approval_check = result.require_check("operator_approval")
    assert result.ok is False
    assert approval_check.ok is False
    assert "stale approval sidecar" in approval_check.detail
    assert str(sidecar_path) in approval_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_approval_path_is_symlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    target_path = tmp_path / "target-approval.json"
    target_path.write_text('{"approved": true}\n', encoding="utf-8")
    approval_path = approval_dir / "first-order.json"
    approval_path.symlink_to(target_path)

    result = await run_live_preflight(
        _settings(approval_path=approval_path),
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    approval_check = result.require_check("operator_approval")
    assert result.ok is False
    assert approval_check.ok is False
    assert "regular file" in approval_check.detail
    assert str(approval_path) in approval_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_first_order_audit_directory_is_not_owner_writable(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    audit_dir = tmp_path / "secure-audit"
    audit_dir.mkdir(mode=0o700)
    audit_dir.chmod(0o500)
    settings = _settings(approval_path=approval_dir / "first-order.json").model_copy(
        update={"live_first_order_audit_path": str(audit_dir / "audit.jsonl")}
    )

    try:
        result = await run_live_preflight(
            settings,
            pool=cast(asyncpg.Pool, _Pool(_Connection())),
            venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
            skip_venue=True,
        )
    finally:
        audit_dir.chmod(0o700)

    audit_check = result.require_check("first_order_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "is not owner-writable" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_first_order_audit_directory_is_symlink(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure-approval"
    approval_dir.mkdir(mode=0o700)
    audit_dir = tmp_path / "secure-audit"
    audit_dir.mkdir(mode=0o700)
    symlink_parent = tmp_path / "first-order-audit-parent-link"
    symlink_parent.symlink_to(audit_dir, target_is_directory=True)
    settings = _settings(approval_path=approval_dir / "first-order.json").model_copy(
        update={
            "live_first_order_audit_path": str(
                symlink_parent / "first-order-audit.jsonl"
            )
        }
    )

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    audit_check = result.require_check("first_order_audit")
    assert result.ok is False
    assert audit_check.ok is False
    assert "parent path is not a directory" in audit_check.detail


@pytest.mark.asyncio
async def test_live_preflight_fails_when_operator_attestation_is_placeholder(
    tmp_path: Path,
) -> None:
    approval_dir = tmp_path / "secure"
    approval_dir.mkdir(mode=0o700)
    settings = _settings(approval_path=approval_dir / "first-order.json")
    settings.live_exit_criteria_ratified_by = "__FILL_IN_OPERATOR_ID__"

    result = await run_live_preflight(
        settings,
        pool=cast(asyncpg.Pool, _Pool(_Connection())),
        venue_reconciler=cast(PolymarketVenueAccountReconciler, _MatchingVenueReconciler()),
        skip_venue=True,
    )

    live_config = result.require_check("live_config")
    venue_check = result.require_check("venue_reconciliation")
    assert result.ok is False
    assert live_config.ok is False
    assert "contains placeholder" in live_config.detail
    assert venue_check.detail == "skipped because LIVE config validation failed"
