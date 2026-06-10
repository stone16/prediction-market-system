from __future__ import annotations

import os
import stat
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from pms.config import RiskSettings
from pms.core.enums import TimeInForce
from pms.core.models import EvalRecord, TradeDecision
from pms.metrics import (
    LLM_BUDGET_EXHAUSTED_TOTAL_METRIC,
    LLM_DAILY_COST_LIMIT_USDC_METRIC,
    LLM_DAILY_COST_USDC_METRIC,
    LLM_ESTIMATED_COST_USDC_TOTAL_METRIC,
    SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC,
    SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC,
    SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC,
    SELECTION_FUNNEL_ROUTED_TOTAL_METRIC,
    SELECTION_FUNNEL_SELECTED_TOTAL_METRIC,
    SELECTION_FUNNEL_TRADED_TOTAL_METRIC,
)
from scripts.paper_report import (
    ExecutionConcentration,
    PaperSoakGateConfig,
    PaperReportMetrics,
    PaperReportProvenance,
    TradeCostBreakdown,
    _fetch_api_list_pages,
    _fetch_api_payload,
    evaluate_paper_soak_gate,
    build_paper_report_diagnostics,
    load_live_metrics,
    main,
    metrics_from_api_payloads,
    render_report,
)


def test_paper_report_renders_empty_day_without_crashing() -> None:
    report = render_report(
        PaperReportMetrics.empty(report_date=date(2026, 5, 3)),
        risk=RiskSettings(max_total_exposure=50.0),
    )

    assert "# Paper Daily Report - 2026-05-03" in report
    assert "| Decisions made | 0 |" in report
    assert "| Brier score (14d rolling) | N/A | < 0.20 |" in report
    assert "| Market baseline Brier (14d rolling) | N/A | - |" in report
    assert "| Brier improvement vs baseline | N/A | > 0 |" in report
    assert "| Fill rate | N/A | > 0 |" in report
    assert "| Average fee (bps) | N/A | - |" in report
    assert "| Average net edge after costs (bps) | N/A | > 0 |" in report
    assert "| Unresolved incidents | 0 | 0 required |" in report
    assert "| (none today) | - | - |" in report
    assert "No trades today." in report
    assert "No controller rejection reasons recorded." in report


def test_paper_report_contains_all_gate_three_metrics() -> None:
    metrics = PaperReportMetrics(
        report_date=date(2026, 5, 7),
        strategy="ripple_v2",
        day_of_soak=4,
        decisions_made=12,
        decisions_accepted=3,
        decisions_rejected=9,
        fills=2,
        average_slippage_bps=10.0,
        todays_pnl=1.25,
        cumulative_pnl=3.5,
        max_drawdown_pct=4.0,
        open_positions=2,
        total_exposure=8.75,
        brier_score_7d=0.18,
        baseline_brier_score_7d=0.23,
        brier_improvement_7d=0.05,
        fill_rate=0.5,
        hit_rate=0.5,
        average_edge_bps=35.0,
        average_fee_bps=2.5,
        average_net_edge_bps=22.5,
        sharpe_ratio=0.7,
        unresolved_incidents=1,
        rejection_reasons=(("missing_no_token", 2),),
        risk_events=(("12:00", "rate_limit_exceeded", "halted"),),
    )

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert "| Controller diagnostic sample | 9 | - |" in report
    assert "| Decisions rejected |" not in report
    assert "| Entry fills | 2 |" in report
    assert "| Distinct traded markets | N/A | >= 3 by soak end |" in report
    assert "| Max risk group fill share | N/A | <= 60% by soak end |" in report
    assert "| Today's P&L | +$1.25 |" in report
    assert "| Max exposure | $50.00 |" in report
    assert "| Brier score (14d rolling) | 0.18 | < 0.20 |" in report
    assert "| Market baseline Brier (14d rolling) | 0.23 | - |" in report
    assert "| Brier improvement vs baseline | 0.05 | > 0 |" in report
    assert "| Fill rate | 50.0% | > 0 |" in report
    assert "| Hit rate (all trades) | 50.0% | > 45% |" in report
    assert "| Average edge (bps) | 35.0 | > 5 |" in report
    assert "| Average fee (bps) | 2.5 | - |" in report
    assert "| Average net edge after costs (bps) | 22.5 | > 0 |" in report
    assert "| Sharpe ratio (cumulative) | 0.70 | > 0 |" in report
    assert "| Unresolved incidents | 1 | 0 required |" in report
    assert "| missing_no_token | 2 |" in report


def test_paper_report_escapes_freeform_table_values() -> None:
    metrics = PaperReportMetrics(
        report_date=date(2026, 5, 4),
        strategy="default|paper\nv1",
        risk_events=(
            ("12|00", "bad\ntrigger", "operator saw | FAIL | text"),
        ),
        trade_costs=(
            TradeCostBreakdown(
                decision_id="decision|1",
                market_id="market\n1",
                gross_edge=0.12,
                spread_cost=0.01,
                net_edge=0.11,
            ),
        ),
        rejection_reasons=(("missing|required\nfactor", 1),),
        clamp_rejections=(("market|clamp\n1", 2),),
    )

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert "| Strategy | default\\|paper v1 | - |" in report
    assert "| 12\\|00 | bad trigger | operator saw \\| FAIL \\| text |" in report
    assert "| decision\\|1 | market 1 | 12.0% | 1.0% | 0.0% | 0.0% | 11.0% |" in report
    assert "| missing\\|required factor | 1 |" in report
    assert "| market\\|clamp 1 | 2 |" in report


def test_paper_soak_gate_passes_when_metrics_meet_go_live_thresholds() -> None:
    metrics = _passing_gate_metrics()

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert gate.ok is True
    assert all(check.ok for check in gate.checks)
    assert gate.require_check("readiness").detail == (
        "status=ready; eod_scheduler=disabled; event_loop=ready; "
        "halt_subscriber=disabled; sensors=ready"
    )
    assert gate.require_check("brier_improvement").detail == "0.0500 > 0.0000"
    assert gate.require_check("distinct_markets").detail == "3 >= 3"
    assert gate.require_check("distinct_risk_groups").detail == "3 >= 3"


def test_paper_soak_gate_fails_missing_or_bad_production_metrics() -> None:
    metrics = _passing_gate_metrics(
        day_of_soak=4,
        decisions_accepted=0,
        fills=0,
        brier_improvement_7d=None,
        sharpe_ratio=-0.1,
        average_net_edge_bps=-1.0,
        unresolved_incidents=1,
        risk_events=(("12:00", "sensor stale", "investigate"),),
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert gate.ok is False
    assert gate.require_check("soak_days").ok is False
    assert gate.require_check("decisions_accepted").detail == "0 < 30"
    assert gate.require_check("fills").detail == "0 < 50"
    assert gate.require_check("brier_improvement").detail == "missing"
    assert gate.require_check("sharpe_ratio").detail == "-0.1000 <= 0.0000"
    assert gate.require_check("average_net_edge_bps").detail == "-1.0000 <= 0.0000"
    assert gate.require_check("unresolved_incidents").detail == "1 unresolved"
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_without_ready_readiness_evidence() -> None:
    metrics = _passing_gate_metrics(
        readiness_status="not_ready",
        readiness_checks=(
            ("sensors", "ready"),
            ("event_loop", "shutting_down"),
            ("halt_subscriber", "disabled"),
        ),
    )

    gate = evaluate_paper_soak_gate(metrics, risk=RiskSettings())

    assert gate.ok is False
    assert gate.require_check("readiness").detail == (
        "status=not_ready; event_loop=shutting_down"
    )


def test_paper_soak_gate_fails_concentrated_execution_sample() -> None:
    metrics = replace(
        _passing_gate_metrics(),
        execution_concentration=ExecutionConcentration(
            entry_fills=50,
            distinct_markets=1,
            distinct_risk_groups=1,
            missing_risk_group_fills=0,
            max_market_fill_share=1.0,
            max_risk_group_fill_share=1.0,
        ),
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert gate.ok is False
    assert gate.require_check("distinct_markets").detail == "1 < 3"
    assert gate.require_check("distinct_risk_groups").detail == "1 < 3"
    assert gate.require_check("max_market_fill_share").detail == "1.0000 > 0.6000"
    assert gate.require_check("max_risk_group_fill_share").detail == "1.0000 > 0.6000"


def test_paper_soak_gate_fails_without_concrete_strategy_version_evidence() -> None:
    metrics = _passing_gate_metrics(strategy="unknown")

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert gate.ok is False
    assert gate.require_check("strategy_evidence").detail == (
        "missing concrete strategy_id@strategy_version_id"
    )


def test_paper_soak_gate_rejects_paper_only_strategy_evidence() -> None:
    metrics = _passing_gate_metrics(
        strategy=(
            "h1_flb@live-v1, "
            "paper_canary_v1@canary-v1, "
            "paper_multi_factor_v1@paper-v1"
        ),
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert gate.ok is False
    assert gate.require_check("strategy_evidence").detail == (
        "paper-only strategy cannot be final GO evidence: "
        "paper_canary_v1@canary-v1, paper_multi_factor_v1@paper-v1"
    )


def test_paper_soak_gate_requires_launch_sample_size() -> None:
    metrics = _passing_gate_metrics(decisions_accepted=29, fills=49)

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert gate.ok is False
    assert gate.require_check("decisions_accepted").detail == "29 < 30"
    assert gate.require_check("fills").detail == "49 < 50"


def test_paper_report_renders_machine_checkable_go_no_go_gate() -> None:
    report = render_report(
        _passing_gate_metrics(),
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert "## Go/No-Go Gate" in report
    assert "**Decision:** GO" in report
    assert "| brier_improvement | PASS | 0.0500 > 0.0000 |" in report
    assert "| distinct_markets | PASS | 3 >= 3 |" in report
    assert "| max_risk_group_fill_share | PASS | 0.4000 <= 0.6000 |" in report


def test_paper_report_require_go_returns_nonzero_when_gate_fails(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "  max_drawdown_pct: 20.0",
                "  max_daily_loss_usdc: 20.0",
                "  max_open_positions: 5",
                "",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--dry-run",
            "--require-go",
        ]
    )

    assert exit_code == 1
    assert "**Decision:** NO-GO" in capsys.readouterr().out


def test_paper_report_require_go_rejects_future_report_date(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    future_date = datetime.now(tz=UTC).date() + timedelta(days=1)

    exit_code = main(
        [
            "--date",
            future_date.isoformat(),
            "--config",
            str(config_path),
            "--offline",
            "--dry-run",
            "--require-go",
        ]
    )

    output = capsys.readouterr()
    assert exit_code == 1
    assert "paper report --require-go date must not be in the future" in output.err
    assert "**Decision:**" not in output.out


def test_paper_report_dry_run_marks_report_as_non_persisted(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--dry-run",
        ]
    )

    report = capsys.readouterr().out
    assert exit_code == 0
    assert "| generated_by | scripts/paper_report.py |" in report
    assert "| generated_at | " in report
    assert "| artifact_mode | dry_run |" in report
    assert "| output_path | stdout |" in report


def test_paper_report_persisted_file_marks_report_as_persisted(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "reports"
    expected_output_path = output_dir / "2026-05-03.md"

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out.strip() == str(expected_output_path)
    report = expected_output_path.read_text(encoding="utf-8")
    assert "| generated_by | scripts/paper_report.py |" in report
    assert "| generated_at | " in report
    assert "| artifact_mode | persisted |" in report
    assert f"| output_path | {expected_output_path} |" in report


def test_paper_report_persisted_file_can_target_exact_output_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_path = tmp_path / "reports" / "paper-soak-go-report.md"

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out.strip() == str(output_path)
    report = output_path.read_text(encoding="utf-8")
    assert "| artifact_mode | persisted |" in report
    assert f"| output_path | {output_path} |" in report
    assert not (output_path.parent / "2026-05-03.md").exists()


def test_paper_report_provenance_records_input_snapshot_hash() -> None:
    input_snapshot_sha256 = sha256(b"paper report API snapshot").hexdigest()

    report = render_report(
        PaperReportMetrics.empty(report_date=date(2026, 5, 3)),
        risk=RiskSettings(max_total_exposure=50.0),
        provenance=PaperReportProvenance(
            artifact_mode="persisted",
            output_path="/secure/pms/paper-soak-go-report.md",
            input_snapshot_sha256=input_snapshot_sha256,
        ),
    )

    assert f"| input_snapshot_sha256 | {input_snapshot_sha256} |" in report


def test_paper_report_returns_operator_error_for_unsafe_config_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_target = tmp_path / "config.yaml"
    config_target.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "config-link.yaml"
    config_path.symlink_to(config_target)
    output_path = tmp_path / "reports" / "paper-soak-go-report.md"

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output",
            str(output_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "Config file cannot be read safely" in captured.err
    assert str(config_path) in captured.err
    assert not output_path.exists()


def test_paper_report_returns_operator_error_for_malformed_report_date(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--date",
            "2026/05/03",
            "--config",
            str(config_path),
            "--offline",
            "--dry-run",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "--date must be YYYY-MM-DD" in captured.err
    assert "2026/05/03" in captured.err


def test_paper_report_records_absolute_output_path_for_relative_output_dir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output-dir",
            "../secure/paper-reports",
        ]
    )

    expected_output_path = (
        tmp_path / "secure" / "paper-reports" / "2026-05-03.md"
    )
    assert exit_code == 0
    assert capsys.readouterr().out.strip() == str(expected_output_path)
    report = expected_output_path.read_text(encoding="utf-8")
    assert f"| output_path | {expected_output_path} |" in report


def test_paper_report_creates_persisted_output_directory_private(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "reports"

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert stat.S_IMODE(output_dir.stat().st_mode) == 0o700


def test_paper_report_require_go_rejects_output_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    config_path = repo_root / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = repo_root / "docs" / "paper-reports"
    monkeypatch.chdir(repo_root)

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--require-go",
            "--output-dir",
            str(output_dir),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "working tree" in captured.err
    assert not (output_dir / "2026-05-03.md").exists()


def test_paper_report_require_go_rejects_output_reusing_local_secret_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    secret_path = secure_dir / "2026-05-03.md"
    original_secret_text = "polymarket:\n  private_key: original-secret\n"
    secret_path.write_text(original_secret_text, encoding="utf-8")
    secret_path.chmod(0o600)
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--require-go",
            "--output-dir",
            str(secure_dir),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "LIVE local secret file" in captured.err
    assert secret_path.read_text(encoding="utf-8") == original_secret_text


def test_paper_report_refuses_permissive_persisted_output_directory(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "shared-reports"
    output_dir.mkdir(mode=0o700)
    output_dir.chmod(0o755)

    try:
        exit_code = main(
            [
                "--date",
                "2026-05-03",
                "--config",
                str(config_path),
                "--offline",
                "--output-dir",
                str(output_dir),
            ]
        )
        captured = capsys.readouterr()
    finally:
        output_dir.chmod(0o700)

    assert exit_code == 2
    assert "paper report output directory" in captured.err
    assert "too permissive" in captured.err
    assert not (output_dir / "2026-05-03.md").exists()


def test_paper_report_refuses_symlink_persisted_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "reports"
    output_dir.mkdir(mode=0o700)
    target_path = tmp_path / "target-report.md"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    expected_output_path = output_dir / "2026-05-03.md"
    expected_output_path.symlink_to(target_path)

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output-dir",
            str(output_dir),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "regular file" in captured.err
    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"


def test_paper_report_refuses_hardlinked_persisted_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "reports"
    output_dir.mkdir(mode=0o700)
    target_path = tmp_path / "target-report.md"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    expected_output_path = output_dir / "2026-05-03.md"
    os.link(target_path, expected_output_path)

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output-dir",
            str(output_dir),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "single-link" in captured.err
    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"


def test_paper_report_hardlink_swap_during_atomic_publish_keeps_linked_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "reports"
    output_dir.mkdir(mode=0o700)
    target_path = tmp_path / "target-report.md"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    expected_output_path = output_dir / "2026-05-03.md"
    expected_output_path.write_text("old report\n", encoding="utf-8")
    real_replace = os.replace
    swapped = False

    def swapping_replace(
        src: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        dst: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    ) -> None:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(dst)))
        if observed_path == expected_output_path and not swapped:
            swapped = True
            expected_output_path.unlink()
            os.link(target_path, expected_output_path)
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", swapping_replace)

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output-dir",
            str(output_dir),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert swapped is True
    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"
    assert "**Decision:** NO-GO" in expected_output_path.read_text(encoding="utf-8")


def test_paper_report_preserves_existing_output_when_truncate_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "reports"
    output_dir.mkdir(mode=0o700)
    expected_output_path = output_dir / "2026-05-03.md"
    original_report_text = "# Existing paper-soak GO report\n"
    expected_output_path.write_text(original_report_text, encoding="utf-8")
    expected_output_path.chmod(0o600)
    real_ftruncate = os.ftruncate

    def truncate_then_fail(fd: int, length: int) -> None:
        real_ftruncate(fd, length)
        raise OSError("simulated paper report truncate failure")

    monkeypatch.setattr(os, "ftruncate", truncate_then_fail)

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output-dir",
            str(output_dir),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "simulated paper report truncate failure" in captured.err
    assert expected_output_path.read_text(encoding="utf-8") == original_report_text


def test_paper_report_does_not_publish_new_output_when_truncate_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "reports"
    output_dir.mkdir(mode=0o700)
    expected_output_path = output_dir / "2026-05-03.md"
    real_ftruncate = os.ftruncate

    def truncate_then_fail(fd: int, length: int) -> None:
        real_ftruncate(fd, length)
        raise OSError("simulated paper report truncate failure")

    monkeypatch.setattr(os, "ftruncate", truncate_then_fail)

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output-dir",
            str(output_dir),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "simulated paper report truncate failure" in captured.err
    assert not expected_output_path.exists()


def test_paper_report_overwrite_clamps_output_permissions(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_total_exposure: 50.0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "reports"
    output_dir.mkdir(mode=0o700)
    expected_output_path = output_dir / "2026-05-03.md"
    expected_output_path.write_text("pre-existing report\n", encoding="utf-8")
    expected_output_path.chmod(0o644)

    exit_code = main(
        [
            "--date",
            "2026-05-03",
            "--config",
            str(config_path),
            "--offline",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert stat.S_IMODE(expected_output_path.stat().st_mode) == 0o600


def test_metrics_from_api_payloads_uses_live_runner_status() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T09:03:03.240858+00:00",
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "stale",
                    "last_signal_at": "2026-05-04T01:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 17,
                "diagnostics_total": 3,
                "diagnostic_counts": {
                    "missing_no_token": 2,
                    "missing_required_factors": 1,
                },
            },
            "actuator": {
                "fills_total": 4,
                "halt_recovery_cycles_7d": 2,
            },
            "evaluator": {
                "brier_overall": 0.18,
                "baseline_brier_overall": 0.23,
                "brier_improvement_overall": 0.05,
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {
                "unresolved_feedback_total": 2,
            },
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.25,
            "slippage_bps": 20.0,
            "pnl_series": _pnl_series(date(2026, 5, 5), pnl=1.0),
        },
        decisions=[
            {
                "decision_id": "d-1",
                "market_id": "m-1",
                "prob_estimate": 0.64,
                "limit_price": 0.60,
                "expected_edge": 0.04,
                "spread_bps_at_decision": 100,
                "created_at": "2026-05-05T00:00:00+00:00",
            },
            {
                "decision_id": "d-2",
                "market_id": "m-2",
                "prob_estimate": 0.72,
                "limit_price": 0.69,
                "expected_edge": 0.03,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
            },
        ],
        trades={
            "trades": [
                {
                    "fill_notional_usdc": 100.0,
                    "fees": 0.02,
                    "filled_at": "2026-05-05T00:00:00+00:00",
                },
                {
                    "fill_notional_usdc": 100.0,
                    "fees": 0.03,
                    "filled_at": "2026-05-05T00:00:00+00:00",
                },
            ],
        },
        positions={
            "positions": [
                {"locked_usdc": 3.0, "unrealized_pnl": 1.25},
                {"locked_usdc": 2.0, "unrealized_pnl": -0.25},
            ],
        },
        strategies={
            "strategies": [
                {
                    "strategy_id": "default",
                    "active_version_id": "default-v2",
                }
            ],
        },
    )

    assert metrics.strategy == "default@default-v2"
    assert metrics.day_of_soak == 2
    assert metrics.decisions_made == 2
    assert metrics.decisions_rejected == 3
    assert metrics.fills == 2
    assert metrics.open_positions == 2
    assert metrics.total_exposure == 5.0
    assert metrics.cumulative_pnl == 1.0
    assert metrics.brier_score_7d == 0.16
    assert metrics.baseline_brier_score_7d == 0.24
    assert metrics.brier_improvement_7d == 0.08
    assert metrics.fill_rate == pytest.approx(0.5)
    assert metrics.hit_rate == pytest.approx(0.25)
    assert metrics.average_slippage_bps == pytest.approx(20.0)
    assert metrics.average_fee_bps == pytest.approx(2.5)
    assert metrics.average_edge_bps == pytest.approx(350.0)
    assert metrics.average_net_edge_bps == pytest.approx(288.2375)
    assert metrics.trade_costs == (
        TradeCostBreakdown(
            decision_id="d-1",
            market_id="m-1",
            gross_edge=0.04,
            spread_cost=0.006,
            net_edge=0.034,
        ),
        TradeCostBreakdown(
            decision_id="d-2",
            market_id="m-2",
            gross_edge=0.03,
            spread_cost=0.00345,
            net_edge=0.026549999999999997,
        ),
    )
    assert metrics.rejection_reasons == (
        ("missing_no_token", 2),
        ("missing_required_factors", 1),
    )
    assert metrics.unresolved_incidents == 2
    assert (
        "sensor",
        "MarketDataSensor stale",
        "last_signal_at=2026-05-04T01:00:00+00:00",
    ) in metrics.risk_events
    assert (
        "actuator",
        "halt_recovery_cycles_7d",
        "2 recovered halt cycle(s) in trailing 7d",
    ) in metrics.risk_events


def test_trade_cost_breakdown_uses_decision_evidence_cost_basis() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T00:00:00+00:00",
            "sensors": [],
            "controller": {"decisions_total": 1, "diagnostic_counts": {}},
            "actuator": {"fills_total": 1},
            "evaluator": {},
            "quality": {},
        },
        decisions=[
            {
                "decision_id": "d-cost-evidence",
                "market_id": "m-cost-evidence",
                "prob_estimate": 0.62,
                "limit_price": 0.41,
                "expected_edge": 0.21,
                "spread_bps_at_decision": 80,
                "created_at": "2026-05-05T00:00:00+00:00",
                "decision_evidence": {
                    "fee_edge_at_decision": 0.0177,
                    "slippage_edge_at_decision": 0.00205,
                    "net_edge_after_costs": 0.18697,
                },
            }
        ],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.trade_costs == (
        TradeCostBreakdown(
            decision_id="d-cost-evidence",
            market_id="m-cost-evidence",
            gross_edge=0.21,
            spread_cost=0.00328,
            fee_cost=0.0177,
            slippage_cost=0.00205,
            net_edge=0.18697,
        ),
    )

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert "## Trade Cost Decomposition" in report
    assert (
        "| d-cost-evidence | m-cost-evidence | 21.0% | 0.3% | 1.8% | 0.2% | 18.7% |"
        in report
    )


def test_metrics_from_api_payloads_counts_only_elapsed_soak_days() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 31),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-01T23:30:00+00:00",
            "runtime_continuity": {
                "source": "postgres_runtime_heartbeats",
                "healthy_days": 29,
                "last_observed_at": "2026-05-31T00:10:00+00:00",
            },
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 0, "halt_recovery_cycles_7d": 0},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        trades={},
        positions={},
    )

    assert metrics.day_of_soak == 29


def test_metrics_from_api_payloads_treats_zero_sample_win_rate_as_missing() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 31),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-31T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 1, "halt_recovery_cycles_7d": 0},
            "evaluator": {"eval_records_total": 0},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "record_count": 0,
            "fill_rate": 1.0,
            "win_rate": 0.0,
            "slippage_bps": 0.0,
            "pnl_series": _pnl_series(date(2026, 5, 31), pnl=0.0),
        },
        decisions=[
            {
                "decision_id": "d-1",
                "market_id": "m-1",
                "prob_estimate": 0.64,
                "limit_price": 0.60,
                "expected_edge": 0.04,
                "spread_bps_at_decision": 10,
                "created_at": "2026-05-31T00:00:00+00:00",
            },
        ],
        trades={
            "trades": [
                {
                    "fill_notional_usdc": 2.0,
                    "fees": 0.0,
                    "filled_at": "2026-05-31T00:00:01+00:00",
                }
            ],
        },
        positions={"positions": []},
    )

    assert metrics.hit_rate is None
    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(max_total_exposure=50.0),
    )
    assert gate.require_check("hit_rate").detail == "missing"


def test_metrics_from_api_payloads_preserves_resolved_zero_win_rate() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 31),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-31T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 1, "halt_recovery_cycles_7d": 0},
            "evaluator": {"eval_records_total": 1},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "record_count": 1,
            "fill_rate": 1.0,
            "win_rate": 0.0,
            "slippage_bps": 0.0,
            "pnl_series": _pnl_series(date(2026, 5, 31), pnl=0.0),
        },
        decisions=[
            {
                "decision_id": "d-1",
                "market_id": "m-1",
                "prob_estimate": 0.64,
                "limit_price": 0.60,
                "expected_edge": 0.04,
                "spread_bps_at_decision": 10,
                "created_at": "2026-05-31T00:00:00+00:00",
            },
        ],
        trades={
            "trades": [
                {
                    "fill_notional_usdc": 2.0,
                    "fees": 0.0,
                    "filled_at": "2026-05-31T00:00:01+00:00",
                }
            ],
        },
        positions={"positions": []},
    )

    assert metrics.hit_rate == 0.0


def test_report_net_edge_gate_excludes_position_exit_decisions() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "controller": {
                "decisions_total": 2,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 2},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "slippage_bps": 0.0,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
        },
        decisions=[
            {
                "decision_id": "entry-1",
                "market_id": "m-1",
                "expected_edge": 0.10,
                "spread_bps_at_decision": 100,
                "created_at": "2026-05-30T00:00:00+00:00",
            },
            {
                "decision_id": "exit-stop_loss-1",
                "market_id": "m-1",
                "expected_edge": 0.0,
                "spread_bps_at_decision": 100,
                "created_at": "2026-05-30T00:01:00+00:00",
            },
        ],
        trades={
            "trades": [
                {
                    "decision_id": "entry-1",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.40,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                },
                {
                    "decision_id": "exit-stop_loss-1",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.80,
                    "filled_at": "2026-05-30T00:01:00+00:00",
                },
            ],
        },
        positions={"positions": []},
    )

    assert metrics.average_fee_bps == pytest.approx(600.0)
    assert metrics.average_edge_bps == pytest.approx(1000.0)
    assert metrics.average_net_edge_bps == pytest.approx(500.0)
    assert metrics.trade_costs == (
        TradeCostBreakdown(
            decision_id="entry-1",
            market_id="m-1",
            gross_edge=0.10,
            spread_cost=0.01,
            net_edge=0.09000000000000001,
        ),
    )


def test_report_net_edge_gate_honors_spread_already_in_price_evidence() -> None:
    """Ask-frame decisions persist spread_already_in_price=True because
    expected_edge = p - ask already pays the spread; the go/no-go gate input
    must not subtract a re-derived spread cost a second time."""
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 1},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "slippage_bps": 0.0,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
        },
        decisions=[
            {
                "decision_id": "entry-ask-1",
                "market_id": "m-1",
                "prob_estimate": 0.55,
                "limit_price": 0.50,
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
                "decision_evidence": {
                    "spread_already_in_price": True,
                    "spread_edge_at_decision": 0.0,
                    "fee_edge_at_decision": 0.0035,
                    "slippage_edge_at_decision": 0.0025,
                    "net_edge_after_costs": 0.044,
                },
            },
        ],
        trades={
            "trades": [
                {
                    "decision_id": "entry-ask-1",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.10,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                },
            ],
        },
        positions={"positions": []},
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(max_total_exposure=50.0),
    )

    # 500 bps gross - 0 spread (already inside the ask frame) - 0 slippage
    # - 50 bps fee; the unfixed re-derivation charged 25 bps spread again.
    assert metrics.average_edge_bps == pytest.approx(500.0)
    assert metrics.average_net_edge_bps == pytest.approx(450.0)
    assert (
        gate.require_check("average_net_edge_bps").detail == "450.0000 > 0.0000"
    )


def test_trade_cost_breakdown_honors_spread_already_in_price_evidence() -> None:
    """Breakdown rows for flagged decisions must stay internally consistent:
    net_edge == gross_edge - spread_cost - fee_cost - slippage_cost."""
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T00:00:00+00:00",
            "sensors": [],
            "controller": {"decisions_total": 1, "diagnostic_counts": {}},
            "actuator": {"fills_total": 1},
            "evaluator": {},
            "quality": {},
        },
        decisions=[
            {
                "decision_id": "d-ask-frame",
                "market_id": "m-ask-frame",
                "prob_estimate": 0.55,
                "limit_price": 0.50,
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
                "decision_evidence": {
                    "spread_already_in_price": True,
                    "spread_edge_at_decision": 0.0,
                    "fee_edge_at_decision": 0.0035,
                    "slippage_edge_at_decision": 0.0025,
                    "net_edge_after_costs": 0.044,
                },
            }
        ],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.trade_costs == (
        TradeCostBreakdown(
            decision_id="d-ask-frame",
            market_id="m-ask-frame",
            gross_edge=0.05,
            spread_cost=0.0,
            fee_cost=0.0035,
            slippage_cost=0.0025,
            net_edge=0.044,
        ),
    )


def test_metrics_from_api_payloads_flags_active_actuator_halt() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T09:03:03.240858+00:00",
            "sensors": [],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {
                "fills_total": 1,
                "halted": True,
                "halt_reason": "daily_loss_limit",
                "halt_triggered_at": "2026-05-05T00:00:00+00:00",
            },
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={},
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
        strategies={
            "strategies": [
                {"strategy_id": "default", "active_version_id": "default-v2"}
            ]
        },
    )

    assert (
        "actuator",
        "active_halt",
        "daily_loss_limit since 2026-05-05T00:00:00+00:00",
    ) in metrics.risk_events


def test_metrics_from_api_payloads_derives_report_day_pnl_from_pnl_series() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": [
                {
                    "recorded_at": "2026-05-29T23:59:00+00:00",
                    "pnl": 5.0,
                },
                {
                    "recorded_at": "2026-05-30T12:00:00+00:00",
                    "pnl": 2.5,
                },
                {
                    "recorded_at": "2026-05-31T00:00:00+00:00",
                    "pnl": 999.0,
                },
            ],
        },
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.todays_pnl == pytest.approx(-2.5)
    assert (
        "report generation",
        "daily P&L evidence missing",
        "metrics.pnl_series is required",
    ) not in metrics.risk_events


def test_metrics_from_api_payloads_uses_pnl_series_for_cumulative_pnl() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "pnl_series": [
                {
                    "recorded_at": "2026-05-29T23:59:00+00:00",
                    "pnl": 5.0,
                },
                {
                    "recorded_at": "2026-05-30T12:00:00+00:00",
                    "pnl": 7.5,
                },
                {
                    "recorded_at": "2026-05-31T00:00:00+00:00",
                    "pnl": 999.0,
                },
            ],
        },
        decisions=[],
        trades={"trades": []},
        positions={"positions": [{"locked_usdc": 10.0, "unrealized_pnl": 99.0}]},
    )

    assert metrics.cumulative_pnl == pytest.approx(7.5)


def test_metrics_from_api_payloads_deducts_llm_cost_from_pnl() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 0},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "pnl_series": [
                {
                    "recorded_at": "2026-05-30T12:00:00+00:00",
                    "pnl": 2.0,
                },
            ],
            LLM_DAILY_COST_USDC_METRIC: 0.75,
            LLM_ESTIMATED_COST_USDC_TOTAL_METRIC: 1.25,
        },
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.llm_cost_usdc == pytest.approx(1.25)
    assert metrics.todays_pnl == pytest.approx(1.25)
    assert metrics.cumulative_pnl == pytest.approx(0.75)


def test_metrics_from_api_payloads_rejects_process_global_llm_cost_for_window() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 0},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "window_started_at": "2026-04-30T00:00:00+00:00",
            "window_ended_at": "2026-05-31T00:00:00+00:00",
            "pnl_series": [
                {
                    "recorded_at": "2026-05-30T12:00:00+00:00",
                    "pnl": 2.0,
                },
            ],
            LLM_DAILY_COST_USDC_METRIC: 0.75,
            LLM_ESTIMATED_COST_USDC_TOTAL_METRIC: 1.25,
        },
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.llm_cost_usdc == pytest.approx(0.0)
    assert metrics.todays_pnl == pytest.approx(1.25)
    assert metrics.cumulative_pnl == pytest.approx(2.0)
    assert (
        "llm",
        "windowed LLM cost evidence missing",
        "pms_llm_estimated_cost_usdc_total=1.2500 is process-global and cannot "
        "be deducted from a paper-soak window",
    ) in metrics.risk_events


def test_metrics_from_api_payloads_flags_llm_budget_exhaustion() -> None:
    report_date = date(2026, 5, 30)

    metrics = metrics_from_api_payloads(
        report_date=report_date,
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 0},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "pnl_series": _pnl_series(report_date, pnl=0.0),
            LLM_BUDGET_EXHAUSTED_TOTAL_METRIC: 2.0,
            LLM_DAILY_COST_USDC_METRIC: 0.05,
            LLM_DAILY_COST_LIMIT_USDC_METRIC: 0.05,
        },
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.llm_budget_exhaustions == 2
    assert (
        "llm",
        "daily LLM budget exhausted",
        "2 exhaustion(s); daily_cost=0.0500, limit=0.0500; "
        "forecasts are falling back to non-LLM branches",
    ) in metrics.risk_events


def test_metrics_from_api_payloads_falls_back_to_quote_mtm_pnl_series() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "pnl_series": [],
            "max_drawdown_pct": 0.0,
            "quote_calibration": {
                "max_drawdown_pct": 3.5,
                "pnl_series": [
                    {
                        "recorded_at": "2026-05-29T23:59:00+00:00",
                        "pnl": 4.0,
                        "source": "quote_mtm",
                    },
                    {
                        "recorded_at": "2026-05-30T12:00:00+00:00",
                        "pnl": 1.25,
                        "source": "quote_mtm",
                    },
                    {
                        "recorded_at": "2026-05-31T00:00:00+00:00",
                        "pnl": 999.0,
                        "source": "quote_mtm",
                    },
                ]
            },
        },
        decisions=[],
        trades={"trades": []},
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.25},
                {"locked_usdc": 5.0, "unrealized_pnl": -0.25},
            ]
        },
    )

    assert metrics.todays_pnl == pytest.approx(-2.75)
    assert metrics.cumulative_pnl == pytest.approx(1.25)
    assert metrics.current_unrealized_pnl == pytest.approx(2.0)
    assert metrics.pnl_source == "quote_mtm"
    assert metrics.max_drawdown_pct == pytest.approx(3.5)
    assert (
        "report generation",
        "daily P&L evidence missing",
        "metrics.pnl_series is empty",
    ) not in metrics.risk_events


def test_paper_report_renders_current_open_position_mtm_separately_from_gate_pnl() -> None:
    report = render_report(
        PaperReportMetrics(
            report_date=date(2026, 5, 30),
            todays_pnl=-2.75,
            cumulative_pnl=1.25,
            pnl_source="quote_mtm",
            current_unrealized_pnl=2.0,
        ),
        risk=RiskSettings(),
    )

    assert "| Cumulative P&L | +$1.25 | > 0 by soak end |" in report
    assert "| P&L source | quote_mtm | - |" in report
    assert "| Current open-position MTM | +$2.00 | informational |" in report


def test_metrics_from_api_payloads_prefers_final_pnl_series_over_quote_mtm() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "pnl_series": [
                {
                    "recorded_at": "2026-05-30T12:00:00+00:00",
                    "pnl": 7.5,
                }
            ],
            "quote_calibration": {
                "pnl_series": [
                    {
                        "recorded_at": "2026-05-30T12:00:00+00:00",
                        "pnl": -9.0,
                        "source": "quote_mtm",
                    }
                ]
            },
        },
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.todays_pnl == pytest.approx(7.5)
    assert metrics.cumulative_pnl == pytest.approx(7.5)
    assert metrics.pnl_source == "final_eval"


def test_metrics_from_api_payloads_counts_downstream_statuses_as_accepted() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 8,
                "diagnostics_total": 5,
                "diagnostic_counts": {"decision_net_edge_not_positive": 5},
            },
            "actuator": {"fills_total": 6},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"pnl_series": _pnl_series(date(2026, 5, 30), pnl=1.0)},
        decisions=[
            {
                "decision_id": "decision-accepted",
                "status": "accepted",
                "created_at": "2026-05-30T01:00:00+00:00",
            },
            {
                "decision_id": "decision-queued",
                "status": "queued",
                "created_at": "2026-05-30T02:00:00+00:00",
            },
            {
                "decision_id": "decision-submitted",
                "status": "submitted",
                "created_at": "2026-05-30T03:00:00+00:00",
            },
            {
                "decision_id": "decision-partial",
                "status": "partially_filled",
                "created_at": "2026-05-30T04:00:00+00:00",
            },
            {
                "decision_id": "decision-filled",
                "status": "filled",
                "created_at": "2026-05-30T05:00:00+00:00",
            },
            {
                "decision_id": "decision-matched",
                "status": "matched",
                "created_at": "2026-05-30T06:00:00+00:00",
            },
            {
                "decision_id": "decision-open",
                "status": "pending",
                "created_at": "2026-05-30T07:00:00+00:00",
            },
            {
                "decision_id": "exit-stop_loss-filled",
                "status": "filled",
                "created_at": "2026-05-30T08:00:00+00:00",
            },
        ],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.decisions_accepted == 6


def test_metrics_from_api_payloads_counts_entry_fills_for_sample_gate() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 3,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 3},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"pnl_series": _pnl_series(date(2026, 5, 30), pnl=1.0)},
        decisions=[
            {
                "decision_id": "decision-entry",
                "created_at": "2026-05-30T01:00:00+00:00",
            },
            {
                "decision_id": "exit-stop_loss-1",
                "created_at": "2026-05-30T02:00:00+00:00",
            },
            {
                "decision_id": "exit-profit_take-2",
                "created_at": "2026-05-30T03:00:00+00:00",
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": "trade-entry",
                    "decision_id": "decision-entry",
                    "filled_at": "2026-05-30T01:00:01+00:00",
                },
                {
                    "trade_id": "trade-exit-stop",
                    "decision_id": "exit-stop_loss-1",
                    "filled_at": "2026-05-30T02:00:01+00:00",
                },
                {
                    "trade_id": "trade-exit-profit",
                    "decision_id": "exit-profit_take-2",
                    "filled_at": "2026-05-30T03:00:01+00:00",
                },
            ]
        },
        positions={"positions": []},
    )

    assert metrics.fills == 1
    assert metrics.decisions_accepted == 1


def test_metrics_from_api_payloads_flags_concentrated_entry_fill_sample() -> None:
    report_date = date(2026, 5, 30)
    metrics = metrics_from_api_payloads(
        report_date=report_date,
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 50,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 50},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "pnl_series": _pnl_series(report_date, pnl=1.0),
            **_baseline_score_metrics(),
        },
        decisions=[
            {
                "decision_id": f"decision-entry-{index}",
                "market_id": "market-concentrated",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 10,
                "decision_evidence": _decision_evidence(),
                "created_at": "2026-05-30T01:00:00+00:00",
            }
            for index in range(50)
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-entry-{index}",
                    "decision_id": f"decision-entry-{index}",
                    "market_id": "market-concentrated",
                    "risk_group_id": "event:concentrated",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T01:00:01+00:00",
                }
                for index in range(50)
            ]
        },
        positions={"positions": []},
    )

    assert metrics.execution_concentration == ExecutionConcentration(
        entry_fills=50,
        distinct_markets=1,
        distinct_risk_groups=1,
        missing_risk_group_fills=0,
        max_market_fill_share=1.0,
        max_risk_group_fill_share=1.0,
    )
    assert (
        "report generation",
        "execution market concentration too high",
        "distinct_markets=1 < 3",
    ) in metrics.risk_events
    assert (
        "report generation",
        "execution risk group concentration too high",
        "distinct_risk_groups=1 < 3",
    ) in metrics.risk_events
    assert evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(max_total_exposure=50.0),
    ).require_check("max_risk_group_fill_share").ok is False


def test_paper_report_renders_pnl_source() -> None:
    metrics = _passing_gate_metrics(risk_events=())

    report = render_report(
        metrics,
        risk=RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0),
    )

    assert "| P&L source | final_eval | -" in report


def test_metrics_from_api_payloads_flags_unprofitable_active_strategy_metrics() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"pnl_series": _pnl_series(date(2026, 5, 30), pnl=10.0)},
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
        strategies={
            "strategies": [
                {"strategy_id": "winner", "active_version_id": "winner-v1"},
                {"strategy_id": "loser", "active_version_id": "loser-v1"},
            ]
        },
        strategy_metrics={
            "strategies": [
                {
                    "strategy_id": "winner",
                    "strategy_version_id": "winner-v1",
                    "record_count": 4,
                    "insufficient_samples": False,
                    "pnl": 2.0,
                    "fill_rate": 0.5,
                    "brier_improvement_overall": 0.01,
                },
                {
                    "strategy_id": "loser",
                    "strategy_version_id": "loser-v1",
                    "record_count": 4,
                    "insufficient_samples": False,
                    "pnl": -1.0,
                    "fill_rate": 0.5,
                    "brier_improvement_overall": 0.01,
                },
            ]
        },
    )

    assert (
        "strategy",
        "active strategy pnl not positive",
        "loser@loser-v1 pnl=-1.0000",
    ) in metrics.risk_events


def test_insufficient_strategy_samples_do_not_emit_derived_zero_metric_events() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"pnl_series": _pnl_series(date(2026, 5, 30), pnl=10.0)},
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
        strategies={
            "strategies": [
                {"strategy_id": "new", "active_version_id": "new-v1"},
            ]
        },
        strategy_metrics={
            "strategies": [
                {
                    "strategy_id": "new",
                    "strategy_version_id": "new-v1",
                    "record_count": 0,
                    "insufficient_samples": True,
                    "pnl": 0.0,
                    "fill_rate": 0.0,
                    "brier_improvement_overall": 0.0,
                },
            ]
        },
    )

    assert (
        "strategy",
        "active strategy samples insufficient",
        "new@new-v1 record_count=0",
    ) in metrics.risk_events
    assert not any(
        event[0] == "strategy"
        and event[1]
        in {
            "active strategy pnl not positive",
            "active strategy fill rate not positive",
            "active strategy brier improvement not positive",
        }
        for event in metrics.risk_events
    )


def test_insufficient_strategy_samples_still_flag_negative_quote_mtm_pnl() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 2},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"pnl_series": _pnl_series(date(2026, 5, 30), pnl=10.0)},
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
        strategies={
            "strategies": [
                {"strategy_id": "new", "active_version_id": "new-v1"},
            ]
        },
        strategy_metrics={
            "strategies": [
                {
                    "strategy_id": "new",
                    "strategy_version_id": "new-v1",
                    "record_count": 0,
                    "insufficient_samples": True,
                    "pnl": -0.25,
                    "pnl_source": "quote_mtm",
                    "fill_rate": 1.0,
                    "brier_improvement_overall": None,
                    "quote_record_count": 2,
                    "quote_mtm_pnl": -0.25,
                },
            ]
        },
    )

    assert (
        "strategy",
        "active strategy samples insufficient",
        "new@new-v1 record_count=0",
    ) in metrics.risk_events
    assert (
        "strategy",
        "active strategy quote-mtm pnl not positive",
        "new@new-v1 quote_mtm_pnl=-0.2500",
    ) in metrics.risk_events
    assert not any(
        event[0] == "strategy"
        and event[1] == "active strategy pnl not positive"
        for event in metrics.risk_events
    )


def test_metrics_from_api_payloads_reads_max_drawdown_pct_from_metrics_payload() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "max_drawdown_pct": 12.5,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
        },
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.max_drawdown_pct == pytest.approx(12.5)


def test_metrics_from_api_payloads_reads_sharpe_ratio_from_metrics_payload() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "max_drawdown_pct": 12.5,
            "sharpe_ratio": 1.5,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
        },
        decisions=[],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.sharpe_ratio == pytest.approx(1.5)


def test_paper_soak_gate_fails_when_rejection_reason_evidence_is_missing() -> None:
    metrics = _passing_metrics_from_api_payloads(
        controller={
            "decisions_total": 20,
            "diagnostics_total": 1,
            "diagnostic_counts": None,
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert (
        "report generation",
        "controller rejection evidence missing",
        "status.controller.diagnostic_counts is required when diagnostics_total > 0",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_rejection_reason_evidence_is_incomplete() -> None:
    metrics = _passing_metrics_from_api_payloads(
        controller={
            "decisions_total": 20,
            "diagnostics_total": 3,
            "diagnostic_counts": {"missing_no_token": 1},
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert (
        "report generation",
        "controller rejection evidence incomplete",
        "status.controller.diagnostic_counts sum 1 is less than diagnostics_total 3",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_rejection_reason_evidence_is_overcounted() -> None:
    metrics = _passing_metrics_from_api_payloads(
        controller={
            "decisions_total": 20,
            "diagnostics_total": 1,
            "diagnostic_counts": {"missing_no_token": 2},
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert (
        "report generation",
        "controller rejection evidence inconsistent",
        "status.controller.diagnostic_counts sum 2 does not match diagnostics_total 1",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_rejection_reason_key_is_blank() -> None:
    metrics = _passing_metrics_from_api_payloads(
        controller={
            "decisions_total": 20,
            "diagnostics_total": 1,
            "diagnostic_counts": {"": 1},
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert (
        "report generation",
        "controller rejection evidence malformed",
        "status.controller.diagnostic_counts keys must be non-empty strings",
    ) in metrics.risk_events
    assert metrics.rejection_reasons == ()
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_metrics_from_api_payloads_surfaces_live_clamp_rejection_summary() -> None:
    metrics = _passing_metrics_from_api_payloads(
        controller={
            "decisions_total": 20,
            "diagnostics_total": 37,
            "diagnostic_counts": {"calibration_clamp_rejected": 37},
        },
    )

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert metrics.rejection_reasons == (("calibration_clamp_rejected", 37),)
    assert metrics.clamp_rejections == (("aggregate", 37),)
    assert "| calibration_clamp_rejected | 37 |" in report
    assert "| aggregate | 37 |" in report
    assert "No clamp rejections recorded." not in report


def test_metrics_from_api_payloads_surfaces_live_selection_funnel() -> None:
    metrics = _passing_metrics_from_api_payloads(
        controller={
            "decisions_total": 20,
            "diagnostics_total": 3,
            "diagnostic_counts": {"decision_edge_not_positive": 3},
        },
        metrics_override={
            SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC: 120.0,
            SELECTION_FUNNEL_SELECTED_TOTAL_METRIC: 40.0,
            SELECTION_FUNNEL_ROUTED_TOTAL_METRIC: 23.0,
            SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC: 11.0,
            SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC: 4.0,
            SELECTION_FUNNEL_TRADED_TOTAL_METRIC: 3.0,
        },
    )

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert metrics.selection_funnel is not None
    assert metrics.selection_funnel.discovered == 120
    assert metrics.selection_funnel.selected == 40
    assert metrics.selection_funnel.routed == 23
    assert metrics.selection_funnel.forecasted == 11
    assert metrics.selection_funnel.controller_emitted == 4
    assert metrics.selection_funnel.traded == 3
    assert "| Discovered | 120 |" in report
    assert "| Forecasted | 11 |" in report
    assert "| Controller Emitted | 4 |" in report
    assert "| Traded | 3 |" in report
    assert "No funnel events recorded." not in report


def test_metrics_from_api_payloads_flags_inconsistent_selection_funnel() -> None:
    metrics = _passing_metrics_from_api_payloads(
        metrics_override={
            SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC: 120.0,
            SELECTION_FUNNEL_SELECTED_TOTAL_METRIC: 40.0,
            SELECTION_FUNNEL_ROUTED_TOTAL_METRIC: 23.0,
            SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC: 11.0,
            SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC: 5.0,
            SELECTION_FUNNEL_TRADED_TOTAL_METRIC: 6.0,
        },
    )

    assert metrics.selection_funnel is not None
    assert metrics.selection_funnel.traded == 6
    assert (
        "selection_funnel",
        "selection funnel stage count inverted",
        "traded=6 > controller_emitted=5",
    ) in metrics.risk_events


def test_paper_soak_gate_fails_without_persisted_runtime_continuity_evidence() -> None:
    metrics = _passing_metrics_from_api_payloads(include_runtime_continuity=False)

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert (
        "report generation",
        "runtime continuity evidence missing",
        "status.runtime_continuity from postgres_runtime_heartbeats is required once soak_days >= 30",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_current_controller_runtime_is_unavailable() -> None:
    metrics = _passing_metrics_from_api_payloads(
        controller={"current_runtimes_total": 0}
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert (
        "controller",
        "controller runtime unavailable",
        "status.controller.current_runtimes_total=0",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_runtime_heartbeat_observed_detachment() -> None:
    continuity = _runtime_continuity_status()
    continuity["unhealthy_heartbeat_count"] = 1
    continuity["min_controller_runtimes"] = 0
    metrics = _passing_metrics_from_api_payloads(runtime_continuity=continuity)

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert (
        "runtime",
        "unhealthy runtime heartbeats",
        "unhealthy_heartbeat_count=1",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_daily_pnl_evidence_is_missing() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            **_baseline_score_metrics(),
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
                "decision_evidence": _decision_evidence(),
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "market_id": f"m-paper-soak-{index % 4}",
                    "risk_group_id": f"event:paper-soak-{index % 3}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert (
        "report generation",
        "daily P&L evidence missing",
        "metrics.pnl_series is required",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_metrics_from_api_payloads_summarizes_secondary_baseline_evidence() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T00:00:00+00:00",
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-05T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 2,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 1},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 5)),
            "baseline_brier_by_source": {
                "market_implied": 0.24,
                "mid_quote": 0.23,
                "last_trade": 0.22,
                "category_prior": 0.21,
            },
            "brier_improvement_by_source": {
                "market_implied": 0.08,
                "mid_quote": 0.07,
                "last_trade": 0.06,
                "category_prior": 0.05,
            },
        },
        decisions=[
            {
                "decision_id": "d-baseline-1",
                "market_id": "m-baseline-1",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
                "decision_evidence": {
                    "market_implied_baseline_prob_estimate": 0.60,
                    "mid_quote_baseline_prob_estimate": 0.59,
                    "last_trade_baseline_prob_estimate": 0.58,
                    "category_prior_baseline_prob_estimate": 0.55,
                },
            },
            {
                "decision_id": "d-baseline-2",
                "market_id": "m-baseline-2",
                "expected_edge": 0.04,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
                "decision_evidence": {
                    "market_implied_baseline_prob_estimate": 0.42,
                    "mid_quote_baseline_prob_estimate": 0.43,
                },
            },
        ],
        trades={
            "trades": [
                {
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-05T00:00:00+00:00",
                }
            ],
        },
        positions={"positions": []},
    )

    assert metrics.baseline_evidence is not None
    assert metrics.baseline_evidence.decisions == 2
    assert metrics.baseline_evidence.market_implied_count == 2
    assert metrics.baseline_evidence.mid_quote_count == 2
    assert metrics.baseline_evidence.last_trade_count == 1
    assert metrics.baseline_evidence.category_prior_count == 1
    assert metrics.risk_events == ()

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert "## Baseline Evidence Coverage" in report
    assert "| market_implied | 2 / 2 | 100.0% |" in report
    assert "| mid_quote | 2 / 2 | 100.0% |" in report
    assert "| last_trade | 1 / 2 | 50.0% |" in report
    assert "| category_prior | 1 / 2 | 50.0% |" in report


def test_baseline_evidence_coverage_uses_reported_decision_denominator() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T00:00:00+00:00",
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-05T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 2,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 1},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 5)),
            "baseline_brier_by_source": {
                "market_implied": 0.24,
                "mid_quote": 0.23,
            },
            "brier_improvement_by_source": {
                "market_implied": 0.08,
                "mid_quote": 0.07,
            },
        },
        decisions=[
            {
                "decision_id": "d-with-evidence",
                "market_id": "m-with-evidence",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
                "decision_evidence": {
                    "market_implied_baseline_prob_estimate": 0.60,
                    "mid_quote_baseline_prob_estimate": 0.59,
                },
            },
            {
                "decision_id": "d-without-evidence",
                "market_id": "m-without-evidence",
                "expected_edge": 0.04,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
            },
        ],
        trades={
            "trades": [
                {
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-05T00:00:00+00:00",
                }
            ],
        },
        positions={"positions": []},
    )

    assert metrics.baseline_evidence is not None
    assert metrics.baseline_evidence.decisions == 2
    assert metrics.baseline_evidence.market_implied_count == 1
    assert metrics.baseline_evidence.mid_quote_count == 1
    assert (
        "report generation",
        "secondary baseline evidence incomplete",
        "1 reported decision(s) lack decision_evidence",
    ) in metrics.risk_events

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert "| market_implied | 1 / 2 | 50.0% |" in report
    assert "| mid_quote | 1 / 2 | 50.0% |" in report


def test_baseline_evidence_coverage_excludes_position_exit_decisions() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T00:00:00+00:00",
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-05T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 2,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 2},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 1.0,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 5)),
            "baseline_brier_by_source": {
                "market_implied": 0.24,
                "mid_quote": 0.23,
            },
            "brier_improvement_by_source": {
                "market_implied": 0.08,
                "mid_quote": 0.07,
            },
        },
        decisions=[
            {
                "decision_id": "entry-with-evidence",
                "market_id": "m-entry",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
                "decision_evidence": {
                    "market_implied_baseline_prob_estimate": 0.60,
                    "mid_quote_baseline_prob_estimate": 0.59,
                },
            },
            {
                "decision_id": "exit-stop_loss-no-evidence",
                "market_id": "m-entry",
                "expected_edge": 0.0,
                "spread_bps_at_decision": None,
                "created_at": "2026-05-05T00:01:00+00:00",
            },
        ],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.baseline_evidence is not None
    assert metrics.baseline_evidence.decisions == 1
    assert metrics.baseline_evidence.market_implied_count == 1
    assert metrics.baseline_evidence.mid_quote_count == 1
    assert not any(
        event[1] == "secondary baseline evidence incomplete"
        for event in metrics.risk_events
    )


def test_paper_soak_gate_fails_when_reported_decisions_lack_baseline_evidence() -> None:
    metrics = _passing_metrics_from_api_payloads(include_decision_evidence=False)

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.baseline_evidence is not None
    assert metrics.baseline_evidence.decisions == 1
    assert metrics.baseline_evidence.market_implied_count == 0
    assert metrics.baseline_evidence.mid_quote_count == 0
    assert (
        "report generation",
        "secondary baseline evidence incomplete",
        "1 reported decision(s) lack decision_evidence",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_decision_payload_is_incomplete() -> None:
    metrics = _passing_metrics_from_api_payloads(
        controller={
            "decisions_total": 2,
            "diagnostics_total": 0,
            "diagnostic_counts": {},
        },
        decision_payload_count=1,
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.decisions_made == 1
    assert metrics.baseline_evidence is not None
    assert metrics.baseline_evidence.decisions == 1
    assert (
        "report generation",
        "decision payload incomplete",
        "/decisions returned 1 row(s), but /status.controller.decisions_total reports 2",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_metrics_from_api_payloads_uses_durable_decision_rows_for_decisions_made() -> None:
    metrics = _passing_metrics_from_api_payloads(
        controller={
            "decisions_total": 1,
            "diagnostics_total": 0,
            "diagnostic_counts": {},
        },
        decision_payload_count=3,
    )

    assert metrics.decisions_made == 3
    assert not any(
        trigger == "decision payload incomplete"
        for _event_time, trigger, _status in metrics.risk_events
    )


def test_metrics_from_api_payloads_surfaces_secondary_baseline_scores() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T00:00:00+00:00",
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-05T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 2,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 1},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "baseline_brier_by_source": {
                "market_implied": 0.24,
                "mid_quote": 0.22,
                "last_trade": 0.23,
            },
            "brier_improvement_by_source": {
                "market_implied": 0.08,
                "mid_quote": 0.06,
                "last_trade": 0.07,
            },
        },
        decisions=[
            {
                "decision_id": "d-baseline-1",
                "market_id": "m-baseline-1",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
            },
        ],
        trades={
            "trades": [
                {
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-05T00:00:00+00:00",
                }
            ],
        },
        positions={"positions": []},
    )

    assert metrics.baseline_brier_by_source == {
        "market_implied": 0.24,
        "mid_quote": 0.22,
        "last_trade": 0.23,
    }
    assert metrics.brier_improvement_by_source == {
        "market_implied": 0.08,
        "mid_quote": 0.06,
        "last_trade": 0.07,
    }

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert "## Secondary Baseline Brier" in report
    assert "| market_implied | 0.2400 | 0.0800 |" in report
    assert "| mid_quote | 0.2200 | 0.0600 |" in report
    assert "| last_trade | 0.2300 | 0.0700 |" in report


def test_paper_soak_gate_fails_when_secondary_baseline_does_not_improve() -> None:
    metrics = PaperReportMetrics(
        report_date=date(2026, 5, 30),
        strategy="default@default-v2",
        day_of_soak=30,
        fills=10,
        fill_rate=0.4,
        average_slippage_bps=10.0,
        cumulative_pnl=2.0,
        max_drawdown_pct=5.0,
        open_positions=2,
        total_exposure=10.0,
        brier_score_7d=0.18,
        baseline_brier_score_7d=0.23,
        brier_improvement_7d=0.05,
        hit_rate=0.5,
        average_edge_bps=20.0,
        average_fee_bps=2.0,
        average_net_edge_bps=5.0,
        baseline_brier_by_source={"mid_quote": 0.17},
        brier_improvement_by_source={"mid_quote": -0.01},
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert gate.ok is False
    assert gate.require_check("secondary_brier_improvement:mid_quote").detail == (
        "-0.0100 <= 0.0000"
    )


def test_metrics_from_api_payloads_flags_invalid_secondary_baseline_scores() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T00:00:00+00:00",
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-05T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 2,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 1},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "baseline_brier_by_source": {
                "mid_quote": "nan",
                "last_trade": 0.23,
            },
            "brier_improvement_by_source": {
                "mid_quote": 0.06,
            },
        },
        decisions=[
            {
                "decision_id": "d-baseline-1",
                "market_id": "m-baseline-1",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
            },
        ],
        trades={
            "trades": [
                {
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-05T00:00:00+00:00",
                }
            ],
        },
        positions={"positions": []},
    )

    assert metrics.baseline_brier_by_source == {"last_trade": 0.23}
    assert (
        "report generation",
        "non-finite numeric evidence",
        "metrics.baseline_brier_by_source.mid_quote must be finite",
    ) in metrics.risk_events
    assert (
        "report generation",
        "secondary baseline score incomplete",
        "last_trade baseline_brier_by_source lacks brier_improvement_by_source",
    ) in metrics.risk_events
    assert (
        "report generation",
        "secondary baseline score incomplete",
        "mid_quote brier_improvement_by_source lacks baseline_brier_by_source",
    ) in metrics.risk_events


def test_paper_soak_gate_fails_when_secondary_baseline_source_label_is_placeholder() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "max_drawdown_pct": 12.5,
            "sharpe_ratio": 1.5,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
            "baseline_brier_by_source": {
                "placeholder_baseline": 0.24,
                "Market Price": 0.25,
            },
            "brier_improvement_by_source": {
                "placeholder_baseline": 0.08,
                "Market Price": 0.07,
            },
        },
        decisions=[],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
        strategies={
            "strategies": [
                {
                    "strategy_id": "default",
                    "active_version_id": "default-v1",
                }
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.baseline_brier_by_source == {}
    assert metrics.brier_improvement_by_source == {}
    assert (
        "report generation",
        "secondary baseline source invalid",
        "metrics.baseline_brier_by_source source must be concrete lowercase snake_case: placeholder_baseline",
    ) in metrics.risk_events
    assert (
        "report generation",
        "secondary baseline source invalid",
        "metrics.brier_improvement_by_source source must be concrete lowercase snake_case: Market Price",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "4 risk event(s)"


def test_metrics_from_api_payloads_flags_missing_required_baseline_evidence() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T00:00:00+00:00",
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-05T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 0},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"fill_rate": 0.5, "win_rate": 0.5, "slippage_bps": 10.0},
        decisions=[
            {
                "decision_id": "d-baseline-missing",
                "market_id": "m-baseline-missing",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-05T00:00:00+00:00",
                "decision_evidence": {
                    "market_implied_baseline_prob_estimate": 0.60,
                },
            },
        ],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert metrics.baseline_evidence is not None
    assert metrics.baseline_evidence.mid_quote_count == 0
    assert (
        "report generation",
        "secondary baseline evidence incomplete",
        "1 decision(s) with decision_evidence lack mid_quote_baseline_prob_estimate",
    ) in metrics.risk_events


def test_secondary_baseline_scores_are_not_due_before_resolved_eval_samples() -> None:
    report_date = date(2026, 5, 30)
    metrics = metrics_from_api_payloads(
        report_date=report_date,
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 0},
            "evaluator": {
                "eval_records_total": 0,
                "brier_14d": None,
                "baseline_brier_14d": None,
                "brier_improvement_14d": None,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"pnl_series": _pnl_series(report_date)},
        decisions=[
            {
                "decision_id": "d-no-resolved-eval",
                "market_id": "m-no-resolved-eval",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
                "decision_evidence": {
                    "market_implied_baseline_prob_estimate": 0.60,
                    "mid_quote_baseline_prob_estimate": 0.59,
                },
            },
        ],
        trades={"trades": []},
        positions={"positions": []},
    )

    assert not any(
        trigger == "secondary baseline score incomplete"
        for _event_time, trigger, _status in metrics.risk_events
    )


def test_paper_soak_gate_fails_when_covered_secondary_baseline_lacks_score() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "max_drawdown_pct": 12.5,
            "sharpe_ratio": 1.5,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
            "baseline_brier_by_source": {
                "market_implied": 0.24,
                "mid_quote": 0.23,
            },
            "brier_improvement_by_source": {
                "market_implied": 0.08,
                "mid_quote": 0.07,
            },
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
                "decision_evidence": {
                    "market_implied_baseline_prob_estimate": 0.60,
                    "mid_quote_baseline_prob_estimate": 0.59,
                    "category_prior_baseline_prob_estimate": 0.55,
                },
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
        strategies={
            "strategies": [
                {
                    "strategy_id": "default",
                    "active_version_id": "default-v1",
                }
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.baseline_evidence is not None
    assert metrics.baseline_evidence.category_prior_count == 1
    assert (
        "report generation",
        "secondary baseline score incomplete",
        "category_prior decision-time evidence lacks baseline_brier_by_source",
    ) in metrics.risk_events
    assert (
        "report generation",
        "secondary baseline score incomplete",
        "category_prior decision-time evidence lacks brier_improvement_by_source",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "2 risk event(s)"


def test_metrics_from_api_payloads_prefers_active_strategy_versions_over_status_label() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "strategy": "default",
            "runner_started_at": "2026-05-03T09:03:03.240858+00:00",
        },
        trades={"trades": []},
        positions={"positions": []},
        strategies={
            "strategies": [
                {
                    "strategy_id": "default",
                    "active_version_id": "default-v2",
                },
                {
                    "strategy_id": "h1-flb",
                    "active_version_id": "h1-flb-v7",
                },
            ],
        },
    )

    assert metrics.strategy == "default@default-v2, h1-flb@h1-flb-v7"


def test_metrics_from_api_payloads_ignores_inactive_strategy_rows_for_label() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-03T09:03:03.240858+00:00",
        },
        trades={"trades": []},
        positions={"positions": []},
        strategies={
            "strategies": [
                {"strategy_id": "default", "active_version_id": None},
                {
                    "strategy_id": "paper_multi_factor_v1",
                    "active_version_id": "paper-v1",
                },
            ],
        },
    )

    assert metrics.strategy == "paper_multi_factor_v1@paper-v1"


def test_metrics_from_api_payloads_derives_execution_fill_rate_from_window_rows() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-05T00:00:00+00:00",
            "controller": {
                "decisions_total": 4,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 2},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"fill_rate": 0.0},
        decisions=[
            {"decision_id": f"d-{index}", "created_at": "2026-05-05T00:00:00+00:00"}
            for index in range(4)
        ],
        trades={
            "trades": [
                {"trade_id": f"t-{index}", "filled_at": "2026-05-05T00:00:00+00:00"}
                for index in range(2)
            ]
        },
        positions={"positions": []},
    )

    assert metrics.fill_rate == pytest.approx(0.5)


def test_metrics_from_api_payloads_uses_decimal_internals_for_position_pnl() -> None:
    report_date = date(2026, 5, 30)

    metrics = metrics_from_api_payloads(
        report_date=report_date,
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-29T00:00:00+00:00",
            "controller": {"decisions_total": 0, "diagnostics_total": 0},
            "actuator": {"fills_total": 0},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"pnl_series": _pnl_series(report_date, pnl=0.0)},
        trades={"trades": []},
        positions={
            "positions": [
                {"locked_usdc": 1.0, "unrealized_pnl": 0.1},
                {"locked_usdc": 1.0, "unrealized_pnl": 0.2},
            ],
        },
    )

    assert metrics.current_unrealized_pnl == 0.3


def test_metrics_from_api_payloads_surfaces_position_mark_sources() -> None:
    report_date = date(2026, 5, 30)

    metrics = metrics_from_api_payloads(
        report_date=report_date,
        status={
            "mode": "paper",
            "runner_started_at": "2026-05-29T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 0},
            "evaluator": {},
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={"pnl_series": _pnl_series(report_date, pnl=0.0)},
        trades={"trades": []},
        positions={
            "positions": [
                {"locked_usdc": 1.0, "unrealized_pnl": 0.1, "mark_source": "clob"},
                {"locked_usdc": 1.0, "unrealized_pnl": 0.2, "mark_source": "gamma"},
                {"locked_usdc": 1.0, "unrealized_pnl": 0.3},
            ],
        },
    )

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert metrics.position_mark_sources == (
        ("clob", 1),
        ("gamma", 1),
        ("unknown", 1),
    )
    assert (
        "positions",
        "open-position MTM uses non-CLOB mark source",
        "gamma=1, unknown=1; current open-position MTM is informational",
    ) in metrics.risk_events
    assert "## Position Mark Sources" in report
    assert "| clob | 1 |" in report
    assert "| gamma | 1 |" in report
    assert "| unknown | 1 |" in report


def test_paper_soak_gate_fails_when_strategies_endpoint_has_no_active_versions() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "strategy": "default@legacy-v1",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
            **_baseline_score_metrics(),
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
                "decision_evidence": _decision_evidence(),
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
        strategies={"strategies": []},
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.strategy == "default@legacy-v1"
    assert (
        "report generation",
        "/strategies active version evidence missing",
        "paper-soak GO reports require active strategy_id@strategy_version_id rows",
    ) in metrics.risk_events
    assert gate.require_check("strategy_evidence").ok is True
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_fetch_api_payload_rejects_duplicate_json_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DuplicateKeyResponse:
        def __enter__(self) -> DuplicateKeyResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"mode": "paper", "mode": "live"}'

    def fake_urlopen(request: object, timeout: float) -> DuplicateKeyResponse:
        assert timeout == 5.0
        return DuplicateKeyResponse()

    monkeypatch.setattr("scripts.paper_report.urlopen", fake_urlopen)

    payload, error = _fetch_api_payload(
        api_base_url="http://api.test",
        path="/status",
        api_token=None,
    )

    assert payload == {}
    assert error == "duplicate JSON key: mode"


def test_load_live_metrics_fetches_metrics_for_soak_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetched_json_paths: list[str] = []
    paginated_until_values: list[str] = []

    def fake_fetch_api_json(
        *,
        api_base_url: str,
        path: str,
        api_token: str | None,
    ) -> tuple[dict[str, object], str | None]:
        assert api_base_url == "http://api.test"
        assert api_token == "token"
        fetched_json_paths.append(path)
        if path == "/status":
            return (
                {
                    "mode": "paper",
                    "runner_started_at": "2026-04-30T00:00:00+00:00",
                    "runtime_continuity": _runtime_continuity_status(),
                    "sensors": [
                        {
                            "name": "MarketDataSensor",
                            "status": "running",
                            "last_signal_at": "2026-05-30T00:00:00+00:00",
                        }
                    ],
                    "controller": {
                        "decisions_total": 20,
                        "diagnostics_total": 0,
                        "diagnostic_counts": {},
                    },
                    "actuator": {"fills_total": 10},
                    "evaluator": {
                        "brier_14d": 0.16,
                        "baseline_brier_14d": 0.24,
                        "brier_improvement_14d": 0.08,
                    },
                    "supervision": {"unresolved_feedback_total": 0},
                },
                None,
            )
        if path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "locked_usdc": 10.0,
                            "unrealized_pnl": 2.0,
                            "mark_source": "clob",
                        }
                    ]
                },
                None,
            )
        if path == "/readiness":
            return (
                {
                    "status": "ready",
                    "checks": {
                        "sensors": "ready",
                        "event_loop": "ready",
                        "halt_subscriber": "disabled",
                        "eod_scheduler": "disabled",
                    },
                },
                None,
            )
        if path.startswith("/metrics"):
            return (
                {
                    "fill_rate": 0.5,
                    "win_rate": 0.5,
                    "slippage_bps": 10.0,
                    "window_started_at": "2026-04-30T00:00:00+00:00",
                    "window_ended_at": "2026-05-31T00:00:00+00:00",
                },
                None,
            )
        if path == "/strategies":
            return ({"strategies": []}, None)
        if path.startswith("/strategies/metrics"):
            return ({"strategies": []}, None)
        raise AssertionError(f"unexpected JSON path: {path}")

    def fake_fetch_api_payload(
        *,
        api_base_url: str,
        path: str,
        api_token: str | None,
    ) -> tuple[object, str | None]:
        assert api_base_url == "http://api.test"
        assert api_token == "token"
        parsed = urlsplit(path)
        query = parse_qs(parsed.query)
        assert query["limit"] == ["200"]
        assert query["offset"] == ["0"]
        assert len(query["until"]) == 1
        paginated_until_values.append(query["until"][0])
        if parsed.path == "/trades":
            return (
                {
                    "trades": [
                        {
                            "trade_id": f"trade-{index}",
                            "fill_notional_usdc": 10.0,
                            "fees": 0.01,
                            "filled_at": "2026-05-30T00:00:00+00:00",
                        }
                        for index in range(10)
                    ]
                },
                None,
            )
        assert parsed.path == "/decisions"
        return (
            [
                {
                    "decision_id": "d-paper-soak",
                    "market_id": "m-paper-soak",
                    "expected_edge": 0.05,
                    "spread_bps_at_decision": 50,
                    "created_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            None,
        )

    monkeypatch.setattr("scripts.paper_report._fetch_api_json", fake_fetch_api_json)
    monkeypatch.setattr("scripts.paper_report._fetch_api_payload", fake_fetch_api_payload)

    metrics = load_live_metrics(
        report_date=date(2026, 5, 30),
        api_base_url="http://api.test",
        api_token="token",
    )

    metrics_path = next(path for path in fetched_json_paths if path.startswith("/metrics"))
    parsed = urlsplit(metrics_path)
    query = parse_qs(parsed.query)
    assert parsed.path == "/metrics"
    assert query == {
        "since": ["2026-04-30T00:00:00+00:00"],
        "until": ["2026-05-31T00:00:00+00:00"],
    }
    assert "/readiness" in fetched_json_paths
    strategy_metrics_path = next(
        path for path in fetched_json_paths if path.startswith("/strategies/metrics")
    )
    strategy_metrics_query = parse_qs(urlsplit(strategy_metrics_path).query)
    assert strategy_metrics_query == {
        "since": ["2026-04-30T00:00:00+00:00"],
        "until": ["2026-05-31T00:00:00+00:00"],
    }
    assert len(set(paginated_until_values)) == 1
    assert metrics.average_slippage_bps == pytest.approx(10.0)
    assert isinstance(metrics.input_snapshot_sha256, str)
    assert len(metrics.input_snapshot_sha256) == 64
    int(metrics.input_snapshot_sha256, 16)


def test_fetch_api_list_pages_follows_limit_offset_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths: list[str] = []

    def fake_fetch_api_payload(
        *,
        api_base_url: str,
        path: str,
        api_token: str | None,
    ) -> tuple[object, str | None]:
        assert api_base_url == "http://api.test"
        assert api_token == "token"
        paths.append(path)
        if path == "/decisions?limit=200&offset=0":
            return ([{"decision_id": f"d-{index}"} for index in range(200)], None)
        if path == "/decisions?limit=200&offset=200":
            return ([{"decision_id": "d-200"}], None)
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("scripts.paper_report._fetch_api_payload", fake_fetch_api_payload)

    rows, error = _fetch_api_list_pages(
        api_base_url="http://api.test",
        path="/decisions",
        api_token="token",
    )

    assert error is None
    assert len(rows) == 201
    assert paths == [
        "/decisions?limit=200&offset=0",
        "/decisions?limit=200&offset=200",
    ]


def test_fetch_api_list_pages_handles_envelope_pagination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths: list[str] = []

    def fake_fetch_api_payload(
        *,
        api_base_url: str,
        path: str,
        api_token: str | None,
    ) -> tuple[object, str | None]:
        assert api_base_url == "http://api.test"
        assert api_token == "token"
        paths.append(path)
        if path == "/trades?limit=200&offset=0":
            return (
                {"trades": [{"trade_id": f"t-{index}"} for index in range(200)]},
                None,
            )
        if path == "/trades?limit=200&offset=200":
            return ({"trades": [{"trade_id": "t-200"}]}, None)
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("scripts.paper_report._fetch_api_payload", fake_fetch_api_payload)

    rows, error = _fetch_api_list_pages(
        api_base_url="http://api.test",
        path="/trades",
        api_token="token",
        payload_key="trades",
    )

    assert error is None
    assert len(rows) == 201
    assert rows[-1] == {"trade_id": "t-200"}
    assert paths == [
        "/trades?limit=200&offset=0",
        "/trades?limit=200&offset=200",
    ]


def test_fetch_api_list_pages_fails_when_pagination_does_not_advance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = [{"decision_id": f"d-{index}"} for index in range(200)]

    def fake_fetch_api_payload(
        *,
        api_base_url: str,
        path: str,
        api_token: str | None,
    ) -> tuple[object, str | None]:
        del api_base_url, path, api_token
        return (list(page), None)

    monkeypatch.setattr("scripts.paper_report._fetch_api_payload", fake_fetch_api_payload)

    rows, error = _fetch_api_list_pages(
        api_base_url="http://api.test",
        path="/decisions",
        api_token="token",
    )

    assert rows == []
    assert error == "pagination did not advance"


def test_paper_soak_gate_uses_persisted_trade_rows_for_fill_sample() -> None:
    trades = [
        {
            "trade_id": f"trade-{index}",
            "fill_notional_usdc": 10.0,
            "fees": 0.01,
            "filled_at": "2026-05-30T00:00:00+00:00",
        }
        for index in range(49)
    ]
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 50},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
            },
        ],
        trades={"trades": trades},
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.fills == 49
    assert gate.ok is False
    assert gate.require_check("fills").detail == "49 < 50"


def test_paper_soak_gate_excludes_trades_outside_soak_window() -> None:
    trades = [
        {
            "trade_id": f"trade-{index}",
            "fill_notional_usdc": 10.0,
            "fees": 0.01,
            "filled_at": "2026-05-30T00:00:00+00:00",
        }
        for index in range(49)
    ]
    trades.append(
        {
            "trade_id": "trade-before-soak",
            "fill_notional_usdc": 10.0,
            "fees": 0.01,
            "filled_at": "2026-04-29T23:59:59+00:00",
        }
    )
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 50},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
            },
        ],
        trades={"trades": trades},
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.fills == 49
    assert gate.ok is False
    assert gate.require_check("fills").detail == "49 < 50"


def test_paper_soak_gate_excludes_decisions_outside_soak_window() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
        },
        decisions=[
            {
                "decision_id": "d-before-soak",
                "market_id": "m-before-soak",
                "expected_edge": 0.50,
                "spread_bps_at_decision": 50,
                "created_at": "2026-04-29T23:59:59+00:00",
            },
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.001,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.average_edge_bps == pytest.approx(10.0)
    assert metrics.average_net_edge_bps == pytest.approx(-60.0)
    assert gate.ok is False
    assert gate.require_check("average_net_edge_bps").detail == "-60.0000 <= 0.0000"


def test_paper_soak_gate_preserves_negative_decision_edge_sign() -> None:
    decisions = [
        {
            "decision_id": f"d-negative-edge-{index}",
            "market_id": f"m-negative-edge-{index % 3}",
            "expected_edge": -0.05,
            "spread_bps_at_decision": 0,
            "decision_evidence": _decision_evidence(),
            "created_at": "2026-05-30T00:00:00+00:00",
        }
        for index in range(50)
    ]
    trades = [
        {
            "trade_id": f"trade-negative-edge-{index}",
            "decision_id": f"d-negative-edge-{index}",
            "market_id": f"m-negative-edge-{index % 3}",
            "risk_group_id": f"event:negative-edge-{index % 3}",
            "fill_notional_usdc": 1.0,
            "fees": 0.0,
            "filled_at": "2026-05-30T00:00:01+00:00",
        }
        for index in range(50)
    ]
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 50,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 50},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 1.0,
            "win_rate": 0.5,
            "slippage_bps": 0.0,
            "pnl_series": _pnl_series(date(2026, 5, 30), pnl=1.0),
            **_baseline_score_metrics(),
        },
        decisions=decisions,
        trades={"trades": trades},
        positions={"positions": []},
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.average_edge_bps == pytest.approx(-500.0)
    assert metrics.average_net_edge_bps == pytest.approx(-500.0)
    assert gate.require_check("average_edge_bps").detail == "-500.0000 <= 5.0000"
    assert gate.require_check("average_net_edge_bps").detail == "-500.0000 <= 0.0000"


def test_paper_soak_gate_fails_when_metrics_payload_contains_non_finite_value() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": "inf",
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
            **_baseline_score_metrics(),
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
                "decision_evidence": _decision_evidence(),
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.fill_rate is None
    assert (
        "report generation",
        "non-finite numeric evidence",
        "metrics.fill_rate must be finite",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("fill_rate").detail == "missing"
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_position_payload_contains_non_finite_value() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
            **_baseline_score_metrics(),
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
                "decision_evidence": _decision_evidence(),
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {
                    "locked_usdc": "nan",
                    "unrealized_pnl": 2.0,
                    "mark_source": "clob",
                },
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.total_exposure == 0.0
    assert (
        "report generation",
        "non-finite numeric evidence",
        "positions.locked_usdc must be finite",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_trade_payload_contains_non_finite_fee_value() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
            **_baseline_score_metrics(),
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
                "decision_evidence": _decision_evidence(),
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": "nan",
                    "fee_bps": 1.0,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.average_net_edge_bps == pytest.approx(439.0)
    assert (
        "report generation",
        "non-finite numeric evidence",
        "trades.fees must be finite",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_decision_payload_contains_non_finite_edge_value() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 1,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
            **_baseline_score_metrics(),
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "prob_estimate": 0.55,
                "limit_price": 0.50,
                "expected_edge": "nan",
                "spread_bps_at_decision": 50,
                "created_at": "2026-05-30T00:00:00+00:00",
                "decision_evidence": _decision_evidence(),
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.average_net_edge_bps == pytest.approx(465.0)
    assert (
        "report generation",
        "non-finite numeric evidence",
        "decisions.expected_edge must be finite",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_unresolved_incident_evidence_is_missing() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "paper",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert metrics.unresolved_incidents == 0
    assert (
        "report generation",
        "unresolved incident evidence missing",
        "status.supervision.unresolved_feedback_total is required",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_soak_gate_fails_when_status_mode_is_not_paper() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 30),
        status={
            "mode": "backtest",
            "runner_started_at": "2026-04-30T00:00:00+00:00",
            "runtime_continuity": _runtime_continuity_status(),
            "sensors": [
                {
                    "name": "MarketDataSensor",
                    "status": "running",
                    "last_signal_at": "2026-05-30T00:00:00+00:00",
                }
            ],
            "controller": {
                "decisions_total": 0,
                "diagnostics_total": 0,
                "diagnostic_counts": {},
            },
            "actuator": {"fills_total": 10},
            "evaluator": {
                "brier_14d": 0.16,
                "baseline_brier_14d": 0.24,
                "brier_improvement_14d": 0.08,
            },
            "supervision": {"unresolved_feedback_total": 0},
        },
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "pnl_series": _pnl_series(date(2026, 5, 30)),
        },
        decisions=[
            {
                "decision_id": "d-paper-soak",
                "market_id": "m-paper-soak",
                "expected_edge": 0.05,
                "spread_bps_at_decision": 50,
            },
        ],
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
    )

    gate = evaluate_paper_soak_gate(
        metrics,
        risk=RiskSettings(
            max_total_exposure=50.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
        ),
    )

    assert (
        "report generation",
        "paper mode evidence missing",
        "status.mode must be paper for paper-soak GO reports",
    ) in metrics.risk_events
    assert gate.ok is False
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_metrics_from_api_payloads_records_missing_status_as_risk_event() -> None:
    metrics = metrics_from_api_payloads(
        report_date=date(2026, 5, 5),
        status={},
        trades={},
        positions={},
    )

    assert metrics.day_of_soak == 0
    assert (
        "report generation",
        "runner_started_at missing",
        "check /status",
    ) in metrics.risk_events
    assert (
        "report generation",
        "paper mode evidence missing",
        "status.mode must be paper for paper-soak GO reports",
    ) in metrics.risk_events
    assert (
        "report generation",
        "unresolved incident evidence missing",
        "status.supervision.unresolved_feedback_total is required",
    ) in metrics.risk_events


def test_paper_soak_gate_fails_when_readiness_is_not_ready() -> None:
    metrics = _passing_metrics_from_api_payloads(
        readiness={
            "status": "not_ready",
            "checks": {
                "sensors": "ready",
                "event_loop": "shutting_down",
                "halt_subscriber": "disabled",
            },
        }
    )

    assert (
        "readiness",
        "readiness not ready",
        "status=not_ready; event_loop=shutting_down",
    ) in metrics.risk_events
    gate = evaluate_paper_soak_gate(metrics, risk=RiskSettings())
    assert gate.require_check("risk_events").detail == "1 risk event(s)"


def test_paper_report_renders_calibration_and_cost_diagnostics() -> None:
    diagnostics = build_paper_report_diagnostics(
        eval_records=[
            *[_eval_record(f"low-{index}", prob=0.12, outcome=1.0) for index in range(3)],
            *[_eval_record(f"mid-{index}", prob=0.55, outcome=1.0) for index in range(3)],
            *[_eval_record(f"mid-loss-{index}", prob=0.58, outcome=0.0) for index in range(2)],
        ],
        decisions=[
            _decision(
                decision_id="d-cost-1",
                market_id="m-cost",
                prob_estimate=0.64,
                limit_price=0.52,
                spread_bps_at_decision=80,
            )
        ],
        log_events=[
            {"event": "clamp_rejection", "market_id": "m-extreme"},
            {"event": "clamp_rejection", "market_id": "m-extreme"},
            {"event": "funnel_selector", "discovered_count": 12, "selected_count": 5},
            {"event": "funnel_router", "routed_count": 4},
            {"event": "funnel_pipeline", "forecasted_count": 3, "traded_count": 1},
        ],
    )
    metrics = PaperReportMetrics(
        report_date=date(2026, 5, 8),
        fills=1,
        reliability_bins=diagnostics.reliability_bins,
        trade_costs=diagnostics.trade_costs,
        clamp_rejections=diagnostics.clamp_rejections,
        selection_funnel=diagnostics.selection_funnel,
    )

    report = render_report(metrics, risk=RiskSettings(max_total_exposure=50.0))

    assert "## Calibration Reliability" in report
    assert "| [10%-20%) | 3 | insufficient data |" in report
    assert "| [50%-60%) | 5 | 60.0% |" in report
    assert "## Trade Cost Decomposition" in report
    assert "| d-cost-1 | m-cost | 12.0% | 0.4% | 0.0% | 0.3% | 11.3% |" in report
    assert "## Extreme Probability Rejections" in report
    assert "| m-extreme | 2 |" in report
    assert "## Selection Funnel" in report
    assert "| Discovered | 12 |" in report
    assert "| Selected | 5 |" in report
    assert "| Routed | 4 |" in report
    assert "| Forecasted | 3 |" in report
    assert "| Controller Emitted | 1 |" in report


def _eval_record(decision_id: str, *, prob: float, outcome: float) -> EvalRecord:
    return EvalRecord(
        market_id=f"market-{decision_id}",
        decision_id=decision_id,
        strategy_id="paper",
        strategy_version_id="paper-v1",
        prob_estimate=prob,
        resolved_outcome=outcome,
        brier_score=(prob - outcome) ** 2,
        fill_status="filled",
        recorded_at=datetime(2026, 5, 8, tzinfo=UTC),
        citations=[],
    )


def _passing_gate_metrics(
    *,
    strategy: str = "default@default-v2",
    day_of_soak: int = 30,
    decisions_accepted: int = 50,
    fills: int = 50,
    brier_improvement_7d: float | None = 0.05,
    sharpe_ratio: float | None = 0.5,
    average_net_edge_bps: float | None = 5.0,
    unresolved_incidents: int = 0,
    risk_events: tuple[tuple[str, str, str], ...] = (),
    readiness_status: str = "ready",
    readiness_checks: tuple[tuple[str, str], ...] = (
        ("sensors", "ready"),
        ("event_loop", "ready"),
        ("halt_subscriber", "disabled"),
        ("eod_scheduler", "disabled"),
    ),
) -> PaperReportMetrics:
    return PaperReportMetrics(
        report_date=date(2026, 5, 30),
        strategy=strategy,
        day_of_soak=day_of_soak,
        decisions_made=100,
        decisions_accepted=decisions_accepted,
        fills=fills,
        fill_rate=0.4,
        average_slippage_bps=10.0,
        todays_pnl=0.0,
        cumulative_pnl=2.0,
        max_drawdown_pct=5.0,
        open_positions=2,
        total_exposure=10.0,
        brier_score_7d=0.18,
        baseline_brier_score_7d=0.23,
        brier_improvement_7d=brier_improvement_7d,
        hit_rate=0.5,
        average_edge_bps=20.0,
        average_fee_bps=2.0,
        average_net_edge_bps=average_net_edge_bps,
        sharpe_ratio=sharpe_ratio,
        readiness_status=readiness_status,
        readiness_checks=readiness_checks,
        unresolved_incidents=unresolved_incidents,
        risk_events=risk_events,
        execution_concentration=ExecutionConcentration(
            entry_fills=fills,
            distinct_markets=3,
            distinct_risk_groups=3,
            missing_risk_group_fills=0,
            max_market_fill_share=0.4,
            max_risk_group_fill_share=0.4,
        ),
    )


def _pnl_series(report_date: date, *, pnl: float = 0.0) -> list[dict[str, object]]:
    return [
        {
            "recorded_at": (
                datetime(
                    report_date.year,
                    report_date.month,
                    report_date.day,
                    tzinfo=UTC,
                ).isoformat()
            ),
            "pnl": pnl,
        }
    ]


def _runtime_continuity_status() -> dict[str, object]:
    return {
        "source": "postgres_runtime_heartbeats",
        "healthy_days": 30,
        "max_gap_seconds": 60.0,
        "unhealthy_heartbeat_count": 0,
        "min_controller_runtimes": 1,
    }


def _baseline_score_metrics() -> dict[str, object]:
    return {
        "baseline_brier_by_source": {
            "market_implied": 0.24,
            "mid_quote": 0.23,
        },
        "brier_improvement_by_source": {
            "market_implied": 0.08,
            "mid_quote": 0.07,
        },
    }


def _decision_evidence() -> dict[str, float]:
    return {
        "market_implied_baseline_prob_estimate": 0.60,
        "mid_quote_baseline_prob_estimate": 0.59,
    }


def _passing_metrics_from_api_payloads(
    *,
    controller: dict[str, object] | None = None,
    include_decision_evidence: bool = True,
    decision_payload_count: int | None = None,
    include_runtime_continuity: bool = True,
    runtime_continuity: dict[str, object] | None = None,
    readiness: dict[str, object] | None = None,
    metrics_override: dict[str, object] | None = None,
) -> PaperReportMetrics:
    report_date = date(2026, 5, 30)
    controller_payload: dict[str, object] = {
        "current_runtimes_total": 1,
        "decisions_total": 1,
        "diagnostics_total": 0,
        "diagnostic_counts": {},
    }
    if controller is not None:
        controller_payload.update(controller)

    raw_decisions_total = controller_payload.get("decisions_total")
    decisions_total = raw_decisions_total if isinstance(raw_decisions_total, int) else 1
    decision_count = (
        max(0, decisions_total)
        if decision_payload_count is None
        else max(0, decision_payload_count)
    )
    decision_rows: list[dict[str, object]] = []
    for index in range(decision_count):
        decision_row: dict[str, object] = {
            "decision_id": f"d-paper-soak-{index}",
            "market_id": f"m-paper-soak-{index}",
            "expected_edge": 0.05,
            "spread_bps_at_decision": 50,
            "created_at": "2026-05-30T00:00:00+00:00",
        }
        if include_decision_evidence:
            decision_row["decision_evidence"] = _decision_evidence()
        decision_rows.append(decision_row)

    status_payload: dict[str, object] = {
        "mode": "paper",
        "runner_started_at": "2026-04-30T00:00:00+00:00",
        "sensors": [
            {
                "name": "MarketDataSensor",
                "status": "running",
                "last_signal_at": "2026-05-30T00:00:00+00:00",
            }
        ],
        "controller": controller_payload,
        "actuator": {"fills_total": 10},
        "evaluator": {
            "brier_14d": 0.16,
            "baseline_brier_14d": 0.24,
            "brier_improvement_14d": 0.08,
        },
        "supervision": {"unresolved_feedback_total": 0},
    }
    if include_runtime_continuity:
        status_payload["runtime_continuity"] = (
            _runtime_continuity_status()
            if runtime_continuity is None
            else runtime_continuity
        )

    return metrics_from_api_payloads(
        report_date=report_date,
        status=status_payload,
        metrics={
            "fill_rate": 0.5,
            "win_rate": 0.5,
            "slippage_bps": 10.0,
            "max_drawdown_pct": 12.5,
            "sharpe_ratio": 1.5,
            "pnl_series": _pnl_series(report_date),
            **_baseline_score_metrics(),
            **(metrics_override or {}),
        },
        decisions=decision_rows,
        readiness=(
            {
                "status": "ready",
                "checks": {
                    "sensors": "ready",
                    "event_loop": "ready",
                    "halt_subscriber": "disabled",
                    "eod_scheduler": "disabled",
                },
            }
            if readiness is None
            else readiness
        ),
        trades={
            "trades": [
                {
                    "trade_id": f"trade-{index}",
                    "fill_notional_usdc": 10.0,
                    "fees": 0.01,
                    "filled_at": "2026-05-30T00:00:00+00:00",
                }
                for index in range(10)
            ]
        },
        positions={
            "positions": [
                {"locked_usdc": 10.0, "unrealized_pnl": 2.0, "mark_source": "clob"},
            ],
        },
        strategies={
            "strategies": [
                {
                    "strategy_id": "default",
                    "active_version_id": "default-v1",
                }
            ],
        },
    )


def _decision(
    *,
    decision_id: str,
    market_id: str,
    prob_estimate: float,
    limit_price: float,
    spread_bps_at_decision: int,
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id=market_id,
        token_id="token",
        venue="polymarket",
        side="BUY",
        notional_usdc=1.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=[],
        prob_estimate=prob_estimate,
        expected_edge=prob_estimate - limit_price,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"op-{decision_id}",
        strategy_id="paper",
        strategy_version_id="paper-v1",
        limit_price=limit_price,
        action="BUY",
        spread_bps_at_decision=spread_bps_at_decision,
    )
