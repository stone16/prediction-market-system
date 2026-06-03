from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FILES_TO_SCAN = [
    ROOT / "README.md",
    ROOT / "CLAUDE.md",
    ROOT / "agent_docs" / "architecture-invariants.md",
    ROOT / "agent_docs" / "promoted-rules.md",
    ROOT / "agent_docs" / "project-roadmap.md",
    ROOT / "src" / "pms" / "actuator" / "CLAUDE.md",
    ROOT / "src" / "pms" / "controller" / "CLAUDE.md",
    ROOT / "src" / "pms" / "sensor" / "CLAUDE.md",
    ROOT / "src" / "pms" / "evaluation" / "CLAUDE.md",
]
KALSHI_KEYWORD = "Kalshi"
STUB_MARKERS = ("not implemented", "reserved", "stub", "NotImplementedError")


def test_readme_and_claude_explicitly_document_gated_polymarket_live_mode() -> None:
    readme_text = (ROOT / "README.md").read_text(encoding="utf-8")
    claude_text = (ROOT / "CLAUDE.md").read_text(encoding="utf-8")

    assert "gated Polymarket `live`" in readme_text
    assert "live_trading_enabled=true" in readme_text
    assert "operator_approval_mode=every_order" in readme_text
    assert "operator gate" in readme_text
    assert "gated Polymarket `live`" in claude_text
    assert "live_trading_enabled=true" in claude_text
    assert "operator_approval_mode=every_order" in claude_text
    assert "operator gate" in claude_text


def test_readme_paper_api_examples_use_bearer_token_when_token_is_configured() -> None:
    readme_text = (ROOT / "README.md").read_text(encoding="utf-8")

    assert 'export PMS_API_TOKEN="$(openssl rand -hex 32)"' in readme_text
    assert "Authorization: Bearer $PMS_API_TOKEN" in readme_text
    assert "scripts/paper_report.py reads the same token" in readme_text


def test_paper_soak_docs_explicitly_start_runner_after_api_control_plane() -> None:
    readme_text = (ROOT / "README.md").read_text(encoding="utf-8")
    runbook_text = (ROOT / "docs" / "operations" / "live-polymarket-runbook.md").read_text(
        encoding="utf-8"
    )
    normalized_readme = _normalized_doc_text(readme_text)
    normalized_runbook = _normalized_doc_text(runbook_text)

    expected_control_plane_warning = (
        "The `pms-api` command starts the API control plane; it does not start "
        "the runner until an authenticated `POST /run/start` succeeds."
    )
    expected_start_command = "http://127.0.0.1:8000/run/start"

    assert expected_control_plane_warning in normalized_readme
    assert expected_control_plane_warning in normalized_runbook
    assert expected_start_command in readme_text
    assert expected_start_command in runbook_text


def test_readme_autostart_example_mentions_required_discord_webhook() -> None:
    readme_text = (ROOT / "README.md").read_text(encoding="utf-8")
    normalized = _normalized_doc_text(readme_text)

    assert "PMS_AUTO_START=1 requires PMS_DISCORD__WEBHOOK_URL" in normalized
    assert "PMS_AUTO_START=1 uv run pms-api" not in readme_text


def _normalized_doc_text(text: str) -> str:
    lines = [
        line.lstrip("#").strip()
        for line in text.splitlines()
    ]
    return " ".join(" ".join(lines).split())


def _configured_flb_fee_rate(config_text: str) -> str:
    for line in config_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("flb_fee_rate:"):
            return stripped.split(":", maxsplit=1)[1].strip()
    raise AssertionError("config.live-soak.yaml missing flb_fee_rate")


def _configured_llm_enabled(config_text: str) -> bool:
    in_llm_section = False
    for line in config_text.splitlines():
        if line.startswith("llm:"):
            in_llm_section = True
            continue
        if in_llm_section and line and not line.startswith(" "):
            break
        stripped = line.strip()
        if in_llm_section and stripped.startswith("enabled:"):
            raw_value = stripped.split(":", maxsplit=1)[1].strip()
            if raw_value == "true":
                return True
            if raw_value == "false":
                return False
            raise AssertionError(f"unexpected llm.enabled value: {raw_value}")
    raise AssertionError("config.live-soak.yaml missing llm.enabled")


def test_readme_paper_soak_status_mentions_required_launch_artifacts() -> None:
    readme_text = (ROOT / "README.md").read_text(encoding="utf-8")
    normalized = " ".join(readme_text.split())

    assert "Paper Soak Blocked Pending Launch Artifacts" in readme_text
    assert (
        "does not start until `/secure/pms/category-prior-observations.csv`, "
        "`/secure/pms/flb-calibration.csv`, and the FLB `.provenance.json` "
        "sidecar exist"
    ) in normalized
    assert "not credentials" in normalized
    assert "not a launch artifact" in normalized


def test_launch_fee_rate_docs_match_live_soak_config() -> None:
    config_text = (ROOT / "config.live-soak.yaml").read_text(encoding="utf-8")
    runbook_text = (
        ROOT / "docs" / "operations" / "live-polymarket-runbook.md"
    ).read_text(encoding="utf-8")
    readiness_text = (
        ROOT / "agent_docs" / "production-readiness-2026-05.md"
    ).read_text(encoding="utf-8")
    fee_rate = _configured_flb_fee_rate(config_text)

    assert f"flb_fee_rate: {fee_rate}" in runbook_text
    assert f"--fee-rate {fee_rate}" in runbook_text
    assert f"`strategies.flb_fee_rate={fee_rate}`" in readiness_text
    assert f"confirm `{fee_rate}`" in readiness_text
    assert "--fee-rate 0.04" not in runbook_text
    assert "`strategies.flb_fee_rate=0.04`" not in readiness_text


def test_live_runbook_llm_guidance_matches_live_soak_config() -> None:
    config_text = (ROOT / "config.live-soak.yaml").read_text(encoding="utf-8")
    runbook_text = (
        ROOT / "docs" / "operations" / "live-polymarket-runbook.md"
    ).read_text(encoding="utf-8")
    normalized_runbook = _normalized_doc_text(runbook_text)

    assert _configured_llm_enabled(config_text) is False
    assert (
        "The committed paper-soak config keeps `llm.enabled: false`"
        in normalized_runbook
    )
    assert "committed paper-soak config enables the LLM forecaster" not in runbook_text


def test_readme_llm_guidance_matches_live_soak_config() -> None:
    config_text = (ROOT / "config.live-soak.yaml").read_text(encoding="utf-8")
    readme_text = (ROOT / "README.md").read_text(encoding="utf-8")
    normalized_readme = _normalized_doc_text(readme_text)

    assert _configured_llm_enabled(config_text) is False
    assert (
        "H1 FLB keeps `llm.enabled: false` for the launch soak"
        in normalized_readme
    )
    assert "and the LLM forecaster" not in readme_text


def test_live_soak_config_llm_secret_comment_matches_disabled_llm() -> None:
    config_text = (ROOT / "config.live-soak.yaml").read_text(encoding="utf-8")

    assert _configured_llm_enabled(config_text) is False
    assert (
        "PMS_LLM__API_KEY is required only if you explicitly enable LLM"
        in config_text
    )
    assert "(always required)" not in config_text
    assert "Required env vars when running this config" not in config_text


def test_live_runbook_first_order_example_includes_outcome_and_reconciliation_gate() -> None:
    runbook_text = (ROOT / "docs" / "operations" / "live-polymarket-runbook.md").read_text(
        encoding="utf-8"
    )

    assert '"outcome": "NO"' in runbook_text
    assert "live_account_reconciliation_required: true" in runbook_text
    assert "live_exit_criteria_ratified_by" in runbook_text
    assert "live_compliance_jurisdiction" in runbook_text
    assert "live_paper_soak_report_path" in runbook_text
    assert "live_operator_rehearsal_report_path" in runbook_text
    assert "live_first_order_audit_path" in runbook_text
    assert "polymarket.first_live_order_approval_path" in runbook_text
    assert "permanently-denying operator gate" in runbook_text
    assert "PMS_LIVE_FIRST_ORDER_AUDIT_PATH" in runbook_text
    assert "reject shared paths" in runbook_text
    assert "reject repo-local approval and" in runbook_text
    assert "audit paths, unusable approval and audit path parents" in runbook_text
    assert "permissive approval" in runbook_text
    assert "private and owner-writable" in runbook_text
    assert "max_exposure_per_risk_group" in runbook_text
    assert "max_exposure_per_risk_group=$1" in runbook_text
    assert "llm.max_daily_llm_cost_usdc=$0.05" in runbook_text
    assert "risk_group_id" in runbook_text
    assert "decisions without a risk group are rejected" in runbook_text
    assert "market_data_freshness" in runbook_text
    assert "book_snapshots" in runbook_text
    assert "book_levels" in runbook_text
    assert "two-sided snapshot" in runbook_text
    assert "venue pUSD balance and allowance must cover `risk.max_total_exposure`" in runbook_text
    assert "balance/allowance sync endpoint for collateral" in runbook_text
    assert "`3` (`POLY_1271`) for new API deposit-wallet credentials" in runbook_text
    assert "average_net_edge_bps" in runbook_text
    assert "unresolved_incidents" in runbook_text
    assert "risk_events" in runbook_text
    assert "operator_approval_mode: every_order" in runbook_text
    assert "Direct `PolymarketActuator` use in true `mode: live`" in runbook_text
    assert "cannot bypass the\nstartup artifact gate" in runbook_text
    assert "PAPER_SOAK_REPORT_DATE" in runbook_text
    assert "--output /secure/pms/paper-soak-go-report.md" in runbook_text
    assert "requires at least 50 simulated fills before the report can pass" in runbook_text
    assert "requires at least 10 simulated fills before the report can pass" not in runbook_text
    assert "`artifact_mode` set to `persisted`" in runbook_text
    assert "`generated_at` timestamp" in runbook_text
    assert "Dry-run output is marked `dry_run`" in runbook_text
    assert "persisted provenance `output_path` must match" in runbook_text
    assert "compliance review timestamps to be at or after" in runbook_text
    assert "--output /secure/pms/credentialed-preflight.json" in runbook_text
    assert "`artifact_mode: credentialed_preflight`" in runbook_text
    assert "`final_go_no_go_valid: true`" in runbook_text
    assert "`database_url_override_used`" in runbook_text
    assert "`settings_fingerprint`" in runbook_text
    assert "`database_url_override_used: false`" in runbook_text
    assert "artifacts generated with `--database-url`" in runbook_text
    assert "are marked `artifact_mode: incomplete_preflight`" in runbook_text
    assert "not final go/no-go valid exits nonzero" in runbook_text.replace("\n", " ")
    assert "`live_preflight_artifact_max_age_s`" in runbook_text
    assert "older than `live_preflight_artifact_max_age_s`" in runbook_text.replace(
        "\n", " "
    )
    assert "credentialed preflight artifact parent directory must be private" in runbook_text
    assert "`artifact_mode: incomplete_preflight`" in runbook_text
    assert "`final_go_no_go_valid: false`" in runbook_text
    assert "database connection" in runbook_text
    assert "`password=` fragments" in runbook_text
    assert "redacted" in runbook_text
    assert "pms-live reconcile-submission-unknown" in runbook_text
    assert "--config config.live.yaml" in runbook_text
    assert "--database-url` only as an explicit override" in runbook_text
    assert "`--venue-order-id` is required when status is `filled` or `open`" in runbook_text
    assert "checks the Alembic schema head before writing" in runbook_text
    assert "`updated: false`" in runbook_text
    assert "redacted `error` field" in runbook_text
    assert "pms-live reconcile-live-order" in runbook_text
    assert "--output /secure/pms/first-live-order-reconciliation.json" in runbook_text
    assert "`artifact_mode:\npost_live_order_reconciliation`" in runbook_text
    assert "`final_post_live_valid: true`" in runbook_text
    assert "`credentialed_preflight_artifact`" in runbook_text
    assert "generated before the live order's\npersisted `submitted_at`" in runbook_text
    assert "persisted\nfill plus pre-submit quote hash/source" in runbook_text
    assert "incomplete_post_live_order_reconciliation" in runbook_text
    assert "time_in_force: IOC" in runbook_text
    assert "permits only `IOC` or `FOK`" in runbook_text
    assert "secret_source: local_file" in runbook_text
    assert "chmod 600" in runbook_text
    assert "placeholder markers" in runbook_text
    assert "future-dated" in runbook_text
    assert "operator-rehearsal-report.md" in runbook_text
    assert "`scripts/rehearse_first_order.py`" in runbook_text
    assert "`artifact_mode` set to `persisted`" in runbook_text
    assert "a parseable `generated_at`" in runbook_text
    assert "`output_path` matching" in runbook_text
    assert "`live_operator_rehearsal_report_path`" in runbook_text
    assert "`fresh_approval_required`" in runbook_text
    assert "`<approval-path>.meta.json`" in runbook_text
    assert "`polymarket.operator_approval_max_age_s`" in runbook_text
    assert "`approval_sha256`" in runbook_text
    assert "canonical SHA-256 hash" in runbook_text
    assert "bare approval JSON" in runbook_text
    assert "decision_evidence" in runbook_text
    assert "book hash" in runbook_text
    assert "fails if an approval JSON already exists" in runbook_text
    assert "`alpha_source`, `edge_model_source`" in runbook_text
    assert "`calibration_source`, and `evidence_source`" in runbook_text
    assert "Authorization: Bearer $PMS_API_TOKEN" in runbook_text
    assert "http://127.0.0.1:8000/run/stop" in runbook_text
    assert "protected by `PMS_API_TOKEN`" in runbook_text


def test_live_exit_criteria_are_machine_observable_and_no_longer_todo_decisions() -> None:
    exit_criteria_text = (
        ROOT / "docs" / "operations" / "live-exit-criteria.md"
    ).read_text(encoding="utf-8")
    runbook_text = (ROOT / "docs" / "operations" / "live-polymarket-runbook.md").read_text(
        encoding="utf-8"
    )

    assert "TODO_DECISION" not in exit_criteria_text
    assert "TODO_DECISION" not in runbook_text
    assert "halt_recovery_cycles_7d" in exit_criteria_text
    assert "brier_improvement_14d" in exit_criteria_text


def test_kalshi_mentions_are_stubbed_and_live_launch_docs_are_not_stale() -> None:
    offending_lines: list[str] = []

    for path in FILES_TO_SCAN:
        if not path.exists():
            continue
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if KALSHI_KEYWORD in line:
                if not any(marker in line for marker in STUB_MARKERS):
                    offending_lines.append(f"{path.relative_to(ROOT)}:{line_number}:{line}")

    assert offending_lines == []

    readme_text = (ROOT / "README.md").read_text(encoding="utf-8")
    runbook_text = (ROOT / "docs" / "operations" / "live-polymarket-runbook.md").read_text(
        encoding="utf-8"
    )
    stale_live_markers = (
        "stub-gated",
        "not implemented until paper soak",
    )
    for marker in stale_live_markers:
        assert marker not in readme_text
        assert marker not in runbook_text
    assert "Do not create the approval JSON before preflight" in readme_text
    assert "--output /secure/pms/credentialed-preflight.json" in readme_text
    assert "pms-live reconcile-live-order" in readme_text
    assert "--output /secure/pms/first-live-order-reconciliation.json" in readme_text
    assert "credentialed preflight artifact to still validate" in readme_text
    assert "PAPER_SOAK_REPORT_DATE" in readme_text
    assert "--output /secure/pms/paper-soak-go-report.md" in readme_text
    assert "--calibration-csv /secure/pms/flb-calibration.csv" in readme_text
    assert "--calibration-source-label warehouse-flb-v1" in readme_text
    normalized_readme_text = " ".join(readme_text.split())
    assert (
        "--calibration-provenance-json "
        "/secure/pms/flb-calibration.csv.provenance.json"
    ) in normalized_readme_text
    assert "max_exposure_per_risk_group=$1" in readme_text
    assert "max_exposure_per_risk_group=$15" not in readme_text
    assert "scripts/prepare_local_paper_soak_config.py" in readme_text
    assert "scripts/prepare_local_paper_soak_config.py" in runbook_text
    assert "--paper-canary" in runbook_text
    assert "scripts/install_paper_canary_strategy.py" in runbook_text
    assert "--archive-default" in runbook_text
    assert "--sample-modulus 1" in runbook_text
    assert "paper_soak_strategy_id: null" in runbook_text
    assert "flb_calibration_path: null" in runbook_text
    assert "Decimal-equivalent settled vectors" in runbook_text
    assert "PMS_SECURE_DIR" in readme_text
    assert "PMS_SECURE_DIR" in runbook_text
    assert "private artifact parent" in readme_text
    assert "private artifact parent" in runbook_text
    assert "scripts/check_paper_soak_artifacts.py" in readme_text
    assert "scripts/check_paper_soak_artifacts.py" in runbook_text
    assert "credentialed preflight artifact is missing/invalid" in readme_text
    assert "strategy_id`, `strategy_version_id`" in runbook_text
    assert "strategy_evidence` to match the final paper-soak" in runbook_text
    assert "Paper-only strategies" in runbook_text
    assert "`paper_canary_v1` cannot be final GO evidence" in runbook_text
    assert "Create the approval JSON only after preview review" in readme_text
    assert "true LIVE template leaves LLM disabled by default" in readme_text
    assert (
        "The committed paper-soak config keeps `llm.enabled: false`"
        in _normalized_doc_text(runbook_text)
    )
    assert "PMS_LLM__API_KEY is required only if you explicitly enable LLM" in runbook_text
    assert "PMS_RUN_LIVE_PREFLIGHT=1" in runbook_text
    assert "tests/integration/test_live_credentialed_preflight.py" in runbook_text
    assert "test fixture reports are rejected" in runbook_text
    fly_runbook_text = (
        ROOT / "docs" / "operations" / "fly-deploy-runbook.md"
    ).read_text(encoding="utf-8")
    assert "fly.live.toml.example" in fly_runbook_text
    assert "fly volumes create pms_paper_soak_secure" in fly_runbook_text
    assert "install -d -m 700 /secure/pms" in fly_runbook_text
    assert "/secure/pms/category-prior-observations.csv" in fly_runbook_text
    assert "/secure/pms/flb-calibration.csv" in fly_runbook_text
    assert "/secure/pms/flb-calibration.csv.provenance.json" in fly_runbook_text
    assert "fly deploy -c fly.live.toml" in fly_runbook_text
    assert "DATABASE_URL" in fly_runbook_text
