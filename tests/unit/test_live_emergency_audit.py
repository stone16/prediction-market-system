from __future__ import annotations

import json
import os
import stat
from dataclasses import replace
from pathlib import Path

import pytest

from pms.core.enums import TimeInForce
from pms.core.models import TradeDecision
from pms import live_cli
from pms.live_cli import build_parser
from pms.storage.live_emergency_audit import LiveEmergencyAuditWriter


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="decision-emergency-audit",
        market_id="market-emergency-audit",
        token_id="token-emergency-audit",
        venue="polymarket",
        side="BUY",
        notional_usdc=5.0,
        order_type="limit",
        max_slippage_bps=25,
        stop_conditions=["unit-test"],
        prob_estimate=0.6,
        expected_edge=0.1,
        time_in_force=TimeInForce.IOC,
        opportunity_id="opportunity-emergency-audit",
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        limit_price=0.5,
    )


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_creates_private_parent(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "deep" / "nested" / "live-emergency-audit.jsonl"
    writer = LiveEmergencyAuditWriter(audit_path)

    await writer.append(
        phase="submit_failed",
        decision=_decision(),
        order_state=None,
        error=RuntimeError("venue timeout"),
    )

    assert audit_path.exists()
    assert stat.S_IMODE(audit_path.parent.stat().st_mode) == 0o700


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_refuses_permissive_parent(
    tmp_path: Path,
) -> None:
    audit_dir = tmp_path / "permissive-emergency-audit"
    audit_dir.mkdir(mode=0o700)
    audit_dir.chmod(0o755)
    audit_path = audit_dir / "live-emergency-audit.jsonl"
    writer = LiveEmergencyAuditWriter(audit_path)

    try:
        with pytest.raises(OSError, match="too permissive"):
            await writer.append(
                phase="submit_failed",
                decision=_decision(),
                order_state=None,
                error=RuntimeError("venue timeout"),
            )
    finally:
        audit_dir.chmod(0o700)

    assert not audit_path.exists()


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_refuses_symlink_parent(
    tmp_path: Path,
) -> None:
    audit_dir = tmp_path / "emergency-audit-target"
    audit_dir.mkdir(mode=0o700)
    symlink_parent = tmp_path / "emergency-audit-link"
    symlink_parent.symlink_to(audit_dir, target_is_directory=True)
    audit_path = symlink_parent / "live-emergency-audit.jsonl"
    writer = LiveEmergencyAuditWriter(audit_path)

    with pytest.raises(OSError, match="parent path is not a directory"):
        await writer.append(
            phase="submit_failed",
            decision=_decision(),
            order_state=None,
            error=RuntimeError("venue timeout"),
        )

    assert not (audit_dir / "live-emergency-audit.jsonl").exists()


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_refuses_symlink_path(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target-emergency-audit.jsonl"
    target_path.write_text("target must not be appended\n", encoding="utf-8")
    audit_path = tmp_path / "live-emergency-audit.jsonl"
    audit_path.symlink_to(target_path)
    writer = LiveEmergencyAuditWriter(audit_path)

    with pytest.raises(OSError):
        await writer.append(
            phase="submit_failed",
            decision=_decision(),
            order_state=None,
            error=RuntimeError("venue timeout"),
        )

    assert target_path.read_text(encoding="utf-8") == "target must not be appended\n"


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_refuses_hardlinked_path(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target-emergency-audit.jsonl"
    target_path.write_text("target must not be appended\n", encoding="utf-8")
    audit_path = tmp_path / "live-emergency-audit.jsonl"
    os.link(target_path, audit_path)
    writer = LiveEmergencyAuditWriter(audit_path)

    with pytest.raises(OSError, match="single-link"):
        await writer.append(
            phase="submit_failed",
            decision=_decision(),
            order_state=None,
            error=RuntimeError("venue timeout"),
        )

    assert target_path.read_text(encoding="utf-8") == "target must not be appended\n"


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_refuses_hardlink_swap_during_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_path = tmp_path / "target-emergency-audit.jsonl"
    target_path.write_text("target must not be appended\n", encoding="utf-8")
    audit_path = tmp_path / "live-emergency-audit.jsonl"
    audit_path.write_text("old audit\n", encoding="utf-8")
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == audit_path and flags & os.O_WRONLY and not swapped:
            swapped = True
            audit_path.unlink()
            os.link(target_path, audit_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)
    writer = LiveEmergencyAuditWriter(audit_path)

    with pytest.raises(OSError, match="single-link"):
        await writer.append(
            phase="submit_failed",
            decision=_decision(),
            order_state=None,
            error=RuntimeError("venue timeout"),
        )

    assert swapped is True
    assert target_path.read_text(encoding="utf-8") == "target must not be appended\n"


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_retries_short_os_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_path = tmp_path / "secure" / "live-emergency-audit.jsonl"
    writer = LiveEmergencyAuditWriter(audit_path)
    real_write = os.write
    write_sizes: list[int] = []

    def short_write(fd: int, data: bytes) -> int:
        if len(data) > 1:
            chunk_size = max(1, len(data) // 2)
            write_sizes.append(chunk_size)
            return real_write(fd, data[:chunk_size])
        write_sizes.append(len(data))
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", short_write)

    await writer.append(
        phase="submit_failed",
        decision=_decision(),
        order_state=None,
        error=RuntimeError("venue timeout"),
    )

    assert len(write_sizes) > 1
    line = audit_path.read_text(encoding="utf-8")
    assert line.endswith("\n")
    assert json.loads(line)["phase"] == "submit_failed"


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_fsyncs_before_returning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_path = tmp_path / "secure" / "live-emergency-audit.jsonl"
    writer = LiveEmergencyAuditWriter(audit_path)
    fsync_calls: list[int] = []
    real_fsync = os.fsync

    def recording_fsync(fd: int) -> None:
        fsync_calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", recording_fsync)

    await writer.append_manual_stop(
        stopped_by="operator-1",
        reason="venue reconciliation mismatch",
        runner_stopped=True,
        credentials_rotated=True,
        runtime_secrets_removed=True,
        venue_open_orders_reviewed=True,
        database_reconciled=True,
        restart_mode="paper",
    )

    assert fsync_calls, "emergency audit append must fsync before returning"


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_rejects_non_finite_audit_evidence(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "secure" / "live-emergency-audit.jsonl"
    writer = LiveEmergencyAuditWriter(audit_path)

    with pytest.raises(ValueError, match="requested_notional_usdc"):
        await writer.append(
            phase="submit_failed",
            decision=replace(_decision(), notional_usdc=float("nan")),
            order_state=None,
            error=RuntimeError("venue timeout"),
        )

    assert not audit_path.exists()


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_appends_manual_stop_record(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "secure" / "live-emergency-audit.jsonl"
    writer = LiveEmergencyAuditWriter(audit_path)

    await writer.append_manual_stop(
        stopped_by="operator-1",
        reason="venue reconciliation mismatch",
        runner_stopped=True,
        credentials_rotated=True,
        runtime_secrets_removed=True,
        venue_open_orders_reviewed=True,
        database_reconciled=True,
        restart_mode="paper",
    )

    record = json.loads(audit_path.read_text(encoding="utf-8"))
    assert record["phase"] == "manual_emergency_stop"
    assert record["event"] == "manual_emergency_stop"
    assert record["stopped_by"] == "operator-1"
    assert record["reason"] == "venue reconciliation mismatch"
    assert record["runner_stopped"] is True
    assert record["credentials_rotated"] is True
    assert record["runtime_secrets_removed"] is True
    assert record["venue_open_orders_reviewed"] is True
    assert record["database_reconciled"] is True
    assert record["restart_mode"] == "paper"


@pytest.mark.asyncio
async def test_live_emergency_audit_writer_rejects_incomplete_manual_stop(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "secure" / "live-emergency-audit.jsonl"
    writer = LiveEmergencyAuditWriter(audit_path)

    with pytest.raises(ValueError, match="credentials_rotated"):
        await writer.append_manual_stop(
            stopped_by="operator-1",
            reason="venue reconciliation mismatch",
            runner_stopped=True,
            credentials_rotated=False,
            runtime_secrets_removed=True,
            venue_open_orders_reviewed=True,
            database_reconciled=True,
            restart_mode="paper",
        )

    assert not audit_path.exists()


@pytest.mark.parametrize(
    "stopped_by",
    (
        "operator|forged",
        "operator\nforged",
        "operator\rforged",
        "replace-me",
    ),
)
@pytest.mark.asyncio
async def test_live_emergency_audit_writer_rejects_non_actionable_stopped_by(
    tmp_path: Path,
    stopped_by: str,
) -> None:
    audit_path = tmp_path / "secure" / "live-emergency-audit.jsonl"
    writer = LiveEmergencyAuditWriter(audit_path)

    with pytest.raises(ValueError, match="stopped_by"):
        await writer.append_manual_stop(
            stopped_by=stopped_by,
            reason="venue reconciliation mismatch",
            runner_stopped=True,
            credentials_rotated=True,
            runtime_secrets_removed=True,
            venue_open_orders_reviewed=True,
            database_reconciled=True,
            restart_mode="paper",
        )

    assert not audit_path.exists()


def test_pms_live_cli_parses_record_emergency_stop_command() -> None:
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            "config.live.yaml",
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    assert args.command == "record-emergency-stop"
    assert args.config == "config.live.yaml"
    assert args.stopped_by == "operator-1"
    assert args.reason == "venue reconciliation mismatch"
    assert args.runner_stopped is True
    assert args.credentials_rotated is True
    assert args.runtime_secrets_removed is True
    assert args.venue_open_orders_reviewed is True
    assert args.database_reconciled is True
    assert args.restart_mode == "paper"


@pytest.mark.asyncio
async def test_pms_live_cli_records_emergency_stop_to_configured_audit_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    audit_path = tmp_path / "secure" / "live-emergency-audit.jsonl"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        f"live_emergency_audit_path: {audit_path}\n",
        encoding="utf-8",
    )
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            str(config_path),
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    record = json.loads(audit_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload == {
        "recorded": True,
        "event": "manual_emergency_stop",
        "path": str(audit_path),
    }
    assert record["stopped_by"] == "operator-1"


@pytest.mark.asyncio
async def test_pms_live_cli_record_emergency_stop_requires_explicit_audit_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    monkeypatch.chdir(repo_root)
    monkeypatch.delenv("PMS_CONFIG_PATH", raising=False)
    monkeypatch.delenv("PMS_LIVE_EMERGENCY_AUDIT_PATH", raising=False)
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["recorded"] is False
    assert payload["event"] == "manual_emergency_stop"
    assert "live_emergency_audit_path is required" in payload["error"]
    assert not (repo_root / ".data" / "live-emergency-audit.jsonl").exists()


@pytest.mark.asyncio
async def test_pms_live_cli_record_emergency_stop_rejects_working_tree_audit_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    monkeypatch.chdir(repo_root)
    config_path = repo_root / "config.live.yaml"
    config_path.write_text(
        "live_emergency_audit_path: secure/live-emergency-audit.jsonl\n",
        encoding="utf-8",
    )
    audit_path = repo_root / "secure" / "live-emergency-audit.jsonl"
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            "config.live.yaml",
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["recorded"] is False
    assert payload["event"] == "manual_emergency_stop"
    assert "outside the working tree" in payload["error"]
    assert not audit_path.exists()


@pytest.mark.asyncio
async def test_pms_live_cli_record_emergency_stop_rejects_placeholder_audit_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    audit_path = secure_dir / "__FILL_IN_LIVE_EMERGENCY_AUDIT__.jsonl"
    config_path = repo_root / "config.live.yaml"
    config_path.write_text(
        f"live_emergency_audit_path: {audit_path}\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            "config.live.yaml",
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["recorded"] is False
    assert payload["event"] == "manual_emergency_stop"
    assert "placeholder" in payload["error"]
    assert not audit_path.exists()


@pytest.mark.asyncio
async def test_pms_live_cli_record_emergency_stop_rejects_preflight_artifact_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    preflight_path = secure_dir / "credentialed-preflight.json"
    original_preflight_content = '{"artifact_mode": "credentialed_preflight"}\n'
    preflight_path.write_text(original_preflight_content, encoding="utf-8")
    config_path = repo_root / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            (
                f"live_emergency_audit_path: {preflight_path}",
                f"live_preflight_artifact_path: {preflight_path}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            "config.live.yaml",
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["recorded"] is False
    assert payload["event"] == "manual_emergency_stop"
    assert "credentialed preflight artifact" in payload["error"]
    assert preflight_path.read_text(encoding="utf-8") == original_preflight_content


@pytest.mark.asyncio
async def test_pms_live_cli_record_emergency_stop_rejects_config_file_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    config_path = secure_dir / "config.live.yaml"
    original_config_content = f"live_emergency_audit_path: {config_path}\n"
    config_path.write_text(original_config_content, encoding="utf-8")
    monkeypatch.chdir(repo_root)
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            str(config_path),
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["recorded"] is False
    assert payload["event"] == "manual_emergency_stop"
    assert "config file" in payload["error"]
    assert config_path.read_text(encoding="utf-8") == original_config_content


@pytest.mark.asyncio
async def test_pms_live_cli_record_emergency_stop_rejects_missing_config_file_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    config_path = secure_dir / "config.live.yaml"
    monkeypatch.chdir(repo_root)
    monkeypatch.setenv("PMS_LIVE_EMERGENCY_AUDIT_PATH", str(config_path))
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            str(config_path),
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["recorded"] is False
    assert payload["event"] == "manual_emergency_stop"
    assert "config file" in payload["error"]
    assert not config_path.exists()


@pytest.mark.asyncio
async def test_pms_live_cli_record_emergency_stop_rejects_local_secret_file_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    secret_path = secure_dir / "polymarket.local-secrets.yaml"
    original_secret_content = "polymarket:\n  api_key: live-api-key\n"
    secret_path.write_text(original_secret_content, encoding="utf-8")
    config_path = repo_root / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            (
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
                f"live_emergency_audit_path: {secret_path}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            "config.live.yaml",
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["recorded"] is False
    assert payload["event"] == "manual_emergency_stop"
    assert "local secret file" in payload["error"]
    assert secret_path.read_text(encoding="utf-8") == original_secret_content


@pytest.mark.asyncio
async def test_pms_live_cli_record_emergency_stop_rejects_env_local_secret_file_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(mode=0o700)
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    secret_path = secure_dir / "polymarket.local-secrets.yaml"
    original_secret_content = "polymarket:\n  api_key: live-api-key\n"
    secret_path.write_text(original_secret_content, encoding="utf-8")
    monkeypatch.chdir(repo_root)
    monkeypatch.setenv("PMS_LIVE_EMERGENCY_AUDIT_PATH", str(secret_path))
    monkeypatch.setenv("PMS_LOCAL_SECRET_FILE", str(secret_path))
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            "missing-config.live.yaml",
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["recorded"] is False
    assert payload["event"] == "manual_emergency_stop"
    assert "local secret file" in payload["error"]
    assert secret_path.read_text(encoding="utf-8") == original_secret_content


@pytest.mark.asyncio
async def test_pms_live_cli_emergency_stop_reports_absolute_audit_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    monkeypatch.chdir(repo_root)
    audit_path = Path("../secure/live-emergency-audit.jsonl")
    expected_audit_path = secure_dir / "live-emergency-audit.jsonl"
    config_path = repo_root / "config.live.yaml"
    config_path.write_text(
        f"live_emergency_audit_path: {audit_path}\n",
        encoding="utf-8",
    )
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            "config.live.yaml",
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    payload = json.loads(capsys.readouterr().out)
    record = json.loads(expected_audit_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload == {
        "recorded": True,
        "event": "manual_emergency_stop",
        "path": str(expected_audit_path),
    }
    assert record["event"] == "manual_emergency_stop"


@pytest.mark.asyncio
async def test_pms_live_cli_records_emergency_stop_after_local_secret_removed(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    audit_path = tmp_path / "secure" / "live-emergency-audit.jsonl"
    removed_secret_path = tmp_path / "removed" / "polymarket-secrets.yaml"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            (
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {removed_secret_path}",
                f"live_emergency_audit_path: {audit_path}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    args = build_parser().parse_args(
        [
            "record-emergency-stop",
            "--config",
            str(config_path),
            "--stopped-by",
            "operator-1",
            "--reason",
            "venue reconciliation mismatch",
            "--runner-stopped",
            "--credentials-rotated",
            "--runtime-secrets-removed",
            "--venue-open-orders-reviewed",
            "--database-reconciled",
            "--restart-mode",
            "paper",
        ]
    )

    exit_code = await live_cli._main_async(args)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    record = json.loads(audit_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["recorded"] is True
    assert record["event"] == "manual_emergency_stop"
    assert record["stopped_by"] == "operator-1"
