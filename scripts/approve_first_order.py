"""Operator helper for the first-live-order approval gate (STO-10).

Given an `OperatorApprovalRequiredError` message (or the equivalent
preview fields) and an `approver_id`, write:

  1. The approval JSON at the configured path. Fields match exactly
     what `_approval_payload_matches`
     (`src/pms/actuator/adapters/polymarket.py`) checks, so the gate
     matches on the next decision without operator typo risk.
  2. The sidecar `<path>.meta.json` so
     `FileFirstLiveOrderGate.read_approver_id` populates the
     `approver_id` field on every audit-log event for this
     authorization.

Both files are written with mode 0o600 — the runner UID is the only
reader. By default the helper refuses to overwrite an existing approval
file (would clobber a still-pending authorization); pass `--force` to
override.

Operator usage:

    uv run python scripts/approve_first_order.py \\
        --from-error 'First Polymarket live order requires operator \\
            approval: venue=polymarket market=... token=... side=BUY \\
            outcome=YES max_notional_usdc=5.0 limit_price=0.4 \\
            max_slippage_bps=50' \\
        --approver-id alice@example \\
        --path /data/pms/first-order.json
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import stat
import sys
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import yaml

from pms.config import safe_load_yaml_no_duplicate_keys, yaml_load_error_message


@dataclass(frozen=True, slots=True)
class ApprovalPreview:
    venue: str
    market_id: str
    token_id: str | None
    side: str
    outcome: str
    max_notional_usdc: float
    limit_price: float
    max_slippage_bps: int


# Map error-message keys (e.g. `market=`) to ApprovalPreview field names.
# The actuator's f-string format uses the short keys for readability;
# the JSON the gate matches uses the long names. Centralise the mapping.
_KEY_ALIASES: dict[str, str] = {
    "venue": "venue",
    "market": "market_id",
    "token": "token_id",
    "side": "side",
    "outcome": "outcome",
    "max_notional_usdc": "max_notional_usdc",
    "limit_price": "limit_price",
    "max_slippage_bps": "max_slippage_bps",
}

_TOKEN_RE = re.compile(r"(\w+)=(\S+)")


def parse_preview_from_error(message: str) -> ApprovalPreview:
    """Parse an `OperatorApprovalRequiredError` message into an
    `ApprovalPreview`. Raises ValueError if any required field is
    missing — better than silently producing a partial JSON that would
    fail the gate match without a clear cause."""
    raw: dict[str, str] = {}
    for match in _TOKEN_RE.finditer(message):
        key = match.group(1)
        value = match.group(2)
        if key in _KEY_ALIASES:
            raw[_KEY_ALIASES[key]] = value

    required_fields = (
        "venue",
        "market_id",
        "token_id",
        "side",
        "outcome",
        "max_notional_usdc",
        "limit_price",
        "max_slippage_bps",
    )
    missing = [field for field in required_fields if field not in raw]
    if missing:
        raise ValueError(
            f"missing required preview fields in error message: {missing!r}"
        )

    token_id: str | None = raw["token_id"]
    if token_id == "None":
        token_id = None

    preview = ApprovalPreview(
        venue=raw["venue"],
        market_id=raw["market_id"],
        token_id=token_id,
        side=raw["side"],
        outcome=raw["outcome"],
        max_notional_usdc=float(raw["max_notional_usdc"]),
        limit_price=float(raw["limit_price"]),
        max_slippage_bps=int(raw["max_slippage_bps"]),
    )
    _require_actionable_preview(preview)
    return preview


def write_approval(
    preview: ApprovalPreview,
    *,
    path: Path,
    approver_id: str,
    ts: datetime,
    force: bool = False,
) -> tuple[Path, Path]:
    """Write the approval JSON and the sidecar `<path>.meta.json`.

    Returns the (approval_path, sidecar_path) pair. Both files have
    mode 0o600. Refuses to overwrite an existing approval file unless
    `force=True` — guards against clobbering a pending authorization.
    """
    sidecar_path = Path(str(path) + ".meta.json")

    _require_actionable_preview(preview)
    normalized_approver_id = _require_actionable_approver_id(approver_id)
    _require_approval_path_outside_working_tree(path)
    _require_regular_file_or_absent(path)
    _require_regular_file_or_absent(sidecar_path)

    if not force:
        if path.exists():
            raise FileExistsError(
                f"approval file already exists at {path}; pass force=True to overwrite"
            )
        if sidecar_path.exists():
            raise FileExistsError(
                f"approval sidecar already exists at {sidecar_path}; "
                "pass force=True to overwrite"
            )

    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    _require_private_parent_directory(path)
    _require_regular_file_or_absent(path)
    _require_regular_file_or_absent(sidecar_path)

    payload: dict[str, Any] = {"approved": True, **asdict(preview)}
    sidecar_payload: dict[str, Any] = {
        "approver_id": normalized_approver_id,
        "approval_sha256": approval_payload_hash(payload),
        "ts": ts.isoformat(),
    }

    # Write order matters: the gate matches on the approval JSON, then
    # `read_approver_id` reads the sidecar. If the approval JSON
    # appeared first, a running actuator could match between the two
    # writes and emit `approval_matched` with `approver_id: null`.
    # Writing the sidecar first guarantees identity is on disk before
    # the approval is observable to the gate. If either write raises,
    # the approval JSON is never newly published; when this invocation
    # created the sidecar, remove it so final preflight/startup are not
    # blocked by stale metadata from a failed helper run.
    previous_approval_content = _read_secret_file_if_present(path)
    previous_sidecar_content = _read_secret_file_if_present(sidecar_path)
    sidecar_written = False
    approval_written = False
    try:
        _write_secret_file(
            sidecar_path,
            json.dumps(sidecar_payload, allow_nan=False, sort_keys=True),
        )
        sidecar_written = True
        _write_secret_file(path, json.dumps(payload, allow_nan=False, sort_keys=True))
        approval_written = True
    except BaseException:
        if not approval_written:
            try:
                if previous_approval_content is None:
                    _unlink_regular_single_link_file_if_present(path)
                else:
                    _write_secret_bytes(path, previous_approval_content)
            except BaseException:
                pass
        if sidecar_written:
            try:
                if previous_sidecar_content is None:
                    _unlink_regular_single_link_file_if_present(sidecar_path)
                else:
                    _write_secret_bytes(sidecar_path, previous_sidecar_content)
            except BaseException:
                pass
        raise

    return path, sidecar_path


def _require_actionable_preview(preview: ApprovalPreview) -> None:
    _require_concrete_preview_text("venue", preview.venue)
    if preview.venue != "polymarket":
        raise ValueError("venue must be polymarket")

    _require_concrete_preview_text("market_id", preview.market_id)
    if preview.token_id is not None:
        _require_concrete_preview_text("token_id", preview.token_id)

    _require_concrete_preview_text("side", preview.side)
    if preview.side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")

    _require_concrete_preview_text("outcome", preview.outcome)
    if preview.outcome not in {"YES", "NO"}:
        raise ValueError("outcome must be YES or NO")

    _require_finite_preview_float(
        "max_notional_usdc",
        preview.max_notional_usdc,
    )
    if preview.max_notional_usdc <= 0.0:
        raise ValueError("max_notional_usdc must be > 0")

    _require_finite_preview_float("limit_price", preview.limit_price)
    if not 0.0 < preview.limit_price <= 1.0:
        raise ValueError("limit_price must be > 0 and <= 1")

    if preview.max_slippage_bps < 0:
        raise ValueError("max_slippage_bps must be >= 0")


def _require_concrete_preview_text(field_name: str, value: str) -> None:
    normalized = value.strip()
    if normalized == "":
        raise ValueError(f"{field_name} must be non-empty")
    if normalized != value:
        raise ValueError(f"{field_name} must not have surrounding whitespace")
    if _looks_like_placeholder(normalized):
        raise ValueError(f"{field_name} must not contain a placeholder")
    if any(character in normalized for character in ("|", "\n", "\r")):
        raise ValueError(
            f"{field_name} must not contain Markdown table delimiters or newlines"
        )


def _require_finite_preview_float(field_name: str, value: float) -> None:
    if math.isfinite(value):
        return
    raise ValueError(f"{field_name} must be finite")


def _read_secret_file_if_present(path: Path) -> bytes | None:
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    _require_regular_file_or_absent(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except FileNotFoundError:
        return None
    try:
        _require_open_regular_single_link_file(fd, path)
        with os.fdopen(fd, "rb") as file:
            fd = -1
            return file.read()
    finally:
        if fd >= 0:
            os.close(fd)


def _write_secret_bytes(path: Path, content: bytes) -> None:
    _write_secret_bytes_atomically(path, content)


def _write_secret_bytes_atomically(path: Path, content: bytes) -> None:
    _require_regular_file_or_absent(path)
    fd, temp_path = _open_secret_temp_file(path)
    published = False
    try:
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(fd, "wb") as file:
            fd = -1
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        _require_regular_file_or_absent(path)
        os.replace(temp_path, path)
        published = True
        _fsync_parent_directory(path)
    finally:
        if fd >= 0:
            os.close(fd)
        if not published:
            _unlink_regular_single_link_file_if_present(temp_path)


def _unlink_regular_single_link_file_if_present(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_nlink != 1:
        return
    path.unlink()


def _write_secret_file(path: Path, content: str) -> None:
    """Write `content` to `path` with mode 0o600.

    Stages into a private file in the same directory, fsyncs it, then
    atomically replaces the final path so failed writes never expose a
    partial approval artifact."""
    _write_secret_bytes_atomically(path, f"{content}\n".encode("utf-8"))


def _open_secret_temp_file(path: Path) -> tuple[int, Path]:
    _require_regular_file_or_absent(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    for _ in range(16):
        temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            fd = os.open(temp_path, flags, 0o600)
        except FileExistsError:
            continue
        try:
            _require_open_regular_single_link_file(fd, temp_path)
            os.fchmod(fd, 0o600)
        except BaseException:
            os.close(fd)
            _unlink_regular_single_link_file_if_present(temp_path)
            raise
        return fd, temp_path
    raise FileExistsError(f"could not create temporary approval artifact for {path}")


def _fsync_parent_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path.parent, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        return
    finally:
        os.close(fd)


def _require_open_regular_single_link_file(fd: int, path: Path) -> None:
    path_stat = os.fstat(fd)
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError(f"approval artifact path is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(f"approval artifact path is not a single-link file: {path}")


def _require_regular_file_or_absent(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    mode = path_stat.st_mode
    if not stat.S_ISREG(mode):
        raise OSError(f"approval artifact path is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(f"approval artifact path is not a single-link file: {path}")


def _require_private_parent_directory(path: Path) -> None:
    parent = path.parent
    try:
        parent_stat = parent.lstat()
    except FileNotFoundError:
        raise OSError(f"approval artifact parent directory does not exist: {parent}")
    if not stat.S_ISDIR(parent_stat.st_mode):
        raise OSError(f"approval artifact parent path is not a directory: {parent}")
    mode = stat.S_IMODE(parent_stat.st_mode)
    if mode & 0o077:
        raise OSError(
            f"approval artifact parent directory {parent} is too permissive; "
            f"run `chmod 700 {parent}`."
        )
    if not mode & stat.S_IWUSR:
        raise OSError(
            f"approval artifact parent directory {parent} is not owner-writable; "
            f"run `chmod 700 {parent}`."
        )


def _require_actionable_approver_id(approver_id: str) -> str:
    normalized = approver_id.strip()
    if normalized == "":
        raise ValueError("approver_id must be non-empty")
    if _looks_like_placeholder(normalized):
        raise ValueError("approver_id must not contain a placeholder")
    if any(character in normalized for character in ("|", "\n", "\r")):
        raise ValueError(
            "approver_id must not contain Markdown table delimiters or newlines"
        )
    return normalized


def _looks_like_placeholder(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "":
        return False
    placeholder_markers = (
        "fill_in",
        "__fill",
        "<",
        ">",
        "todo",
        "replace",
        "placeholder",
    )
    return any(marker in normalized for marker in placeholder_markers)


def _require_approval_path_outside_working_tree(path: Path) -> None:
    configured_path = _absolute_path_without_symlink_resolution(path)
    resolved_path = path.expanduser().resolve(strict=False)
    working_tree = _working_tree_root(Path.cwd().resolve(strict=False))
    working_trees = [working_tree]
    for candidate in (configured_path, resolved_path):
        candidate_working_tree = _containing_working_tree_root(candidate)
        if candidate_working_tree is not None:
            working_trees.append(candidate_working_tree)

    for working_tree_candidate in dict.fromkeys(working_trees):
        if working_tree_candidate.parent == working_tree_candidate:
            continue
        for candidate in (configured_path, resolved_path):
            try:
                candidate.relative_to(working_tree_candidate)
            except ValueError:
                continue
            raise OSError(
                "approval artifact path must live outside the working tree: "
                f"{candidate}"
            )


def _absolute_path_without_symlink_resolution(path: Path) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return Path(os.path.abspath(expanded))


def _working_tree_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return start


def _containing_working_tree_root(path: Path) -> Path | None:
    start = path if path.is_dir() else path.parent
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def approval_payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a first-live-order approval JSON + sidecar.",
    )
    parser.add_argument(
        "--from-error",
        required=True,
        help="The OperatorApprovalRequiredError message to parse.",
    )
    parser.add_argument(
        "--approver-id",
        required=True,
        help="Identity of the human authorizing this order.",
    )
    parser.add_argument(
        "--path",
        default=None,
        help=(
            "Approval file path. Defaults to "
            "polymarket.first_live_order_approval_path from --config, then "
            "$PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH."
        ),
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("PMS_CONFIG_PATH"),
        help=(
            "PMS YAML config path used to guard against clobbering live "
            "secret, audit, preflight, and readiness artifacts. Defaults to "
            "$PMS_CONFIG_PATH when set."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing approval file. Default: refuse.",
    )
    args = parser.parse_args(argv)

    config_path = Path(args.config) if args.config else None
    config_data: dict[str, object] = {}
    if config_path is not None:
        try:
            config_data = _load_approval_helper_config(config_path)
        except (OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

    raw_approval_path = args.path
    if not raw_approval_path:
        raw_approval_path = _configured_approval_path(config_data)

    if not raw_approval_path:
        parser.error(
            "no approval path provided; pass --path, configure "
            "polymarket.first_live_order_approval_path, or set "
            "PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH"
        )

    try:
        preview = parse_preview_from_error(args.from_error)
    except ValueError as exc:
        # parser.error() is typed NoReturn (calls sys.exit), so no
        # follow-up return statement is needed and mypy strict (with
        # warn_unreachable) flags any dead code after.
        parser.error(f"could not parse error message: {exc}")

    normalized_approver_id = _require_actionable_approver_id(args.approver_id)
    ts = datetime.now(tz=UTC)
    approval_target = Path(raw_approval_path)
    try:
        if config_path is not None:
            _require_approval_outputs_distinct_from_configured_live_inputs(
                approval_target,
                config_path=config_path,
                config_data=config_data,
            )
        approval_path, sidecar_path = write_approval(
            preview,
            path=approval_target,
            approver_id=normalized_approver_id,
            ts=ts,
            force=args.force,
        )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(f"✓ Wrote approval JSON: {_display_path(approval_path)}")
    print(f"✓ Wrote sidecar:       {_display_path(sidecar_path)}")
    print(f"✓ Approver ID:         {normalized_approver_id}")
    print(f"✓ Timestamp:           {ts.isoformat()}")
    print(
        "The first-order gate will match on the next decision and submit; "
        "consume() will then unlink both files."
    )
    return 0


def _display_path(path: Path) -> str:
    return str(path.expanduser().resolve(strict=False))


def _load_approval_helper_config(config_path: Path) -> dict[str, object]:
    if not config_path.exists():
        return {}
    try:
        loaded = safe_load_yaml_no_duplicate_keys(
            _read_config_bytes_no_follow(config_path).decode("utf-8")
        )
    except OSError as exc:
        msg = f"Config file cannot be read safely: {config_path}"
        raise ValueError(msg) from exc
    except yaml.YAMLError as exc:
        msg = yaml_load_error_message(
            "Config file is not valid YAML",
            config_path,
            exc,
        )
        raise ValueError(msg) from None
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        msg = f"Expected mapping in config file {config_path}"
        raise ValueError(msg)
    if not all(isinstance(key, str) for key in loaded):
        msg = f"Expected string keys in config file {config_path}"
        raise ValueError(msg)
    return cast(dict[str, object], loaded)


def _read_config_bytes_no_follow(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(f"config path is not a regular file: {path}")
        if path_stat.st_nlink != 1:
            raise OSError(f"config path is not a single-link file: {path}")
        with os.fdopen(fd, "rb") as file:
            fd = -1
            return file.read()
    finally:
        if fd >= 0:
            os.close(fd)


def _configured_approval_path(config_data: Mapping[str, object]) -> str | None:
    return _configured_nested_str(
        config_data,
        "polymarket",
        "first_live_order_approval_path",
        env_name="PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH",
    )


def _require_approval_outputs_distinct_from_configured_live_inputs(
    approval_path: Path,
    *,
    config_path: Path,
    config_data: Mapping[str, object],
) -> None:
    output_paths = (
        ("approval artifact path", approval_path),
        ("approval sidecar path", Path(str(approval_path) + ".meta.json")),
    )
    configured_approval_path = _configured_approval_path(config_data)
    if (
        configured_approval_path is not None
        and configured_approval_path.strip() != ""
    ):
        configured_identities = _path_identities(Path(configured_approval_path))
        if not _path_identities_overlap(
            _path_identities(approval_path),
            configured_identities,
        ):
            msg = (
                "approval artifact path must match configured LIVE operator "
                f"approval path: {approval_path}"
            )
            raise ValueError(msg)

    protected_paths = (
        ("LIVE config file", str(config_path)),
        *_configured_live_input_paths(config_data),
    )
    for output_label, output_path in output_paths:
        output_identities = _path_identities(output_path)
        for protected_label, raw_path in protected_paths:
            if raw_path is None or raw_path.strip() == "":
                continue
            protected_identities = _path_identities(Path(raw_path))
            if not _path_identities_overlap(output_identities, protected_identities):
                continue
            msg = (
                f"{output_label} must be distinct from "
                f"{protected_label}: {output_path}"
            )
            raise ValueError(msg)


def _configured_live_input_paths(
    config_data: Mapping[str, object],
) -> tuple[tuple[str, str | None], ...]:
    return (
        (
            "LIVE credentialed preflight artifact",
            _configured_str(
                config_data,
                "live_preflight_artifact_path",
                env_name="PMS_LIVE_PREFLIGHT_ARTIFACT_PATH",
            ),
        ),
        (
            "LIVE first-order audit path",
            _configured_str(
                config_data,
                "live_first_order_audit_path",
                env_name="PMS_LIVE_FIRST_ORDER_AUDIT_PATH",
            ),
        ),
        (
            "LIVE emergency audit path",
            _configured_str(
                config_data,
                "live_emergency_audit_path",
                env_name="PMS_LIVE_EMERGENCY_AUDIT_PATH",
            ),
        ),
        (
            "LIVE local secret file",
            _configured_str(
                config_data,
                "local_secret_file",
                env_name="PMS_LOCAL_SECRET_FILE",
            ),
        ),
        (
            "LIVE paper soak GO report",
            _configured_str(
                config_data,
                "live_paper_soak_report_path",
                env_name="PMS_LIVE_PAPER_SOAK_REPORT_PATH",
            ),
        ),
        (
            "LIVE operator rehearsal report",
            _configured_str(
                config_data,
                "live_operator_rehearsal_report_path",
                env_name="PMS_LIVE_OPERATOR_REHEARSAL_REPORT_PATH",
            ),
        ),
        (
            "LIVE execution-model artifact",
            _configured_str(
                config_data,
                "live_execution_model_path",
                env_name="PMS_LIVE_EXECUTION_MODEL_PATH",
            ),
        ),
        (
            "LIVE paper-vs-backtest execution diff artifact",
            _configured_str(
                config_data,
                "live_paper_backtest_diff_path",
                env_name="PMS_LIVE_PAPER_BACKTEST_DIFF_PATH",
            ),
        ),
        (
            "LIVE category-prior artifact",
            _configured_nested_str(
                config_data,
                "controller",
                "category_prior_observations_path",
                env_name="PMS_CONTROLLER__CATEGORY_PRIOR_OBSERVATIONS_PATH",
            ),
        ),
        (
            "LIVE FLB calibration artifact",
            _configured_nested_str(
                config_data,
                "strategies",
                "flb_calibration_path",
                env_name="PMS_STRATEGIES__FLB_CALIBRATION_PATH",
            ),
        ),
        (
            "LIVE discord alert directory",
            _configured_nested_str(
                config_data,
                "discord",
                "alert_dir",
                env_name="PMS_DISCORD__ALERT_DIR",
            ),
        ),
    )


def _configured_str(
    config_data: Mapping[str, object],
    key: str,
    *,
    env_name: str | None = None,
) -> str | None:
    value = config_data.get(key)
    if isinstance(value, str):
        return value
    if env_name is None:
        return None
    env_value = os.environ.get(env_name)
    if env_value is None:
        return None
    return env_value


def _configured_nested_str(
    config_data: Mapping[str, object],
    section: str,
    key: str,
    *,
    env_name: str | None = None,
) -> str | None:
    section_data = config_data.get(section)
    if isinstance(section_data, Mapping):
        value = section_data.get(key)
        if isinstance(value, str):
            return value
    if env_name is None:
        return None
    env_value = os.environ.get(env_name)
    if env_value is None:
        return None
    return env_value


def _path_identities(path: Path) -> frozenset[Path]:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return frozenset(
        (
            Path(os.path.abspath(expanded)),
            expanded.resolve(strict=False),
        )
    )


def _path_identities_overlap(left: frozenset[Path], right: frozenset[Path]) -> bool:
    return any(
        _paths_overlap(left_path, right_path)
        for left_path in left
        for right_path in right
    )


def _paths_overlap(left: Path, right: Path) -> bool:
    if left == right:
        return True
    try:
        right.relative_to(left)
    except ValueError:
        pass
    else:
        return True
    try:
        left.relative_to(right)
    except ValueError:
        return False
    return True


if __name__ == "__main__":
    sys.exit(main())
