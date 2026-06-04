"""Export the strict H1 FLB warehouse CSV from Dune raw SQL.

The produced CSV is the non-secret input for ``scripts/flb_data_feasibility.py
--source warehouse-csv``.  Dune authentication is read from ``DUNE_API_KEY`` by
default so the secret stays out of CLI history and config files.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import httpx

from scripts.artifact_path_safety import require_path_outside_working_tree
from scripts.flb_data_feasibility import (
    FAVORITE_SIGNAL_NAME,
    LONGSHOT_SIGNAL_NAME,
    SAMPLE_GATE_MIN,
    SampleGateResult,
    check_sample_gate,
    load_warehouse_markets,
)


DUNE_API_BASE_URL = "https://api.dune.com/api/v1"
DEFAULT_SQL_PATH = Path("docs/research/flb_polymarket_resolved_binary_dune.sql")
SUCCESS_STATE = "QUERY_STATE_COMPLETED"
FAILED_STATES = frozenset(
    {
        "QUERY_STATE_FAILED",
        "QUERY_STATE_CANCELLED",
        "QUERY_STATE_EXPIRED",
        "QUERY_STATE_COMPLETED_PARTIAL",
    }
)
PERFORMANCE_TIERS = frozenset({"small", "medium", "large"})


class DuneExecutionFailed(RuntimeError):
    """Raised when Dune reports a terminal non-success execution state."""


class FlbSampleGateFailed(RuntimeError):
    """Raised when a valid export is not launch-viable for H1 FLB."""


@dataclass(frozen=True, slots=True)
class DuneExportStats:
    execution_id: str
    market_count: int
    skipped_50_50_count: int
    longshot_count: int
    favorite_count: int
    output_path: Path


def export_flb_warehouse_from_dune(
    *,
    sql_path: Path,
    output_path: Path,
    api_key: str,
    base_url: str = DUNE_API_BASE_URL,
    performance: str = "medium",
    poll_interval_s: float = 10.0,
    timeout_s: float = 3_600.0,
    require_sample_gate: bool = True,
    http_client: httpx.Client | None = None,
) -> DuneExportStats:
    """Execute a Dune SQL export and publish a validated warehouse CSV.

    The downloaded CSV is written to a temporary file, parsed by the same
    strict warehouse loader used by FLB calibration, and only then atomically
    published to ``output_path``.
    """
    if performance not in PERFORMANCE_TIERS:
        msg = f"performance must be one of: {', '.join(sorted(PERFORMANCE_TIERS))}"
        raise ValueError(msg)
    if poll_interval_s < 0.0:
        msg = "poll_interval_s must be non-negative"
        raise ValueError(msg)
    if timeout_s <= 0.0:
        msg = "timeout_s must be positive"
        raise ValueError(msg)

    output_path = output_path.expanduser()
    require_path_outside_working_tree(
        output_path,
        label="Dune warehouse export output path",
    )
    _prepare_private_parent(output_path)

    sql = sql_path.read_text(encoding="utf-8").strip()
    if sql == "":
        msg = f"Dune SQL file is empty: {sql_path}"
        raise ValueError(msg)

    if http_client is not None:
        return _export_with_client(
            client=http_client,
            sql=sql,
            output_path=output_path,
            api_key=api_key,
            performance=performance,
            poll_interval_s=poll_interval_s,
            timeout_s=timeout_s,
            require_sample_gate=require_sample_gate,
        )

    with httpx.Client(base_url=base_url, timeout=60.0) as client:
        return _export_with_client(
            client=client,
            sql=sql,
            output_path=output_path,
            api_key=api_key,
            performance=performance,
            poll_interval_s=poll_interval_s,
            timeout_s=timeout_s,
            require_sample_gate=require_sample_gate,
        )


def _export_with_client(
    *,
    client: httpx.Client,
    sql: str,
    output_path: Path,
    api_key: str,
    performance: str,
    poll_interval_s: float,
    timeout_s: float,
    require_sample_gate: bool,
) -> DuneExportStats:
    execution_id = _submit_sql(
        client=client,
        sql=sql,
        api_key=api_key,
        performance=performance,
    )
    _wait_for_success(
        client=client,
        execution_id=execution_id,
        api_key=api_key,
        poll_interval_s=poll_interval_s,
        timeout_s=timeout_s,
    )
    csv_text = _download_csv(
        client=client,
        execution_id=execution_id,
        api_key=api_key,
    )
    return _publish_validated_csv(
        csv_text=csv_text,
        output_path=output_path,
        execution_id=execution_id,
        require_sample_gate=require_sample_gate,
    )


def _submit_sql(
    *,
    client: httpx.Client,
    sql: str,
    api_key: str,
    performance: str,
) -> str:
    response = client.post(
        "/sql/execute",
        headers=_auth_headers(api_key),
        json={"sql": sql, "performance": performance},
    )
    response.raise_for_status()
    payload = _json_mapping(response)
    execution_id = _required_text(payload, "execution_id")
    return execution_id


def _wait_for_success(
    *,
    client: httpx.Client,
    execution_id: str,
    api_key: str,
    poll_interval_s: float,
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        status = _fetch_status(client=client, execution_id=execution_id, api_key=api_key)
        state = _optional_text(status, "state")
        if state == SUCCESS_STATE:
            return
        if state in FAILED_STATES:
            raise DuneExecutionFailed(_format_failed_execution(execution_id, status))
        if bool(status.get("is_execution_finished")):
            raise DuneExecutionFailed(_format_failed_execution(execution_id, status))
        if time.monotonic() >= deadline:
            msg = f"Dune execution timed out before completion: {execution_id}"
            raise TimeoutError(msg)
        if poll_interval_s > 0.0:
            time.sleep(poll_interval_s)


def _fetch_status(
    *,
    client: httpx.Client,
    execution_id: str,
    api_key: str,
) -> Mapping[str, object]:
    response = client.get(
        f"/execution/{execution_id}/status",
        headers=_auth_headers(api_key),
    )
    response.raise_for_status()
    return _json_mapping(response)


def _download_csv(
    *,
    client: httpx.Client,
    execution_id: str,
    api_key: str,
) -> str:
    response = client.get(
        f"/execution/{execution_id}/results/csv",
        headers=_auth_headers(api_key),
    )
    response.raise_for_status()
    text = response.text
    if text.strip() == "":
        msg = f"Dune execution returned an empty CSV result: {execution_id}"
        raise ValueError(msg)
    return text


def _publish_validated_csv(
    *,
    csv_text: str,
    output_path: Path,
    execution_id: str,
    require_sample_gate: bool,
) -> DuneExportStats:
    output_path = output_path.expanduser()
    require_path_outside_working_tree(
        output_path,
        label="Dune warehouse export output path",
    )
    _prepare_private_parent(output_path)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        text=True,
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as csv_file:
            csv_file.write(csv_text)
        temp_path.chmod(0o600)
        markets, skipped_50_50 = load_warehouse_markets(temp_path)
        if not markets:
            msg = "Dune warehouse export contained no strict resolved binary markets"
            raise ValueError(msg)
        gate = check_sample_gate(markets)
        if require_sample_gate and not gate.passed:
            raise FlbSampleGateFailed(_sample_gate_failure_message(gate))
        temp_path.replace(output_path)
        output_path.chmod(0o600)
        return DuneExportStats(
            execution_id=execution_id,
            market_count=len(markets),
            skipped_50_50_count=skipped_50_50,
            longshot_count=gate.longshot_count,
            favorite_count=gate.favorite_count,
            output_path=output_path,
        )
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _prepare_private_parent(path: Path) -> None:
    parent = path.parent
    if parent.exists():
        mode = parent.lstat().st_mode
        if not parent.is_dir() or parent.is_symlink():
            msg = f"output parent is not a directory: {parent}"
            raise ValueError(msg)
    else:
        parent.mkdir(parents=True, mode=0o700)
        parent.chmod(0o700)

    permissions = parent.stat().st_mode & 0o777
    if permissions & 0o077:
        msg = f"output parent {parent} is too permissive; run chmod 700"
        raise ValueError(msg)
    if not permissions & 0o200:
        msg = f"output parent {parent} is not owner-writable; run chmod 700"
        raise ValueError(msg)


def _sample_gate_failure_message(gate: SampleGateResult) -> str:
    failures: list[str] = []
    if not gate.longshot_passed:
        failures.append(
            f"{LONGSHOT_SIGNAL_NAME} {gate.longshot_count} < {SAMPLE_GATE_MIN}"
        )
    if not gate.favorite_passed:
        failures.append(
            f"{FAVORITE_SIGNAL_NAME} {gate.favorite_count} < {SAMPLE_GATE_MIN}"
        )
    return "insufficient FLB runtime samples: " + "; ".join(failures)


def _format_failed_execution(execution_id: str, status: Mapping[str, object]) -> str:
    state = _optional_text(status, "state") or "unknown"
    error = status.get("error")
    if isinstance(error, Mapping):
        error_message = _optional_text(error, "message")
    else:
        error_message = None
    if error_message is None:
        error_message = _optional_text(status, "error_message")
    suffix = f": {error_message}" if error_message else ""
    return f"Dune execution {execution_id} ended in {state}{suffix}"


def _auth_headers(api_key: str) -> dict[str, str]:
    return {"X-Dune-Api-Key": api_key}


def _json_mapping(response: httpx.Response) -> Mapping[str, object]:
    payload: object = response.json()
    if not isinstance(payload, Mapping):
        msg = "Expected Dune API JSON response to be an object"
        raise ValueError(msg)
    return payload


def _required_text(payload: Mapping[str, object], key: str) -> str:
    value = _optional_text(payload, key)
    if value is None:
        msg = f"Dune API response missing {key}"
        raise ValueError(msg)
    return value


def _optional_text(payload: Mapping[str, object], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str):
        text = value.strip()
        return text if text != "" else None
    return None


def _api_key_from_env(env_var: str) -> str:
    api_key = os.environ.get(env_var)
    if api_key is None or api_key.strip() == "":
        msg = f"{env_var} is required for Dune export"
        raise ValueError(msg)
    return api_key.strip()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export and validate the strict H1 FLB Dune warehouse CSV."
    )
    parser.add_argument(
        "--sql",
        type=Path,
        default=DEFAULT_SQL_PATH,
        help=f"Dune SQL file. Default: {DEFAULT_SQL_PATH}",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--api-key-env",
        default="DUNE_API_KEY",
        help="Environment variable containing the Dune API key.",
    )
    parser.add_argument(
        "--base-url",
        default=DUNE_API_BASE_URL,
        help=f"Dune API base URL. Default: {DUNE_API_BASE_URL}",
    )
    parser.add_argument(
        "--performance",
        choices=sorted(PERFORMANCE_TIERS),
        default="medium",
        help="Dune execution tier.",
    )
    parser.add_argument("--poll-interval-s", type=float, default=10.0)
    parser.add_argument("--timeout-s", type=float, default=3_600.0)
    parser.add_argument(
        "--allow-under-sampled",
        action="store_true",
        help=(
            "Publish a semantically valid export even if the H1 FLB sample "
            "gate fails. Do not use this for launch artifacts."
        ),
    )
    args = parser.parse_args(argv)

    try:
        stats = export_flb_warehouse_from_dune(
            sql_path=args.sql,
            output_path=args.output,
            api_key=_api_key_from_env(str(args.api_key_env)),
            base_url=str(args.base_url),
            performance=str(args.performance),
            poll_interval_s=float(args.poll_interval_s),
            timeout_s=float(args.timeout_s),
            require_sample_gate=not bool(args.allow_under_sampled),
        )
    except FlbSampleGateFailed as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except (DuneExecutionFailed, OSError, TimeoutError, ValueError, httpx.HTTPError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(
        "Dune FLB warehouse export completed "
        f"execution_id={stats.execution_id} "
        f"markets={stats.market_count} "
        f"skipped_50_50={stats.skipped_50_50_count} "
        f"longshot_count={stats.longshot_count} "
        f"favorite_count={stats.favorite_count} "
        f"output={stats.output_path}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
