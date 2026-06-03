from __future__ import annotations

import csv
import json
import stat
from pathlib import Path

import httpx
import pytest

from scripts.export_flb_warehouse_from_dune import (
    DuneExecutionFailed,
    export_flb_warehouse_from_dune,
    main,
)


WAREHOUSE_COLUMNS = [
    "market_id",
    "question",
    "entry_yes_price",
    "yes_payout",
    "no_payout",
    "volume",
    "liquidity",
    "entry_timestamp",
    "resolved_at",
    "category",
]


def _warehouse_csv(rows: list[dict[str, str]]) -> str:
    output: list[str] = []

    class _ListWriter:
        def write(self, value: str) -> int:
            output.append(value)
            return len(value)

    writer = csv.DictWriter(_ListWriter(), fieldnames=WAREHOUSE_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return "".join(output)


def _warehouse_row(
    *,
    market_id: str,
    entry_yes_price: str = "0.05",
    yes_payout: str = "0",
    no_payout: str = "1",
) -> dict[str, str]:
    return {
        "market_id": market_id,
        "question": f"Warehouse market {market_id}?",
        "entry_yes_price": entry_yes_price,
        "yes_payout": yes_payout,
        "no_payout": no_payout,
        "volume": "10000",
        "liquidity": "500",
        "entry_timestamp": "2025-12-01T00:00:00Z",
        "resolved_at": "2026-01-01T00:00:00Z",
        "category": "politics",
    }


def test_dune_export_executes_raw_sql_polls_and_publishes_validated_csv(
    tmp_path: Path,
) -> None:
    sql_path = tmp_path / "query.sql"
    sql_path.write_text("select * from launch_source", encoding="utf-8")
    output_path = tmp_path / "secure" / "polymarket_resolved_binary.csv"
    requests: list[httpx.Request] = []
    csv_text = _warehouse_csv([
        _warehouse_row(market_id="longshot-1"),
        _warehouse_row(
            market_id="favorite-1",
            entry_yes_price="0.95",
            yes_payout="1",
            no_payout="0",
        ),
    ])

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/sql/execute":
            assert request.headers["X-Dune-Api-Key"] == "redacted-api-key"
            assert json.loads(request.content) == {
                "performance": "large",
                "sql": "select * from launch_source",
            }
            return httpx.Response(
                200,
                json={"execution_id": "exec-1", "state": "QUERY_STATE_PENDING"},
            )
        if request.url.path == "/api/v1/execution/exec-1/status":
            return httpx.Response(
                200,
                json={
                    "execution_id": "exec-1",
                    "is_execution_finished": True,
                    "state": "QUERY_STATE_COMPLETED",
                },
            )
        if request.url.path == "/api/v1/execution/exec-1/results/csv":
            return httpx.Response(200, text=csv_text)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="https://api.dune.com/api/v1",
    ) as client:
        stats = export_flb_warehouse_from_dune(
            sql_path=sql_path,
            output_path=output_path,
            api_key="redacted-api-key",
            performance="large",
            poll_interval_s=0.0,
            timeout_s=1.0,
            require_sample_gate=False,
            http_client=client,
        )

    assert [request.url.path for request in requests] == [
        "/api/v1/sql/execute",
        "/api/v1/execution/exec-1/status",
        "/api/v1/execution/exec-1/results/csv",
    ]
    assert stats.execution_id == "exec-1"
    assert stats.market_count == 2
    assert stats.skipped_50_50_count == 0
    assert stats.longshot_count == 1
    assert stats.favorite_count == 1
    assert output_path.read_text(encoding="utf-8").splitlines() == csv_text.splitlines()
    assert stat.S_IMODE(output_path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(output_path.stat().st_mode) == 0o600


def test_dune_export_rejects_invalid_warehouse_csv_without_publishing(
    tmp_path: Path,
) -> None:
    sql_path = tmp_path / "query.sql"
    sql_path.write_text("select invalid", encoding="utf-8")
    output_path = tmp_path / "secure" / "polymarket_resolved_binary.csv"
    output_path.parent.mkdir(mode=0o700)
    output_path.write_text("existing export\n", encoding="utf-8")
    invalid_csv = _warehouse_csv([
        _warehouse_row(market_id="invalid", yes_payout="0.995", no_payout="0.005")
    ])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/sql/execute":
            return httpx.Response(200, json={"execution_id": "exec-invalid"})
        if request.url.path == "/api/v1/execution/exec-invalid/status":
            return httpx.Response(
                200,
                json={
                    "execution_id": "exec-invalid",
                    "is_execution_finished": True,
                    "state": "QUERY_STATE_COMPLETED",
                },
            )
        if request.url.path == "/api/v1/execution/exec-invalid/results/csv":
            return httpx.Response(200, text=invalid_csv)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="https://api.dune.com/api/v1",
    ) as client:
        with pytest.raises(ValueError, match="expected exact final payout vector"):
            export_flb_warehouse_from_dune(
                sql_path=sql_path,
                output_path=output_path,
                api_key="redacted-api-key",
                poll_interval_s=0.0,
                timeout_s=1.0,
                require_sample_gate=False,
                http_client=client,
            )

    assert output_path.read_text(encoding="utf-8") == "existing export\n"


def test_dune_export_rejects_output_inside_working_tree_without_publishing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    (repo_dir / ".git").mkdir()
    sql_path = repo_dir / "query.sql"
    sql_path.write_text("select * from launch_source", encoding="utf-8")
    output_dir = repo_dir / "secure"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "polymarket_resolved_binary.csv"
    requests: list[httpx.Request] = []
    monkeypatch.chdir(repo_dir)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500, json={"error": "should not be called"})

    with httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="https://api.dune.com/api/v1",
    ) as client:
        with pytest.raises(ValueError, match="outside the working tree"):
            export_flb_warehouse_from_dune(
                sql_path=sql_path,
                output_path=output_path,
                api_key="redacted-api-key",
                performance="large",
                poll_interval_s=0.0,
                timeout_s=1.0,
                require_sample_gate=False,
                http_client=client,
            )

    assert requests == []
    assert not output_path.exists()


def test_dune_export_requires_runtime_sample_gate_before_publishing(
    tmp_path: Path,
) -> None:
    sql_path = tmp_path / "query.sql"
    sql_path.write_text("select under_sampled", encoding="utf-8")
    output_path = tmp_path / "secure" / "polymarket_resolved_binary.csv"
    csv_text = _warehouse_csv([_warehouse_row(market_id="longshot-1")])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/sql/execute":
            return httpx.Response(200, json={"execution_id": "exec-under"})
        if request.url.path == "/api/v1/execution/exec-under/status":
            return httpx.Response(
                200,
                json={
                    "execution_id": "exec-under",
                    "is_execution_finished": True,
                    "state": "QUERY_STATE_COMPLETED",
                },
            )
        if request.url.path == "/api/v1/execution/exec-under/results/csv":
            return httpx.Response(200, text=csv_text)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="https://api.dune.com/api/v1",
    ) as client:
        with pytest.raises(RuntimeError, match="insufficient FLB runtime samples"):
            export_flb_warehouse_from_dune(
                sql_path=sql_path,
                output_path=output_path,
                api_key="redacted-api-key",
                poll_interval_s=0.0,
                timeout_s=1.0,
                http_client=client,
            )

    assert not output_path.exists()


def test_dune_export_reports_failed_execution_without_api_key_leak(
    tmp_path: Path,
) -> None:
    sql_path = tmp_path / "query.sql"
    sql_path.write_text("select broken", encoding="utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/sql/execute":
            return httpx.Response(200, json={"execution_id": "exec-failed"})
        if request.url.path == "/api/v1/execution/exec-failed/status":
            return httpx.Response(
                200,
                json={
                    "execution_id": "exec-failed",
                    "is_execution_finished": True,
                    "state": "QUERY_STATE_FAILED",
                    "error": {"message": "line 7: bad column"},
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="https://api.dune.com/api/v1",
    ) as client:
        with pytest.raises(DuneExecutionFailed) as exc_info:
            export_flb_warehouse_from_dune(
                sql_path=sql_path,
                output_path=tmp_path / "secure" / "out.csv",
                api_key="secret-that-must-not-leak",
                poll_interval_s=0.0,
                timeout_s=1.0,
                http_client=client,
            )

    message = str(exc_info.value)
    assert "QUERY_STATE_FAILED" in message
    assert "line 7: bad column" in message
    assert "secret-that-must-not-leak" not in message


def test_cli_requires_api_key_from_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sql_path = tmp_path / "query.sql"
    sql_path.write_text("select 1", encoding="utf-8")
    monkeypatch.delenv("DUNE_API_KEY", raising=False)

    exit_code = main([
        "--sql",
        str(sql_path),
        "--output",
        str(tmp_path / "secure" / "out.csv"),
    ])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "DUNE_API_KEY is required for Dune export" in captured.err
