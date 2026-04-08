"""Tests for pms.tool_harness.subprocess_runner (P2-02).

These tests cover the unit-level behavior of the subprocess runner with
all real subprocess calls monkey-patched out — they must run in <1s and
require no network. The single end-to-end integration test that runs
``py-clob-client`` against the real Polymarket API is gated behind
``@pytest.mark.integration`` so CI can opt out via ``-m 'not integration'``.

Coverage:

* :func:`make_subprocess_test_fn` happy path with a fake ``_prepare_env``
* install timeout / failure → survival item returns ``False`` with error
* probe missing → survival item returns ``False`` cleanly
* probe success / failure / missing-credentials (exit 2)
* :class:`SubprocessRunnerFactory` reuses one session per candidate
* :meth:`SubprocessRunnerFactory.cleanup_all` releases temp dirs
* unsupported language returns clear error
* unsupported install command shape rejected by parser
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from pms.tool_harness import (
    Candidate,
    SubprocessRunnerFactory,
    SubprocessSession,
    UnsupportedCandidateError,
    make_subprocess_test_fn,
)
from pms.tool_harness import subprocess_runner as sr
from pms.tool_harness.schema import SurvivalGateItem


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_python_candidate(
    name: str = "fake-python-tool",
    install: str = "pip install fake-python-tool==1.2.3",
) -> Candidate:
    return Candidate(
        name=name,
        repo="https://example.com/fake",
        language="python",
        install=install,
        platforms=["polymarket"],
        module="data_connector",
        notes="fake test candidate",
        config={},
    )


def _make_typescript_candidate(name: str = "fake-ts-tool") -> Candidate:
    return Candidate(
        name=name,
        repo="https://example.com/fake-ts",
        language="typescript",
        install="npm install fake-ts-tool@2.0.0",
        platforms=["polymarket"],
        module="data_connector",
        notes="fake ts candidate",
        config={},
    )


def _make_rust_candidate(name: str = "fake-rs-tool") -> Candidate:
    return Candidate(
        name=name,
        repo="https://example.com/fake-rs",
        language="rust",
        install="cargo add fake-rs-tool@0.1.0",
        platforms=["polymarket"],
        module="data_connector",
        notes="fake rust candidate",
        config={},
    )


def _gate_item(item_id: str, timeout: int = 10) -> SurvivalGateItem:
    return SurvivalGateItem(
        id=item_id, test=f"{item_id} test", timeout_seconds=timeout
    )


@dataclass
class _StubResult:
    rc: int
    stdout: str = ""
    stderr: str = ""


def _stub_run_subprocess(
    monkeypatch: pytest.MonkeyPatch, results: dict[str, _StubResult]
) -> list[list[str]]:
    """Replace ``_run_subprocess`` with a deterministic stub.

    ``results`` keys are matched against the first arg of each invocation
    (the executable name) so we can return different results for ``uv``,
    ``pip``, ``node``, etc.
    """
    calls: list[list[str]] = []

    async def fake(
        argv: list[str],
        cwd: Path,
        timeout_seconds: int,
        env: dict[str, str] | None = None,
    ) -> tuple[int, str, str]:
        calls.append(list(argv))
        # Match against the leaf binary name (e.g. "/tmp/.../python" → "python").
        leaf = Path(argv[0]).name
        for key, result in results.items():
            if key == leaf or key in argv[0]:
                return result.rc, result.stdout, result.stderr
        # Fallback: success.
        return 0, "", ""

    monkeypatch.setattr(sr, "_run_subprocess", fake)
    return calls


# ---------------------------------------------------------------------------
# Install command parser
# ---------------------------------------------------------------------------


def test_parse_python_install_pip_form() -> None:
    """``pip install <pkg>`` is rewritten to ``uv pip install --python <venv> <pkg>``.

    The rewrite is intentional: ``uv venv`` produces a venv without pip
    installed, so a literal ``<venv>/bin/python -m pip install`` would
    crash with "No module named pip". Routing through ``uv pip install
    --python`` lets uv resolve directly against the target venv.
    """
    argv = sr._parse_python_install(
        "pip install py-clob-client==0.34.6", Path("/tmp/v/bin/python")
    )
    assert argv[:5] == ["uv", "pip", "install", "--python", "/tmp/v/bin/python"]
    assert "py-clob-client==0.34.6" in argv


def test_parse_python_install_uv_pip_form() -> None:
    argv = sr._parse_python_install(
        "uv pip install httpx", Path("/tmp/v/bin/python")
    )
    assert argv[:5] == ["uv", "pip", "install", "--python", "/tmp/v/bin/python"]
    assert "httpx" in argv


def test_parse_python_install_rejects_uv_sync() -> None:
    with pytest.raises(UnsupportedCandidateError):
        sr._parse_python_install("uv sync", Path("/tmp/v/bin/python"))


def test_parse_python_install_rejects_make_install() -> None:
    with pytest.raises(UnsupportedCandidateError):
        sr._parse_python_install("make install", Path("/tmp/v/bin/python"))


def test_parse_npm_install_happy_path() -> None:
    argv = sr._parse_npm_install("npm install pmxtjs@2.25.2")
    assert argv == ["npm", "install", "pmxtjs@2.25.2"]


def test_parse_npm_install_rejects_other_shapes() -> None:
    with pytest.raises(UnsupportedCandidateError):
        sr._parse_npm_install("yarn add pmxtjs")


def test_parse_cargo_install_happy_path() -> None:
    argv = sr._parse_cargo_install("cargo add polymarket-rtds@0.1.0")
    assert argv == ["cargo", "add", "polymarket-rtds@0.1.0"]


def test_parse_cargo_install_rejects_other_shapes() -> None:
    with pytest.raises(UnsupportedCandidateError):
        sr._parse_cargo_install("cargo build --release")


# ---------------------------------------------------------------------------
# Slug helper
# ---------------------------------------------------------------------------


def test_safe_slug_replaces_non_alphanumeric() -> None:
    assert sr._safe_slug("py-clob-client") == "py_clob_client"
    assert sr._safe_slug("Polymarket/RTDS Client!") == "polymarket_rtds_client"
    assert sr._safe_slug("___") == ""


# ---------------------------------------------------------------------------
# SubprocessSession — install path
# ---------------------------------------------------------------------------


async def test_install_success_then_probe_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path: install succeeds, probe exists and exits 0."""
    candidate = _make_python_candidate()

    # Stub subprocess: every call returns rc=0.
    _stub_run_subprocess(monkeypatch, {})

    # Make sure the temp dir cleanup branch doesn't actually rmtree
    # something that doesn't exist — patch _CandidateEnv.cleanup to no-op.
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)

    # Provide a probe script that exists on disk.
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()
    probe = probes_dir / "fake_python_tool_probe.py"
    probe.write_text("#!/usr/bin/env python\nimport sys\nsys.exit(0)\n")

    session = SubprocessSession(candidate, probes_dir=probes_dir)
    install_ok = await session.survival_check(_gate_item("install"))
    assert install_ok is True
    assert session._install_succeeded is True
    probe_ok = await session.survival_check(_gate_item("connect"))
    assert probe_ok is True
    session.cleanup()


async def test_install_failure_marks_session_failed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = _make_python_candidate()
    # `uv` is now called twice: once for `uv venv` and once for
    # `uv pip install --python <venv> ...`. The first call (uv venv) must
    # succeed; the second (uv pip install) must fail with a package error.
    call_count = {"n": 0}

    async def fake(
        argv: list[str],
        cwd: Path,
        timeout_seconds: int,
        env: dict[str, str] | None = None,
    ) -> tuple[int, str, str]:
        call_count["n"] += 1
        # Call 1: uv venv → success
        # Call 2: uv pip install → failure
        if call_count["n"] == 1:
            return 0, "", ""
        return 1, "", "ERROR: Could not find package"

    monkeypatch.setattr(sr, "_run_subprocess", fake)
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)

    session = SubprocessSession(candidate, probes_dir=tmp_path / "probes")
    install_ok = await session.survival_check(_gate_item("install"))
    assert install_ok is False
    assert "Could not find package" in (session.install_error or "")


async def test_install_uv_venv_failure_propagates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = _make_python_candidate()
    _stub_run_subprocess(
        monkeypatch, {"uv": _StubResult(2, stderr="uv: command unavailable")}
    )
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)

    session = SubprocessSession(candidate, probes_dir=tmp_path / "probes")
    ok = await session.survival_check(_gate_item("install"))
    assert ok is False
    assert "uv venv failed" in (session.install_error or "")


# ---------------------------------------------------------------------------
# SubprocessSession — probe paths
# ---------------------------------------------------------------------------


async def test_probe_missing_returns_false(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = _make_python_candidate()
    _stub_run_subprocess(monkeypatch, {})
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()  # exists but empty

    session = SubprocessSession(candidate, probes_dir=probes_dir)
    assert await session.survival_check(_gate_item("install")) is True
    assert await session.survival_check(_gate_item("connect")) is False


async def test_probe_exit_2_classified_as_missing_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = _make_python_candidate()
    # First call (uv venv) returns 0; second call (pip install) returns 0;
    # third call (probe) returns 2.
    call_count = {"n": 0}

    async def fake(
        argv: list[str],
        cwd: Path,
        timeout_seconds: int,
        env: dict[str, str] | None = None,
    ) -> tuple[int, str, str]:
        call_count["n"] += 1
        # uv venv → 0; python -m pip install → 0; python probe.py → 2
        if call_count["n"] == 3:
            return 2, "", "missing API key"
        return 0, "", ""

    monkeypatch.setattr(sr, "_run_subprocess", fake)
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)

    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()
    (probes_dir / "fake_python_tool_probe.py").write_text("import sys; sys.exit(2)\n")

    session = SubprocessSession(candidate, probes_dir=probes_dir)
    assert await session.survival_check(_gate_item("install")) is True
    # Probe exit 2 → survival false; the runner records the error string
    assert await session.survival_check(_gate_item("connect")) is False


async def test_probe_runs_lazily_when_install_item_skipped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If a benchmark omits the ``install`` item, the first probe item
    must still trigger an install lazily."""
    candidate = _make_python_candidate()
    _stub_run_subprocess(monkeypatch, {})
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)

    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()
    (probes_dir / "fake_python_tool_probe.py").write_text("import sys; sys.exit(0)\n")

    session = SubprocessSession(candidate, probes_dir=probes_dir)
    # No prior install item — going straight to a probe.
    ok = await session.survival_check(_gate_item("fetch_one"))
    assert ok is True
    assert session._install_succeeded is True


# ---------------------------------------------------------------------------
# Language dispatch
# ---------------------------------------------------------------------------


async def test_unsupported_language_marks_install_failed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = Candidate(
        name="java-tool",
        repo="https://example.com/java-tool",
        language="java",
        install="./build.sh",
        platforms=["polymarket"],
        module="data_connector",
        notes="fake java",
        config={},
    )
    session = SubprocessSession(candidate, probes_dir=tmp_path / "probes")
    ok = await session.survival_check(_gate_item("install"))
    assert ok is False
    assert "java" in (session.install_error or "")


async def test_typescript_install_uses_npm(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = _make_typescript_candidate()
    calls = _stub_run_subprocess(monkeypatch, {})
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)

    session = SubprocessSession(candidate, probes_dir=tmp_path / "probes")
    ok = await session.survival_check(_gate_item("install", timeout=120))
    assert ok is True
    leafs = [Path(c[0]).name for c in calls]
    assert "npm" in leafs  # both `npm init -y` and `npm install` go through npm


async def test_rust_install_uses_cargo(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = _make_rust_candidate()
    calls = _stub_run_subprocess(monkeypatch, {})
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)

    session = SubprocessSession(candidate, probes_dir=tmp_path / "probes")
    ok = await session.survival_check(_gate_item("install", timeout=120))
    assert ok is True
    leafs = [Path(c[0]).name for c in calls]
    assert "cargo" in leafs


# ---------------------------------------------------------------------------
# SubprocessRunnerFactory
# ---------------------------------------------------------------------------


async def test_factory_reuses_one_session_per_candidate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = _make_python_candidate()
    calls = _stub_run_subprocess(monkeypatch, {})
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()
    (probes_dir / "fake_python_tool_probe.py").write_text("import sys; sys.exit(0)\n")

    factory = SubprocessRunnerFactory(probes_dir=probes_dir)
    await factory.survival_check(candidate, _gate_item("install"))
    await factory.survival_check(candidate, _gate_item("connect"))
    await factory.survival_check(candidate, _gate_item("fetch_one"))

    # `uv venv` and `uv pip install --python <venv> ...` should each be
    # called exactly once even though three survival items were checked.
    # Both go through the `uv` binary, so we look for them by argv
    # signature instead of binary leaf.
    venv_calls = [c for c in calls if len(c) >= 2 and c[0] == "uv" and c[1] == "venv"]
    pip_calls = [c for c in calls if len(c) >= 3 and c[:3] == ["uv", "pip", "install"]]
    assert len(venv_calls) == 1, f"uv venv ran more than once: {venv_calls}"
    assert len(pip_calls) == 1, f"uv pip install ran more than once: {pip_calls}"
    factory.cleanup_all()


async def test_factory_cleanup_all_releases_sessions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = _make_python_candidate()
    _stub_run_subprocess(monkeypatch, {})
    cleanup_calls = {"n": 0}

    def track_cleanup(self: sr._CandidateEnv) -> None:
        cleanup_calls["n"] += 1

    monkeypatch.setattr(sr._CandidateEnv, "cleanup", track_cleanup)

    factory = SubprocessRunnerFactory(probes_dir=tmp_path / "probes")
    await factory.survival_check(candidate, _gate_item("install"))
    factory.cleanup_all()
    assert cleanup_calls["n"] == 1
    assert factory._sessions == {}


# ---------------------------------------------------------------------------
# make_subprocess_test_fn convenience wrapper
# ---------------------------------------------------------------------------


async def test_make_subprocess_test_fn_returns_callable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate = _make_python_candidate()
    _stub_run_subprocess(monkeypatch, {})
    monkeypatch.setattr(sr._CandidateEnv, "cleanup", lambda self: None)

    fn = make_subprocess_test_fn(candidate, probes_dir=tmp_path / "probes")
    assert callable(fn)
    ok = await fn(_gate_item("install"))
    assert ok is True


# ---------------------------------------------------------------------------
# _run_subprocess timeout behavior — exercise the real primitive without
# spawning a child process by feeding it ``true``.
# ---------------------------------------------------------------------------


async def test_run_subprocess_returns_zero_for_true(tmp_path: Path) -> None:
    rc, out, err = await sr._run_subprocess(
        ["true"], cwd=tmp_path, timeout_seconds=5
    )
    assert rc == 0
    assert out == ""
    assert err == ""


async def test_run_subprocess_timeout_kills_child(tmp_path: Path) -> None:
    rc, out, err = await sr._run_subprocess(
        ["sleep", "5"], cwd=tmp_path, timeout_seconds=1
    )
    assert rc == 124
    assert "timed out" in err


# ---------------------------------------------------------------------------
# Integration test — runs the real py-clob-client probe end-to-end.
#
# Gated on ``PMS_RUN_INTEGRATION=1`` because pytest does not auto-deselect
# by marker; we want this off by default in CI/local runs but easily
# togglable for manual end-to-end verification. The ``integration`` marker
# is still applied so ``pytest -m integration`` works as expected.
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("PMS_RUN_INTEGRATION") != "1",
    reason="set PMS_RUN_INTEGRATION=1 to run real-network integration tests",
)
async def test_py_clob_client_probe_end_to_end() -> None:
    probe_path = (
        Path(__file__).resolve().parents[1]
        / "candidates"
        / "probes"
        / "py_clob_client_probe.py"
    )
    if not probe_path.exists():
        pytest.skip(f"probe script not yet present: {probe_path}")

    candidate = Candidate(
        name="py-clob-client",
        repo="https://github.com/Polymarket/py-clob-client",
        language="python",
        install="pip install py-clob-client==0.34.6",
        platforms=["polymarket"],
        module="data_connector",
        notes="real integration test",
        config={},
    )
    factory = SubprocessRunnerFactory(probes_dir=probe_path.parent)
    try:
        install_ok = await factory.survival_check(
            candidate, _gate_item("install", timeout=300)
        )
        assert install_ok is True, "py-clob-client failed to install"
        probe_ok = await factory.survival_check(
            candidate, _gate_item("fetch_one", timeout=60)
        )
        assert probe_ok is True, "py-clob-client probe failed"
    finally:
        factory.cleanup_all()
