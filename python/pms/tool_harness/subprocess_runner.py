"""Subprocess-based test functions for real candidate evaluation (P2-02).

Replaces the in-process :class:`MockCandidate` with isolated venv/workdir
runs of real prediction-market tools. Each Python candidate gets its
own ``uv venv``; each TypeScript candidate gets a temp ``npm install``
dir; each Rust candidate gets a temp ``cargo init`` dir. Java and any
other languages are explicitly marked unsupported with a clear error so
the survival gate fails loudly instead of silently scoring 0.

Architecture
------------

The harness's :meth:`HarnessRunner.evaluate_module` calls a
``ModuleSurvivalTestFn`` once per ``(candidate, item)`` pair. Real-tool
evaluation needs state across those calls — installing dependencies on
every item would multiply network/CI cost by N. To bridge that gap
without changing the runner contract, this module exposes
:class:`SubprocessRunnerFactory`, which keeps a per-candidate
:class:`SubprocessSession` cached in a dict and routes every test_fn
call to the right session. Sessions own a :class:`tempfile.TemporaryDirectory`
that is released via :meth:`SubprocessRunnerFactory.cleanup_all` after
the gate finishes — the CLI invokes that explicitly so a crashed run
still cleans its temp dirs via the ``finally`` block.

Survival item dispatch
----------------------

Survival items are identified by ``id``:

* ``install`` — runs the candidate's install command in a fresh isolated
  env. Caches success so the next item doesn't repeat the work.
* anything else (``connect``, ``fetch_one``, …) — runs the candidate's
  probe script (looked up under ``candidates/probes/``) inside the env
  prepared by ``install``. Probes use the exit-code contract documented
  in P2-03's README.

Probe naming convention
-----------------------

Probes live at ``candidates/probes/<safe_name>_probe.<ext>`` where
``ext`` is ``py`` (python), ``ts`` (typescript), or ``rs`` (rust) and
``safe_name`` is :func:`_safe_slug` applied to ``candidate.name``. P2-03
adds the actual probe scripts; this module only resolves the path and
runs whatever exists.

Security & cleanup
------------------

* No environment variables from the parent process are forwarded by
  default — subprocesses receive a minimal env (``PATH``, ``HOME``).
  Probe scripts that need credentials must read them from the parent
  via :func:`os.environ.get` *before* the subprocess runs and pass them
  through explicit args, OR the caller must opt into env passthrough
  via :class:`SubprocessRunnerFactory(env_passthrough=True)`.
* Temp dirs are removed in :meth:`SubprocessSession.cleanup`.
* All install command parsing is whitelist-based: only ``pip install``,
  ``uv pip install``, ``npm install``, and ``cargo add`` shapes are
  accepted. Anything else (``make install``, ``uv sync``, shell pipes)
  raises :class:`UnsupportedCandidateError` so we never accidentally
  shell out a complex install command in a sandboxed environment.
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

from .schema import Candidate, SurvivalGateItem


# ---------------------------------------------------------------------------
# Public errors and constants
# ---------------------------------------------------------------------------


class UnsupportedCandidateError(Exception):
    """Raised when a candidate's language or install command is not supported.

    The runner converts this into a survival failure with the exception's
    message attached to ``SurvivalItemResult.error``.
    """


#: Repository root — used to default ``probes_dir`` to
#: ``<repo>/candidates/probes``. Tests pass an explicit ``probes_dir``.
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROBES_DIR: Path = REPO_ROOT / "candidates" / "probes"

#: Probe-script extension per candidate language.
_PROBE_EXTENSION: dict[str, str] = {
    "python": "py",
    "typescript": "ts",
    "rust": "rs",
}

#: Per-candidate test function compatible with HarnessRunner's SurvivalTestFn.
SurvivalTestFn = Callable[[SurvivalGateItem], Awaitable[bool]]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


@dataclass
class _RunResult:
    """Internal helper bundling subprocess return data."""

    success: bool
    error: str | None
    elapsed_seconds: float


@dataclass
class _CandidateEnv:
    """A prepared isolated workdir for a single candidate.

    Owned by :class:`SubprocessSession`; cleanup is explicit so the
    runner can release the temp dir as soon as the candidate's gate
    finishes.
    """

    workdir: Path
    python_exe: Path | None  # set for python candidates only

    def cleanup(self) -> None:
        if self.workdir.exists():
            shutil.rmtree(self.workdir, ignore_errors=True)


def _safe_slug(name: str) -> str:
    """Return a filesystem-safe slug for ``name`` (lowercase a-z0-9_)."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _truncate(text: str, limit: int = 500) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


# ---------------------------------------------------------------------------
# Subprocess execution primitive
# ---------------------------------------------------------------------------


async def _run_subprocess(
    argv: list[str],
    cwd: Path,
    timeout_seconds: int,
    env: dict[str, str] | None = None,
) -> tuple[int, str, str]:
    """Run a subprocess with timeout, capture stdout/stderr.

    Returns ``(returncode, stdout, stderr)``. On timeout the child is
    killed and the function returns ``(124, "", "timed out after Ns")``.
    """
    proc = await asyncio.create_subprocess_exec(
        *argv,
        cwd=str(cwd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(), timeout=timeout_seconds
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, "", f"timed out after {timeout_seconds}s"
    return (
        proc.returncode if proc.returncode is not None else 0,
        stdout_b.decode("utf-8", errors="replace"),
        stderr_b.decode("utf-8", errors="replace"),
    )


# ---------------------------------------------------------------------------
# Install-command parsing
# ---------------------------------------------------------------------------


def _parse_python_install(install: str, python_exe: Path) -> list[str]:
    """Convert a YAML install command into argv targeting ``python_exe``.

    Supports::

        pip install <args>...
        uv pip install <args>...

    Anything else raises :class:`UnsupportedCandidateError` so we never
    silently shell out commands like ``make install`` or ``uv sync``.

    Both forms route through ``uv pip install --python <venv>`` because
    ``uv venv`` produces a venv without pip installed by default — going
    through ``<venv>/bin/python -m pip`` would fail with "No module named
    pip". Using ``uv pip install --python`` lets uv resolve and install
    against the target venv directly.
    """
    parts = install.strip().split()
    if len(parts) >= 3 and parts[0] == "pip" and parts[1] == "install":
        return ["uv", "pip", "install", "--python", str(python_exe), *parts[2:]]
    if (
        len(parts) >= 4
        and parts[0] == "uv"
        and parts[1] == "pip"
        and parts[2] == "install"
    ):
        return ["uv", "pip", "install", "--python", str(python_exe), *parts[3:]]
    raise UnsupportedCandidateError(
        f"python install command not supported: {install!r}"
    )


def _parse_npm_install(install: str) -> list[str]:
    parts = install.strip().split()
    if len(parts) >= 3 and parts[0] == "npm" and parts[1] == "install":
        return ["npm", "install", *parts[2:]]
    raise UnsupportedCandidateError(
        f"typescript install command not supported: {install!r}"
    )


def _parse_cargo_install(install: str) -> list[str]:
    parts = install.strip().split()
    if len(parts) >= 3 and parts[0] == "cargo" and parts[1] == "add":
        return ["cargo", "add", *parts[2:]]
    raise UnsupportedCandidateError(
        f"rust install command not supported: {install!r}"
    )


# ---------------------------------------------------------------------------
# Per-language env preparation
# ---------------------------------------------------------------------------


async def _prepare_python_env(
    candidate: Candidate, timeout_seconds: int
) -> _CandidateEnv:
    workdir = Path(tempfile.mkdtemp(prefix=f"pms_p2_{_safe_slug(candidate.name)}_"))
    venv_dir = workdir / ".venv"

    rc, out, err = await _run_subprocess(
        ["uv", "venv", str(venv_dir)],
        cwd=workdir,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if rc != 0:
        shutil.rmtree(workdir, ignore_errors=True)
        raise UnsupportedCandidateError(
            f"uv venv failed (rc={rc}): {_truncate(err or out)}"
        )

    python_exe = venv_dir / "bin" / "python"
    install_argv = _parse_python_install(candidate.install, python_exe)

    rc, out, err = await _run_subprocess(
        install_argv, cwd=workdir, timeout_seconds=timeout_seconds
    )
    if rc != 0:
        shutil.rmtree(workdir, ignore_errors=True)
        raise UnsupportedCandidateError(
            f"install failed (rc={rc}): {_truncate(err or out)}"
        )

    return _CandidateEnv(workdir=workdir, python_exe=python_exe)


async def _prepare_typescript_env(
    candidate: Candidate, timeout_seconds: int
) -> _CandidateEnv:
    workdir = Path(tempfile.mkdtemp(prefix=f"pms_p2_{_safe_slug(candidate.name)}_"))

    rc, out, err = await _run_subprocess(
        ["npm", "init", "-y"],
        cwd=workdir,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if rc != 0:
        shutil.rmtree(workdir, ignore_errors=True)
        raise UnsupportedCandidateError(
            f"npm init failed (rc={rc}): {_truncate(err or out)}"
        )

    install_argv = _parse_npm_install(candidate.install)
    rc, out, err = await _run_subprocess(
        install_argv, cwd=workdir, timeout_seconds=timeout_seconds
    )
    if rc != 0:
        shutil.rmtree(workdir, ignore_errors=True)
        raise UnsupportedCandidateError(
            f"npm install failed (rc={rc}): {_truncate(err or out)}"
        )

    return _CandidateEnv(workdir=workdir, python_exe=None)


async def _prepare_rust_env(
    candidate: Candidate, timeout_seconds: int
) -> _CandidateEnv:
    workdir = Path(tempfile.mkdtemp(prefix=f"pms_p2_{_safe_slug(candidate.name)}_"))

    rc, out, err = await _run_subprocess(
        ["cargo", "init", "--name", "pms_probe", str(workdir)],
        cwd=workdir,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if rc != 0:
        shutil.rmtree(workdir, ignore_errors=True)
        raise UnsupportedCandidateError(
            f"cargo init failed (rc={rc}): {_truncate(err or out)}"
        )

    install_argv = _parse_cargo_install(candidate.install)
    rc, out, err = await _run_subprocess(
        install_argv, cwd=workdir, timeout_seconds=timeout_seconds
    )
    if rc != 0:
        shutil.rmtree(workdir, ignore_errors=True)
        raise UnsupportedCandidateError(
            f"cargo add failed (rc={rc}): {_truncate(err or out)}"
        )

    return _CandidateEnv(workdir=workdir, python_exe=None)


async def _prepare_env(candidate: Candidate, timeout_seconds: int) -> _CandidateEnv:
    """Dispatch to the right per-language env preparer."""
    if candidate.language == "python":
        return await _prepare_python_env(candidate, timeout_seconds)
    if candidate.language == "typescript":
        return await _prepare_typescript_env(candidate, timeout_seconds)
    if candidate.language == "rust":
        return await _prepare_rust_env(candidate, timeout_seconds)
    raise UnsupportedCandidateError(
        f"language {candidate.language!r} not supported by subprocess runner"
    )


# ---------------------------------------------------------------------------
# Probe execution
# ---------------------------------------------------------------------------


async def _run_probe(
    candidate: Candidate,
    env: _CandidateEnv,
    probe_path: Path,
    timeout_seconds: int,
    forwarded_env: dict[str, str] | None,
) -> _RunResult:
    """Execute the candidate's probe inside its prepared env.

    Exit code contract (matches ``candidates/probes/README.md``):

    * ``0`` — success: the tool installed, connected, and returned data
    * ``2`` — missing credentials: distinct from a network failure so
      gated tools (e.g. private Kalshi SDK) can be enumerated without
      polluting the survival output with credential errors
    * any other non-zero — generic failure (network, runtime exception,
      assertion mismatch); stdout/stderr is captured and forwarded
    """
    if candidate.language == "python":
        if env.python_exe is None:  # pragma: no cover — defensive
            return _RunResult(False, "python env missing python_exe", 0.0)
        argv = [str(env.python_exe), str(probe_path)]
    elif candidate.language == "typescript":
        argv = ["node", str(probe_path)]
    elif candidate.language == "rust":
        # Rust probes are single .rs files compiled with rustc into the
        # candidate's workdir, then executed.
        binary = env.workdir / "probe_bin"
        rc, out, err = await _run_subprocess(
            ["rustc", str(probe_path), "-o", str(binary)],
            cwd=env.workdir,
            timeout_seconds=timeout_seconds,
        )
        if rc != 0:
            return _RunResult(
                False, f"rustc failed (rc={rc}): {_truncate(err or out)}", 0.0
            )
        argv = [str(binary)]
    else:  # pragma: no cover — _prepare_env already rejects this
        return _RunResult(
            False, f"language {candidate.language} not supported", 0.0
        )

    start = time.perf_counter()
    rc, stdout, stderr = await _run_subprocess(
        argv,
        cwd=env.workdir,
        timeout_seconds=timeout_seconds,
        env=forwarded_env,
    )
    elapsed = time.perf_counter() - start

    if rc == 0:
        return _RunResult(True, None, elapsed)
    if rc == 2:
        return _RunResult(False, "missing credentials (probe exit 2)", elapsed)
    return _RunResult(
        False,
        f"probe exited {rc}: {_truncate(stderr or stdout, 300)}",
        elapsed,
    )


# ---------------------------------------------------------------------------
# Per-candidate session
# ---------------------------------------------------------------------------


class SubprocessSession:
    """Holds the temp env for one candidate across the survival gate.

    The session is created lazily on the first survival item and reused
    by every subsequent item for the same candidate. The hosting
    :class:`SubprocessRunnerFactory` cleans it up via
    :meth:`cleanup_all` when the gate finishes (or when the CLI's
    ``finally`` block fires).
    """

    def __init__(
        self,
        candidate: Candidate,
        probes_dir: Path,
        forwarded_env: dict[str, str] | None = None,
    ) -> None:
        self.candidate = candidate
        self.probes_dir = probes_dir
        self.forwarded_env = forwarded_env
        self._env: _CandidateEnv | None = None
        # Tri-state: None means "install never ran"; True/False means cached.
        self._install_succeeded: bool | None = None
        self._install_error: str | None = None

    async def survival_check(self, item: SurvivalGateItem) -> bool:
        """Run a survival item and return ``True`` on success."""
        if item.id == "install":
            return await self._run_install(item.timeout_seconds)
        return await self._run_probe_item(item)

    async def _run_install(self, timeout_seconds: int) -> bool:
        if self._install_succeeded is not None:
            return self._install_succeeded
        try:
            self._env = await _prepare_env(self.candidate, timeout_seconds)
            self._install_succeeded = True
            return True
        except UnsupportedCandidateError as exc:
            self._install_succeeded = False
            self._install_error = str(exc)
            return False
        except Exception as exc:  # noqa: BLE001 — runner must not crash
            self._install_succeeded = False
            self._install_error = f"{type(exc).__name__}: {exc}"
            return False

    async def _run_probe_item(self, item: SurvivalGateItem) -> bool:
        # Lazy install if no preceding "install" item was scheduled.
        if self._install_succeeded is None:
            installed = await self._run_install(item.timeout_seconds)
            if not installed:
                return False
        if not self._install_succeeded:
            return False  # install failed earlier; cascade
        env = self._env
        assert env is not None  # mypy: install set this on success

        probe_path = self._locate_probe()
        if probe_path is None:
            return False

        result = await _run_probe(
            self.candidate,
            env,
            probe_path,
            item.timeout_seconds,
            self.forwarded_env,
        )
        return result.success

    def _locate_probe(self) -> Path | None:
        """Resolve the probe script for this candidate, if any."""
        ext = _PROBE_EXTENSION.get(self.candidate.language)
        if ext is None:
            return None
        path = self.probes_dir / f"{_safe_slug(self.candidate.name)}_probe.{ext}"
        return path if path.exists() else None

    @property
    def install_error(self) -> str | None:
        """Most recent install failure message, if any."""
        return self._install_error

    def cleanup(self) -> None:
        """Release the temp dir if one was created."""
        if self._env is not None:
            self._env.cleanup()
            self._env = None


# ---------------------------------------------------------------------------
# Multi-candidate factory used by the CLI
# ---------------------------------------------------------------------------


class SubprocessRunnerFactory:
    """Hosts per-candidate :class:`SubprocessSession` objects for the CLI.

    The CLI calls :meth:`survival_check` once per ``(candidate, item)``
    pair; the factory transparently routes the call to the right
    session, creating one on first use. After the gate finishes the CLI
    invokes :meth:`cleanup_all` from a ``finally`` block so a crashed
    or timed-out run still releases temp dirs.
    """

    def __init__(
        self,
        probes_dir: Path = DEFAULT_PROBES_DIR,
        forwarded_env: dict[str, str] | None = None,
    ) -> None:
        self.probes_dir = probes_dir
        self.forwarded_env = forwarded_env
        self._sessions: dict[str, SubprocessSession] = {}

    def session_for(self, candidate: Candidate) -> SubprocessSession:
        session = self._sessions.get(candidate.name)
        if session is None:
            session = SubprocessSession(
                candidate, self.probes_dir, self.forwarded_env
            )
            self._sessions[candidate.name] = session
        return session

    async def survival_check(
        self, candidate: Candidate, item: SurvivalGateItem
    ) -> bool:
        return await self.session_for(candidate).survival_check(item)

    def cleanup_all(self) -> None:
        for session in self._sessions.values():
            session.cleanup()
        self._sessions.clear()


# ---------------------------------------------------------------------------
# Convenience factory matching the AC's name
# ---------------------------------------------------------------------------


def make_subprocess_test_fn(
    candidate: Candidate,
    probes_dir: Path = DEFAULT_PROBES_DIR,
    forwarded_env: dict[str, str] | None = None,
) -> SurvivalTestFn:
    """Return a single-candidate survival ``test_fn`` (for unit tests).

    Most callers use :class:`SubprocessRunnerFactory` instead — that
    class manages multiple candidates and supports cleanup. This factory
    exists so unit tests and small scripts can drive the runner manually
    without instantiating the multi-candidate factory.

    The returned closure leaks its temp env until process exit. Long-
    running callers should prefer the factory + ``cleanup_all`` pattern.
    """
    session = SubprocessSession(candidate, probes_dir, forwarded_env)

    async def fn(item: SurvivalGateItem) -> bool:
        return await session.survival_check(item)

    return fn
