"""Tests for the real-tool candidate YAMLs in ``candidates/``.

P2-01 acceptance: every YAML file under ``candidates/`` must parse to a
valid :class:`pms.tool_harness.schema.Candidate` dataclass and the set
must cover all 10 modules from the auto-research catalog.

These tests are intentionally schema-only — they do **not** install or
run any real tool. Subprocess execution lands in P2-02.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pms.tool_harness import Candidate, load_candidate

REPO_ROOT = Path(__file__).resolve().parents[1]
CANDIDATES_DIR = REPO_ROOT / "candidates"

# All ten modules surveyed by the auto-research catalog. P2-01 requires at
# least one real-tool candidate covering each of these slots.
EXPECTED_MODULES: frozenset[str] = frozenset(
    {
        "data_connector",
        "realtime_feed",
        "data_normalizer",
        "embedding_engine",
        "correlation_detector",
        "arbitrage_calculator",
        "order_executor",
        "risk_manager",
        "backtesting_engine",
        "analytics_dashboard",
    }
)

# The mock candidate is shipped for harness self-tests; exclude it from
# the "real candidates" set so the count assertion only measures Phase 2
# tools coming from the auto-research catalog.
MOCK_FILENAME = "mock_connector.yaml"


def _real_candidate_files() -> list[Path]:
    return sorted(
        p for p in CANDIDATES_DIR.glob("*.yaml") if p.name != MOCK_FILENAME
    )


def test_candidates_dir_exists() -> None:
    assert CANDIDATES_DIR.exists(), f"missing candidates dir: {CANDIDATES_DIR}"


def test_at_least_ten_real_candidates() -> None:
    """P2-01 acceptance: ``candidates/`` ships ≥10 real-tool YAMLs."""
    files = _real_candidate_files()
    assert len(files) >= 10, (
        f"expected ≥10 real-tool candidate YAMLs, found {len(files)}: "
        f"{[p.name for p in files]}"
    )


@pytest.mark.parametrize("candidate_path", _real_candidate_files(), ids=lambda p: p.name)
def test_real_candidate_parses(candidate_path: Path) -> None:
    """Each real candidate YAML must parse to a valid Candidate dataclass.

    Loader will raise ``BenchmarkValidationError`` on any schema breach,
    which pytest reports as a test failure with the field path baked in.
    """
    candidate = load_candidate(candidate_path)
    assert isinstance(candidate, Candidate)
    # Required fields must be non-empty strings.
    assert candidate.name, f"empty name in {candidate_path}"
    assert candidate.repo, f"empty repo in {candidate_path}"
    assert candidate.language, f"empty language in {candidate_path}"
    assert candidate.install, f"empty install in {candidate_path}"
    assert candidate.module, f"empty module in {candidate_path}"
    assert candidate.notes, f"empty notes in {candidate_path}"
    assert candidate.platforms, f"empty platforms in {candidate_path}"
    # Module must be one of the 10 known catalog modules — guards against
    # typos like ``data_connecter`` slipping in unnoticed.
    assert candidate.module in EXPECTED_MODULES, (
        f"unknown module {candidate.module!r} in {candidate_path}; "
        f"expected one of {sorted(EXPECTED_MODULES)}"
    )


def test_all_ten_modules_have_at_least_one_candidate() -> None:
    """Every catalog module must have ≥1 real candidate ready to evaluate."""
    covered: set[str] = set()
    for path in _real_candidate_files():
        covered.add(load_candidate(path).module)
    missing = EXPECTED_MODULES - covered
    assert not missing, f"modules with no real candidate: {sorted(missing)}"


# ---------------------------------------------------------------------------
# P2-03: probe scripts exist for the priority candidates and follow the
# documented exit-code contract (must reference exit codes 0/1/2 somewhere
# in the source so the README contract stays load-bearing).
# ---------------------------------------------------------------------------

PROBES_DIR = CANDIDATES_DIR / "probes"
PRIORITY_PROBES: list[tuple[str, str]] = [
    ("py_clob_client", "py"),
    ("kalshi_python_sync", "py"),
    ("sentence_transformers", "py"),
    ("pmxt", "ts"),
    ("real_time_data_client", "ts"),
]


def test_probes_dir_has_readme() -> None:
    readme = PROBES_DIR / "README.md"
    assert readme.exists(), f"probes README missing: {readme}"
    text = readme.read_text(encoding="utf-8")
    assert "Exit-code contract" in text
    # The contract documents at least the three load-bearing exit codes.
    for code in ("`0`", "`1`", "`2`"):
        assert code in text, f"probe README missing exit code {code}"


@pytest.mark.parametrize("stem,ext", PRIORITY_PROBES, ids=lambda v: str(v))
def test_priority_probe_script_exists(stem: str, ext: str) -> None:
    probe = PROBES_DIR / f"{stem}_probe.{ext}"
    assert probe.exists(), f"missing priority probe: {probe}"


@pytest.mark.parametrize("stem,ext", PRIORITY_PROBES, ids=lambda v: str(v))
def test_priority_probe_implements_exit_contract(stem: str, ext: str) -> None:
    """Each priority probe must reference exit codes 0/1 (success/failure).

    Probes that depend on credentials must additionally reference exit 2
    (the credential branch). We use a permissive substring check rather
    than parsing the source so the contract holds for both Python and
    TypeScript probes.
    """
    probe = PROBES_DIR / f"{stem}_probe.{ext}"
    text = probe.read_text(encoding="utf-8")
    # Generic failure path is mandatory.
    assert "1" in text and ("exit" in text.lower() or "return 1" in text), (
        f"{probe.name} does not appear to use exit code 1"
    )
    # Credentialed Kalshi probe MUST also implement the exit-2 path.
    if stem == "kalshi_python_sync":
        assert "2" in text and "credential" in text.lower(), (
            f"{probe.name} must handle missing credentials with exit 2"
        )
