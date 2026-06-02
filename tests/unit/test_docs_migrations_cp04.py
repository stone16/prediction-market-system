from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
README_PATH = ROOT / "README.md"
CLAUDE_PATH = ROOT / "CLAUDE.md"
GITIGNORE_PATH = ROOT / ".gitignore"
STRATEGY_AUTHORING_GUIDE_PATH = ROOT / "agent_docs" / "strategy-authoring-guide.md"
MIGRATIONS_DOC_PATH = ROOT / "docs" / "operations" / "migrations.md"
LIVE_POLYMARKET_RUNBOOK_PATH = ROOT / "docs" / "operations" / "live-polymarket-runbook.md"


def _section(text: str, heading: str) -> str:
    _, _, remainder = text.partition(heading)
    assert remainder
    next_heading = remainder.find("\n## ")
    if next_heading == -1:
        return remainder
    return remainder[:next_heading]


def test_readme_quick_start_uses_alembic_and_documents_database_url() -> None:
    quick_start = _section(
        README_PATH.read_text(encoding="utf-8"),
        "## Quick start",
    )

    assert "docker compose up -d postgres" in quick_start
    assert "uv run alembic upgrade head" in quick_start
    assert "psql -f schema.sql" not in quick_start
    assert "DATABASE_URL" in quick_start
    assert "uv run alembic downgrade base" in quick_start
    assert "schema.sql is a reference artifact, not the runtime source" in quick_start


def test_claude_canonical_gates_show_alembic_before_integration_pytest() -> None:
    canonical_gates = _section(
        CLAUDE_PATH.read_text(encoding="utf-8"),
        "## Canonical gates",
    )

    alembic_index = canonical_gates.index("uv run alembic upgrade head")
    integration_index = canonical_gates.index("PMS_RUN_INTEGRATION=1 uv run pytest")

    assert alembic_index < integration_index


def test_local_integration_gate_docs_require_compose_backed_database_url() -> None:
    readme_development = _section(
        README_PATH.read_text(encoding="utf-8"),
        "## Development",
    )
    canonical_gates = _section(
        CLAUDE_PATH.read_text(encoding="utf-8"),
        "## Canonical gates",
    )

    for section_text in (readme_development, canonical_gates):
        assert "docker compose up -d postgres" in section_text
        assert (
            "export PMS_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_test"
            in section_text
        )
        assert "PMS_RUN_INTEGRATION=1 uv run pytest -q -m integration" in section_text


def test_compose_backed_local_docs_use_database_created_by_compose() -> None:
    compose_dsn = "postgres://postgres:postgres@localhost:5432/pms_test"
    missing_compose_dsn = "postgres://postgres:postgres@localhost:5432/pms_dev"
    sections = (
        _section(README_PATH.read_text(encoding="utf-8"), "## Quick start"),
        _section(README_PATH.read_text(encoding="utf-8"), "### Step-by-Step: From Zero to Paper Soak"),
        _section(
            LIVE_POLYMARKET_RUNBOOK_PATH.read_text(encoding="utf-8"),
            "## PAPER Soak",
        ),
        _section(
            STRATEGY_AUTHORING_GUIDE_PATH.read_text(encoding="utf-8"),
            "## 1. Prerequisites",
        ),
    )

    for section_text in sections:
        assert "docker compose up -d postgres" in section_text
        assert compose_dsn in section_text
        assert missing_compose_dsn not in section_text


def test_readme_development_documents_all_ci_gates() -> None:
    readme_development = _section(
        README_PATH.read_text(encoding="utf-8"),
        "## Development",
    )

    assert "uv run lint-imports" in readme_development
    assert "(cd dashboard && npm ci && npm run test:ci)" in readme_development
    assert "import-linter contracts" in readme_development
    assert "dashboard Vitest" in readme_development


def test_claude_canonical_gates_document_all_ci_gates() -> None:
    canonical_gates = _section(
        CLAUDE_PATH.read_text(encoding="utf-8"),
        "## Canonical gates",
    )

    assert "uv run lint-imports" in canonical_gates
    assert "(cd dashboard && npm ci && npm run test:ci)" in canonical_gates
    assert "import-linter contracts" in canonical_gates
    assert "dashboard Vitest" in canonical_gates


def test_living_gate_docs_do_not_publish_stale_count_snapshots() -> None:
    stale_snapshots = (
        "337 passing",
        "337 pass",
        "85 skipped",
        "196 source files",
        "see baseline below",
    )

    for path in (CLAUDE_PATH, README_PATH, STRATEGY_AUTHORING_GUIDE_PATH):
        text = path.read_text(encoding="utf-8")
        for snapshot in stale_snapshots:
            assert snapshot not in text, f"{path.relative_to(ROOT)} contains {snapshot!r}"


def test_strategy_authoring_prerequisites_document_all_current_ci_gates() -> None:
    prerequisites = _section(
        STRATEGY_AUTHORING_GUIDE_PATH.read_text(encoding="utf-8"),
        "## 1. Prerequisites",
    )

    assert "uv run pytest -q" in prerequisites
    assert "uv run mypy src/ tests/ --strict" in prerequisites
    assert "uv run lint-imports" in prerequisites
    assert "(cd dashboard && npm ci && npm run test:ci)" in prerequisites


def test_dashboard_coverage_output_is_ignored() -> None:
    gitignore = GITIGNORE_PATH.read_text(encoding="utf-8")

    assert "dashboard/coverage/" in gitignore


def test_migrations_doc_exists_with_required_sections_and_policy() -> None:
    text = MIGRATIONS_DOC_PATH.read_text(encoding="utf-8")

    assert "## Authoring" in text
    assert "## Applying" in text
    assert "## Rolling back" in text
    assert "## Why we don't autogenerate" in text
    assert "do not run `alembic revision --autogenerate`" in text
    assert "forbidden by §B of the pms-correctness-bundle-v1 spec" in text
