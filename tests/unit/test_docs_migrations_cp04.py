from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
README_PATH = ROOT / "README.md"
CLAUDE_PATH = ROOT / "CLAUDE.md"
MIGRATIONS_DOC_PATH = ROOT / "docs" / "operations" / "migrations.md"


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


def test_migrations_doc_exists_with_required_sections_and_policy() -> None:
    text = MIGRATIONS_DOC_PATH.read_text(encoding="utf-8")

    assert "## Authoring" in text
    assert "## Applying" in text
    assert "## Rolling back" in text
    assert "## Why we don't autogenerate" in text
    assert "do not run `alembic revision --autogenerate`" in text
    assert "forbidden by §B of the pms-correctness-bundle-v1 spec" in text

