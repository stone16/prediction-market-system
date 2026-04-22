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
KEYWORDS = ("live mode", "live trading", "live execution", "Kalshi")
STUB_MARKERS = ("not implemented", "reserved", "stub", "NotImplementedError")


def test_readme_and_claude_explicitly_document_polymarket_stub_gate() -> None:
    readme_text = (ROOT / "README.md").read_text(encoding="utf-8")
    claude_text = (ROOT / "CLAUDE.md").read_text(encoding="utf-8")

    assert "polymarket.py:23-25" in readme_text
    assert "NotImplementedError" in readme_text
    assert "polymarket.py:23-25" in claude_text
    assert "NotImplementedError" in claude_text


def test_live_and_kalshi_mentions_are_stubbed_not_capability_claims() -> None:
    offending_lines: list[str] = []

    for path in FILES_TO_SCAN:
        if not path.exists():
            continue
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if any(keyword in line for keyword in KEYWORDS):
                if not any(marker in line for marker in STUB_MARKERS):
                    offending_lines.append(f"{path.relative_to(ROOT)}:{line_number}:{line}")

    assert offending_lines == []
