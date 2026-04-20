from __future__ import annotations

import subprocess


def test_pms_research_help_lists_sweep_and_worker_subcommands() -> None:
    result = subprocess.run(
        ["uv", "run", "pms-research", "--help"],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "sweep" in result.stdout
    assert "worker" in result.stdout
