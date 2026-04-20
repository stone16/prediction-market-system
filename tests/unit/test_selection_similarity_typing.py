from __future__ import annotations

from pathlib import Path
import subprocess


def test_selection_similarity_metric_requires_denominator_at_type_check_time() -> None:
    fixture_path = Path("typing_fixtures/selection_similarity_missing_denominator.py")

    result = subprocess.run(
        ["uv", "run", "mypy", "--strict", str(fixture_path)],
        text=True,
        capture_output=True,
        check=False,
    )

    normalized_output = result.stdout.replace('"', "'")

    assert result.returncode != 0
    assert f"{fixture_path}:3: error: Missing positional argument 'denominator'" in normalized_output
    assert "[call-arg]" in normalized_output
