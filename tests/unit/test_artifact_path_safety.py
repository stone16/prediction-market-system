from __future__ import annotations

from pathlib import Path

import pytest

from scripts import artifact_path_safety


def test_scripts_artifact_path_safety_exposes_private_parent_guard(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "permissive"
    artifact_dir.mkdir()
    artifact_dir.chmod(0o755)

    with pytest.raises(ValueError, match="too permissive"):
        artifact_path_safety.require_private_parent(
            artifact_dir / "launch-artifact.csv",
            label="launch artifact",
        )
