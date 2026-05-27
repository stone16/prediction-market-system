from __future__ import annotations

from io import StringIO
import os
from pathlib import Path
import sys

import pytest

import pms.research.cli as research_cli


def _write_sweep_spec(path: Path) -> None:
    path.write_text("base_spec: {}\nexec_config: {}\n", encoding="utf-8")


def test_load_yaml_payload_rejects_symlink_spec_file(tmp_path: Path) -> None:
    target_path = tmp_path / "target-sweep.yaml"
    _write_sweep_spec(target_path)
    spec_path = tmp_path / "sweep.yaml"
    spec_path.symlink_to(target_path)

    with pytest.raises(ValueError, match="research sweep spec cannot be read safely"):
        research_cli._load_yaml_payload(str(spec_path))


def test_load_yaml_payload_opens_spec_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    spec_path = tmp_path / "sweep.yaml"
    _write_sweep_spec(spec_path)
    observed: list[tuple[Path, int]] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        observed.append((Path(os.fsdecode(os.fspath(path_arg))), flags))
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", recording_open)

    payload = research_cli._load_yaml_payload(str(spec_path))

    observed_by_path = {observed_path: flags for observed_path, flags in observed}
    assert payload == {"base_spec": {}, "exec_config": {}}
    assert observed_by_path[spec_path] & no_follow_flag


def test_load_yaml_payload_rejects_hardlink_swap_during_spec_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec_path = tmp_path / "sweep.yaml"
    _write_sweep_spec(spec_path)
    replacement_source = tmp_path / "replacement-sweep.yaml"
    _write_sweep_spec(replacement_source)
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == spec_path and not swapped:
            swapped = True
            spec_path.unlink()
            os.link(replacement_source, spec_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="research sweep spec cannot be read safely"):
        research_cli._load_yaml_payload(str(spec_path))

    assert swapped is True


def test_load_yaml_payload_keeps_stdin_support(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "stdin",
        StringIO("base_spec: {}\nexec_config: {}\n"),
    )

    payload = research_cli._load_yaml_payload("-")

    assert payload == {"base_spec": {}, "exec_config": {}}
