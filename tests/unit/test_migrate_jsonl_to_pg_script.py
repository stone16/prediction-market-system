from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from scripts.migrate_jsonl_to_pg import _load_rows


def _parse_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return payload


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")


def test_load_rows_rejects_symlink_jsonl_file(tmp_path: Path) -> None:
    target_path = tmp_path / "target-feedback.jsonl"
    _write_jsonl(target_path, [{"feedback_id": "fb-1"}])
    path = tmp_path / "feedback.jsonl"
    path.symlink_to(target_path)

    with pytest.raises(ValueError, match="cannot be read safely"):
        _load_rows(path, _parse_payload)


def test_load_rows_rejects_malformed_jsonl_row(tmp_path: Path) -> None:
    path = tmp_path / "feedback.jsonl"
    path.write_text('{"feedback_id": "fb-1"\n', encoding="utf-8")

    with pytest.raises(ValueError, match="feedback\\.jsonl:1: invalid JSON row"):
        _load_rows(path, _parse_payload)


def test_load_rows_rejects_duplicate_jsonl_key(tmp_path: Path) -> None:
    path = tmp_path / "feedback.jsonl"
    path.write_text('{"feedback_id": "fb-1", "feedback_id": "fb-2"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="feedback\\.jsonl:1: duplicate JSON key"):
        _load_rows(path, _parse_payload)


def test_load_rows_opens_jsonl_file_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    path = tmp_path / "feedback.jsonl"
    _write_jsonl(path, [{"feedback_id": "fb-1"}])
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

    rows = _load_rows(path, _parse_payload)

    observed_by_path = {observed_path: flags for observed_path, flags in observed}
    assert rows == [{"feedback_id": "fb-1"}]
    assert observed_by_path[path] & no_follow_flag


def test_load_rows_rejects_hardlink_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "feedback.jsonl"
    _write_jsonl(path, [{"feedback_id": "fb-1"}])
    replacement_source = tmp_path / "replacement-feedback.jsonl"
    _write_jsonl(replacement_source, [{"feedback_id": "fb-2"}])
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == path and not swapped:
            swapped = True
            path.unlink()
            os.link(replacement_source, path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="cannot be read safely"):
        _load_rows(path, _parse_payload)

    assert swapped is True
