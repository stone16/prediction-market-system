from __future__ import annotations

import os
import stat
from pathlib import Path


def require_path_outside_working_tree(
    path: Path,
    *,
    label: str,
    error_type: type[Exception] = ValueError,
) -> None:
    configured_path = _absolute_path_without_symlink_resolution(path)
    resolved_path = path.expanduser().resolve(strict=False)
    working_tree = _working_tree_root(Path.cwd().resolve(strict=False))
    working_trees = [working_tree]
    for candidate in (configured_path, resolved_path):
        candidate_working_tree = _containing_working_tree_root(candidate)
        if candidate_working_tree is not None:
            working_trees.append(candidate_working_tree)

    for working_tree_candidate in dict.fromkeys(working_trees):
        if working_tree_candidate.parent == working_tree_candidate:
            continue
        for candidate in (configured_path, resolved_path):
            try:
                candidate.relative_to(working_tree_candidate)
            except ValueError:
                continue
            msg = f"{label} must live outside the working tree: {candidate}"
            raise error_type(msg)


def require_private_parent(
    path: Path,
    *,
    label: str,
    error_type: type[Exception] = ValueError,
) -> None:
    parent = path.parent
    try:
        mode = parent.lstat().st_mode
    except FileNotFoundError as exc:
        msg = f"{label} parent directory does not exist: {parent}"
        raise error_type(msg) from exc
    if not stat.S_ISDIR(mode):
        msg = f"{label} parent directory is not a directory: {parent}"
        raise error_type(msg)
    permissions = stat.S_IMODE(mode)
    if permissions & 0o077:
        msg = f"{label} parent directory {parent} is too permissive; run chmod 700"
        raise error_type(msg)
    if not permissions & stat.S_IWUSR:
        msg = f"{label} parent directory {parent} is not owner-writable; run chmod 700"
        raise error_type(msg)


def _absolute_path_without_symlink_resolution(path: Path) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return Path(os.path.abspath(expanded))


def _working_tree_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return start


def _containing_working_tree_root(path: Path) -> Path | None:
    start = path if path.is_dir() else path.parent
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return None
