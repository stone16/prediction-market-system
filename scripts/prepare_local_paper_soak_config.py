"""Prepare a repo-ignored local PAPER-soak config with private artifact paths."""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path


DEFAULT_SOURCE = Path("config.live-soak.yaml")
DEFAULT_OUTPUT = Path("config.local.live-soak.yaml")
DEFAULT_SECURE_DIR = Path("~/.local/share/pms/secure")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Copy config.live-soak.yaml to a local paper-soak config and rewrite "
            "launch artifact paths to a private user-writable directory."
        )
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--secure-dir", type=Path, default=DEFAULT_SECURE_DIR)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an existing local config.",
    )
    args = parser.parse_args(argv)

    try:
        output_path = _prepare_local_paper_soak_config(
            source=args.source,
            output=args.output,
            secure_dir=args.secure_dir,
            overwrite=bool(args.overwrite),
        )
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"local paper-soak config written: {output_path}")
    print(f"local artifact directory ready: {Path(args.secure_dir).expanduser()}")
    return 0


def _prepare_local_paper_soak_config(
    *,
    source: Path,
    output: Path,
    secure_dir: Path,
    overwrite: bool,
) -> Path:
    source_path = source.expanduser()
    output_path = output.expanduser()
    artifact_dir = secure_dir.expanduser()
    if source_path.resolve() == output_path.resolve():
        msg = "source and output config paths must be distinct"
        raise ValueError(msg)
    text = source_path.read_text(encoding="utf-8")
    calibration_path = artifact_dir / "flb-calibration.csv"
    category_prior_path = artifact_dir / "category-prior-observations.csv"

    text = _replace_single(
        text,
        "  category_prior_observations_path: null",
        f"  category_prior_observations_path: {_yaml_string(category_prior_path)}",
        field_name="controller.category_prior_observations_path",
    )
    text = _replace_single(
        text,
        "  flb_calibration_path: /secure/pms/flb-calibration.csv",
        f"  flb_calibration_path: {_yaml_string(calibration_path)}",
        field_name="strategies.flb_calibration_path",
    )

    _prepare_private_artifact_dir(artifact_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_text_no_follow(output_path, text, overwrite=overwrite)
    return output_path


def _replace_single(
    text: str,
    old: str,
    new: str,
    *,
    field_name: str,
) -> str:
    occurrences = text.count(old)
    if occurrences != 1:
        msg = (
            f"expected exactly one committed {field_name} source value to replace; "
            f"found {occurrences}"
        )
        raise ValueError(msg)
    return text.replace(old, new, 1)


def _prepare_private_artifact_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    mode = path.lstat().st_mode
    if not stat.S_ISDIR(mode):
        msg = f"local artifact path is not a directory: {path}"
        raise OSError(msg)
    permissions = stat.S_IMODE(mode)
    if permissions != 0o700:
        msg = f"local artifact directory must be chmod 700: {path}"
        raise OSError(msg)


def _write_text_no_follow(path: Path, content: str, *, overwrite: bool) -> None:
    _require_output_replaceable(path, overwrite=overwrite)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    temp_path = Path(temp_name)
    published = False
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        temp_path.chmod(0o600)
        _require_output_replaceable(path, overwrite=overwrite)
        os.replace(temp_path, path)
        published = True
    finally:
        if not published:
            temp_path.unlink(missing_ok=True)


def _require_output_replaceable(path: Path, *, overwrite: bool) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not overwrite:
        msg = f"output config already exists; pass --overwrite to replace: {path}"
        raise ValueError(msg)
    if not stat.S_ISREG(path_stat.st_mode):
        msg = f"output config path is not a regular file: {path}"
        raise OSError(msg)
    if path_stat.st_nlink != 1:
        msg = f"output config path is not a single-link file: {path}"
        raise OSError(msg)


def _yaml_string(path: Path) -> str:
    return json.dumps(os.fspath(path), ensure_ascii=True)


if __name__ == "__main__":
    sys.exit(main())
