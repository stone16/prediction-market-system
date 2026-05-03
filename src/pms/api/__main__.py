from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

import uvicorn

from pms.config import load_settings


LOOPBACK_API_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pms-api", description="Run the PMS FastAPI server.")
    parser.add_argument(
        "--host",
        default=None,
        help="Deprecated. PMS_API_HOST is authoritative for the bind host.",
    )
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    parser.add_argument("--reload", action="store_true", help="Enable hot-reload for development.")
    parser.add_argument(
        "--config",
        default=None,
        help="YAML config path. Also available as PMS_CONFIG_PATH.",
    )
    return parser


def _startup_gate_message(host: str) -> str:
    return (
        f"Refusing to start pms-api on non-loopback PMS_API_HOST={host!r} without PMS_API_TOKEN. "
        "Set PMS_API_TOKEN or change PMS_API_HOST to 127.0.0.1, localhost, or ::1."
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    if args.config is not None:
        os.environ["PMS_CONFIG_PATH"] = args.config
    settings = load_settings(args.config)
    host = settings.api_host

    if not settings.api_token and host not in LOOPBACK_API_HOSTS:
        print(_startup_gate_message(host), file=sys.stderr)
        return 1

    uvicorn.run(
        "pms.api.app:create_app",
        factory=True,
        host=host,
        port=args.port,
        log_level=args.log_level,
        reload=args.reload,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
