from __future__ import annotations

import argparse
import asyncio
import json
from typing import NoReturn, cast

import asyncpg

from pms.config import PMSSettings
from pms.storage.live_reconciliation import (
    SubmissionUnknownReconciliationStore,
    SubmissionUnknownResolutionStatus,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pms-live")
    subparsers = parser.add_subparsers(dest="command", required=True)
    reconcile = subparsers.add_parser(
        "reconcile-submission-unknown",
        help="Resolve a submission_unknown live incident after venue reconciliation.",
    )
    reconcile.add_argument("--decision-id", required=True)
    reconcile.add_argument("--venue-order-id")
    reconcile.add_argument(
        "--status",
        required=True,
        choices=("filled", "not_found", "open"),
    )
    reconcile.add_argument("--reconciled-by", required=True)
    reconcile.add_argument("--note")
    reconcile.add_argument("--database-url")
    return parser


async def _main_async(args: argparse.Namespace) -> int:
    if args.command == "reconcile-submission-unknown":
        database_url = args.database_url or PMSSettings().database.dsn
        pool = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=1)
        try:
            updated = await SubmissionUnknownReconciliationStore(
                pool
            ).reconcile_submission_unknown(
                decision_id=cast(str, args.decision_id),
                venue_order_id=cast(str | None, args.venue_order_id),
                status=cast(SubmissionUnknownResolutionStatus, args.status),
                reconciled_by=cast(str, args.reconciled_by),
                note=cast(str | None, args.note),
            )
        finally:
            await pool.close()
        print(
            json.dumps(
                {
                    "updated": updated,
                    "decision_id": args.decision_id,
                    "status": args.status,
                },
                sort_keys=True,
            )
        )
        return 0 if updated else 1
    _unreachable(args.command)


def _unreachable(command: object) -> NoReturn:
    raise RuntimeError(f"unsupported pms-live command: {command!r}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
