"""``rqueue`` command line: migrations and operational maintenance (§7).

Migrations run only from here (or from an explicit call to
:func:`rqueue.migrations.migrate`). Importing rqueue, constructing a Queue, or
starting a Worker never touches DDL.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from collections.abc import Sequence
from datetime import timedelta

import asyncpg

from rqueue import migrations
from rqueue.admin import Admin
from rqueue.errors import RqueueError
from rqueue.health import check_readiness
from rqueue.roles import Capability, provision_role

__all__ = ["main"]

_ENV_URL = "RQUEUE_DATABASE_URL"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rqueue", description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get(_ENV_URL),
        help=f"PostgreSQL connection URL (default: ${_ENV_URL})",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("RQUEUE_SCHEMA", "task_queue"),
        help="schema holding the queue tables (default: task_queue)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)

    migrate = subparsers.add_parser(
        "migrate", help="apply pending migrations (forward-only, advisory-locked)"
    )
    migrate.add_argument(
        "--target",
        type=int,
        default=None,
        help="stop after this migration version, for testing an upgrade path",
    )

    subparsers.add_parser("status", help="show applied and pending migrations")

    readiness = subparsers.add_parser(
        "readiness", help="probe connectivity, schema version, and runtime liveness"
    )
    readiness.add_argument("--queue", default=None)
    readiness.add_argument("--require-scheduler", action="store_true")
    readiness.add_argument("--no-require-worker", action="store_true")

    purge = subparsers.add_parser(
        "purge", help="delete terminal jobs older than a retention window"
    )
    purge.add_argument("--queue", default=None)
    purge.add_argument("--retention-days", type=float, required=True)
    purge.add_argument("--limit", type=int, default=10000)

    grant = subparsers.add_parser(
        "grant-role", help="create or repair a least-privilege role"
    )
    grant.add_argument("--role", required=True)
    grant.add_argument(
        "--capability",
        action="append",
        required=True,
        choices=[capability.value for capability in Capability],
    )
    grant.add_argument(
        "--queue",
        action="append",
        default=None,
        help="queue this role may reach; repeatable, defaults to '*'",
    )
    grant.add_argument("--password", default=os.environ.get("RQUEUE_ROLE_PASSWORD"))

    return parser


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        print(f"error: pass --database-url or set ${_ENV_URL}", file=sys.stderr)
        return 2

    connection = await asyncpg.connect(args.database_url)
    try:
        return await _dispatch(args, connection)
    finally:
        await connection.close()


async def _dispatch(args: argparse.Namespace, connection: asyncpg.Connection) -> int:
    if args.command == "migrate":
        applied = await migrations.migrate(
            connection, schema=args.schema, target=args.target
        )
        if not applied:
            _out(f"schema {args.schema} is already up to date")
        for migration in applied:
            _out(f"applied {migration.version:04d}_{migration.name}")
        return 0

    if args.command == "status":
        state = await migrations.status(connection, schema=args.schema)
        _out(
            json.dumps(
                {
                    "schema": state.schema,
                    "current_version": state.current_version,
                    "latest_version": state.latest_version,
                    "applied": list(state.applied),
                    "pending": [f"{m.version:04d}_{m.name}" for m in state.pending],
                    "up_to_date": state.up_to_date,
                },
                indent=2,
            )
        )
        return 0 if state.up_to_date else 1

    if args.command == "readiness":
        report = await check_readiness(
            _SingleConnectionPool(connection),
            schema=args.schema,
            queue=args.queue,
            require_worker=not args.no_require_worker,
            require_scheduler=args.require_scheduler,
        )
        _out(
            json.dumps(
                {
                    "ready": report.ready,
                    "connected": report.connected,
                    "schema_version": report.schema_version,
                    "expected_schema_version": report.expected_schema_version,
                    "migrations_up_to_date": report.migrations_up_to_date,
                    "workers": list(report.workers),
                    "schedulers": list(report.schedulers),
                    "problems": list(report.problems),
                },
                indent=2,
            )
        )
        return 0 if report.ready else 1

    if args.command == "purge":
        admin = Admin(_SingleConnectionPool(connection), schema=args.schema)
        removed = await admin.purge(
            queue=args.queue,
            retention=timedelta(days=args.retention_days),
            limit=args.limit,
        )
        _out(f"purged {removed} job(s)")
        return 0

    if args.command == "grant-role":
        await provision_role(
            connection,
            role=args.role,
            capabilities=[Capability(name) for name in args.capability],
            schema=args.schema,
            queues=tuple(args.queue or ("*",)),
            password=args.password,
        )
        _out(f"role {args.role} provisioned")
        return 0

    raise AssertionError(f"unhandled command {args.command!r}")  # pragma: no cover


class _SingleConnectionPool:
    """Adapt one connection to the tiny slice of the pool API admin code uses.

    The CLI is a one-shot process; opening a pool to run a single statement
    would be ceremony without benefit.
    """

    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    def acquire(self) -> _SingleConnectionPool:
        return self

    async def __aenter__(self) -> asyncpg.Connection:
        return self._connection

    async def __aexit__(self, *exc_info: object) -> None:
        return None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s - %(message)s",
    )
    try:
        return asyncio.run(_run(args))
    except RqueueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _out(message: str) -> None:
    print(message)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
