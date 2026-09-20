"""Create and remove disposable PostgreSQL databases for the test tasks.

Mirrors picv-2025's ``scripts/database.py``, on asyncpg rather than psycopg so
this repository keeps a single database driver (docs/requirements.md §2).
"""

from __future__ import annotations

import argparse
import asyncio
import re
from dataclasses import dataclass
from urllib.parse import SplitResult, quote, urlsplit, urlunsplit

import asyncpg

# Names come from scripts/integration.sh, which builds them from a timestamp
# and a pid -- but they are interpolated into DDL, so validate rather than
# trust. asyncpg has no identifier-quoting helper; PostgreSQL's own
# format('%I') does the quoting below.
_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class DatabaseTarget:
    """An administrative connection and a connection to one database."""

    name: str
    maintenance_url: str
    database_url: str


def _check(name: str, kind: str) -> str:
    if not _NAME_RE.match(name) or len(name) > 63:
        raise SystemExit(f"invalid {kind}: {name!r}")
    return name


def _netloc(parts: SplitResult, user: str | None, password: str | None) -> str:
    hostname = parts.hostname
    if hostname is None:
        raise SystemExit("database URL must include a hostname")
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"

    port = parts.port
    host = f"{hostname}:{port}" if port is not None else hostname
    if user is None and password is None:
        return parts.netloc

    credentials = quote(user or "", safe="")
    if password is not None:
        credentials += f":{quote(password, safe='')}"
    return f"{credentials}@{host}"


def database_target(
    base_url: str,
    name: str,
    *,
    user: str | None = None,
    password: str | None = None,
) -> DatabaseTarget:
    """Build maintenance and target URLs without interpolating SQL values."""
    parts = urlsplit(base_url)
    maintenance_url = urlunsplit(parts._replace(path="/postgres"))
    database_url = urlunsplit(
        parts._replace(netloc=_netloc(parts, user, password), path=f"/{name}")
    )
    return DatabaseTarget(name, maintenance_url, database_url)


async def _ddl(connection: asyncpg.Connection, template: str, *args: str) -> None:
    statement = await connection.fetchval(
        "SELECT format($1::text, "
        + ", ".join(f"${index + 2}::text" for index in range(len(args)))
        + ")",
        template,
        *args,
    )
    await connection.execute(statement)


async def create_database(base_url: str, name: str) -> DatabaseTarget:
    target = database_target(base_url, _check(name, "database name"))
    connection = await asyncpg.connect(target.maintenance_url)
    try:
        await _ddl(connection, "CREATE DATABASE %I", target.name)
    finally:
        await connection.close()
    return target


async def drop_database(base_url: str, name: str, role: str | None) -> None:
    target = database_target(base_url, _check(name, "database name"))
    connection = await asyncpg.connect(target.maintenance_url)
    try:
        await _ddl(connection, "DROP DATABASE IF EXISTS %I WITH (FORCE)", target.name)
        if role:
            # The database is gone, so nothing it owned can block the drop.
            await _ddl(connection, "DROP ROLE IF EXISTS %I", _check(role, "role name"))
    finally:
        await connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("--base-url", required=True)
    create.add_argument("--name", required=True)

    url = subparsers.add_parser("url")
    url.add_argument("--base-url", required=True)
    url.add_argument("--name", required=True)
    url.add_argument("--user", required=True)
    url.add_argument("--password", required=True)

    drop = subparsers.add_parser("drop")
    drop.add_argument("--base-url", required=True)
    drop.add_argument("--name", required=True)
    drop.add_argument("--role")

    args = parser.parse_args()
    if args.command == "create":
        target = asyncio.run(create_database(args.base_url, args.name))
        print(target.database_url)
    elif args.command == "url":
        print(
            database_target(
                args.base_url,
                args.name,
                user=args.user,
                password=args.password,
            ).database_url
        )
    else:
        asyncio.run(drop_database(args.base_url, args.name, args.role))


if __name__ == "__main__":
    main()
