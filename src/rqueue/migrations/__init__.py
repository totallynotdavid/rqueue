"""Forward-only migration runner (REQUIREMENTS.md §7).

Nothing here runs implicitly. The only entry points are
``rqueue migrate`` (see :mod:`rqueue.cli`) and :func:`migrate`, which a test or
a deployment script calls deliberately. Importing :mod:`rqueue` creates no
schema and opens no connection.
"""

from __future__ import annotations

import hashlib
import re
import zlib
from dataclasses import dataclass
from importlib import resources
from typing import Final

import asyncpg

from rqueue.errors import MigrationError
from rqueue.limits import validate_identifier

__all__ = [
    "Migration",
    "MigrationStatus",
    "current_version",
    "load_migrations",
    "migrate",
    "status",
]

_FILENAME_RE: Final = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")
_NO_TRANSACTION_MARKER: Final = "-- rqueue:no-transaction"

#: `pg_advisory_lock` namespace for rqueue. The lock key is this value mixed
#: with the target schema name, so migrating two schemas in one database does
#: not serialize needlessly while two migrators of one schema do.
_ADVISORY_LOCK_NAMESPACE: Final = 0x5251_5545  # "RQUE"


@dataclass(frozen=True, slots=True)
class Migration:
    """One numbered migration file."""

    version: int
    name: str
    sql: str
    checksum: str

    @property
    def transactional(self) -> bool:
        """Whether this migration may run inside a transaction.

        PostgreSQL runs DDL transactionally, so every migration shipped today
        is transactional. The escape hatch exists for the statements that
        cannot be (``CREATE INDEX CONCURRENTLY``, ``ALTER TYPE ... ADD VALUE``
        on older servers); such a file declares itself with a
        ``-- rqueue:no-transaction`` marker on its first line.
        """
        return not self.sql.lstrip().startswith(_NO_TRANSACTION_MARKER)


@dataclass(frozen=True, slots=True)
class MigrationStatus:
    """What the database has, and what it is missing."""

    schema: str
    current_version: int
    latest_version: int
    applied: tuple[int, ...]
    pending: tuple[Migration, ...]

    @property
    def up_to_date(self) -> bool:
        return not self.pending


def load_migrations() -> tuple[Migration, ...]:
    """Read the packaged migration files, ordered by version."""
    found: list[Migration] = []
    for entry in resources.files(__package__).iterdir():
        match = _FILENAME_RE.match(entry.name)
        if match is None:
            continue
        sql = entry.read_text(encoding="utf-8")
        found.append(
            Migration(
                version=int(match.group(1)),
                name=match.group(2),
                sql=sql,
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            )
        )
    found.sort(key=lambda migration: migration.version)
    versions = [migration.version for migration in found]
    if len(set(versions)) != len(versions):
        raise MigrationError(f"duplicate migration versions: {versions}")
    if versions != list(range(1, len(versions) + 1)):
        raise MigrationError(
            f"migration versions must be contiguous from 1: {versions}"
        )
    return tuple(found)


def _advisory_lock_key(schema: str) -> int:
    """A stable signed 64-bit key for ``pg_advisory_lock(bigint)``."""
    mixed = (_ADVISORY_LOCK_NAMESPACE << 32) | zlib.crc32(schema.encode("utf-8"))
    # pg_advisory_lock takes a signed bigint; fold into that range.
    return mixed - (1 << 64) if mixed >= (1 << 63) else mixed


async def _bootstrap(connection: asyncpg.Connection, schema: str) -> None:
    """Create the schema and the ledger this runner needs to make decisions.

    This is the one place rqueue issues DDL outside a migration file, and it
    only happens inside the explicit migrate command.
    """
    await connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    await connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.schema_migrations (
            version    integer     PRIMARY KEY,
            name       text        NOT NULL,
            checksum   text        NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )


async def _applied(
    connection: asyncpg.Connection, schema: str
) -> dict[int, tuple[str, str]]:
    rows = await connection.fetch(
        f"SELECT version, name, checksum FROM {schema}.schema_migrations"
    )
    return {row["version"]: (row["name"], row["checksum"]) for row in rows}


async def status(connection: asyncpg.Connection, *, schema: str) -> MigrationStatus:
    """Report applied and pending migrations without changing anything."""
    schema = validate_identifier(schema, kind="schema")
    migrations = load_migrations()
    exists = await connection.fetchval(
        "SELECT to_regclass($1) IS NOT NULL",
        f"{schema}.schema_migrations",
    )
    applied = await _applied(connection, schema) if exists else {}
    _verify_forward_only(migrations, applied)
    pending = tuple(m for m in migrations if m.version not in applied)
    return MigrationStatus(
        schema=schema,
        current_version=max(applied, default=0),
        latest_version=max((m.version for m in migrations), default=0),
        applied=tuple(sorted(applied)),
        pending=pending,
    )


async def current_version(connection: asyncpg.Connection, *, schema: str) -> int:
    """The highest applied migration version, or 0 for an untouched schema."""
    return (await status(connection, schema=schema)).current_version


def _verify_forward_only(
    migrations: tuple[Migration, ...], applied: dict[int, tuple[str, str]]
) -> None:
    by_version = {migration.version: migration for migration in migrations}
    for version, (name, checksum) in sorted(applied.items()):
        migration = by_version.get(version)
        if migration is None:
            raise MigrationError(
                f"the database has migration {version:04d}_{name} applied, but this "
                "rqueue version does not ship it; migrations are forward-only, so "
                "downgrading the package below the applied schema is not supported"
            )
        if migration.checksum != checksum:
            raise MigrationError(
                f"migration {version:04d}_{migration.name} was modified after it was "
                f"applied (recorded checksum {checksum[:12]}, packaged "
                f"{migration.checksum[:12]}); write a new migration instead"
            )


def _failure(migration: Migration, exc: asyncpg.PostgresError) -> str:
    """Name the migration that failed, and let PostgreSQL say why.

    A migration can fail deliberately -- 0005 refuses to run while the previous
    release's workers are still heartbeating -- so this is a message an
    operator has to act on, not a traceback. asyncpg's rendering already
    carries the DETAIL and HINT such a message is written into; only the
    identity of the failing file is missing.
    """
    return f"migration {migration.version:04d}_{migration.name} failed: {exc}"


async def migrate(
    connection: asyncpg.Connection,
    *,
    schema: str = "task_queue",
    target: int | None = None,
) -> tuple[Migration, ...]:
    """Apply every pending migration and return the ones applied.

    Takes a session advisory lock for the duration, so concurrent deployments
    of the same schema queue up rather than racing. Each migration runs in its
    own transaction unless it declares otherwise, which means a failure part
    way through a batch leaves the ledger and the schema consistent with each
    other at the last migration that fully succeeded.
    """
    schema = validate_identifier(schema, kind="schema")
    migrations = load_migrations()
    if target is not None:
        migrations = tuple(m for m in migrations if m.version <= target)

    lock_key = _advisory_lock_key(schema)
    await connection.execute("SELECT pg_advisory_lock($1)", lock_key)
    try:
        await _bootstrap(connection, schema)
        applied = await _applied(connection, schema)
        _verify_forward_only(load_migrations(), applied)

        performed: list[Migration] = []
        for migration in migrations:
            if migration.version in applied:
                continue
            sql = migration.sql.replace("{schema}", schema)
            record = (
                f"INSERT INTO {schema}.schema_migrations (version, name, checksum) "
                "VALUES ($1, $2, $3)"
            )
            try:
                if migration.transactional:
                    async with connection.transaction():
                        await connection.execute(sql)
                        await connection.execute(
                            record,
                            migration.version,
                            migration.name,
                            migration.checksum,
                        )
                else:
                    await connection.execute(sql)
                    await connection.execute(
                        record, migration.version, migration.name, migration.checksum
                    )
            except asyncpg.PostgresError as exc:
                raise MigrationError(_failure(migration, exc)) from exc
            performed.append(migration)
        return tuple(performed)
    finally:
        await connection.execute("SELECT pg_advisory_unlock($1)", lock_key)
