"""Least-privilege PostgreSQL roles for producers, workers, and schedulers (§8).

Three separations matter here:

* the **migration role** owns the schema and is the only role that runs DDL;
* a **producer** may enqueue and read, but not transition or delete anything;
* a **worker** may claim and transition, but still cannot change the schema.

Queue scoping is enforced by the row-level-security policies installed in
migration 0002, driven by rows in ``role_queue_grants`` -- so the queue names a
role may touch arrive as bind parameters, never as interpolated SQL.

Role and schema names *are* identifiers, and PostgreSQL has no bind parameter
for an identifier. DDL statements (``CREATE ROLE``, ``GRANT``, ...) are
assembled server-side with ``format('... %I ... %L', $1, $2)`` and then
executed, so quoting is done by PostgreSQL itself. The plain DML against
``role_queue_grants`` interpolates the schema directly instead; that is safe
only because the schema has already passed
:func:`~rqueue.limits.validate_identifier`, the same protection
:mod:`rqueue.storage` relies on.
"""

from __future__ import annotations

import enum
from collections.abc import Sequence
from typing import TYPE_CHECKING

from rqueue.errors import ValidationError
from rqueue.limits import validate_identifier

if TYPE_CHECKING:
    import asyncpg

__all__ = ["Capability", "grant_queues", "provision_role", "revoke_role"]


class Capability(enum.StrEnum):
    """What a role is allowed to do with the queue."""

    #: Enqueue jobs and read them back.
    PRODUCE = "produce"
    #: Claim, heartbeat, and transition jobs.
    CONSUME = "consume"
    #: Read and fire periodic schedules.
    SCHEDULE = "schedule"
    #: Read-only inspection.
    INSPECT = "inspect"


# (table, privileges) per capability. Deliberately explicit rather than
# "GRANT ALL ON ALL TABLES": a producer that can DELETE FROM jobs is not a
# least-privilege producer.
#
# The producer's ``UPDATE (updated_at)`` on ``jobs`` is column-scoped on
# purpose. Every enqueue is an ``INSERT ... ON CONFLICT ... DO UPDATE SET
# updated_at = jobs.updated_at`` (see :meth:`rqueue.storage.Storage.insert_job`
# for why that no-op update, and not ``DO NOTHING``, is the right conflict
# path), and PostgreSQL demands UPDATE privilege on every column named in a
# ``DO UPDATE SET`` -- even one that writes a column back to its own value. A
# whole-table UPDATE would also let a producer rewrite ``state``, ``payload``,
# or ``attempt``, which is exactly what §8 says a producer may not do.
_TABLE_GRANTS: dict[Capability, tuple[tuple[str, str], ...]] = {
    Capability.PRODUCE: (
        ("jobs", "SELECT, INSERT, UPDATE (updated_at)"),
        ("job_attempts", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
    Capability.CONSUME: (
        ("jobs", "SELECT, INSERT, UPDATE"),
        ("job_attempts", "SELECT, INSERT, UPDATE"),
        ("concurrency_slots", "SELECT, INSERT, UPDATE, DELETE"),
        ("runtime_heartbeats", "SELECT, INSERT, UPDATE"),
        ("schema_migrations", "SELECT"),
    ),
    Capability.SCHEDULE: (
        ("jobs", "SELECT, INSERT"),
        ("schedules", "SELECT, INSERT, UPDATE"),
        ("schedule_occurrences", "SELECT, INSERT"),
        ("runtime_heartbeats", "SELECT, INSERT, UPDATE"),
        ("schema_migrations", "SELECT"),
    ),
    Capability.INSPECT: (
        ("jobs", "SELECT"),
        ("job_attempts", "SELECT"),
        ("schedules", "SELECT"),
        ("schedule_occurrences", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
}


def _drop_subsumed_column_privileges(privileges: set[str]) -> set[str]:
    """Drop column-scoped privileges a whole-table grant already covers.

    Capabilities are merged per table, so a role holding both PRODUCE and
    CONSUME collects ``UPDATE (updated_at)`` from one and a whole-table
    ``UPDATE`` from the other. Emitting both in one ``GRANT`` is redundant --
    the table-wide privilege subsumes the column list -- so the narrower entry
    is resolved away here rather than left for PostgreSQL to absorb silently.
    """
    resolved = set(privileges)
    for privilege in privileges:
        action, separator, _ = privilege.partition("(")
        if separator and action.strip() in privileges:
            resolved.discard(privilege)
    return resolved


async def _ddl(
    connection: asyncpg.Connection, template: str, *args: str | None
) -> None:
    """Build one DDL statement with PostgreSQL's own quoting, then run it.

    The template and every value are bind parameters to ``format()``; the only
    thing this module ever sends as literal SQL text is the result PostgreSQL
    itself produced, with ``%I``/``%L`` already quoted.
    """
    placeholders = ", ".join(f"${index + 2}::text" for index in range(len(args)))
    statement = await connection.fetchval(
        f"SELECT format($1::text, {placeholders})", template, *args
    )
    await connection.execute(statement)


async def provision_role(
    connection: asyncpg.Connection,
    *,
    role: str,
    capabilities: Sequence[Capability | str],
    schema: str = "task_queue",
    queues: Sequence[str] = ("*",),
    password: str | None = None,
    database: str | None = None,
) -> None:
    """Create or repair one scoped role.

    Re-running this repairs a role that has drifted: the revokes come first, so
    an over-granted role is narrowed back to exactly the capabilities asked for
    here. It never grants DDL -- schema ownership stays with the migration role.
    """
    role = validate_identifier(role, kind="role name")
    schema = validate_identifier(schema, kind="schema")
    resolved = [Capability(capability) for capability in capabilities]
    if not resolved:
        raise ValidationError("a role needs at least one capability")
    if not queues:
        raise ValidationError("a role needs at least one queue grant")

    exists = await connection.fetchval(
        "SELECT 1 FROM pg_roles WHERE rolname = $1", role
    )
    action = "ALTER ROLE" if exists else "CREATE ROLE"
    if password is None:
        await _ddl(connection, action + " %I LOGIN", role)
    else:
        await _ddl(connection, action + " %I LOGIN PASSWORD %L", role, password)

    dbname = database or await connection.fetchval("SELECT current_database()")
    await _ddl(connection, "REVOKE CREATE ON DATABASE %I FROM %I", dbname, role)
    await _ddl(connection, "REVOKE ALL PRIVILEGES ON SCHEMA %I FROM %I", schema, role)
    await _ddl(
        connection,
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM %I",
        schema,
        role,
    )
    await _ddl(connection, "GRANT CONNECT ON DATABASE %I TO %I", dbname, role)
    await _ddl(connection, "GRANT USAGE ON SCHEMA %I TO %I", schema, role)
    # The RLS policies read this table as the querying role, so the role must
    # be able to see its own grants.
    await _ddl(
        connection,
        "GRANT SELECT ON %I.role_queue_grants TO %I",
        schema,
        role,
    )

    granted: dict[str, set[str]] = {}
    for capability in resolved:
        for table, privileges in _TABLE_GRANTS[capability]:
            granted.setdefault(table, set()).update(
                part.strip() for part in privileges.split(",")
            )
    for table, merged in sorted(granted.items()):
        allowed = _drop_subsumed_column_privileges(merged)
        await _ddl(
            connection,
            "GRANT " + ", ".join(sorted(allowed)) + " ON %I.%I TO %I",
            schema,
            table,
            role,
        )

    await grant_queues(connection, role=role, schema=schema, queues=queues)


async def grant_queues(
    connection: asyncpg.Connection,
    *,
    role: str,
    schema: str = "task_queue",
    queues: Sequence[str],
    replace: bool = True,
) -> None:
    """Set the queues a role may reach. ``'*'`` means every queue."""
    schema = validate_identifier(schema, kind="schema")
    if replace:
        await connection.execute(
            f"DELETE FROM {schema}.role_queue_grants WHERE role_name = $1", role
        )
    await connection.executemany(
        f"""
        INSERT INTO {schema}.role_queue_grants (role_name, queue)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        [(role, queue) for queue in queues],
    )


async def revoke_role(
    connection: asyncpg.Connection,
    *,
    role: str,
    schema: str = "task_queue",
    drop: bool = False,
) -> None:
    """Strip every privilege from a role, and optionally drop it."""
    role = validate_identifier(role, kind="role name")
    schema = validate_identifier(schema, kind="schema")
    await connection.execute(
        f"DELETE FROM {schema}.role_queue_grants WHERE role_name = $1", role
    )
    await _ddl(
        connection,
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM %I",
        schema,
        role,
    )
    await _ddl(connection, "REVOKE ALL PRIVILEGES ON SCHEMA %I FROM %I", schema, role)
    if drop:
        await _ddl(connection, "DROP OWNED BY %I", role)
        await _ddl(connection, "DROP ROLE IF EXISTS %I", role)
