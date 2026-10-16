"""Least-privilege PostgreSQL roles for producers, workers, and schedulers.

Queue scoping comes from row-level-security policies driven by rows in
``role_queue_grants``. Queue names reach SQL as bind parameters.

DDL statements (``CREATE ROLE``, ``GRANT``, ...) are assembled server-side with
``format('... %I ... %L', $1, $2)``, because PostgreSQL has no bind parameter
for an identifier. The DML against ``role_queue_grants`` interpolates the
schema, which has passed :func:`~rqueue.limits.validate_identifier`.
"""

from __future__ import annotations

import enum
import re
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Final

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
    #: Delete terminal jobs, through the bounded purge routine and only there.
    PURGE = "purge"


# (table, privileges) per capability.
#
# ``UPDATE (updated_at)`` on ``jobs`` is column-scoped because enqueue is an
# ``INSERT ... ON CONFLICT DO UPDATE SET updated_at``, and PostgreSQL requires
# UPDATE on every column that clause names.
_TABLE_GRANTS: dict[Capability, tuple[tuple[str, str], ...]] = {
    Capability.PRODUCE: (
        ("jobs", "SELECT, INSERT, UPDATE (updated_at)"),
        ("job_attempts", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
    Capability.CONSUME: (
        # No INSERT, and UPDATE leaves out ``task``, ``payload``, ``queue`` and
        # ``cancel_requested``, so a worker cannot turn a job into other work.
        (
            "jobs",
            "SELECT, UPDATE (state, attempt, scheduled_at, started_at, "
            "finished_at, worker_id, lease_token, leased_until, heartbeat_at, "
            "error_type, error_message, retry_policy, updated_at)",
        ),
        (
            "job_attempts",
            "SELECT, INSERT, UPDATE (finished_at, outcome, error_type, error_message)",
        ),
        ("concurrency_slots", "SELECT, INSERT, UPDATE, DELETE"),
        ("runtime_heartbeats", "SELECT, INSERT, UPDATE"),
        # SELECT only: pausing is an operator action.
        ("queue_pauses", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
    Capability.SCHEDULE: (
        ("jobs", "SELECT, INSERT, UPDATE (updated_at)"),
        ("schedules", "SELECT, INSERT, UPDATE"),
        ("schedule_occurrences", "SELECT, INSERT"),
        # DELETE, unlike the worker, so a tick can retract dropped queues.
        ("runtime_heartbeats", "SELECT, INSERT, UPDATE, DELETE"),
        ("schema_migrations", "SELECT"),
    ),
    Capability.INSPECT: (
        ("jobs", "SELECT"),
        ("job_attempts", "SELECT"),
        ("concurrency_slots", "SELECT"),
        ("schedules", "SELECT"),
        ("schedule_occurrences", "SELECT"),
        ("queue_pauses", "SELECT"),
        ("runtime_heartbeats", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
    Capability.PURGE: (
        ("jobs", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
}

#: Which runtime component a capability makes a role. The policies on
#: ``runtime_heartbeats`` accept only a `kind` the role is entitled to.
_RUNTIME_KINDS: Final[dict[Capability, str]] = {
    Capability.CONSUME: "worker",
    Capability.SCHEDULE: "scheduler",
}

#: The argument list is part of a PostgreSQL function's identity, so a grant
#: has to spell it out.
PURGE_FUNCTION: Final = ("purge_terminal_jobs", "text, text[], timestamptz, integer")

# (function name, argument types) per capability. Separate from _TABLE_GRANTS
# because REVOKE ... ON ALL TABLES does not touch routines.
_FUNCTION_GRANTS: dict[Capability, tuple[tuple[str, str], ...]] = {
    Capability.PURGE: (PURGE_FUNCTION,),
}

#: Role attributes that must not survive provisioning, with the clause that
#: clears each. PostgreSQL lets a caller clear only an attribute it holds
#: itself, so a repair emits only the clauses it needs.
_ELEVATED_ATTRIBUTES: Final = (
    ("rolsuper", "NOSUPERUSER"),
    ("rolcreatedb", "NOCREATEDB"),
    ("rolcreaterole", "NOCREATEROLE"),
    ("rolbypassrls", "NOBYPASSRLS"),
    ("rolreplication", "NOREPLICATION"),
    # Refuses a default: with NOINHERIT, a later membership stays inert until
    # someone SETs the role.
    ("rolinherit", "NOINHERIT"),
)


_PRIVILEGE: Final = re.compile(r"([A-Z]+)\s*(?:\(([^)]*)\))?")


def _resolve_privileges(grants: Iterable[str]) -> list[str]:
    """Merge the privilege lists of several capabilities for one table.

    Each entry is a ``GRANT`` privilege list such as
    ``"SELECT, UPDATE (a, b)"``.
    Capabilities are merged per table, so a role holding both PRODUCE and
    CONSUME collects ``UPDATE (updated_at)`` from one and a list of columns
    from the other. Column lists for one action are united into a single
    clause, and an action granted on the whole table swallows its column lists,
    so the ``GRANT`` that results says each thing once.
    """
    whole: set[str] = set()
    columns: dict[str, set[str]] = {}
    for grant in grants:
        for action, listed in _PRIVILEGE.findall(grant):
            if listed:
                columns.setdefault(action, set()).update(
                    column.strip() for column in listed.split(",")
                )
            else:
                whole.add(action)
    resolved = sorted(whole)
    resolved.extend(
        f"{action} ({', '.join(sorted(listed))})"
        for action, listed in sorted(columns.items())
        if action not in whole
    )
    return resolved


async def _ddl(
    connection: asyncpg.Connection, template: str, *args: str | None
) -> None:
    """Build one DDL statement with PostgreSQL's own quoting, then run it."""
    placeholders = ", ".join(f"${index + 2}::text" for index in range(len(args)))
    statement = await connection.fetchval(
        f"SELECT format($1::text, {placeholders})", template, *args
    )
    await connection.execute(statement)


async def _revoke_memberships(connection: asyncpg.Connection, role: str) -> None:
    """Take back every membership an existing role holds.

    ``provision_role`` grants none, so each one is drift.
    """
    inherited = await connection.fetch(
        """
        SELECT granted.rolname AS granted, grantor.rolname AS grantor
        FROM pg_auth_members AS m
        JOIN pg_roles AS granted ON granted.oid = m.roleid
        JOIN pg_roles AS holder ON holder.oid = m.member
        JOIN pg_roles AS grantor ON grantor.oid = m.grantor
        WHERE holder.rolname = $1
        ORDER BY granted.rolname, grantor.rolname
        """,
        role,
    )
    if not inherited:
        return
    # From PostgreSQL 16 a membership can be granted by several roles, and a
    # bare REVOKE removes only the current role's grant. GRANTED BY reaches the
    # others, but it is a syntax error on 14 and 15, which record one grantor.
    if await _tracks_multiple_grantors(connection):
        for row in inherited:
            await _ddl(
                connection,
                "REVOKE %I FROM %I GRANTED BY %I",
                row["granted"],
                role,
                row["grantor"],
            )
        return
    for granted in sorted({row["granted"] for row in inherited}):
        await _ddl(connection, "REVOKE %I FROM %I", granted, role)


async def _tracks_multiple_grantors(connection: asyncpg.Connection) -> bool:
    """Whether this server records more than one grantor per membership.

    That is PostgreSQL 16, the release that accepts ``GRANTED BY`` on REVOKE.
    """
    version: int = await connection.fetchval(
        "SELECT current_setting('server_version_num')::integer"
    )
    return version >= 160000


def _all_attribute_clauses() -> str:
    """Every ``NO...`` clause, for a role being created.

    They are PostgreSQL's defaults, so stating them needs no privilege.
    """
    return "".join(f" {clause}" for _, clause in _ELEVATED_ATTRIBUTES)


async def _elevated_attribute_clauses(connection: asyncpg.Connection, role: str) -> str:
    """Only the ``NO...`` clauses an existing role actually needs.

    Clearing an attribute requires holding it, even when the target role lacks
    it, so the full set would fail on an ordinary provisioning connection. A
    role over-privileged for this connection fails loudly.
    """
    current = await connection.fetchrow(
        "SELECT "
        + ", ".join(column for column, _ in _ELEVATED_ATTRIBUTES)
        + " FROM pg_roles WHERE rolname = $1",
        role,
    )
    if current is None:  # pragma: no cover - the role was dropped under us
        return ""
    return "".join(
        f" {clause}" for column, clause in _ELEVATED_ATTRIBUTES if current[column]
    )


async def _reject_owned_objects(
    connection: asyncpg.Connection, role: str, *, schema: str, database: str
) -> None:
    """Refuse to provision a role that owns anything in the schema.

    Ownership is not an ACL privilege, so no ``REVOKE`` narrows it. It is
    refused rather than repaired, because reassigning objects is a decision
    about who should hold them.
    """
    owned = await connection.fetch(
        """
        SELECT format('%s %I.%I', CASE c.relkind
                   WHEN 'r' THEN 'table' WHEN 'p' THEN 'table'
                   WHEN 'v' THEN 'view'  WHEN 'm' THEN 'materialized view'
                   WHEN 'S' THEN 'sequence'
                   ELSE 'relation' END, n.nspname, c.relname) AS described
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_roles AS r ON r.oid = c.relowner
        WHERE n.nspname = $1 AND r.rolname = $2
          -- Indexes and identity sequences belong to their table.
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend AS d
              WHERE d.classid = 'pg_class'::regclass
                AND d.objid = c.oid
                AND d.deptype IN ('i', 'a')
          )
        UNION ALL
        -- Every other owned object in the schema, whatever its catalog.
        -- pg_shdepend has one row per owned object, so a new object kind
        -- needs no new branch. The bootstrap superuser has no rows there,
        -- which is why relations, schemas and databases are read directly.
        SELECT pg_describe_object(dep.classid, dep.objid, 0)
        FROM pg_shdepend AS dep
        JOIN pg_roles AS r ON r.oid = dep.refobjid
        WHERE dep.dbid = (
                  SELECT d.oid FROM pg_database AS d
                  WHERE d.datname = current_database()
              )
          AND dep.deptype = 'o'
          AND dep.classid <> 'pg_class'::regclass
          AND r.rolname = $2
          AND EXISTS (
              SELECT 1 FROM pg_depend AS d
              JOIN pg_namespace AS n ON n.oid = d.refobjid
              WHERE d.classid = dep.classid AND d.objid = dep.objid
                AND d.refclassid = 'pg_namespace'::regclass
                AND n.nspname = $1
          )
        UNION ALL
        SELECT format('schema %I', n.nspname)
        FROM pg_namespace AS n
        JOIN pg_roles AS r ON r.oid = n.nspowner
        WHERE n.nspname = $1 AND r.rolname = $2
        UNION ALL
        SELECT format('database %I', d.datname)
        FROM pg_database AS d
        JOIN pg_roles AS r ON r.oid = d.datdba
        WHERE d.datname = $3 AND r.rolname = $2
        ORDER BY 1
        """,
        schema,
        role,
        database,
    )
    if not owned:
        return
    listed = ", ".join(row["described"] for row in owned)
    raise ValidationError(
        f"role {role!r} owns {listed}; ownership outranks every grant this "
        "function makes, so the role cannot be narrowed while it holds them. "
        "Reassign them to the migration role (ALTER ... OWNER TO, or REASSIGN "
        "OWNED BY) and provision again."
    )


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

    Re-running repairs a role that has drifted: the revokes come first, so the
    role ends with exactly the capabilities asked for. It never grants DDL.

    A role that owns an object in the schema, the schema itself, or the
    database is refused. The role is set ``NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOBYPASSRLS NOREPLICATION NOINHERIT`` and loses every membership. A
    connection that cannot clear an attribute the role holds raises.

    The whole body runs in one transaction, so a failure leaves the role as it
    was.
    """
    role = validate_identifier(role, kind="role name")
    schema = validate_identifier(schema, kind="schema")
    resolved = [Capability(capability) for capability in capabilities]
    if not resolved:
        raise ValidationError("a role needs at least one capability")
    if not queues:
        raise ValidationError("a role needs at least one queue grant")

    async with connection.transaction():
        exists = bool(
            await connection.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", role)
        )
        dbname: str = database or await connection.fetchval("SELECT current_database()")
        if exists:
            await _reject_owned_objects(
                connection, role, schema=schema, database=dbname
            )
            action = "ALTER ROLE"
            attributes = await _elevated_attribute_clauses(connection, role)
        else:
            action = "CREATE ROLE"
            attributes = _all_attribute_clauses()
        if password is None:
            await _ddl(connection, action + " %I LOGIN" + attributes, role)
        else:
            await _ddl(
                connection,
                action + " %I LOGIN" + attributes + " PASSWORD %L",
                role,
                password,
            )
        if exists:
            await _revoke_memberships(connection, role)

        await _ddl(connection, "REVOKE CREATE ON DATABASE %I FROM %I", dbname, role)
        await _ddl(
            connection, "REVOKE ALL PRIVILEGES ON SCHEMA %I FROM %I", schema, role
        )
        await _ddl(
            connection,
            "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM %I",
            schema,
            role,
        )
        # ALL TABLES reaches neither routines nor sequences.
        await _ddl(
            connection,
            "REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA %I FROM %I",
            schema,
            role,
        )
        # UPDATE on `jobs.seq`, the claim order's tiebreaker, is `setval`.
        await _ddl(
            connection,
            "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I FROM %I",
            schema,
            role,
        )
        await _ddl(connection, "GRANT CONNECT ON DATABASE %I TO %I", dbname, role)
        await _ddl(connection, "GRANT USAGE ON SCHEMA %I TO %I", schema, role)
        # The RLS policies read this table as the querying role.
        await _ddl(
            connection,
            "GRANT SELECT ON %I.role_queue_grants TO %I",
            schema,
            role,
        )

        granted: dict[str, list[str]] = {}
        for capability in resolved:
            for table, privileges in _TABLE_GRANTS[capability]:
                granted.setdefault(table, []).append(privileges)
        for table, wanted in sorted(granted.items()):
            await _ddl(
                connection,
                "GRANT " + ", ".join(_resolve_privileges(wanted)) + " ON %I.%I TO %I",
                schema,
                table,
                role,
            )

        routines = {
            signature
            for capability in resolved
            for signature in _FUNCTION_GRANTS.get(capability, ())
        }
        for name, argument_types in sorted(routines):
            await _ddl(
                connection,
                f"GRANT EXECUTE ON FUNCTION %I.%I({argument_types}) TO %I",
                schema,
                name,
                role,
            )

        await _set_runtime_kinds(connection, role=role, schema=schema, kinds=resolved)
        await grant_queues(connection, role=role, schema=schema, queues=queues)


async def _set_runtime_kinds(
    connection: asyncpg.Connection,
    *,
    role: str,
    schema: str,
    kinds: Sequence[Capability],
) -> None:
    """Record the component kinds this role may claim liveness as.

    Replaced wholesale, like the queue grants.
    """
    await connection.execute(
        f"DELETE FROM {schema}.role_runtime_kinds WHERE role_name = $1", role
    )
    wanted = sorted({_RUNTIME_KINDS[c] for c in kinds if c in _RUNTIME_KINDS})
    if not wanted:
        return
    await connection.executemany(
        f"""
        INSERT INTO {schema}.role_runtime_kinds (role_name, kind)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        [(role, kind) for kind in wanted],
    )


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
    await connection.execute(
        f"DELETE FROM {schema}.role_runtime_kinds WHERE role_name = $1", role
    )
    await _ddl(
        connection,
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM %I",
        schema,
        role,
    )
    await _ddl(
        connection,
        "REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA %I FROM %I",
        schema,
        role,
    )
    await _ddl(
        connection,
        "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I FROM %I",
        schema,
        role,
    )
    await _ddl(connection, "REVOKE ALL PRIVILEGES ON SCHEMA %I FROM %I", schema, role)
    if drop:
        await _ddl(connection, "DROP OWNED BY %I", role)
        await _ddl(connection, "DROP ROLE IF EXISTS %I", role)
