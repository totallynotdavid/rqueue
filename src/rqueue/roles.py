"""Least-privilege PostgreSQL roles for producers, workers, and schedulers.

Three separations matter here:

* the **migration role** owns the schema and is the only role that runs DDL;
* a **producer** may enqueue and read, but not transition or delete anything;
* a **worker** may claim and transition, but still cannot change the schema.

Queue scoping is enforced by the row-level-security policies installed in
migrations 0002 and 0005, driven by rows in ``role_queue_grants`` -- so the
queue names a role may touch arrive as bind parameters, never as interpolated
SQL.

Two of the separations are enforced by what a capability is *not* given.
:attr:`Capability.CONSUME` has no ``INSERT`` on ``jobs``: claiming and
transitioning work is not the same authority as creating it, and a compromised
worker that can enqueue can hand itself any task the fleet will run.
:attr:`Capability.PURGE` has no ``DELETE`` on ``jobs`` either -- it gets
``EXECUTE`` on the ``SECURITY DEFINER`` routine installed by migration 0006,
which re-derives queue, terminal state, age cutoff, and batch size for itself.
The routine is the safety boundary; the grant only decides who may ask.

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
# or ``attempt``, which a producer must not be able to do.
_TABLE_GRANTS: dict[Capability, tuple[tuple[str, str], ...]] = {
    Capability.PRODUCE: (
        ("jobs", "SELECT, INSERT, UPDATE (updated_at)"),
        ("job_attempts", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
    Capability.CONSUME: (
        # No INSERT. A worker claims and transitions work; creating it is the
        # producer's authority, and a role that holds both should say so by
        # asking for both capabilities.
        ("jobs", "SELECT, UPDATE"),
        ("job_attempts", "SELECT, INSERT, UPDATE"),
        ("concurrency_slots", "SELECT, INSERT, UPDATE, DELETE"),
        ("runtime_heartbeats", "SELECT, INSERT, UPDATE"),
        # The claim query reads the pause table on every claim. SELECT only:
        # pausing a queue is an operator action, not something a worker role
        # may take.
        ("queue_pauses", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
    Capability.SCHEDULE: (
        # UPDATE is column-scoped for the same reason it is under PRODUCE: an
        # enqueue is an INSERT ... ON CONFLICT ... DO UPDATE SET updated_at,
        # and PostgreSQL demands UPDATE on every column that statement names.
        # A scheduler creates jobs; it does not transition them.
        ("jobs", "SELECT, INSERT, UPDATE (updated_at)"),
        ("schedules", "SELECT, INSERT, UPDATE"),
        ("schedule_occurrences", "SELECT, INSERT"),
        # DELETE, unlike the worker's grant: a scheduler's set of queues comes
        # from which schedules are enabled, which an operator changes while it
        # runs, so each tick retracts the heartbeats for queues it no longer
        # serves. A worker's queues are fixed when it is constructed and it has
        # nothing to retract.
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
        # Readiness is an inspection: check_readiness reads worker and
        # scheduler liveness on every probe, and a role that cannot read this
        # table reports a healthy database as unreachable rather than as
        # workerless (see :func:`rqueue.health.check_readiness`).
        ("runtime_heartbeats", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
    # PURGE reads what it is about to delete and is otherwise powerless
    # against the table: the deleting is done by _FUNCTION_GRANTS below.
    Capability.PURGE: (
        ("jobs", "SELECT"),
        ("schema_migrations", "SELECT"),
    ),
}

#: Which runtime component a capability makes a role. A heartbeat names a
#: `kind`, and the policies on ``runtime_heartbeats`` only accept one the role
#: is entitled to, so a worker role cannot claim a scheduler is alive
#: (0008_heartbeat_ownership.sql). A role holding both capabilities claims
#: both, which is correct: one deployment can be both.
_RUNTIME_KINDS: Final[dict[Capability, str]] = {
    Capability.CONSUME: "worker",
    Capability.SCHEDULE: "scheduler",
}

#: Signature of the purge routine installed by migration 0006. The argument
#: list is part of the identity of a PostgreSQL function, so it has to be
#: spelled out to grant on it; it is a module constant rather than a caller's
#: string, and the schema and role around it are still quoted by ``%I``.
PURGE_FUNCTION: Final = ("purge_terminal_jobs", "text, text[], timestamptz, integer")

# (function name, argument types) per capability. Kept apart from
# _TABLE_GRANTS because REVOKE ... ON ALL TABLES does not touch routines: a
# role repaired without the matching routine revoke would keep an EXECUTE it
# is no longer entitled to.
_FUNCTION_GRANTS: dict[Capability, tuple[tuple[str, str], ...]] = {
    Capability.PURGE: (PURGE_FUNCTION,),
}

#: Role attributes that must not survive provisioning, with the clause that
#: clears each. PostgreSQL will only let a caller change an attribute it holds
#: itself -- clearing CREATEDB requires CREATEDB, even when the target does not
#: have it -- so a repair emits only the clauses it actually needs, and a role
#: that is genuinely over-privileged for the provisioning connection fails
#: loudly instead of being quietly left elevated.
_ELEVATED_ATTRIBUTES: Final = (
    ("rolsuper", "NOSUPERUSER"),
    ("rolcreatedb", "NOCREATEDB"),
    ("rolcreaterole", "NOCREATEROLE"),
    ("rolbypassrls", "NOBYPASSRLS"),
    ("rolreplication", "NOREPLICATION"),
    # NOINHERIT is the odd one out: inheritance is PostgreSQL's default, so
    # this is not repairing drift but refusing a default. A runtime role that
    # inherits picks up whatever a later membership grants it, silently; with
    # NOINHERIT a membership is inert until someone deliberately SETs the role.
    ("rolinherit", "NOINHERIT"),
)


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


async def _revoke_memberships(connection: asyncpg.Connection, role: str) -> None:
    """Take back the memberships an existing role has collected.

    Attributes are cleared by the ``ALTER ROLE`` in :func:`provision_role`;
    this is the other half, and it has to happen before the capability grants
    so that the role a caller inspects afterwards is the role this call
    actually described. ``provision_role`` grants no memberships of its own, so
    every membership found here is drift: something a human or another tool
    added, carrying privileges the capability model never accounted for.
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
    # GRANTED BY is not decoration, and it is not portable either.
    #
    # PostgreSQL 16 made one membership grantable several times by different
    # roles, each grant its own row with its own options, and made a bare
    # REVOKE remove only the grant the current role made. A membership someone
    # else granted survives it -- still inheriting, still carrying whatever the
    # group holds -- while this function reports the role narrowed. Naming the
    # grantor is the only way to reach those on 16 and later.
    #
    # It is also the release that added the clause. On 14 and 15 it is a syntax
    # error, and the package supports 14+. Nothing is lost by omitting it there: those
    # versions record a single grantor per membership, so the bare REVOKE that
    # is all they accept removes the membership outright, which is exactly the
    # outcome the clause buys on 16.
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

    PostgreSQL 16, which is also the release that accepts ``GRANTED BY`` on a
    role-membership REVOKE. Asked of the server rather than assumed, because
    the package supports three major versions and the answer differs across
    them.
    """
    version: int = await connection.fetchval(
        "SELECT current_setting('server_version_num')::integer"
    )
    return version >= 160000


def _all_attribute_clauses() -> str:
    """Every ``NO...`` clause, for a role being created.

    They are PostgreSQL's own defaults, so stating them costs no privilege, and
    stating them puts the role's shape in the statement rather than in an
    assumption about server defaults.
    """
    return "".join(f" {clause}" for _, clause in _ELEVATED_ATTRIBUTES)


async def _elevated_attribute_clauses(connection: asyncpg.Connection, role: str) -> str:
    """Only the ``NO...`` clauses an existing role actually needs.

    Clearing an attribute requires holding it: ``NOCREATEDB`` from a connection
    without ``CREATEDB`` is refused even when the target role has no
    ``CREATEDB`` to lose. Emitting the full set would therefore fail on every
    ordinary provisioning connection. Emitting only what is set means a role
    that is genuinely over-privileged for this connection fails loudly, which
    is the right answer -- a least-privilege role that cannot be made least
    privilege is not one.
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

    Ownership is not a privilege in the ACL, so every ``REVOKE`` in this module
    passes straight over it. An owner may ``GRANT`` itself anything it likes,
    and ``ALTER`` or ``DROP`` the object outright -- which makes owning
    ``jobs`` strictly more powerful than any capability, and makes a role that
    owns it un-narrowable by definition. Detected here rather than repaired:
    reassigning objects is a decision about who should hold them, and guessing
    at that from inside a role-provisioning call could hand a table to whoever
    happened to run the deployment script.
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
          -- Indexes and identity sequences cannot be owned separately from
          -- the table they hang off, so naming them would bury the one
          -- object an operator has to act on under a list of its parts.
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend AS d
              WHERE d.classid = 'pg_class'::regclass
                AND d.objid = c.oid
                AND d.deptype IN ('i', 'a')
          )
        UNION ALL
        -- Everything else in the schema, from one branch rather than a dozen.
        --
        -- Routines, types, collations, conversions, operators, operator
        -- classes and families, text-search dictionaries and configurations,
        -- extended statistics and extensions each live in their own catalog
        -- with their own owner column, and every one of them carries the same
        -- authority: ALTER and DROP on an object in a schema this role is
        -- never supposed to shape. Enumerating them catalog by catalog means
        -- the next one PostgreSQL adds is a hole nobody notices, and the
        -- catalogs ship no common view -- but `pg_shdepend` records one row
        -- per owned object whatever catalog it lives in, `pg_depend` says
        -- which schema it is in, and `pg_describe_object` names it the way an
        -- operator would have to write it in an ALTER.
        --
        -- `pg_class` keeps the explicit branch above: a role that owns nothing
        -- but is PostgreSQL's bootstrap superuser gets no `pg_shdepend` rows
        -- at all, and relations are the ownership that matters most to catch.
        -- The schema and database branches below read their catalogs directly
        -- for the same reason, and neither lives in a schema anyway.
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

    Re-running this repairs a role that has drifted: the revokes come first, so
    an over-granted role is narrowed back to exactly the capabilities asked for
    here. It never grants DDL -- schema ownership stays with the migration role.

    A role that *owns* an object in the schema, the schema itself, or the
    database is refused outright: ownership sits above the ACL this function
    edits, so an owner can re-grant itself anything the moment it is narrowed.

    Every failure leaves the role as it was, not only that one. The whole body
    runs in one transaction, so a call that raises part way through has written
    nothing -- the alternative is a role that exists and can log in carrying
    whichever half of a capability set got applied before the error, which is
    the state a caller reading the exception would least expect to find.

    Repair covers the privileges that live outside the schema's grant tables
    too. The role is set ``NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS
    NOREPLICATION NOINHERIT`` and every role membership it holds is revoked,
    before any capability grant is applied. Without that, "repair" would mean
    only that the *table* grants are right, while a ``BYPASSRLS`` role reads
    every queue in the database and a ``SUPERUSER`` role ignores the model
    entirely -- a least-privilege promise this function could not keep.

    There is no way to ask for less. A connection that cannot clear an
    attribute the role holds raises rather than returning a role that is only
    partly narrowed, because the caller's next act is to hand out that role's
    credentials.
    """
    role = validate_identifier(role, kind="role name")
    schema = validate_identifier(schema, kind="schema")
    resolved = [Capability(capability) for capability in capabilities]
    if not resolved:
        raise ValidationError("a role needs at least one capability")
    if not queues:
        raise ValidationError("a role needs at least one queue grant")

    # One transaction for the whole body. Role DDL is transactional in
    # PostgreSQL, so a failure at any step -- a PURGE grant against a schema
    # whose migrations stop short of the routine is the realistic one --
    # takes the role back to exactly what it was. Without it the failure
    # leaves a role that exists, can log in, and holds part of a capability
    # set nobody asked for: the half-narrowed state this function promises
    # never to produce.
    async with connection.transaction():
        exists = bool(
            await connection.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", role)
        )
        dbname: str = database or await connection.fetchval("SELECT current_database()")
        if exists:
            # First, even so: the transaction would undo a later refusal, but
            # this one is a decision about the role rather than a failure, and
            # it costs nothing to reach before any statement is issued.
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
        # Routines are a separate privilege class: the table revoke above leaves
        # EXECUTE untouched, so a role that once held PURGE would keep reaching the
        # purge routine after being re-provisioned without it.
        await _ddl(
            connection,
            "REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA %I FROM %I",
            schema,
            role,
        )
        # Sequences are a third, and the one an audit of table grants misses.
        # `jobs.seq` and `job_attempts.id` are GENERATED ALWAYS AS IDENTITY,
        # which is a sequence object of its own in the privilege system --
        # ALL TABLES does not reach it. UPDATE on one is `setval`, and
        # `jobs.seq` is the claim order's tiebreaker, so a privilege left
        # behind here hands out duplicate sequence values to a role this
        # function has just reported as narrowed.
        await _ddl(
            connection,
            "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I FROM %I",
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

    Replaced wholesale, like the queue grants: re-provisioning narrows a role,
    so a capability dropped from the call has to take its kind with it.
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
    # The identity sequences behind `jobs.seq` and `job_attempts.id`: separate
    # objects that ALL TABLES does not reach, and this function promises to
    # leave nothing.
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
