"""Migration, upgrade, retention cleanup, and least-privilege roles."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Final
from urllib.parse import urlsplit

import asyncpg
import pytest

from rqueue import (
    Admin,
    MigrationError,
    Queue,
    Retry,
    Scheduler,
    ScheduleSpec,
    TaskContext,
    Worker,
    check_readiness,
    migrations,
)
from rqueue.errors import ConfigurationError, ValidationError
from rqueue.limits import MAX_QUEUE_NAME_LENGTH, QUEUE_WILDCARD
from rqueue.models import JobState
from rqueue.roles import Capability, grant_queues, provision_role, revoke_role
from rqueue.storage import JobInsert, Storage
from scripts.database import database_target

from .support import eventually, running

# ------------------------------------------------------------------ migrations


async def structure(connection: asyncpg.Connection, schema: str) -> dict[str, object]:
    """A comparable description of a schema: columns, constraints, indexes."""
    columns = await connection.fetch(
        """
        SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1
        ORDER BY table_name, column_name
        """,
        schema,
    )
    indexes = await connection.fetch(
        """
        SELECT tablename, indexname,
               regexp_replace(indexdef, $1, 'SCHEMA', 'g') AS definition
        FROM pg_indexes WHERE schemaname = $2
        ORDER BY tablename, indexname
        """,
        schema,
        schema,
    )
    constraints = await connection.fetch(
        """
        SELECT rel.relname, con.conname,
               regexp_replace(
                   pg_get_constraintdef(con.oid), $1, 'SCHEMA', 'g'
               ) AS definition
        FROM pg_constraint AS con
        JOIN pg_class AS rel ON rel.oid = con.conrelid
        JOIN pg_namespace AS nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname = $2
        ORDER BY rel.relname, con.conname
        """,
        schema,
        schema,
    )
    return {
        "columns": [tuple(row) for row in columns],
        "indexes": [tuple(row) for row in indexes],
        "constraints": [tuple(row) for row in constraints],
    }


@pytest.fixture
async def scratch_schema(admin_dsn: str) -> object:
    """A throwaway schema so migration tests never touch the suite's own."""
    name = f"mig_{uuid.uuid4().hex[:10]}"
    connection = await asyncpg.connect(admin_dsn)
    try:
        yield (connection, name)
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
        await connection.close()


async def test_migrating_an_empty_database_installs_everything(
    scratch_schema: tuple[asyncpg.Connection, str],
) -> None:
    """Every packaged migration, in order, and the tables they add up to.

    The names are derived rather than transcribed. A second copy of the list
    here only ever caught "a migration was added", which is not a defect, and
    the ordered names are already pinned against a place a reader can check
    them: the migrate transcript in docs/migrations.md.
    """
    connection, schema = scratch_schema
    applied = await migrations.migrate(connection, schema=schema)
    assert [m.name for m in applied] == [m.name for m in migrations.load_migrations()]

    state = await migrations.status(connection, schema=schema)
    assert state.up_to_date
    assert state.current_version == state.latest_version

    tables = {
        row["table_name"]
        for row in await connection.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = $1",
            schema,
        )
    }
    assert tables == {
        "schema_migrations",
        "jobs",
        "job_attempts",
        "concurrency_slots",
        "schedules",
        "schedule_occurrences",
        "runtime_heartbeats",
        "role_queue_grants",
        "role_runtime_kinds",
        "queue_pauses",
    }


async def test_migrating_is_idempotent(
    scratch_schema: tuple[asyncpg.Connection, str],
) -> None:
    connection, schema = scratch_schema
    await migrations.migrate(connection, schema=schema)
    assert await migrations.migrate(connection, schema=schema) == ()


async def seed_for_upgrade(
    connection: asyncpg.Connection, schema: str, *, version: int
) -> None:
    """Whatever ``version`` can hold of what the later migrations transform.

    0005 adds a queue column to `job_attempts`, `concurrency_slots` and
    `schedule_occurrences`, backfills each from the job it references, and then
    declares it NOT NULL. On an empty schema all three are no-ops that cannot
    fail; the rows are what make the upgrade an upgrade.

    `runtime_heartbeats` is deliberately left empty: 0005 and 0008 refuse to
    run while any row is in it, which is its own test. The slot below is seeded
    expired for the same reason -- 0010 refuses to run while one is still
    leased -- and an expired row backfills through 0005 exactly as a live one
    would.
    """
    job = await connection.fetchval(
        f"""
        INSERT INTO {schema}.jobs (queue, task, payload, state, max_attempts)
        VALUES ('q', 't', '{{}}'::jsonb, 'pending', 3) RETURNING id
        """
    )
    await connection.execute(
        f"""
        INSERT INTO {schema}.job_attempts
            (job_id, queue, task, attempt, worker_id, lease_token)
        VALUES ($1, 'q', 't', 1, 'w1', gen_random_uuid())
        """,
        job,
    )
    # From 0005 onward these carry the queue themselves; before it, the column
    # is what the upgrade is about to add.
    scoped = ", queue" if version >= 5 else ""
    scoped_value = ", 'q'" if version >= 5 else ""
    await connection.execute(
        f"""
        INSERT INTO {schema}.concurrency_slots
            (key, job_id, lease_token, worker_id, leased_until{scoped})
        VALUES ('slot', $1, gen_random_uuid(), 'w1',
                now() - interval '1 minute'{scoped_value})
        """,
        job,
    )
    if version >= 2:
        schedule = await connection.fetchval(
            f"""
            INSERT INTO {schema}.schedules (name, queue, task, cron)
            VALUES ('s', 'q', 't', '* * * * *') RETURNING id
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.schedule_occurrences
                (schedule_id, occurrence_at, job_id{scoped})
            VALUES ($1, now(), $2{scoped_value})
            """,
            schedule,
            job,
        )
    if version >= 3:
        await connection.execute(
            f"INSERT INTO {schema}.queue_pauses (queue) VALUES ('q')"
        )


@pytest.mark.parametrize("start", [m.version - 1 for m in migrations.load_migrations()])
async def test_upgrading_from_any_shipped_version_matches_a_fresh_install(
    admin_dsn: str, start: int
) -> None:
    """Migrations are tested from empty *and* from what is already out there.

    Every version, not `latest - 1`. A release that ships one migration makes
    those the same thing, and this test used to say so -- which held right up
    until a release shipped three, at which point `latest - 1` named one of the
    *new* migrations and the only upgrade under test was the internal step
    between two files landing together. The version an operator is actually
    upgrading from is whichever one their last deploy left them on, and no
    expression over the packaged set knows which that is.

    So the parametrization covers all of them, including 0 (a fresh install, by
    a different route than the one below it) and the genuinely old versions
    where a multi-migration span has to compose.
    """
    stepwise = f"mig_step_{uuid.uuid4().hex[:8]}"
    fresh = f"mig_fresh_{uuid.uuid4().hex[:8]}"
    latest = max(m.version for m in migrations.load_migrations())
    connection = await asyncpg.connect(admin_dsn)
    try:
        applied = await migrations.migrate(connection, schema=stepwise, target=start)
        assert [m.version for m in applied] == list(range(1, start + 1))
        assert await migrations.current_version(connection, schema=stepwise) == start

        # That version is a working queue on its own, holding the rows 0005
        # has to carry across: a comparison of two empty schemas would pass
        # over a backfill that only fails when there is something to backfill.
        if start:
            await seed_for_upgrade(connection, stepwise, version=start)

        upgraded = await migrations.migrate(connection, schema=stepwise)
        assert [m.version for m in upgraded] == list(range(start + 1, latest + 1))

        await migrations.migrate(connection, schema=fresh)
        assert await structure(connection, stepwise) == await structure(
            connection, fresh
        )
        # The row written under the older version survived the upgrade.
        expected = 1 if start else 0
        assert (
            await connection.fetchval(f"SELECT count(*) FROM {stepwise}.jobs")
            == expected
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {stepwise} CASCADE")
        await connection.execute(f"DROP SCHEMA IF EXISTS {fresh} CASCADE")
        await connection.close()


async def test_a_failing_migration_leaves_no_partial_schema(
    scratch_schema: tuple[asyncpg.Connection, str],
) -> None:
    """Migrations are transactional, which PostgreSQL permits for all DDL."""
    connection, schema = scratch_schema
    await connection.execute(f"CREATE SCHEMA {schema}")
    await connection.execute(f"CREATE TABLE {schema}.jobs (id int)")

    # A migration that fails is a migration error, whatever PostgreSQL raised
    # underneath: the CLI's one job at that point is to print something the
    # operator can act on, and it only formats rqueue's own errors.
    with pytest.raises(MigrationError) as failure:
        await migrations.migrate(connection, schema=schema)
    assert "0001_core" in str(failure.value)
    assert 'relation "jobs" already exists' in str(failure.value)

    assert await migrations.current_version(connection, schema=schema) == 0
    # Nothing from the aborted migration stuck around.
    assert (
        await connection.fetchval(
            "SELECT to_regclass($1) IS NULL", f"{schema}.job_attempts"
        )
        is True
    )
    assert (
        await connection.fetchval(
            "SELECT to_regproc($1) IS NULL", f"{schema}.notify_job_available"
        )
        is True
    )


async def test_jobs_reject_malformed_retry_policy_data(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """The database refuses invalid or executable policy payloads at insert."""
    valid = {
        "version": 1,
        "max_attempts": 3,
        "initial_backoff": 1.0,
        "max_backoff": 3600.0,
        "multiplier": 2.0,
        "jitter": 0.1,
    }
    malformed = [
        {**valid, "retry_if": "builtins:eval"},
        {**valid, "retry_on": ["builtins:BaseException"]},
        {**valid, "max_attempts": 3.0},
        {**valid, "multiplier": 10**400},
        {**valid, "max_attempts": 0},
        {**valid, "max_attempts": 4},
        {**valid, "jitter": 2.0},
    ]

    async with pool.acquire() as connection:
        for index, policy in enumerate(malformed):
            with pytest.raises(asyncpg.PostgresError):
                await connection.execute(
                    f"""
                    INSERT INTO {queue.schema}.jobs
                        (queue, task, payload, state, max_attempts, retry_policy)
                    VALUES ($1, $2, '{{}}'::jsonb, 'pending', 3, $3::jsonb)
                    """,
                    queue.name,
                    f"malformed-{index}",
                    json.dumps(policy),
                )

        assert (
            await connection.fetchval(
                f"SELECT count(*) FROM {queue.schema}.jobs WHERE queue = $1",
                queue.name,
            )
            == 0
        )


async def test_claim_handles_a_malformed_policy_without_leaking_a_lease(
    scratch_schema: tuple[asyncpg.Connection, str],
) -> None:
    """A legacy/corrupt row fails durably inside the claim transaction."""
    connection, schema = scratch_schema
    await migrations.migrate(connection, schema=schema)
    await connection.execute(
        f"ALTER TABLE {schema}.jobs DROP CONSTRAINT jobs_retry_policy_shape"
    )
    await connection.execute(
        f"""
        INSERT INTO {schema}.jobs
            (queue, task, payload, state, max_attempts, retry_policy)
        VALUES ('q', 'bad-policy', '{{}}'::jsonb, 'pending', 3, '{{}}'::jsonb)
        """
    )
    # Keep a live NOT VALID constraint in place. This models a legacy row
    # that predates the policy shape check: PostgreSQL permits it to remain,
    # but any UPDATE must satisfy the constraint. Claim must first quarantine
    # the policy to NULL, otherwise lease_jobs itself would fail the UPDATE.
    await connection.execute(
        f"""
        ALTER TABLE {schema}.jobs
        ADD CONSTRAINT jobs_retry_policy_shape CHECK (
            retry_policy IS NULL OR retry_policy ? 'version'
        ) NOT VALID
        """
    )

    claimed = await Storage(schema).claim(
        connection,
        queue="q",
        worker_id="worker",
        tasks=["bad-policy"],
        limit=1,
        lease_seconds=30.0,
    )

    assert claimed == []
    row = await connection.fetchrow(
        f"SELECT id, state, attempt, lease_token, error_type, error_message "
        f"FROM {schema}.jobs"
    )
    assert row is not None
    assert row["state"] == "failed"
    assert row["attempt"] == 1
    assert row["lease_token"] is None
    assert row["error_type"] == "ConfigurationError"
    assert "invalid persisted retry policy" in row["error_message"]
    assert "{}" in row["error_message"]
    stored = await Storage(schema).get_job(connection, row["id"])
    assert stored is not None and stored.retry_policy is None
    attempt = await connection.fetchrow(
        f"SELECT outcome FROM {schema}.job_attempts WHERE job_id = "
        f"(SELECT id FROM {schema}.jobs)"
    )
    assert attempt is not None and attempt["outcome"] == "failed"


async def test_a_tampered_migration_is_refused(
    scratch_schema: tuple[asyncpg.Connection, str],
) -> None:
    connection, schema = scratch_schema
    await migrations.migrate(connection, schema=schema)
    await connection.execute(
        f"UPDATE {schema}.schema_migrations SET checksum = 'deadbeef' WHERE version = 1"
    )
    with pytest.raises(MigrationError, match="modified after it was applied"):
        await migrations.status(connection, schema=schema)


async def test_downgrading_below_the_applied_schema_is_refused(
    scratch_schema: tuple[asyncpg.Connection, str],
) -> None:
    connection, schema = scratch_schema
    await migrations.migrate(connection, schema=schema)
    await connection.execute(
        f"INSERT INTO {schema}.schema_migrations (version, name, checksum) "
        "VALUES (99, 'from_the_future', 'x')"
    )
    with pytest.raises(MigrationError, match="forward-only"):
        await migrations.migrate(connection, schema=schema)


# ------------------------------------------------------------------- retention


async def test_retention_cleanup_removes_only_old_terminal_jobs(
    queue: Queue, pool: asyncpg.Pool, admin: Admin
) -> None:
    now = datetime.now(UTC)
    async with pool.acquire() as connection:
        async with connection.transaction():
            old = await queue.enqueue(connection, task="prepare")
            recent = await queue.enqueue(connection, task="prepare")
            live = await queue.enqueue(connection, task="prepare")

        for job_id in (old.id, recent.id):
            claimed = await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id="w",
                tasks=["prepare"],
                limit=1,
                lease_seconds=30,
            )
            await queue.storage.complete(
                connection,
                job_id=claimed[0].job.id,
                lease_token=claimed[0].lease_token,
            )
            del job_id

        # Age one of the finished jobs past the retention window.
        await connection.execute(
            f"UPDATE {queue.schema}.jobs SET finished_at = $2 WHERE id = $1",
            old.id,
            now - timedelta(days=40),
        )
        aged_attempts = await connection.fetchval(
            f"SELECT count(*) FROM {queue.schema}.job_attempts WHERE job_id = $1",
            old.id,
        )
    assert aged_attempts == 1

    removed = await admin.purge(queue=queue.name, retention=timedelta(days=30))
    assert removed == 1
    assert await queue.get_job(old.id) is None
    assert await queue.get_job(recent.id) is not None
    assert await queue.get_job(live.id) is not None

    async with pool.acquire() as connection:
        orphans = await connection.fetchval(
            f"SELECT count(*) FROM {queue.schema}.job_attempts WHERE job_id = $1",
            old.id,
        )
    assert orphans == 0, "attempt records should cascade with their job"


async def test_purge_can_be_narrowed_to_particular_states(
    queue: Queue, pool: asyncpg.Pool, admin: Admin
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            succeeded = await queue.enqueue(connection, task="prepare")
            failed = await queue.enqueue(connection, task="prepare")
        for job_id, terminal in ((succeeded.id, "complete"), (failed.id, "fail")):
            claimed = await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id="w",
                tasks=["prepare"],
                limit=1,
                lease_seconds=30,
            )
            entry = claimed[0]
            if terminal == "complete":
                await queue.storage.complete(
                    connection, job_id=entry.job.id, lease_token=entry.lease_token
                )
            else:
                await queue.storage.fail_terminal(
                    connection,
                    job_id=entry.job.id,
                    lease_token=entry.lease_token,
                    error_type="Boom",
                    error_message="kept for triage",
                )
            del job_id
        await connection.execute(
            f"UPDATE {queue.schema}.jobs SET finished_at = now() - interval '40 days' "
            "WHERE queue = $1",
            queue.name,
        )

    removed = await admin.purge(
        queue=queue.name,
        retention=timedelta(days=30),
        states=[JobState.SUCCEEDED],
    )
    assert removed == 1
    remaining = await queue.list_jobs()
    assert [job.state for job in remaining] == [JobState.FAILED]


async def test_readiness_distinguishes_its_failure_modes(
    pool: asyncpg.Pool, queue: Queue
) -> None:
    report = await check_readiness(pool, queue=queue.name, require_worker=True)
    assert report.connected
    assert report.migrations_up_to_date
    assert not report.worker_available
    assert not report.ready
    assert any("no worker heartbeat" in problem for problem in report.problems)

    relaxed = await check_readiness(pool, queue=queue.name, require_worker=False)
    assert relaxed.ready


# ------------------------------------------------------------ least privilege


async def test_the_provisioned_app_role_is_least_privilege(
    app_dsn: str | None, app_role: str | None, admin_dsn: str
) -> None:
    """The role scripts/integration.sh provisions: scoped, and no more."""
    if not app_dsn or not app_role:
        pytest.skip("run through scripts/integration.sh to get a scoped app role")

    connection = await asyncpg.connect(app_dsn)
    try:
        pool = await asyncpg.create_pool(app_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            allowed = Queue(pool, name="scoped")
            forbidden = Queue(pool, name="unscoped")

            async with connection.transaction():
                job = await allowed.enqueue(connection, task="prepare", payload={})
            assert job.state == JobState.PENDING

            # Out-of-scope queues are invisible and unwritable (RLS).
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await forbidden.enqueue(connection, task="prepare", payload={})

            # A worker capability still works inside the granted queue.
            claimed = await allowed.storage.claim(
                connection,
                queue="scoped",
                worker_id="scoped-worker",
                tasks=["prepare"],
                limit=1,
                lease_seconds=30,
            )
            assert len(claimed) == 1
            await allowed.storage.complete(
                connection,
                job_id=claimed[0].job.id,
                lease_token=claimed[0].lease_token,
            )

            # ... and DDL, deletion, and schema creation stay out of reach.
            for statement in (
                "CREATE TABLE task_queue.sneaky (id int)",
                "CREATE TABLE public.sneaky (id int)",
                "DROP TABLE task_queue.jobs",
                "ALTER TABLE task_queue.jobs ADD COLUMN sneaky int",
                "DELETE FROM task_queue.jobs",
                "TRUNCATE task_queue.jobs",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute(statement)
        finally:
            await pool.close()
    finally:
        await connection.close()


async def test_role_provisioning_is_repeatable_and_revocable(
    admin_dsn: str,
) -> None:
    role = f"rq_role_{uuid.uuid4().hex[:8]}"
    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.PRODUCE],
            queues=["alpha"],
            password="test-password",
        )
        # Re-running narrows an over-granted role back to the declared shape.
        await admin_connection.execute(f"GRANT DELETE ON task_queue.jobs TO {role}")
        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.PRODUCE],
            queues=["alpha"],
            password="test-password",
        )
        privileges = {
            row["privilege_type"]
            for row in await admin_connection.fetch(
                """
                SELECT privilege_type FROM information_schema.role_table_grants
                WHERE grantee = $1 AND table_schema = 'task_queue'
                  AND table_name = 'jobs'
                """,
                role,
            )
        }
        assert privileges == {"SELECT", "INSERT"}

        # The enqueue statement's DO UPDATE needs UPDATE, but only on the one
        # column it writes -- a table-level UPDATE would not show up here.
        updatable = {
            row["column_name"]
            for row in await admin_connection.fetch(
                """
                SELECT column_name FROM information_schema.column_privileges
                WHERE grantee = $1 AND table_schema = 'task_queue'
                  AND table_name = 'jobs' AND privilege_type = 'UPDATE'
                """,
                role,
            )
        }
        assert updatable == {"updated_at"}

        grants = await admin_connection.fetch(
            "SELECT queue FROM task_queue.role_queue_grants WHERE role_name = $1",
            role,
        )
        assert [row["queue"] for row in grants] == ["alpha"]

        await revoke_role(admin_connection, role=role, drop=True)
        assert (
            await admin_connection.fetchval(
                "SELECT count(*) FROM pg_roles WHERE rolname = $1", role
            )
            == 0
        )
    finally:
        await admin_connection.close()


async def test_produce_and_consume_merge_into_one_whole_table_update(
    admin_dsn: str,
) -> None:
    """PRODUCE's column grant is subsumed, not emitted alongside CONSUME's."""
    role = f"rq_role_{uuid.uuid4().hex[:8]}"
    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.PRODUCE, Capability.CONSUME],
            queues=["alpha"],
            password="test-password",
        )
        privileges = {
            row["privilege_type"]
            for row in await admin_connection.fetch(
                """
                SELECT privilege_type FROM information_schema.role_table_grants
                WHERE grantee = $1 AND table_schema = 'task_queue'
                  AND table_name = 'jobs'
                """,
                role,
            )
        }
        assert privileges == {"SELECT", "INSERT", "UPDATE"}
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
        finally:
            await admin_connection.close()


@asynccontextmanager
async def scoped_role_pool(
    admin_dsn: str,
    *,
    capabilities: Sequence[Capability],
    queues: Sequence[str],
    prefix: str = "rq_scoped",
) -> AsyncIterator[asyncpg.Pool]:
    """A pool logged in as a freshly provisioned role, dropped afterwards.

    Grant shapes can be asserted from Python (see ``tests/test_roles.py``), but
    a privilege only really exists once PostgreSQL enforces it against a real
    connection -- so every claim about what a capability cannot do is made here
    by trying it.
    """
    role = f"{prefix}_{uuid.uuid4().hex[:8]}"
    password = "scoped-role-test-password"
    database = urlsplit(admin_dsn).path.lstrip("/")
    target = database_target(admin_dsn, database, user=role, password=password)

    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        await provision_role(
            admin_connection,
            role=role,
            capabilities=capabilities,
            queues=queues,
            password=password,
        )
        pool = await asyncpg.create_pool(target.database_url, min_size=1, max_size=2)
        assert pool is not None
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
        finally:
            await admin_connection.close()


def produce_only_pool(
    admin_dsn: str, queue: str
) -> AbstractAsyncContextManager[asyncpg.Pool]:
    """A pool logged in as a role holding nothing but ``Capability.PRODUCE``."""
    return scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.PRODUCE],
        queues=[queue],
        prefix="rq_producer",
    )


async def test_a_produce_only_role_can_enqueue(admin_dsn: str, queue_name: str) -> None:
    """PRODUCE alone has to be enough to enqueue, both insert paths.

    Every enqueue is an ``INSERT ... ON CONFLICT ... DO UPDATE SET updated_at``
    (see :meth:`rqueue.storage.Storage.insert_job`), and PostgreSQL demands
    UPDATE privilege on ``updated_at`` for that statement even when no conflict
    fires. Existing role coverage provisions produce *and* consume, which hides
    the case this test exists for.
    """
    async with produce_only_pool(admin_dsn, queue_name) as pool:
        queue = Queue(pool, name=queue_name)
        async with pool.acquire() as connection:
            # The plain insert path: no conflict, but the statement still
            # names updated_at in its DO UPDATE.
            async with connection.transaction():
                fresh = await queue.enqueue(connection, task="prepare", payload={})
            assert fresh.state == JobState.PENDING

            # The conflict path itself: a second enqueue on a live dedupe key
            # is resolved by the no-op DO UPDATE, which is what the column
            # grant is actually for.
            async with connection.transaction():
                first = await queue.enqueue(
                    connection,
                    task="prepare",
                    dedupe_key="sim:produce-only",
                    on_conflict="return_existing",
                )
            async with connection.transaction():
                second = await queue.enqueue(
                    connection,
                    task="prepare",
                    dedupe_key="sim:produce-only",
                    on_conflict="return_existing",
                )
            assert second.id == first.id

            rows = await connection.fetch(
                "SELECT id FROM task_queue.jobs WHERE queue = $1", queue_name
            )
            assert len(rows) == 2


async def test_a_produce_only_role_cannot_rewrite_a_job(
    admin_dsn: str, queue_name: str
) -> None:
    """The column grant stayed a column grant.

    A blanket table-level UPDATE would also make this pass silently, so this is
    the half of the fix that proves it is still least privilege.
    """
    async with produce_only_pool(admin_dsn, queue_name) as pool:
        queue = Queue(pool, name=queue_name)
        async with pool.acquire() as connection:
            async with connection.transaction():
                job = await queue.enqueue(connection, task="prepare", payload={})

            for column, value in (
                ("state", "'succeeded'"),
                ("payload", "'{\"owned\": true}'::jsonb"),
                ("attempt", "99"),
                ("max_attempts", "99"),
                ("cancel_requested", "true"),
                ("lease_token", "gen_random_uuid()"),
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute(
                        f"UPDATE task_queue.jobs SET {column} = {value} WHERE id = $1",
                        job.id,
                    )

            # ... while the one column the enqueue statement needs is writable.
            await connection.execute(
                "UPDATE task_queue.jobs SET updated_at = updated_at WHERE id = $1",
                job.id,
            )
            assert await connection.fetchval(
                "SELECT state FROM task_queue.jobs WHERE id = $1", job.id
            ) == str(JobState.PENDING)


# ------------------------------------------------- consuming is not producing


async def test_a_consume_only_role_cannot_enqueue(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """A worker claims and transitions work; it does not create work.

    The grant shape is asserted without a database in ``tests/test_roles.py``;
    this is the half that proves PostgreSQL agrees, because a stray
    ``GRANT INSERT`` from anywhere else would leave that unit test green.
    """
    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.CONSUME],
        queues=[queue_name],
        prefix="rq_consumer",
    ) as consumer_pool:
        queue = Queue(consumer_pool, name=queue_name)
        async with consumer_pool.acquire() as connection:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await queue.enqueue(connection, task="prepare", payload={})

        # An operator-created job is still claimable and transitionable, so the
        # capability remains a working worker role rather than a broken one.
        async with pool.acquire() as connection:
            job = await Queue(pool, name=queue_name).enqueue(
                connection, task="prepare", payload={}
            )

        async with consumer_pool.acquire() as connection:
            claimed = await queue.storage.claim(
                connection,
                queue=queue_name,
                worker_id="consume-only",
                tasks=["prepare"],
                limit=1,
                lease_seconds=30,
            )
            assert [entry.job.id for entry in claimed] == [job.id]
            finished = await queue.storage.complete(
                connection,
                job_id=claimed[0].job.id,
                lease_token=claimed[0].lease_token,
            )
            assert finished.state == JobState.SUCCEEDED


# ------------------------------------------------------------ bounded purging


async def seed_terminal_job(
    pool: asyncpg.Pool, *, queue: str, state: str, age: timedelta
) -> uuid.UUID:
    """One job in ``state``, finished ``age`` ago -- or still pending."""
    finished = None if state == "pending" else datetime.now(UTC) - age
    async with pool.acquire() as connection:
        return await connection.fetchval(  # type: ignore[no-any-return]
            """
            INSERT INTO task_queue.jobs
                (queue, task, payload, state, max_attempts, finished_at)
            VALUES ($1, 'prepare', '{}'::jsonb, $2, 3, $3)
            RETURNING id
            """,
            queue,
            state,
            finished,
        )


async def surviving(pool: asyncpg.Pool, *job_ids: uuid.UUID) -> set[uuid.UUID]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            "SELECT id FROM task_queue.jobs WHERE id = ANY($1::uuid[])", list(job_ids)
        )
    return {row["id"] for row in rows}


async def test_a_purge_role_deletes_only_through_the_routine(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """PURGE grants EXECUTE, never DELETE. The routine is the boundary."""
    old_job = await seed_terminal_job(
        pool, queue=queue_name, state="succeeded", age=timedelta(days=30)
    )
    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.PURGE],
            queues=[queue_name],
            prefix="rq_purger",
        ) as purge_pool,
        purge_pool.acquire() as connection,
    ):
        for statement in (
            "DELETE FROM task_queue.jobs",
            "TRUNCATE task_queue.jobs",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(statement)
        assert await surviving(pool, old_job) == {old_job}

        removed = await connection.fetchval(
            "SELECT task_queue.purge_terminal_jobs($1, $2, $3, $4)",
            queue_name,
            ["succeeded"],
            datetime.now(UTC) - timedelta(days=1),
            100,
        )
        assert removed == 1
        assert await surviving(pool, old_job) == set()


async def test_the_purge_routine_refuses_a_crafted_call(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """Every argument is re-derived inside the routine, not trusted.

    Each call below is one an over-eager or compromised purger could make, and
    each has to fail with the target rows still there -- the point of moving
    the boundary into the database is that the caller's SQL is not what keeps
    live work alive.
    """
    other_queue = f"{queue_name}_other"
    live = await seed_terminal_job(
        pool, queue=queue_name, state="pending", age=timedelta(0)
    )
    fresh = await seed_terminal_job(
        pool, queue=queue_name, state="succeeded", age=timedelta(minutes=1)
    )
    elsewhere = await seed_terminal_job(
        pool, queue=other_queue, state="succeeded", age=timedelta(days=30)
    )
    cutoff = datetime.now(UTC) - timedelta(days=1)

    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.PURGE],
            queues=[queue_name],
            prefix="rq_purger",
        ) as purge_pool,
        purge_pool.acquire() as connection,
    ):

        async def purge(
            queue: str, states: list[str], before: datetime, limit: int
        ) -> int:
            return await connection.fetchval(  # type: ignore[no-any-return]
                "SELECT task_queue.purge_terminal_jobs($1, $2, $3, $4)",
                queue,
                states,
                before,
                limit,
            )

        # A non-terminal state, even one the caller names explicitly.
        with pytest.raises(asyncpg.InvalidParameterValueError):
            await purge(queue_name, ["pending"], cutoff, 100)
        with pytest.raises(asyncpg.InvalidParameterValueError):
            await purge(queue_name, ["succeeded", "leased"], cutoff, 100)

        # A queue this role was never granted, and the grant wildcard used
        # as if it were a delete target.
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await purge(other_queue, ["succeeded"], cutoff, 100)
        with pytest.raises(asyncpg.InvalidParameterValueError):
            await purge("*", ["succeeded"], cutoff, 100)

        # A cutoff that has not happened yet, and an unbounded batch.
        with pytest.raises(asyncpg.InvalidParameterValueError):
            await purge(
                queue_name, ["succeeded"], datetime.now(UTC) + timedelta(days=1), 100
            )
        with pytest.raises(asyncpg.InvalidParameterValueError):
            await purge(queue_name, ["succeeded"], cutoff, 0)
        with pytest.raises(asyncpg.InvalidParameterValueError):
            await purge(queue_name, ["succeeded"], cutoff, 10**9)

        # A legal call whose cutoff simply does not reach the fresh job:
        # zero rows, no error. The routine bounds the delete, it does not
        # widen it to something worth returning.
        assert await purge(queue_name, ["succeeded"], cutoff, 100) == 0

    assert await surviving(pool, live, fresh, elsewhere) == {live, fresh, elsewhere}
    async with pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE id = ANY($1::uuid[])",
            [live, fresh, elsewhere],
        )


async def test_the_purge_routine_is_not_executable_by_default(
    admin_dsn: str, queue_name: str
) -> None:
    """PostgreSQL grants EXECUTE to PUBLIC; migration 0006 takes it back.

    Without that revoke every role in the database -- a producer, an inspector,
    anything with CONNECT -- could delete terminal jobs on any queue it was
    granted, and the PURGE capability would authorize nothing at all.
    """
    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.INSPECT],
            queues=[queue_name],
            prefix="rq_inspector",
        ) as inspect_pool,
        inspect_pool.acquire() as connection,
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.fetchval(
                "SELECT task_queue.purge_terminal_jobs($1, $2, $3, $4)",
                queue_name,
                ["succeeded"],
                datetime.now(UTC) - timedelta(days=1),
                100,
            )


async def test_reprovisioning_without_purge_takes_the_routine_back(
    admin_dsn: str, queue_name: str
) -> None:
    """REVOKE ... ON ALL TABLES does not touch routines; the repair must."""
    role = f"rq_purger_{uuid.uuid4().hex[:8]}"
    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        for capabilities in ([Capability.PURGE], [Capability.INSPECT]):
            await provision_role(
                admin_connection,
                role=role,
                capabilities=capabilities,
                queues=[queue_name],
                password="scoped-role-test-password",
            )
        executable = await admin_connection.fetchval(
            "SELECT has_function_privilege($1, $2, 'EXECUTE')",
            role,
            "task_queue.purge_terminal_jobs(text, text[], timestamptz, integer)",
        )
        assert executable is False
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
        finally:
            await admin_connection.close()


async def test_every_queue_scoped_table_ends_up_with_a_live_policy(
    admin_dsn: str,
) -> None:
    """What `pg_policies` holds after migrating, not what the files say.

    A policy can be dropped and replaced -- 0008 replaces the heartbeat one
    with four narrower ones -- so grepping `CREATE POLICY` across the packaged
    migrations counts statements that have since been undone. This reads the
    database's own answer, which is the only one that binds a query.
    """
    schema = f"pol_{uuid.uuid4().hex[:10]}"
    connection = await asyncpg.connect(admin_dsn)
    try:
        await migrations.migrate(connection, schema=schema)
        rows = await connection.fetch(
            "SELECT tablename, count(*) AS policies FROM pg_policies"
            " WHERE schemaname = $1 GROUP BY tablename",
            schema,
        )
        covered = {row["tablename"]: row["policies"] for row in rows}
        for table in (
            "jobs",
            "job_attempts",
            "concurrency_slots",
            "runtime_heartbeats",
            "schedules",
            "schedule_occurrences",
        ):
            assert covered.get(table), f"{table} has no policy: {covered}"
        # And the two deliberate exclusions are still excluded.
        assert "queue_pauses" not in covered
        assert "role_queue_grants" not in covered
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await connection.close()


#: The CONSUME grant set as it stood before 0008, which is what a role
#: provisioned by the previous release still holds after the migration runs.
#: Written out rather than derived, because the point is that it is *not* what
#: `provision_role` produces today.
PRE_0008_CONSUME_GRANTS: Final = (
    "GRANT USAGE ON SCHEMA {schema} TO {role}",
    "GRANT SELECT ON {schema}.role_queue_grants TO {role}",
    "GRANT SELECT, UPDATE ON {schema}.jobs TO {role}",
    "GRANT SELECT, INSERT, UPDATE ON {schema}.job_attempts TO {role}",
    "GRANT SELECT, INSERT, UPDATE, DELETE ON {schema}.concurrency_slots TO {role}",
    "GRANT SELECT, INSERT, UPDATE ON {schema}.runtime_heartbeats TO {role}",
    "GRANT SELECT ON {schema}.queue_pauses TO {role}",
    "GRANT SELECT ON {schema}.schema_migrations TO {role}",
)


async def test_a_role_from_the_previous_release_still_beats_after_0008(
    admin_dsn: str,
) -> None:
    """0008 must not need a re-provision to let a worker stay alive.

    Its policies ask which component kinds a role may claim, and that lives in
    a table 0008 itself creates. A policy is evaluated as the querying role, so
    reading it directly would require every runtime role to hold SELECT on it
    -- a grant only `provision_role` makes, and one no role provisioned by the
    previous release can have. Every such worker and scheduler would fail on
    its first heartbeat after the migration, and a heartbeat is not optional
    cleanup that can be skipped: it is the liveness mechanism.

    Reading it through a SECURITY DEFINER function EXECUTE-able by PUBLIC is
    what makes the upgrade survivable, and this is the test that says so --
    granting the role SELECT here by hand would test the fix by removing the
    condition it exists for.
    """
    ownership = next(
        m.version
        for m in migrations.load_migrations()
        if m.name == "heartbeat_ownership"
    )
    schema = f"pre8_{uuid.uuid4().hex[:8]}"
    role = f"rq_pre8_{uuid.uuid4().hex[:8]}"
    password = "scoped-role-test-password"
    database = urlsplit(admin_dsn).path.lstrip("/")
    target = database_target(admin_dsn, database, user=role, password=password)
    beat = (
        "INSERT INTO {schema}.runtime_heartbeats (kind, instance, queue, updated_at)"
        " VALUES ('worker', 'w1', 'alpha', now())"
    )

    connection = await asyncpg.connect(admin_dsn)
    try:
        await migrations.migrate(connection, schema=schema, target=ownership - 1)
        await connection.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{password}'")
        await connection.execute(f"GRANT CONNECT ON DATABASE {database} TO {role}")
        for grant in PRE_0008_CONSUME_GRANTS:
            await connection.execute(grant.format(schema=schema, role=role))
        await connection.execute(
            f"INSERT INTO {schema}.role_queue_grants (role_name, queue)"
            " VALUES ($1, 'alpha')",
            role,
        )

        worker = await asyncpg.connect(target.database_url)
        try:
            await worker.execute(beat.format(schema=schema))
        finally:
            await worker.close()

        # The drain 0008 insists on, which is the operator's half of this and
        # not the library's: the fleet stops and the table is cleared. What is
        # being tested is the other half -- that the grants the role already
        # holds carry it across the migration untouched.
        await connection.execute(f"DELETE FROM {schema}.runtime_heartbeats")

        # The upgrade, with nothing re-provisioned and nothing re-granted.
        applied = await migrations.migrate(connection, schema=schema)
        assert [m.name for m in applied] == [
            m.name for m in migrations.load_migrations() if m.version >= ownership
        ]

        worker = await asyncpg.connect(target.database_url)
        try:
            # The write that used to fail with "permission denied for table
            # role_runtime_kinds", on a role holding exactly what the previous
            # release gave it.
            await worker.execute(beat.format(schema=schema))
            assert (
                await worker.fetchval(
                    f"SELECT count(*) FROM {schema}.runtime_heartbeats"
                    " WHERE kind = 'worker' AND instance = 'w1'"
                    " AND role_name = current_user"
                )
                == 1
            )
            # Refreshing it works too, which is every tick after the first.
            assert (
                await worker.execute(
                    f"UPDATE {schema}.runtime_heartbeats SET updated_at = now()"
                    " WHERE kind = 'worker' AND instance = 'w1'"
                )
            ).endswith(" 1")

            # No row is attributed to the migrating role, because the drain
            # 0008 insists on leaves none for the added column to default for.
            # The orphan the column could otherwise produce is real but
            # unreachable: the check runs before the ALTER.
            assert (
                await worker.fetch(
                    f"SELECT role_name FROM {schema}.runtime_heartbeats"
                    " WHERE role_name <> current_user"
                )
                == []
            )
            # Backfilled from the grants it already held, so the tightening
            # still applies to it: it may claim 'worker' and nothing else.
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await worker.execute(
                    f"INSERT INTO {schema}.runtime_heartbeats"
                    " (kind, instance, queue, updated_at)"
                    " VALUES ('scheduler', 'ghost', 'alpha', now())"
                )
        finally:
            await worker.close()
    finally:
        try:
            await connection.execute(f"DROP OWNED BY {role}")
            await connection.execute(f"DROP ROLE IF EXISTS {role}")
            await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await connection.close()


#: The SCHEDULE grant set as the previous release wrote it. The difference
#: that matters is `jobs`: `SELECT, INSERT` then, `SELECT, INSERT,
#: UPDATE (updated_at)` now.
PREVIOUS_RELEASE_SCHEDULE_GRANTS: Final = (
    "GRANT USAGE ON SCHEMA {schema} TO {role}",
    "GRANT SELECT ON {schema}.role_queue_grants TO {role}",
    "GRANT SELECT, INSERT ON {schema}.jobs TO {role}",
    "GRANT SELECT, INSERT, UPDATE ON {schema}.schedules TO {role}",
    "GRANT SELECT, INSERT ON {schema}.schedule_occurrences TO {role}",
    "GRANT SELECT, INSERT, UPDATE ON {schema}.runtime_heartbeats TO {role}",
    "GRANT SELECT ON {schema}.schema_migrations TO {role}",
)

#: A producer of the same vintage: it enqueues too, and it holds no grant on
#: `schedules`, which is the whole of what the repair keys on.
PREVIOUS_RELEASE_PRODUCE_GRANTS: Final = (
    "GRANT USAGE ON SCHEMA {schema} TO {role}",
    "GRANT SELECT ON {schema}.role_queue_grants TO {role}",
    "GRANT SELECT, INSERT ON {schema}.jobs TO {role}",
    "GRANT SELECT ON {schema}.job_attempts TO {role}",
    "GRANT SELECT ON {schema}.schema_migrations TO {role}",
)


def one_job(queue: str) -> JobInsert:
    """The insert an enqueue makes, conflict path and all."""
    return JobInsert(
        id=uuid.uuid4(),
        queue=queue,
        task="t",
        payload_json="{}",
        priority=0,
        max_attempts=3,
        scheduled_at=None,
        dedupe_key=f"d-{uuid.uuid4().hex[:8]}",
        concurrency_key=None,
        timeout_seconds=None,
        metadata_json="{}",
        retry_policy_json=None,
        raise_on_conflict=False,
    )


async def test_0011_repairs_a_scheduler_that_would_otherwise_enqueue_nothing(
    admin_dsn: str,
) -> None:
    """The grant table is part of the release, and nothing reconciles the two.

    `migrate` moves the schema; the privileges a role holds move only when
    `provision_role` runs again. This release adds `UPDATE (updated_at)` on
    `jobs` to SCHEDULE -- every enqueue is an `ON CONFLICT ... DO UPDATE SET
    updated_at`, and PostgreSQL wants UPDATE on every column that names -- so
    an un-reprovisioned scheduler cannot enqueue, and `Scheduler.run` stops
    with the privilege error.

    0011 repairs it on a fingerprint that is exact in one direction: only
    SCHEDULE grants INSERT on `schedules`, and a role it matches already holds
    INSERT on `jobs`, so the repair widens nothing new. The producer below is
    the other direction -- it enqueues as well, and must not be touched.
    """
    repair = next(
        m.version
        for m in migrations.load_migrations()
        if m.name == "scheduler_enqueue_grant"
    )
    schema = f"pre11_{uuid.uuid4().hex[:8]}"
    scheduler = f"rq_pre11_s_{uuid.uuid4().hex[:8]}"
    producer = f"rq_pre11_p_{uuid.uuid4().hex[:8]}"
    password = "scoped-role-test-password"
    database = urlsplit(admin_dsn).path.lstrip("/")
    storage = Storage(schema)

    connection = await asyncpg.connect(admin_dsn)
    try:
        await migrations.migrate(connection, schema=schema, target=repair - 1)
        for role, grants in (
            (scheduler, PREVIOUS_RELEASE_SCHEDULE_GRANTS),
            (producer, PREVIOUS_RELEASE_PRODUCE_GRANTS),
        ):
            await connection.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{password}'")
            await connection.execute(f"GRANT CONNECT ON DATABASE {database} TO {role}")
            for grant in grants:
                await connection.execute(grant.format(schema=schema, role=role))
            await connection.execute(
                f"INSERT INTO {schema}.role_queue_grants (role_name, queue)"
                " VALUES ($1, 'alpha')",
                role,
            )

        target = database_target(admin_dsn, database, user=scheduler, password=password)
        fires = await asyncpg.connect(target.database_url)
        try:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with fires.transaction():
                    await storage.insert_job(fires, one_job("alpha"))
        finally:
            await fires.close()

        applied = await migrations.migrate(connection, schema=schema)
        assert "scheduler_enqueue_grant" in [m.name for m in applied]

        fires = await asyncpg.connect(target.database_url)
        try:
            async with fires.transaction():
                _, created = await storage.insert_job(fires, one_job("alpha"))
            assert created
        finally:
            await fires.close()

        # Exact, not "anything that can insert a job": the producer holds no
        # grant on `schedules`, so the repair passed it over.
        assert not await connection.fetchval(
            f"SELECT has_column_privilege($1, '{schema}.jobs'::regclass,"
            " 'updated_at', 'UPDATE')",
            producer,
        )
    finally:
        try:
            for role in (scheduler, producer):
                await connection.execute(f"DROP OWNED BY {role}")
                await connection.execute(f"DROP ROLE IF EXISTS {role}")
            await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await connection.close()


async def test_a_scheduler_missing_a_privilege_stops_with_a_permission_error(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """A refused statement is the same on every tick, so retrying hides it.

    The role is provisioned in full and then loses `UPDATE (updated_at)` on
    `jobs`, which is what a scheduler provisioned before 0011 lacks. Its first
    firing is refused by PostgreSQL, and `run` has to say so and stop.
    """
    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.SCHEDULE],
        queues=[queue_name],
        prefix="rq_stale_sched",
    ) as scoped:
        role = await scoped.fetchval("SELECT current_user")
        async with pool.acquire() as connection:
            await connection.execute(
                f"REVOKE UPDATE (updated_at) ON task_queue.jobs FROM {role}"
            )

        scheduler = Scheduler(
            Queue(scoped, name=queue_name),
            scheduler_id="stale-scheduler",
            interval=0.05,
            schedules=[
                ScheduleSpec(
                    name=f"every-minute-{queue_name}", task="work", cron="* * * * *"
                )
            ],
        )
        (stored,) = await scheduler.sync()
        async with pool.acquire() as connection:
            await connection.execute(
                "UPDATE task_queue.schedules "
                "SET created_at = created_at - interval '1 hour' WHERE id = $1",
                stored.id,
            )

        with pytest.raises(ConfigurationError, match="privilege"):
            await asyncio.wait_for(scheduler.run(), timeout=15)


async def test_a_worker_missing_a_privilege_stops_with_a_permission_error(
    admin_dsn: str, queue_name: str
) -> None:
    """The same rule for a worker: a role without CONSUME cannot tick."""
    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.PRODUCE],
        queues=[queue_name],
        prefix="rq_stale_worker",
    ) as scoped:
        queue = Queue(scoped, name=queue_name)

        async def handler(payload: object, context: TaskContext) -> None:
            return None

        queue.register(name="work", handler=handler)
        worker = Worker(queue, worker_id="no-consume", poll_interval=0.05)

        with pytest.raises(ConfigurationError, match="privilege"):
            await asyncio.wait_for(worker.run(), timeout=15)


async def test_a_live_concurrency_slot_belongs_to_the_worker_holding_it(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """A slot is the mutual exclusion, so queue scope is not enough for it.

    Every worker on a queue shares the slot table, so a policy that asks only
    "may this role touch this queue?" lets any worker role delete a slot
    another one is holding -- and the moment it is gone the key is acquirable
    by both at once, which is the single thing this table exists to prevent.

    The expiry branch stays open on purpose: acquisition has always been an
    upsert that takes over a slot whose lease has run out, and the holder it
    displaces is by definition not around to hand it over. That is what keeps a
    crashed worker from wedging a key, and it has to work across roles.
    """
    key = f"k-{uuid.uuid4().hex[:8]}"
    storage = Storage("task_queue")
    held = uuid.uuid4()

    async with pool.acquire() as connection:
        job = await connection.fetchval(
            "INSERT INTO task_queue.jobs (queue, task, payload, state, max_attempts)"
            " VALUES ($1, 'prepare', '{}'::jsonb, 'pending', 3) RETURNING id",
            queue_name,
        )

    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.CONSUME],
            queues=[queue_name],
            prefix="rq_slot_one",
        ) as first_pool,
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.CONSUME],
            queues=[queue_name],
            prefix="rq_slot_two",
        ) as second_pool,
        first_pool.acquire() as first,
        second_pool.acquire() as second,
    ):
        assert (
            await first.fetchval(
                storage._sql.acquire_slot, queue_name, key, job, held, "w1", 3600
            )
            == key
        )

        # The live slot is the other worker's, whatever queue they share.
        assert (
            await second.fetchval(
                storage._sql.acquire_slot,
                queue_name,
                key,
                job,
                uuid.uuid4(),
                "w2",
                3600,
            )
            is None
        )
        for statement in (
            "UPDATE task_queue.concurrency_slots SET worker_id = 'stolen'"
            " WHERE key = $1",
            "DELETE FROM task_queue.concurrency_slots WHERE key = $1",
        ):
            assert (await second.execute(statement, key)).endswith(" 0"), statement

        # Its own, it may extend and release.
        assert (
            await first.execute(storage._sql.extend_slot, job, held, 3600)
        ).endswith(" 1")

        # Once the lease runs out, the other worker takes it over -- and
        # becomes its owner, or it could not release what it now holds.
        async with pool.acquire() as connection:
            await connection.execute(
                "UPDATE task_queue.concurrency_slots"
                " SET leased_until = now() - interval '1 second' WHERE key = $1",
                key,
            )
        taken = uuid.uuid4()
        assert (
            await second.fetchval(
                storage._sql.acquire_slot, queue_name, key, job, taken, "w2", 3600
            )
            == key
        )
        async with pool.acquire() as connection:
            owner = await connection.fetchval(
                "SELECT role_name FROM task_queue.concurrency_slots WHERE key = $1",
                key,
            )
        assert owner == await second.fetchval("SELECT current_user")
        assert (await second.execute(storage._sql.release_slot, job, taken)).endswith(
            " 1"
        )

        # And lease recovery clears an expired slot whoever held it, which
        # is the whole point of recovery.
        await first.fetchval(
            storage._sql.acquire_slot,
            queue_name,
            key,
            job,
            uuid.uuid4(),
            "w1",
            3600,
        )
        async with pool.acquire() as connection:
            await connection.execute(
                "UPDATE task_queue.concurrency_slots"
                " SET leased_until = now() - interval '1 second' WHERE key = $1",
                key,
            )
        assert (
            await second.execute(
                "DELETE FROM task_queue.concurrency_slots WHERE key = $1", key
            )
        ).endswith(" 1")

    async with pool.acquire() as connection:
        await connection.execute("DELETE FROM task_queue.jobs WHERE id = $1", job)


async def test_a_heartbeat_can_only_be_written_by_the_component_it_describes(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """Queue scope is the wrong question for a liveness claim.

    "May this role touch this queue?" is right for a job and wrong here: on a
    queue a worker and a scheduler share, it lets either write the other's row.
    A worker role could refresh a scheduler's heartbeat -- readiness then
    reports a scheduler feeding a queue that has none -- or age it out, and a
    scheduler role could delete every worker row on the queue.

    Two things settle it, because `role_name` alone does not. It stops a role
    touching rows it did not write; it does not stop it writing a *new* row
    that claims to be a scheduler, which tells readiness the same lie by
    another route. So the kind a role may claim is recorded at provisioning
    time and checked too.
    """
    shared = f"{queue_name}_shared"
    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.CONSUME],
            queues=[shared],
            prefix="rq_hb_worker",
        ) as worker_pool,
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.SCHEDULE],
            queues=[shared],
            prefix="rq_hb_sched",
        ) as scheduler_pool,
    ):
        beat = (
            "INSERT INTO task_queue.runtime_heartbeats (kind, instance, queue,"
            " updated_at) VALUES ($1, $2, $3, now())"
        )
        async with scheduler_pool.acquire() as scheduler:
            await scheduler.execute(beat, "scheduler", "s1", shared)
        async with worker_pool.acquire() as worker:
            await worker.execute(beat, "worker", "w1", shared)

            # Refreshing and expiring someone else's claim: both no-ops, and
            # silently so -- the policy filters the rows rather than raising.
            for interval in ("1 day", "0 seconds"):
                touched = await worker.execute(
                    "UPDATE task_queue.runtime_heartbeats SET updated_at ="
                    f" now() - interval '{interval}'"
                    " WHERE kind = 'scheduler' AND instance = $1",
                    "s1",
                )
                assert touched.endswith(" 0"), touched

            # Inventing one is refused outright: the row would be the worker's
            # own, so only the kind can stop it.
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await worker.execute(beat, "scheduler", "ghost", shared)

            # Reading stays wide: readiness asks whether *any* scheduler is
            # alive on the queue, and must see one it did not write.
            visible = await worker.fetch(
                "SELECT kind, instance FROM task_queue.runtime_heartbeats"
                " WHERE queue = $1 ORDER BY kind",
                shared,
            )
            assert [(r["kind"], r["instance"]) for r in visible] == [
                ("scheduler", "s1"),
                ("worker", "w1"),
            ]

        async with scheduler_pool.acquire() as scheduler:
            removed = await scheduler.execute(
                "DELETE FROM task_queue.runtime_heartbeats WHERE kind = 'worker'"
            )
            assert removed.endswith(" 0"), removed
            # Its own row it may still refresh and retract.
            assert (
                await scheduler.execute(
                    "UPDATE task_queue.runtime_heartbeats SET updated_at = now()"
                    " WHERE kind = 'scheduler' AND instance = $1",
                    "s1",
                )
            ).endswith(" 1")

    async with pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM task_queue.runtime_heartbeats WHERE queue = $1", shared
        )


# ------------------------------------------------------------------ hardening


async def test_a_failed_provision_writes_nothing(admin_dsn: str) -> None:
    """Every failure leaves the role as it was, not only the ownership refusal.

    `provision_role` issues a dozen statements: create or alter the role, clear
    its attributes, revoke its memberships, revoke and re-grant across the
    database, the schema, each table, each routine, and finally the queue rows.
    Any of them can fail -- granting PURGE against a schema whose migrations
    stop short of the routine is the one an operator actually meets, mid-deploy
    -- and without a transaction around the body the caller gets an exception
    *and* a role that exists, can log in, and holds whichever half of the
    capability set was applied before the error, with no queue grants at all.

    A partly narrowed role is the worst of the three outcomes: absent is
    obvious and correct is correct, but this one reads as provisioned to
    anything that looks at it.
    """
    role = f"rq_atomic_{uuid.uuid4().hex[:8]}"
    fresh = f"rq_atomic_new_{uuid.uuid4().hex[:8]}"
    schema = f"partial_{uuid.uuid4().hex[:10]}"
    connection = await asyncpg.connect(admin_dsn)

    async def snapshot() -> tuple[list[tuple[str, str]], list[str], bool]:
        privileges = await connection.fetch(
            "SELECT table_name, privilege_type FROM information_schema.table_privileges"
            " WHERE grantee = $1 ORDER BY table_name, privilege_type",
            role,
        )
        queues = await connection.fetch(
            f"SELECT queue FROM {schema}.role_queue_grants WHERE role_name = $1"
            " ORDER BY queue",
            role,
        )
        login = await connection.fetchval(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname = $1", role
        )
        return (
            [(row["table_name"], row["privilege_type"]) for row in privileges],
            [row["queue"] for row in queues],
            bool(login),
        )

    try:
        # A complete schema with the purge routine dropped: provisioning needs
        # every grant table, so "stop short of 0006" is no longer a target it
        # can be run against -- and the failure under test was never about the
        # migration level, only about the routine the PURGE grant names.
        await migrations.migrate(connection, schema=schema)
        await connection.execute(
            f"DROP FUNCTION {schema}.purge_terminal_jobs"
            "(text, text[], timestamptz, integer)"
        )
        await provision_role(
            connection,
            role=role,
            capabilities=[Capability.CONSUME],
            schema=schema,
            queues=["alpha"],
            password="scoped-role-test-password",
        )
        before = await snapshot()
        assert before[0] and before[1] == ["alpha"]

        with pytest.raises(asyncpg.UndefinedFunctionError):
            await provision_role(
                connection,
                role=role,
                capabilities=[Capability.INSPECT, Capability.PURGE],
                schema=schema,
                queues=["beta"],
            )
        assert await snapshot() == before

        # And a role that did not exist is not left behind by the same failure.
        with pytest.raises(asyncpg.UndefinedFunctionError):
            await provision_role(
                connection,
                role=fresh,
                capabilities=[Capability.PURGE],
                schema=schema,
                queues=["alpha"],
                password="scoped-role-test-password",
            )
        assert not await connection.fetchval(
            "SELECT 1 FROM pg_roles WHERE rolname = $1", fresh
        )
    finally:
        try:
            await revoke_role(connection, role=role, schema=schema, drop=True)
            await connection.execute(f"DROP ROLE IF EXISTS {fresh}")
            await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await connection.close()


async def test_the_kinds_a_role_may_claim_follow_its_capabilities(
    admin_dsn: str,
) -> None:
    """Replaced on every provisioning call, so narrowing narrows this too.

    The table authorizes liveness claims, so a role that kept a kind after
    losing the capability behind it could go on telling a readiness probe a
    component is alive that provisioning has just taken away.
    """
    role = f"rq_kinds_{uuid.uuid4().hex[:8]}"
    connection = await asyncpg.connect(admin_dsn)

    async def kinds() -> list[str]:
        rows = await connection.fetch(
            "SELECT kind FROM task_queue.role_runtime_kinds"
            " WHERE role_name = $1 ORDER BY kind",
            role,
        )
        return [row["kind"] for row in rows]

    try:
        for capabilities, expected in (
            ([Capability.CONSUME, Capability.SCHEDULE], ["scheduler", "worker"]),
            ([Capability.CONSUME], ["worker"]),
            ([Capability.SCHEDULE], ["scheduler"]),
            # None of these makes a role a runtime component.
            ([Capability.INSPECT], []),
            ([Capability.PRODUCE, Capability.PURGE], []),
        ):
            await provision_role(
                connection,
                role=role,
                capabilities=capabilities,
                queues=["alpha"],
                password="scoped-role-test-password",
            )
            assert await kinds() == expected, capabilities

        await provision_role(
            connection,
            role=role,
            capabilities=[Capability.CONSUME],
            queues=["alpha"],
        )
        assert await kinds() == ["worker"]
        await revoke_role(connection, role=role, drop=False)
        assert await kinds() == []
    finally:
        try:
            await revoke_role(connection, role=role, drop=True)
        finally:
            await connection.close()


#: One namespaced, separately-owned object per catalog the ownership scan has
#: to reach, as (label, CREATE, ALTER ... OWNER TO, DROP, expected wording).
#: `{name}` is a per-test unique suffix; the expected wording is
#: `pg_describe_object`'s, which is what an operator would have to type back
#: into the ALTER the refusal tells them to run.
OWNABLE_OBJECTS: Final = (
    (
        "routine",
        "CREATE FUNCTION task_queue.{name}() RETURNS int"
        " LANGUAGE sql AS $f$ SELECT 1 $f$",
        "ALTER FUNCTION task_queue.{name}() OWNER TO {owner}",
        "DROP FUNCTION IF EXISTS task_queue.{name}()",
        "function task_queue.{name}()",
    ),
    (
        "enum",
        "CREATE TYPE task_queue.{name} AS ENUM ('red', 'green')",
        "ALTER TYPE task_queue.{name} OWNER TO {owner}",
        "DROP TYPE IF EXISTS task_queue.{name}",
        "type task_queue.{name}",
    ),
    (
        "domain",
        "CREATE DOMAIN task_queue.{name} AS int CHECK (VALUE > 0)",
        "ALTER DOMAIN task_queue.{name} OWNER TO {owner}",
        "DROP DOMAIN IF EXISTS task_queue.{name}",
        "type task_queue.{name}",
    ),
    (
        "collation",
        "CREATE COLLATION task_queue.{name} (locale = 'C')",
        "ALTER COLLATION task_queue.{name} OWNER TO {owner}",
        "DROP COLLATION IF EXISTS task_queue.{name}",
        "collation task_queue.{name}",
    ),
    (
        "conversion",
        "CREATE CONVERSION task_queue.{name} FOR 'LATIN1' TO 'UTF8'"
        " FROM iso8859_1_to_utf8",
        "ALTER CONVERSION task_queue.{name} OWNER TO {owner}",
        "DROP CONVERSION IF EXISTS task_queue.{name}",
        "conversion task_queue.{name}",
    ),
    (
        "operator",
        "CREATE OPERATOR task_queue.### (LEFTARG = int, RIGHTARG = int,"
        " FUNCTION = int4pl)",
        "ALTER OPERATOR task_queue.###(int, int) OWNER TO {owner}",
        "DROP OPERATOR IF EXISTS task_queue.###(int, int)",
        "operator task_queue.###(integer,integer)",
    ),
    (
        "operator family",
        "CREATE OPERATOR FAMILY task_queue.{name} USING btree",
        "ALTER OPERATOR FAMILY task_queue.{name} USING btree OWNER TO {owner}",
        "DROP OPERATOR FAMILY IF EXISTS task_queue.{name} USING btree",
        "operator family task_queue.{name} for access method btree",
    ),
    (
        "text search dictionary",
        "CREATE TEXT SEARCH DICTIONARY task_queue.{name} (TEMPLATE = simple)",
        "ALTER TEXT SEARCH DICTIONARY task_queue.{name} OWNER TO {owner}",
        "DROP TEXT SEARCH DICTIONARY IF EXISTS task_queue.{name}",
        "text search dictionary task_queue.{name}",
    ),
    (
        "text search configuration",
        "CREATE TEXT SEARCH CONFIGURATION task_queue.{name} (COPY = simple)",
        "ALTER TEXT SEARCH CONFIGURATION task_queue.{name} OWNER TO {owner}",
        "DROP TEXT SEARCH CONFIGURATION IF EXISTS task_queue.{name}",
        "text search configuration task_queue.{name}",
    ),
    (
        "extended statistics",
        "CREATE STATISTICS task_queue.{name} (ndistinct)"
        " ON queue, task FROM task_queue.jobs",
        "ALTER STATISTICS task_queue.{name} OWNER TO {owner}",
        "DROP STATISTICS IF EXISTS task_queue.{name}",
        "statistics object task_queue.{name}",
    ),
)


@pytest.mark.parametrize(
    ("create", "chown", "drop", "expected"),
    [pytest.param(*case[1:], id=case[0]) for case in OWNABLE_OBJECTS],
)
async def test_provisioning_refuses_an_owner_whatever_catalog_it_is_in(
    admin_dsn: str, create: str, chown: str, drop: str, expected: str
) -> None:
    """Ownership lives in a dozen catalogs, and every one of them is DDL.

    `pg_class` is the one people think of, and each of these is a separate
    catalog with its own owner column that no relation query can see however it
    is written. The authority is identical in all of them -- ALTER and DROP an
    object in a schema a scoped role is never supposed to shape -- so a scan
    that enumerates catalogs by hand is a list that falls behind the server.
    This is the parametrized half of that argument; the other half is that the
    implementation reads the ownership record PostgreSQL keeps for all of them
    at once, rather than growing a branch per entry here.
    """
    name = f"rq_owned_{uuid.uuid4().hex[:8]}"
    role = f"rq_catalog_{uuid.uuid4().hex[:8]}"
    connection = await asyncpg.connect(admin_dsn)
    try:
        await provision_role(
            connection,
            role=role,
            capabilities=[Capability.INSPECT],
            queues=["alpha"],
            password="scoped-role-test-password",
        )
        await connection.execute(create.format(name=name))
        await connection.execute(chown.format(name=name, owner=role))

        with pytest.raises(ValidationError) as refused:
            await provision_role(
                connection,
                role=role,
                capabilities=[Capability.INSPECT],
                queues=["alpha"],
            )
        assert expected.format(name=name) in str(refused.value)

        # And the documented remedy clears it.
        await connection.execute(chown.format(name=name, owner="CURRENT_USER"))
        await provision_role(
            connection,
            role=role,
            capabilities=[Capability.INSPECT],
            queues=["alpha"],
        )
    finally:
        try:
            await connection.execute(drop.format(name=name))
            await revoke_role(connection, role=role, drop=True)
        finally:
            await connection.close()


async def test_provisioning_strips_sequence_privileges_too(admin_dsn: str) -> None:
    """ALL TABLES does not reach a sequence, and identity columns are sequences.

    `jobs.seq` and `job_attempts.id` are GENERATED ALWAYS AS IDENTITY, which in
    the privilege system is a sequence object of its own. A role that acquired
    USAGE/SELECT/UPDATE on one -- an operator's manual grant is the ordinary
    way -- kept it through every later repair, because the revokes named tables
    and routines and nothing else.

    UPDATE on a sequence is `setval`, and `jobs.seq` is the tiebreaker the
    claim order ends with, so what survived was the ability to hand out
    duplicate sequence values from a role this function had just reported as
    narrowed.
    """
    role = f"rq_seq_{uuid.uuid4().hex[:8]}"
    password = "scoped-role-test-password"
    connection = await asyncpg.connect(admin_dsn)

    async def sequence_privileges() -> list[tuple[str, str]]:
        rows = await connection.fetch(
            """
            SELECT c.relname, a.privilege_type
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, '{}')) AS a
            JOIN pg_roles AS r ON r.oid = a.grantee
            WHERE n.nspname = 'task_queue' AND c.relkind = 'S' AND r.rolname = $1
            ORDER BY 1, 2
            """,
            role,
        )
        return [(row["relname"], row["privilege_type"]) for row in rows]

    try:
        await provision_role(
            connection,
            role=role,
            capabilities=[Capability.CONSUME],
            queues=["alpha"],
            password=password,
        )
        sequences = [
            row["relname"]
            for row in await connection.fetch(
                "SELECT c.relname FROM pg_class AS c"
                " JOIN pg_namespace AS n ON n.oid = c.relnamespace"
                " WHERE n.nspname = 'task_queue' AND c.relkind = 'S'"
            )
        ]
        assert sequences, "the schema has no identity sequences to test with"
        for sequence in sequences:
            await connection.execute(
                f"GRANT USAGE, SELECT, UPDATE ON SEQUENCE task_queue.{sequence}"
                f" TO {role}"
            )
        assert await sequence_privileges()

        # Repair, which is what claims to narrow the role back down.
        await provision_role(
            connection,
            role=role,
            capabilities=[Capability.CONSUME],
            queues=["alpha"],
            password=password,
        )
        assert await sequence_privileges() == []

        # And the teardown path, which claims to leave nothing at all.
        for sequence in sequences:
            await connection.execute(
                f"GRANT UPDATE ON SEQUENCE task_queue.{sequence} TO {role}"
            )
        await revoke_role(connection, role=role, drop=False)
        assert await sequence_privileges() == []
    finally:
        try:
            await revoke_role(connection, role=role, drop=True)
        finally:
            await connection.close()


async def test_provisioning_strips_a_role_back_down(admin_dsn: str) -> None:
    """Repair covers what lives outside the schema's grant tables.

    A role someone made SUPERUSER or BYPASSRLS ignores every policy and grant
    below it, so a ``provision_role`` that fixed only the table grants would
    report a least-privilege role while handing over the whole database.
    """
    role = f"rq_elevated_{uuid.uuid4().hex[:8]}"
    parent = f"rq_parent_{uuid.uuid4().hex[:8]}"
    admin_connection = await asyncpg.connect(admin_dsn)
    attributes = (
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolbypassrls, "
        "rolreplication, rolinherit FROM pg_roles WHERE rolname = $1"
    )
    try:
        await admin_connection.execute(f"CREATE ROLE {parent}")
        await admin_connection.execute(
            f"CREATE ROLE {role} LOGIN SUPERUSER CREATEDB CREATEROLE "
            "BYPASSRLS REPLICATION INHERIT"
        )
        await admin_connection.execute(f"GRANT {parent} TO {role}")
        assert all(dict(await admin_connection.fetchrow(attributes, role)).values())

        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.CONSUME],
            queues=["alpha"],
            password="scoped-role-test-password",
        )

        assert not any(dict(await admin_connection.fetchrow(attributes, role)).values())
        memberships = await admin_connection.fetch(
            """
            SELECT granted.rolname FROM pg_auth_members AS m
            JOIN pg_roles AS granted ON granted.oid = m.roleid
            JOIN pg_roles AS holder ON holder.oid = m.member
            WHERE holder.rolname = $1
            """,
            role,
        )
        assert [row["rolname"] for row in memberships] == []
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
            await admin_connection.execute(f"DROP ROLE IF EXISTS {parent}")
        finally:
            await admin_connection.close()


async def test_repair_narrows_a_role_that_was_widened_again(admin_dsn: str) -> None:
    """There is no way to ask ``provision_role`` for less than this.

    Hardening is not a mode the caller selects; a second call on a role that
    someone re-elevated in between narrows it again, because the alternative is
    handing out credentials for a role that is only partly least privilege.
    """
    role = f"rq_rewidened_{uuid.uuid4().hex[:8]}"
    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.INSPECT],
            queues=["alpha"],
            password="scoped-role-test-password",
        )
        await admin_connection.execute(f"ALTER ROLE {role} BYPASSRLS CREATEROLE")
        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.INSPECT],
            queues=["alpha"],
        )
        assert not any(
            dict(
                await admin_connection.fetchrow(
                    "SELECT rolbypassrls, rolcreaterole, rolinherit "
                    "FROM pg_roles WHERE rolname = $1",
                    role,
                )
            ).values()
        )
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
        finally:
            await admin_connection.close()


# ------------------------------------------- row-level security beyond `jobs`


async def test_slots_and_heartbeats_are_queue_scoped_like_jobs(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """0005: the runtime tables sit inside the boundary, not beside it.

    Before this, a role scoped to one queue could read every queue's slot
    leases and worker liveness, and could delete a slot held by a queue it was
    never granted -- releasing another queue's business lock from outside it.
    """
    other_queue = f"{queue_name}_other"
    async with pool.acquire() as connection:
        foreign_job = await connection.fetchval(
            """
            INSERT INTO task_queue.jobs (queue, task, payload, state, max_attempts)
            VALUES ($1, 'prepare', '{}'::jsonb, 'pending', 3) RETURNING id
            """,
            other_queue,
        )
        await connection.execute(
            """
            INSERT INTO task_queue.concurrency_slots
                (queue, key, job_id, lease_token, worker_id, leased_until)
            VALUES ($1, 'shared', $2, gen_random_uuid(), 'foreign', now() + '1h')
            """,
            other_queue,
            foreign_job,
        )
        await connection.execute(
            """
            INSERT INTO task_queue.runtime_heartbeats (kind, instance, queue)
            VALUES ('worker', $1, $2)
            """,
            f"foreign-{uuid.uuid4().hex[:8]}",
            other_queue,
        )

    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.CONSUME],
            queues=[queue_name],
            prefix="rq_consumer",
        ) as consumer_pool,
        consumer_pool.acquire() as connection,
    ):
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.concurrency_slots WHERE queue = $1",
                other_queue,
            )
            == 0
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.runtime_heartbeats WHERE queue = $1",
                other_queue,
            )
            == 0
        )
        # Invisible means unreleasable: a DELETE the policy filters away is a
        # no-op, not an error, which is exactly what RLS does for `jobs`.
        assert (
            await connection.execute(
                "DELETE FROM task_queue.concurrency_slots WHERE key = 'shared'"
            )
            == "DELETE 0"
        )
        for statement, arguments in (
            (
                """
                INSERT INTO task_queue.concurrency_slots
                    (queue, key, job_id, lease_token, worker_id, leased_until)
                VALUES ($1, 'intruder', $2, gen_random_uuid(), 'w', now() + '1h')
                """,
                (other_queue, foreign_job),
            ),
            (
                """
                INSERT INTO task_queue.runtime_heartbeats (kind, instance, queue)
                VALUES ('worker', 'intruder', $1)
                """,
                (other_queue,),
            ),
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(statement, *arguments)

        # The same key on its own queue is still available: slots are scoped,
        # not forbidden.
        own_job = await connection.fetchval(
            "SELECT id FROM task_queue.jobs WHERE queue = $1 LIMIT 1", queue_name
        )
        if own_job is None:
            async with pool.acquire() as owner:
                own_job = await owner.fetchval(
                    """
                    INSERT INTO task_queue.jobs
                        (queue, task, payload, state, max_attempts)
                    VALUES ($1, 'prepare', '{}'::jsonb, 'pending', 3) RETURNING id
                    """,
                    queue_name,
                )
        await connection.execute(
            """
            INSERT INTO task_queue.concurrency_slots
                (queue, key, job_id, lease_token, worker_id, leased_until)
            VALUES ($1, 'shared', $2, gen_random_uuid(), 'own', now() + '1h')
            """,
            queue_name,
            own_job,
        )

    async with pool.acquire() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.concurrency_slots WHERE key = 'shared'"
            )
            == 2
        )
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE queue = ANY($1::text[])",
            [queue_name, other_queue],
        )


async def test_schedules_are_queue_scoped(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """A scheduler role must not see or disable another queue's periodic work.

    `schedules` carries its queue and `Capability.SCHEDULE` can write it, so
    without a policy a role scoped to one queue held an off switch for every
    other queue's schedules -- read them, disable them, repoint them.
    """
    other_queue = f"{queue_name}_other"
    async with pool.acquire() as connection:
        foreign = await connection.fetchval(
            """
            INSERT INTO task_queue.schedules (name, queue, task, cron)
            VALUES ($1, $2, 'prepare', '* * * * *') RETURNING id
            """,
            f"foreign-{uuid.uuid4().hex[:8]}",
            other_queue,
        )

    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.SCHEDULE],
            queues=[queue_name],
            prefix="rq_scheduler",
        ) as scheduler_pool,
        scheduler_pool.acquire() as connection,
    ):
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.schedules WHERE queue = $1",
                other_queue,
            )
            == 0
        )
        assert (
            await connection.execute(
                "UPDATE task_queue.schedules SET enabled = false WHERE queue = $1",
                other_queue,
            )
            == "UPDATE 0"
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(
                """
                INSERT INTO task_queue.schedules (name, queue, task, cron)
                VALUES ($1, $2, 'prepare', '* * * * *')
                """,
                f"intruder-{uuid.uuid4().hex[:8]}",
                other_queue,
            )

        # Its own queue's schedules stay fully writable.
        own = await connection.fetchval(
            """
            INSERT INTO task_queue.schedules (name, queue, task, cron)
            VALUES ($1, $2, 'prepare', '* * * * *') RETURNING id
            """,
            f"own-{uuid.uuid4().hex[:8]}",
            queue_name,
        )
        assert own is not None

    async with pool.acquire() as connection:
        assert await connection.fetchval(
            "SELECT enabled FROM task_queue.schedules WHERE id = $1", foreign
        )


async def test_schedule_occurrences_are_queue_scoped(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """An occurrence row is what makes a firing exactly-once.

    Forging one for a foreign schedule is not a leak but a denial of service:
    that schedule never fires that occurrence again. Foreign-key checks run as
    the table owner, so the FKs to `schedules` and `jobs` do not stop it.
    """
    other_queue = f"{queue_name}_other"
    fired_at = datetime.now(UTC).replace(microsecond=0)
    async with pool.acquire() as connection:
        foreign_schedule = await connection.fetchval(
            """
            INSERT INTO task_queue.schedules (name, queue, task, cron)
            VALUES ($1, $2, 'prepare', '* * * * *') RETURNING id
            """,
            f"foreign-{uuid.uuid4().hex[:8]}",
            other_queue,
        )
        foreign_job = await connection.fetchval(
            """
            INSERT INTO task_queue.jobs (queue, task, payload, state, max_attempts)
            VALUES ($1, 'prepare', '{}'::jsonb, 'pending', 3) RETURNING id
            """,
            other_queue,
        )
        await connection.execute(
            """
            INSERT INTO task_queue.schedule_occurrences
                (schedule_id, occurrence_at, queue, job_id)
            VALUES ($1, $2, $3, $4)
            """,
            foreign_schedule,
            fired_at,
            other_queue,
            foreign_job,
        )

    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.SCHEDULE],
            queues=[queue_name],
            prefix="rq_scheduler",
        ) as scheduler_pool,
        scheduler_pool.acquire() as connection,
    ):
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.schedule_occurrences WHERE queue = $1",
                other_queue,
            )
            == 0
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(
                """
                INSERT INTO task_queue.schedule_occurrences
                    (schedule_id, occurrence_at, queue, job_id)
                VALUES ($1, $2, $3, $4)
                """,
                foreign_schedule,
                fired_at + timedelta(minutes=1),
                other_queue,
                foreign_job,
            )

    async with pool.acquire() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.schedule_occurrences "
                "WHERE schedule_id = $1",
                foreign_schedule,
            )
            == 1
        )
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE queue = ANY($1::text[])",
            [queue_name, other_queue],
        )


async def test_one_instance_id_can_beat_on_two_queues(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """The heartbeat key and the heartbeat policy must agree on identity.

    Instance ids are reused across queues constantly -- a pod name, a
    hostname, a container ordinal. While the key was (kind, instance) and the
    policy was queue-scoped, an id that had ever beaten on another queue left a
    row the next queue's upsert could neither see nor replace, and every tick
    failed on a conflict with an invisible row.
    """
    other_queue = f"{queue_name}_other"
    instance = f"shared-{uuid.uuid4().hex[:8]}"
    storage = Storage("task_queue")
    async with pool.acquire() as connection:
        await storage.record_runtime_heartbeat(
            connection, kind="worker", instance=instance, queue=other_queue
        )

    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.CONSUME],
            queues=[queue_name],
            prefix="rq_consumer",
        ) as consumer_pool,
        consumer_pool.acquire() as connection,
    ):
        for _ in range(2):
            await storage.record_runtime_heartbeat(
                connection, kind="worker", instance=instance, queue=queue_name
            )
        assert await storage.live_instances(
            connection,
            kind="worker",
            since=datetime.now(UTC) - timedelta(minutes=1),
            queue=queue_name,
        ) == [instance]

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            "SELECT queue FROM task_queue.runtime_heartbeats WHERE instance = $1",
            instance,
        )
        assert sorted(row["queue"] for row in rows) == sorted([other_queue, queue_name])
        await connection.execute(
            "DELETE FROM task_queue.runtime_heartbeats WHERE instance = $1", instance
        )


async def test_a_schedule_only_role_can_fire_a_schedule(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """SCHEDULE has to be enough to run the scheduler, end to end.

    Existing coverage runs the scheduler as the schema owner, which hides
    every gap in this capability's grants -- firing is an enqueue, and an
    enqueue needs UPDATE on ``updated_at`` for its conflict path.
    """
    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.SCHEDULE],
        queues=[queue_name],
        prefix="rq_scheduler",
    ) as scheduler_pool:
        scheduler = Scheduler(
            Queue(scheduler_pool, name=queue_name),
            scheduler_id=f"s-{uuid.uuid4().hex[:8]}",
            schedules=[
                ScheduleSpec(
                    name=f"every-minute-{uuid.uuid4().hex[:8]}",
                    task="prepare",
                    cron="* * * * *",
                )
            ],
        )
        await scheduler.sync()
        fired = await scheduler.tick(now=datetime.now(UTC) + timedelta(minutes=2))
        assert [job.queue for job in fired] == [queue_name]

    async with pool.acquire() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.schedule_occurrences WHERE queue = $1",
                queue_name,
            )
            == 1
        )
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE queue = $1", queue_name
        )


async def test_an_inspect_only_role_gets_a_readiness_answer(
    admin_dsn: str, queue_name: str
) -> None:
    """A readiness probe is an inspection, so INSPECT has to be able to run it.

    ``check_readiness`` reports its dependencies separately on purpose; a
    missing grant that turns "no worker heartbeat" into "cannot reach
    PostgreSQL" is the one failure mode that makes it useless in an incident.
    """
    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.INSPECT],
        queues=[queue_name],
        prefix="rq_inspector",
    ) as inspect_pool:
        report = await check_readiness(
            inspect_pool, queue=queue_name, require_worker=True
        )
        assert report.connected
        assert report.migrations_up_to_date
        assert not report.worker_available
        assert any("no worker heartbeat" in problem for problem in report.problems)

        relaxed = await check_readiness(
            inspect_pool, queue=queue_name, require_worker=False
        )
        assert relaxed.ready


async def test_a_queue_label_cannot_be_pointed_at_another_queues_row(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """The policy tests the label; the foreign key tests the reference.

    Forging an occurrence for a foreign schedule under one's own label
    consumes that schedule's occurrence key, so it never fires that occurrence
    on its real queue -- a denial of service reached entirely through a row
    the writer's own policy accepts.
    """
    other_queue = f"{queue_name}_other"
    when = datetime.now(UTC).replace(microsecond=0)
    async with pool.acquire() as connection:
        foreign_schedule = await connection.fetchval(
            """
            INSERT INTO task_queue.schedules (name, queue, task, cron)
            VALUES ($1, $2, 'prepare', '* * * * *') RETURNING id
            """,
            f"foreign-{uuid.uuid4().hex[:8]}",
            other_queue,
        )
        foreign_job = await connection.fetchval(
            """
            INSERT INTO task_queue.jobs (queue, task, payload, state, max_attempts)
            VALUES ($1, 'prepare', '{}'::jsonb, 'pending', 3) RETURNING id
            """,
            other_queue,
        )
        own_job = await connection.fetchval(
            """
            INSERT INTO task_queue.jobs (queue, task, payload, state, max_attempts)
            VALUES ($1, 'prepare', '{}'::jsonb, 'pending', 3) RETURNING id
            """,
            queue_name,
        )
        own_schedule = await connection.fetchval(
            """
            INSERT INTO task_queue.schedules (name, queue, task, cron)
            VALUES ($1, $2, 'prepare', '* * * * *') RETURNING id
            """,
            f"own-{uuid.uuid4().hex[:8]}",
            queue_name,
        )

    async with (
        scoped_role_pool(
            admin_dsn,
            capabilities=[Capability.SCHEDULE, Capability.CONSUME],
            queues=[queue_name],
            prefix="rq_forger",
        ) as forger_pool,
        forger_pool.acquire() as connection,
    ):
        # An occurrence naming a foreign job, wearing a label the policy
        # accepts. Left untied, retention on *that* queue would purge the job,
        # cascade this row away, and free the schedule to fire the instant
        # again -- one queue's purge reopening another queue's occurrence key.
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                """
                INSERT INTO task_queue.schedule_occurrences
                    (schedule_id, occurrence_at, queue, job_id)
                VALUES ($1, $2, $3, $4)
                """,
                own_schedule,
                when,
                queue_name,
                foreign_job,
            )

        # Naming a foreign *schedule* is refused by the policy rather than by
        # a constraint: an occurrence may only name a schedule its writer can
        # see, and schedules are queue-scoped. That is what keeps the
        # occurrence key safe while the key itself stays queue-free.
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(
                """
                INSERT INTO task_queue.schedule_occurrences
                    (schedule_id, occurrence_at, queue, job_id)
                VALUES ($1, $2, $3, $4)
                """,
                foreign_schedule,
                when,
                queue_name,
                own_job,
            )

        # Its own schedule, its own job: allowed.
        await connection.execute(
            """
            INSERT INTO task_queue.schedule_occurrences
                (schedule_id, occurrence_at, queue, job_id)
            VALUES ($1, $2, $3, $4)
            """,
            own_schedule,
            when,
            queue_name,
            own_job,
        )
        # A slot holding a foreign job, likewise.
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                """
                INSERT INTO task_queue.concurrency_slots
                    (queue, key, job_id, lease_token, worker_id, leased_until)
                VALUES ($1, 'forged', $2, gen_random_uuid(), 'w', now() + '1h')
                """,
                queue_name,
                foreign_job,
            )
        # And an attempt record against a foreign job. The unique index on
        # (job_id, attempt) is global, so one forged row here burns that
        # attempt number: the next real claim of that job cannot open its
        # attempt record at all.
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                """
                INSERT INTO task_queue.job_attempts
                    (job_id, queue, task, attempt, worker_id, lease_token)
                VALUES ($1, $2, 'prepare', 1, 'forger', gen_random_uuid())
                """,
                foreign_job,
                queue_name,
            )

    async with pool.acquire() as connection:
        # The foreign schedule's key was never consumed, so its own queue can
        # still fire that instant -- the property the occurrence key is for.
        await connection.execute(
            """
            INSERT INTO task_queue.schedule_occurrences
                (schedule_id, occurrence_at, queue, job_id)
            VALUES ($1, $2, $3, $4)
            """,
            foreign_schedule,
            when,
            other_queue,
            foreign_job,
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.job_attempts WHERE job_id = $1",
                foreign_job,
            )
            == 0
        )
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE queue = ANY($1::text[])",
            [queue_name, other_queue],
        )
        await connection.execute(
            "DELETE FROM task_queue.schedules WHERE queue = ANY($1::text[])",
            [queue_name, other_queue],
        )


async def test_a_scheduler_heartbeats_the_queues_it_actually_serves(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """One scheduler deployment can serve several queues (see `Scheduler`).

    Its liveness is therefore a fact about each of them, not about the queue
    its handle happens to name -- a readiness probe for a queue it feeds has to
    find it. Under the per-queue policies, writing the handle's queue instead
    also fails outright for a role not granted that queue, which is the shape
    this test provokes: the handle names one queue, the role holds another.
    """
    served = f"{queue_name}_served"
    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.SCHEDULE],
        queues=[served],
        prefix="rq_scheduler",
    ) as scheduler_pool:
        instance = f"s-{uuid.uuid4().hex[:8]}"
        scheduler = Scheduler(
            Queue(scheduler_pool, name=queue_name),
            scheduler_id=instance,
            schedules=[
                ScheduleSpec(
                    name=f"served-{uuid.uuid4().hex[:8]}",
                    task="prepare",
                    cron="* * * * *",
                    queue=served,
                )
            ],
        )
        await scheduler.sync()
        fired = await scheduler.tick(now=datetime.now(UTC) + timedelta(minutes=2))
        assert [job.queue for job in fired] == [served]

    async with pool.acquire() as connection:
        beats = await connection.fetch(
            "SELECT queue FROM task_queue.runtime_heartbeats WHERE instance = $1",
            instance,
        )
        assert [row["queue"] for row in beats] == [served]
        await connection.execute(
            "DELETE FROM task_queue.runtime_heartbeats WHERE instance = $1", instance
        )
        await connection.execute("DELETE FROM task_queue.jobs WHERE queue = $1", served)


async def test_a_scheduler_retracts_the_heartbeat_of_a_queue_it_stopped_serving(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """The staleness window is for a process that died, not one that changed.

    Disabling the last schedule on a queue is an ordinary operator action, and
    the scheduler keeps ticking for its other queues -- so nothing overwrites
    the row it already wrote, and nothing marks it wrong. A readiness probe for
    that queue goes on answering "live, and this is the instance feeding it"
    for the whole staleness window, about a scheduler feeding it nothing.

    Time cannot fix this, because time is not what changed. The scheduler is
    still running and still knows what it serves, so it says so.
    """
    dropped = f"{queue_name}_dropped"
    kept = f"{queue_name}_kept"
    instance = f"s-{uuid.uuid4().hex[:8]}"
    fire_at = datetime.now(UTC) + timedelta(minutes=2)

    def specs(*enabled_names: str) -> list[ScheduleSpec]:
        return [
            ScheduleSpec(
                name=f"{name}-{instance}",
                task="prepare",
                cron="* * * * *",
                queue=name,
                enabled=name in enabled_names,
            )
            for name in (dropped, kept)
        ]

    async def beating() -> list[str]:
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT queue FROM task_queue.runtime_heartbeats"
                " WHERE kind = 'scheduler' AND instance = $1 ORDER BY queue",
                instance,
            )
        return [row["queue"] for row in rows]

    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.SCHEDULE],
        queues=[dropped, kept],
        prefix="rq_scheduler",
    ) as scheduler_pool:
        scheduler = Scheduler(
            Queue(scheduler_pool, name=queue_name),
            scheduler_id=instance,
            schedules=specs(dropped, kept),
        )
        await scheduler.sync()
        await scheduler.tick(now=fire_at)
        assert await beating() == sorted([dropped, kept])

        # The operator disables one. The other keeps the scheduler ticking, so
        # this is not a shutdown -- it is a change of responsibility.
        scheduler.schedules = tuple(specs(kept))
        await scheduler.sync()
        await scheduler.tick(now=fire_at + timedelta(minutes=2))
        assert await beating() == [kept]

        ready = await check_readiness(
            pool, queue=dropped, require_worker=False, require_scheduler=True
        )
        assert not ready.scheduler_available
        assert instance not in ready.schedulers

        # And a scheduler left with nothing enabled retracts the rest, rather
        # than leaving its last queue looking fed.
        scheduler.schedules = tuple(specs())
        await scheduler.sync()
        assert await scheduler.tick(now=fire_at + timedelta(minutes=4)) == []
        assert await beating() == []

    async with pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE queue = ANY($1::text[])",
            [dropped, kept],
        )


async def test_a_scheduler_retracts_a_claim_on_a_queue_taken_away_from_it(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """Retraction must not depend on still being entitled to the queue.

    Narrowing a role's queue grants is how an operator hands a queue to someone
    else, and it is exactly when the old claim most needs to go. But a
    heartbeat policy scoped by queue stops matching the role's own row the
    moment the grant is gone, so the row becomes undeletable -- by its writer,
    by every role, forever -- and readiness goes on naming that scheduler as
    feeding a queue that is no longer its.

    Row-level security makes an excluded row invisible rather than forbidden,
    so none of that raises: the delete reports zero rows and the tick looks
    clean. The read policy has to widen with the delete for the same reason --
    the retraction looks the rows up first, and would otherwise be correct and
    never fire.
    """
    dropped = f"{queue_name}_taken"
    kept = f"{queue_name}_mine"
    instance = f"s-{uuid.uuid4().hex[:8]}"
    fire_at = datetime.now(UTC) + timedelta(minutes=2)

    def specs(*names: str) -> list[ScheduleSpec]:
        return [
            ScheduleSpec(
                name=f"{name}-{instance}",
                task="prepare",
                cron="* * * * *",
                queue=name,
            )
            for name in names
        ]

    async def beating() -> list[str]:
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT queue FROM task_queue.runtime_heartbeats"
                " WHERE kind = 'scheduler' AND instance = $1 ORDER BY queue",
                instance,
            )
        return [row["queue"] for row in rows]

    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.SCHEDULE],
        queues=[dropped, kept],
        prefix="rq_scheduler",
    ) as scheduler_pool:
        role = await scheduler_pool.fetchval("SELECT current_user")
        scheduler = Scheduler(
            Queue(scheduler_pool, name=queue_name),
            scheduler_id=instance,
            schedules=specs(dropped, kept),
        )
        await scheduler.sync()
        await scheduler.tick(now=fire_at)
        assert await beating() == sorted([dropped, kept])

        # The operator takes one queue away and disables its schedule.
        admin_connection = await asyncpg.connect(admin_dsn)
        try:
            await grant_queues(admin_connection, role=role, queues=[kept])
            await admin_connection.execute(
                "UPDATE task_queue.schedules SET enabled = false WHERE queue = $1",
                dropped,
            )
        finally:
            await admin_connection.close()
        scheduler.schedules = tuple(specs(kept))

        await scheduler.tick(now=fire_at + timedelta(minutes=2))
        assert await beating() == [kept]

    async with pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE queue = ANY($1::text[])",
            [dropped, kept],
        )
        await connection.execute(
            "DELETE FROM task_queue.runtime_heartbeats WHERE instance = $1", instance
        )


async def test_a_scheduler_without_delete_ticks_instead_of_looping(
    admin_dsn: str,
    pool: asyncpg.Pool,
    queue_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A migration cannot re-grant anything to a role that already exists.

    `DELETE` on `runtime_heartbeats` joined the SCHEDULE grant set with 0008,
    and every scheduler role provisioned before that keeps the old one until an
    operator re-provisions it. A retraction issued unconditionally would then
    fail on *every* tick -- which `run()` retries at its interval, forever --
    over a row the scheduler had no reason to touch, on deployments where
    nothing ever changes.

    So the retraction is asked for only when there is something to retract, and
    when it is refused the claim is left to go stale rather than the tick to
    fail. Scheduling is the scheduler's job; a missing grant is not a reason to
    stop doing it, and the staleness window is exactly the behaviour from
    before retraction existed.
    """
    dropped = f"{queue_name}_dropped"
    kept = f"{queue_name}_kept"
    instance = f"s-{uuid.uuid4().hex[:8]}"
    fire_at = datetime.now(UTC) + timedelta(minutes=2)

    def specs(*enabled_names: str) -> list[ScheduleSpec]:
        return [
            ScheduleSpec(
                name=f"{name}-{instance}",
                task="prepare",
                cron="* * * * *",
                queue=name,
                enabled=name in enabled_names,
            )
            for name in (dropped, kept)
        ]

    async def beating() -> list[str]:
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT queue FROM task_queue.runtime_heartbeats"
                " WHERE kind = 'scheduler' AND instance = $1 ORDER BY queue",
                instance,
            )
        return [row["queue"] for row in rows]

    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.SCHEDULE],
        queues=[dropped, kept],
        prefix="rq_scheduler",
    ) as scheduler_pool:
        role = await scheduler_pool.fetchval("SELECT current_user")
        admin_connection = await asyncpg.connect(admin_dsn)
        try:
            # Exactly what a role provisioned before 0008 holds.
            await admin_connection.execute(
                f"REVOKE DELETE ON task_queue.runtime_heartbeats FROM {role}"
            )
        finally:
            await admin_connection.close()

        scheduler = Scheduler(
            Queue(scheduler_pool, name=queue_name),
            scheduler_id=instance,
            schedules=specs(dropped, kept),
        )
        await scheduler.sync()

        # Nothing to retract: the statement that needs DELETE is never issued,
        # so the ordinary tick of an un-reprovisioned role is unaffected. Asked
        # for and refused would leave the tick working by the grace of its
        # error handling, which is not the same thing -- so count the calls.
        attempts: list[list[str]] = []
        real_clear = scheduler._storage.clear_runtime_heartbeats

        async def counting_clear(connection: asyncpg.Connection, **kwargs: Any) -> int:
            attempts.append(sorted(kwargs["keep"]))
            return await real_clear(connection, **kwargs)

        scheduler._storage.clear_runtime_heartbeats = counting_clear  # type: ignore[method-assign]
        try:
            for _ in range(3):
                await scheduler.tick(now=fire_at)
            assert await beating() == sorted([dropped, kept])
            assert attempts == [], attempts

            # Now there is something to retract, and it cannot be.
            scheduler.schedules = tuple(specs(kept))
            await scheduler.sync()
            with caplog.at_level(logging.WARNING, logger="rqueue.scheduler"):
                for offset in (2, 4, 6):
                    await scheduler.tick(now=fire_at + timedelta(minutes=offset))

            # Still ticking, and the stale claim is left to age out.
            assert await beating() == sorted([dropped, kept])
            # Said once for the set, not once per tick, and it names the remedy.
            warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
            assert len(warnings) == 1, [r.getMessage() for r in warnings]
            assert "provision_role" in warnings[0].getMessage()
            assert dropped in warnings[0].getMessage()

            # Once there is, it is asked for -- and refused, not skipped.
            assert attempts == [[kept]] * 3, attempts

            # And re-provisioning is that remedy.
            admin_connection = await asyncpg.connect(admin_dsn)
            try:
                await admin_connection.execute(
                    f"GRANT DELETE ON task_queue.runtime_heartbeats TO {role}"
                )
            finally:
                await admin_connection.close()
            await scheduler.tick(now=fire_at + timedelta(minutes=8))
            assert await beating() == [kept]
        finally:
            scheduler._storage.clear_runtime_heartbeats = real_clear  # type: ignore[method-assign]

    async with pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE queue = ANY($1::text[])",
            [dropped, kept],
        )
        await connection.execute(
            "DELETE FROM task_queue.runtime_heartbeats WHERE instance = $1", instance
        )


async def test_a_global_liveness_probe_counts_one_process_once(
    pool: asyncpg.Pool, queue_name: str
) -> None:
    """Per-queue heartbeats must not inflate an unfiltered readiness answer."""
    instance = f"one-{uuid.uuid4().hex[:8]}"
    queues = [f"{queue_name}_{index}" for index in range(3)]
    storage = Storage("task_queue")
    async with pool.acquire() as connection:
        for queue in queues:
            await storage.record_runtime_heartbeat(
                connection, kind="scheduler", instance=instance, queue=queue
            )
        # Other tests share this database, so count this instance rather than
        # asserting on the whole table: the defect was three rows for one id.
        live = await storage.live_instances(
            connection,
            kind="scheduler",
            since=datetime.now(UTC) - timedelta(minutes=1),
        )
        assert live.count(instance) == 1
        assert live == sorted(live)

    report = await check_readiness(pool, require_worker=False, require_scheduler=True)
    assert report.schedulers.count(instance) == 1

    async with pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM task_queue.runtime_heartbeats WHERE instance = $1", instance
        )


async def test_migrating_a_schedule_that_changed_queues(admin_dsn: str) -> None:
    """Migrations are tested against what a real database can hold.

    A schedule moved between queues before 0005 leaves occurrences whose jobs
    are on the old queue while the schedule is on the new one. The backfill has
    to pick one, and it picks the job -- the reference whose queue cannot
    change, and the one the constraint holds to.
    """
    schema = f"mig_moved_{uuid.uuid4().hex[:8]}"
    connection = await asyncpg.connect(admin_dsn)
    try:
        await migrations.migrate(connection, schema=schema, target=4)
        schedule = await connection.fetchval(
            f"""
            INSERT INTO {schema}.schedules (name, queue, task, cron)
            VALUES ('mover', 'before', 'prepare', '* * * * *') RETURNING id
            """
        )
        job = await connection.fetchval(
            f"""
            INSERT INTO {schema}.jobs (queue, task, payload, state, max_attempts)
            VALUES ('before', 'prepare', '{{}}'::jsonb, 'pending', 3) RETURNING id
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.schedule_occurrences
                (schedule_id, occurrence_at, job_id)
            VALUES ($1, now(), $2)
            """,
            schedule,
            job,
        )
        await connection.execute(
            f"UPDATE {schema}.schedules SET queue = 'after' WHERE id = $1", schedule
        )

        assert await migrations.migrate(connection, schema=schema)
        assert (
            await connection.fetchval(
                f"SELECT queue FROM {schema}.schedule_occurrences WHERE job_id = $1",
                job,
            )
            == "before"
        )

        # And the schedule can still move, without disturbing what already
        # fired: those jobs are on the queue they were enqueued onto, and the
        # occurrence recording them says so.
        await connection.execute(
            f"UPDATE {schema}.schedules SET queue = 'later' WHERE id = $1", schedule
        )
        assert (
            await connection.fetchval(
                f"SELECT queue FROM {schema}.schedule_occurrences WHERE job_id = $1",
                job,
            )
            == "before"
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await connection.close()


#: The heartbeat upsert as the *previous* release writes it. Migration 0005
#: changes the key this arbiter names, so this statement is the thing that has
#: to be reasoned about during an upgrade -- not a paraphrase of it.
PREVIOUS_RELEASE_HEARTBEAT = """
    INSERT INTO {schema}.runtime_heartbeats
        (kind, instance, queue, updated_at, metadata)
    VALUES ($1, $2, $3, now(), $4::text::jsonb)
    ON CONFLICT (kind, instance) DO UPDATE
    SET queue = EXCLUDED.queue,
        updated_at = now(),
        metadata = EXCLUDED.metadata
"""


async def test_0005_refuses_to_run_under_a_live_previous_release_worker(
    admin_dsn: str,
) -> None:
    """The 0004-to-0005 transition, with an old-version process still running.

    0005 changes the heartbeat key, and the previous release's upsert names the
    old one. That statement is the first in every worker tick and its failure
    is retried forever by the tick's error handler, so applying 0005 under a
    running old worker does not crash it -- it stalls it, claiming nothing.
    The migration therefore refuses, and says how to proceed.
    """
    schema = f"mig_live_{uuid.uuid4().hex[:8]}"
    connection = await asyncpg.connect(admin_dsn)
    old_heartbeat = PREVIOUS_RELEASE_HEARTBEAT.format(schema=schema)
    try:
        await migrations.migrate(connection, schema=schema, target=4)

        # A previous-release worker, alive and beating.
        await connection.execute(old_heartbeat, "worker", "w1", "alpha", "{}")

        with pytest.raises(MigrationError) as refused:
            await migrations.migrate(connection, schema=schema)
        assert "0005_queue_scoped_runtime" in str(refused.value)
        assert "DELETE FROM" in str(refused.value)

        # Nothing was applied: the schema is exactly where it was, so the
        # worker that is still running is still working.
        assert await migrations.current_version(connection, schema=schema) == 4
        await connection.execute(old_heartbeat, "worker", "w1", "alpha", "{}")

        # An old heartbeat is not evidence of anything: a worker with a long
        # poll interval is alive with one. Only an empty table is evidence, and
        # only because a live worker refills it within a poll interval.
        await connection.execute(
            f"UPDATE {schema}.runtime_heartbeats "
            "SET updated_at = now() - interval '3 hours'"
        )
        with pytest.raises(MigrationError):
            await migrations.migrate(connection, schema=schema)
        assert await migrations.current_version(connection, schema=schema) == 4

        # The operator stops the fleet and says so, checkably.
        await connection.execute(f"DELETE FROM {schema}.runtime_heartbeats")
        applied = await migrations.migrate(connection, schema=schema)
        assert "queue_scoped_runtime" in [m.name for m in applied]

        # Past the transition the old statement is dead, as documented, and the
        # new one -- the shape this release ships -- works.
        with pytest.raises(asyncpg.InvalidColumnReferenceError):
            await connection.execute(old_heartbeat, "worker", "w1", "alpha", "{}")
        storage = Storage(schema)
        for queue in ("alpha", "beta"):
            await storage.record_runtime_heartbeat(
                connection, kind="worker", instance="w1", queue=queue
            )
        assert (
            await connection.fetchval(
                f"SELECT count(*) FROM {schema}.runtime_heartbeats "
                "WHERE instance = 'w1'"
            )
            == 2
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await connection.close()


#: The same statement as the release *this* one follows writes it: 0005 put the
#: queue in the key, 0008 adds the role. Same reasoning as the constant above --
#: the upgrade has to be argued against the real statement.
HEARTBEAT_BEFORE_0008 = """
    INSERT INTO {schema}.runtime_heartbeats
        (kind, instance, queue, updated_at, metadata)
    VALUES ($1, $2, $3, now(), $4::text::jsonb)
    ON CONFLICT (kind, instance, queue) DO UPDATE
    SET updated_at = now(),
        metadata = EXCLUDED.metadata
"""


async def test_0008_checks_for_the_live_fleet_itself_rather_than_trusting_0005(
    admin_dsn: str,
) -> None:
    """A database already at 0007 gets the check, not a comment about 0005.

    0005 asks for the same drain, and a body runs exactly once: a deployment
    that applied 0005 in an earlier release never re-executes that check when
    it picks this one up. Inheriting the guarantee by comment would hold only
    for a schema installed from empty in a single `migrate`, and 0008 breaks
    the same statement in the same way -- retried forever, claiming nothing.
    """
    schema = f"mig_0008_{uuid.uuid4().hex[:8]}"
    connection = await asyncpg.connect(admin_dsn)
    old_heartbeat = HEARTBEAT_BEFORE_0008.format(schema=schema)
    try:
        # 0005 ran here, against an empty table, in some earlier release.
        await migrations.migrate(connection, schema=schema, target=7)

        # The fleet is up and beating the way that release beats.
        await connection.execute(old_heartbeat, "worker", "w1", "alpha", "{}")

        with pytest.raises(MigrationError) as refused:
            await migrations.migrate(connection, schema=schema)
        assert "0008_heartbeat_ownership" in str(refused.value)
        assert "DELETE FROM" in str(refused.value)

        # Refused means untouched: the fleet that is still running still works.
        assert await migrations.current_version(connection, schema=schema) == 7
        await connection.execute(old_heartbeat, "worker", "w1", "alpha", "{}")

        await connection.execute(f"DELETE FROM {schema}.runtime_heartbeats")
        applied = await migrations.migrate(connection, schema=schema)
        assert "heartbeat_ownership" in [m.name for m in applied]

        # Past the transition that statement is dead -- which is exactly why
        # the check has to stop the fleet from being in front of it.
        with pytest.raises(asyncpg.InvalidColumnReferenceError):
            await connection.execute(old_heartbeat, "worker", "w1", "alpha", "{}")
        await Storage(schema).record_runtime_heartbeat(
            connection, kind="worker", instance="w1", queue="alpha"
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await connection.close()


async def test_0010_refuses_while_a_slot_is_still_leased(admin_dsn: str) -> None:
    """A slot is not a heartbeat, and an orphaned one is not harmless.

    0010 attributes every pre-existing row to whoever runs it, because nothing
    in the row says who held it. For a heartbeat that self-heals. For a slot it
    does not: the real holder can no longer extend or release the row, so the
    lease expires under a running attempt and the expiry branch of the new
    policy hands the key to the next worker that asks -- the same logical job
    running twice, which is the whole purpose of the table.
    """
    schema = f"mig_0010_{uuid.uuid4().hex[:8]}"
    connection = await asyncpg.connect(admin_dsn)
    try:
        await migrations.migrate(connection, schema=schema, target=9)
        job = await connection.fetchval(
            f"""
            INSERT INTO {schema}.jobs (queue, task, payload, state, max_attempts)
            VALUES ('alpha', 't', '{{}}'::jsonb, 'pending', 3) RETURNING id
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.concurrency_slots
                (key, job_id, lease_token, worker_id, queue, leased_until)
            VALUES ('k', $1, gen_random_uuid(), 'w1', 'alpha',
                    now() + interval '10 minutes')
            """,
            job,
        )

        with pytest.raises(MigrationError) as refused:
            await migrations.migrate(connection, schema=schema)
        assert "0010_slot_ownership" in str(refused.value)
        assert "still leased" in str(refused.value)
        assert await migrations.current_version(connection, schema=schema) == 9

        # An expired slot is a different thing: its holder is gone, and the
        # expiry branches hand the key on the way a crashed worker always has.
        await connection.execute(
            f"UPDATE {schema}.concurrency_slots "
            "SET leased_until = now() - interval '1 minute'"
        )
        applied = await migrations.migrate(connection, schema=schema)
        assert "slot_ownership" in [m.name for m in applied]
        assert (
            await connection.fetchval(
                f"SELECT count(*) FROM {schema}.concurrency_slots"
            )
            == 1
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await connection.close()


async def test_0010_refuses_a_staged_upgrade_that_redeployed_before_it(
    admin_dsn: str,
) -> None:
    """Slots cannot show that the fleet is stopped; heartbeats can.

    A worker between jobs holds no slot at all, so the lease check above sees
    an empty table and a running fleet alike. It has to see the difference: the
    previous release's takeover upsert never sets the owner column 0010 adds,
    so crossing roles it fails the new WITH CHECK. `--target 9`, deploy,
    `--target 10` is the only way to reach this migration with a fleet up, and
    it is what the second check is for.
    """
    schema = f"mig_staged_{uuid.uuid4().hex[:8]}"
    connection = await asyncpg.connect(admin_dsn)
    try:
        await migrations.migrate(connection, schema=schema, target=9)
        await Storage(schema).record_runtime_heartbeat(
            connection, kind="worker", instance="w1", queue="alpha"
        )
        assert (
            await connection.fetchval(
                f"SELECT count(*) FROM {schema}.concurrency_slots"
            )
            == 0
        )

        with pytest.raises(MigrationError) as refused:
            await migrations.migrate(connection, schema=schema, target=10)
        assert "0010_slot_ownership" in str(refused.value)
        assert "heartbeat" in str(refused.value)
        assert await migrations.current_version(connection, schema=schema) == 9

        await connection.execute(f"DELETE FROM {schema}.runtime_heartbeats")
        applied = await migrations.migrate(connection, schema=schema)
        assert "slot_ownership" in [m.name for m in applied]
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await connection.close()


async def test_a_job_keeps_its_own_attempt_budget_when_a_handler_asks_to_retry(
    pool: asyncpg.Pool, queue_name: str
) -> None:
    """`Retry` skips the policy's exception filter, not the job's budget.

    A job enqueued with its own `max_attempts` that is rescheduled past that
    budget comes back pending with `attempt == max_attempts`, which the claim
    predicate excludes -- pending forever, and never run again. The registered
    policy's default is not the authority here; the persisted row is.
    """
    queue = Queue(pool, name=queue_name)

    @queue.task(name="asks_to_retry")
    async def asks_to_retry(payload: object, context: TaskContext) -> None:
        raise Retry(delay=0)

    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(
            connection, task="asks_to_retry", payload={}, max_attempts=1
        )

    worker = Worker(queue, worker_id=f"w-{uuid.uuid4().hex[:8]}", poll_interval=0.05)
    async with running(worker):
        final = await eventually(
            lambda: _terminal(pool, job.id),
            message="the job never reached a terminal state",
        )
    assert final is not None
    assert final["state"] == JobState.FAILED
    assert final["attempt"] == final["max_attempts"] == 1
    assert final["error_type"] == "Retry"


async def _terminal(pool: asyncpg.Pool, job_id: uuid.UUID) -> dict[str, object] | None:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT state, attempt, max_attempts, error_type FROM task_queue.jobs "
            "WHERE id = $1",
            job_id,
        )
    if row is None or not JobState(row["state"]).is_terminal:
        return None
    return dict(row)


async def test_a_scheduler_with_nothing_enabled_writes_no_heartbeat(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """An empty tick has no queue to claim liveness for.

    This handle's queue is a default for schedules that do not name one, not a
    queue this scheduler feeds. Beating against it would claim liveness for
    something nothing is scheduling and, for a role not granted that queue,
    fail on every tick -- which `run()` retries at its interval, forever.
    """
    served = f"{queue_name}_served"
    instance = f"s-{uuid.uuid4().hex[:8]}"
    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.SCHEDULE],
        queues=[served],
        prefix="rq_scheduler",
    ) as scheduler_pool:
        scheduler = Scheduler(
            Queue(scheduler_pool, name=queue_name),
            scheduler_id=instance,
            schedules=[],
        )
        assert await scheduler.tick(now=datetime.now(UTC)) == []

        # And a tick that does fire still reports, so this is a silent no-op
        # only when there is genuinely nothing to report.
        await scheduler.queue.pool.execute(
            """
            INSERT INTO task_queue.schedules (name, queue, task, cron)
            VALUES ($1, $2, 'prepare', '* * * * *')
            """,
            f"served-{uuid.uuid4().hex[:8]}",
            served,
        )
        await scheduler.tick(now=datetime.now(UTC) + timedelta(minutes=2))

    async with pool.acquire() as connection:
        beats = await connection.fetch(
            "SELECT queue FROM task_queue.runtime_heartbeats WHERE instance = $1",
            instance,
        )
        assert [row["queue"] for row in beats] == [served]
        await connection.execute(
            "DELETE FROM task_queue.runtime_heartbeats WHERE instance = $1", instance
        )
        await connection.execute("DELETE FROM task_queue.jobs WHERE queue = $1", served)
        await connection.execute(
            "DELETE FROM task_queue.schedules WHERE queue = $1", served
        )


async def test_provisioning_refuses_a_role_that_owns_the_queue_tables(
    admin_dsn: str,
) -> None:
    """Ownership sits above the ACL, so pruning grants cannot narrow an owner.

    An owner re-grants itself anything, and can ALTER or DROP the table --
    including turning row-level security off. `provision_role` has to say so
    rather than report success on a role it did not actually narrow.
    """
    role = f"rq_owner_{uuid.uuid4().hex[:8]}"
    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.INSPECT],
            queues=["alpha"],
            password="scoped-role-test-password",
        )
        await admin_connection.execute(
            f"ALTER TABLE task_queue.queue_pauses OWNER TO {role}"
        )

        with pytest.raises(ValidationError) as refused:
            await provision_role(
                admin_connection,
                role=role,
                capabilities=[Capability.INSPECT],
                queues=["alpha"],
            )
        assert "task_queue.queue_pauses" in str(refused.value)
        # Indexes and identity sequences follow their table's owner, so naming
        # them would bury the object an operator has to act on.
        assert "index" not in str(refused.value)

        await admin_connection.execute(
            "ALTER TABLE task_queue.queue_pauses OWNER TO CURRENT_USER"
        )
        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.INSPECT],
            queues=["alpha"],
        )
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
        finally:
            await admin_connection.close()


async def test_a_purge_only_role_can_purge_through_admin(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """PURGE has to be enough to run the documented retention API.

    A capability whose only caller is raw SQL is not a capability. `Admin`
    holds no DELETE on `jobs` when it runs as this role, so this passes only
    if the delete goes through the routine that bounds it.
    """
    other_queue = f"{queue_name}_other"
    for queue in (queue_name, other_queue):
        for _ in range(2):
            await seed_terminal_job(
                pool, queue=queue, state="succeeded", age=timedelta(days=30)
            )
    live = await seed_terminal_job(
        pool, queue=queue_name, state="pending", age=timedelta(0)
    )

    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.PURGE],
        queues=[queue_name],
        prefix="rq_purger",
    ) as purge_pool:
        admin = Admin(purge_pool)
        assert await admin.purge(queue=queue_name, retention=timedelta(days=1)) == 2

        # ... and with the wildcard, over every queue it holds. The other
        # queue is invisible to it, so nothing there is touched.
        for _ in range(2):
            await seed_terminal_job(
                pool, queue=queue_name, state="succeeded", age=timedelta(days=30)
            )
        assert await admin.purge(queue="*", retention=timedelta(days=1)) == 2

        # A typed error, not the driver's: the CLI only formats rqueue's own,
        # and "you are not granted that queue" is a configuration answer.
        with pytest.raises(ConfigurationError):
            await admin.purge(queue=other_queue, retention=timedelta(days=1))
        # A live state is refused outright rather than quietly matching
        # nothing -- before a statement is sent, so the answer does not depend
        # on whether any row happened to match. The routine rejects it too;
        # that half is `test_the_purge_routine_refuses_a_crafted_call`.
        with pytest.raises(ValidationError):
            await admin.purge(
                queue=queue_name, retention=timedelta(days=1), states=["pending"]
            )
        with pytest.raises(ValidationError):
            await admin.purge(
                queue=queue_name, retention=timedelta(days=1), limit=10**9
            )

    assert await surviving(pool, live) == {live}
    async with pool.acquire() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.jobs WHERE queue = $1", other_queue
            )
            == 2
        )
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE queue = ANY($1::text[])",
            [queue_name, other_queue],
        )


async def test_a_schedule_that_moves_queues_does_not_refire_its_history(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """One occurrence per (schedule, instant), on whichever queue.

    A scheduler reads `last_occurrence` to bound catch-up. If the rows written
    before a queue move were invisible to it, that history would read as empty
    and every one of those occurrences would fire a second time -- two rows for
    what the guarantee says is one.
    """
    moved_to = f"{queue_name}_moved"
    fired_at = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=5)
    async with pool.acquire() as connection:
        schedule = await connection.fetchval(
            """
            INSERT INTO task_queue.schedules (name, queue, task, cron, created_at)
            VALUES ($1, $2, 'prepare', '* * * * *', now() - interval '1 hour')
            RETURNING id
            """,
            f"mover-{uuid.uuid4().hex[:8]}",
            queue_name,
        )
        job = await connection.fetchval(
            """
            INSERT INTO task_queue.jobs (queue, task, payload, state, max_attempts)
            VALUES ($1, 'prepare', '{}'::jsonb, 'pending', 3) RETURNING id
            """,
            queue_name,
        )
        await connection.execute(
            """
            INSERT INTO task_queue.schedule_occurrences
                (schedule_id, occurrence_at, queue, job_id)
            VALUES ($1, $2, $3, $4)
            """,
            schedule,
            fired_at,
            queue_name,
            job,
        )
        await connection.execute(
            "UPDATE task_queue.schedules SET queue = $1 WHERE id = $2",
            moved_to,
            schedule,
        )

    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.SCHEDULE],
        queues=[moved_to],
        prefix="rq_scheduler",
    ) as scheduler_pool:
        scheduler = Scheduler(
            Queue(scheduler_pool, name=moved_to),
            scheduler_id=f"s-{uuid.uuid4().hex[:8]}",
            catchup=20,
        )
        async with scheduler_pool.acquire() as connection:
            # The history from before the move is visible through the schedule
            # this role now owns, not through the queue the rows carry.
            assert (
                await scheduler.queue.storage.last_occurrence(connection, schedule)
                == fired_at
            )
        await scheduler.tick(now=fired_at + timedelta(minutes=1))

    async with pool.acquire() as connection:
        instants = [
            row["occurrence_at"]
            for row in await connection.fetch(
                "SELECT occurrence_at FROM task_queue.schedule_occurrences "
                "WHERE schedule_id = $1 ORDER BY occurrence_at",
                schedule,
            )
        ]
        assert instants == sorted(set(instants))
        assert instants[0] == fired_at
        await connection.execute(
            "DELETE FROM task_queue.jobs WHERE queue = ANY($1::text[])",
            [queue_name, moved_to],
        )
        await connection.execute(
            "DELETE FROM task_queue.schedules WHERE id = $1", schedule
        )
        await connection.execute(
            "DELETE FROM task_queue.runtime_heartbeats WHERE queue = $1", moved_to
        )


async def test_purge_reports_a_bad_argument_even_with_nothing_to_purge(
    pool: asyncpg.Pool, queue_name: str
) -> None:
    """A fan-out that matches no queue must still refuse a bad call.

    `purge_terminal_jobs` rejects a live state and a future cutoff, but only
    once something calls it. With no queue to visit, an unvalidated fan-out
    returns a quiet `0` -- the caller's bad arguments read as "nothing to do".
    """
    admin = Admin(pool)
    empty = f"{queue_name}_empty"
    day = timedelta(days=1)
    future = timedelta(days=-1)
    # Both the fan-out and the single-queue path, since only the fan-out can
    # skip the routine entirely.
    for target in ("*", empty):
        with pytest.raises(ValidationError):
            await admin.purge(queue=target, retention=day, states=["pending"])
        with pytest.raises(ValidationError):
            await admin.purge(queue=target, retention=day, states=[])
        with pytest.raises(ValidationError):
            await admin.purge(queue=target, retention=future)

    assert await admin.purge(queue=empty, retention=day) == 0


async def test_the_purge_wildcard_means_every_queue_and_nothing_else_does(
    pool: asyncpg.Pool, purge_schema: str
) -> None:
    """`'*'` purges every queue; leaving the queue out purges nothing.

    A delete across the schema has to be written out. `None` is refused rather
    than read as "all", so a caller whose queue variable came back empty does
    not wipe retention history, and the refusal is checked against rows that
    would have gone.
    """
    admin = Admin(pool, schema=purge_schema)
    day = timedelta(days=1)
    for queue in ("a", "b"):
        await seed_finished(
            pool, schema=purge_schema, queue=queue, age=timedelta(days=5), count=2
        )

    for bad in (
        None,
        "",
        "has a space",
        "com*pute",
        "x" * (MAX_QUEUE_NAME_LENGTH + 1),
    ):
        with pytest.raises(ValidationError):
            await admin.purge(queue=bad, retention=day)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        await admin.purge(retention=day)  # type: ignore[call-arg]

    async def remaining() -> int:
        async with pool.acquire() as connection:
            return int(
                await connection.fetchval(f"SELECT count(*) FROM {purge_schema}.jobs")
            )

    assert await remaining() == 4

    assert await admin.purge(queue="a", retention=day) == 2
    assert await remaining() == 2
    assert await admin.purge(queue="*", retention=day) == 2
    assert await remaining() == 0
    assert await admin.purge(queue="*", retention=day) == 0


async def test_the_wildcard_reads_every_queue_in_stats_and_listings(
    pool: asyncpg.Pool, purge_schema: str
) -> None:
    admin = Admin(pool, schema=purge_schema)
    for queue, count in (("a", 2), ("b", 3)):
        await seed_finished(
            pool, schema=purge_schema, queue=queue, age=timedelta(days=5), count=count
        )

    assert (await admin.stats("a")).succeeded == 2
    everything = await admin.stats("*")
    assert everything.queue == "*"
    assert everything.succeeded == 5

    assert len(await admin.list_jobs(queue="a")) == 2
    assert len(await admin.list_jobs(queue="*")) == 5
    assert len(await admin.list_jobs()) == 5
    with pytest.raises(ValidationError):
        await admin.stats("com*pute")
    with pytest.raises(ValidationError):
        await admin.list_jobs(queue="com*pute")


async def test_a_naive_purge_cutoff_is_refused_rather_than_guessed(
    pool: asyncpg.Pool, queue_name: str
) -> None:
    """The cutoff is compared against an aware `now()`, so it must be aware.

    `validate_scheduled_at` already sets the convention for a caller-supplied
    instant: naive is refused, not assumed to be UTC, because guessing moves
    the boundary by the caller's own offset and deletes rows they did not name.
    Without the check the comparison itself raises `TypeError`, which is not an
    `RqueueError` and so leaves the CLI as a traceback.
    """
    admin = Admin(pool)
    naive = datetime(2026, 9, 1)
    for target in ("*", queue_name):
        with pytest.raises(ValidationError, match="timezone-aware"):
            await admin.purge(queue=target, retention=timedelta(days=1), now=naive)

    aware = naive.replace(tzinfo=UTC)
    day = timedelta(days=1)
    assert await admin.purge(queue=queue_name, retention=day, now=aware) == 0


@pytest.fixture
async def purge_schema(pool: asyncpg.Pool) -> AsyncIterator[str]:
    """A migrated schema of this test's own.

    A whole-schema purge is, by definition, not isolated by queue name: it
    looks at every terminal row in the schema to decide which are oldest. Two
    tests reasoning about *which* rows a budget reaches therefore need separate
    schemas, not separate queues.
    """
    name = f"purge_{uuid.uuid4().hex[:10]}"
    async with pool.acquire() as connection:
        await migrations.migrate(connection, schema=name)
    try:
        yield name
    finally:
        async with pool.acquire() as connection:
            await connection.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")


async def seed_finished(
    pool: asyncpg.Pool, *, schema: str, queue: str, age: timedelta, count: int = 1
) -> None:
    """``count`` succeeded jobs on ``queue``, each finished ``age`` ago."""
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.jobs
                (queue, task, payload, state, max_attempts, finished_at)
            SELECT $1, 'prepare', '{{}}'::jsonb, 'succeeded', 3,
                   now() - $2::interval - (g * interval '1 millisecond')
            FROM generate_series(1, $3) AS g
            """,
            queue,
            age,
            count,
        )


async def test_a_bounded_purge_deletes_the_oldest_jobs_first(
    pool: asyncpg.Pool, purge_schema: str
) -> None:
    """Retention is oldest-first, across queues as well as within one.

    A single DELETE ordered by finished_at gave that for free. It is the case
    below that a fan-out gets wrong: one queue holding both the oldest row in
    the schema *and* enough newer ones to exhaust the budget. Visiting queues
    in age order is not enough -- the first queue is entered oldest-first and
    then drained, so its two-day-old rows go while another queue's fifty-day-old
    ones stay. Only a cutoff derived from the budget keeps the invariant.
    """
    mixed, old = "aaa", "zzz"
    assert mixed < old
    await seed_finished(pool, schema=purge_schema, queue=mixed, age=timedelta(days=100))
    await seed_finished(
        pool, schema=purge_schema, queue=mixed, age=timedelta(days=2), count=5
    )
    await seed_finished(
        pool, schema=purge_schema, queue=old, age=timedelta(days=50), count=5
    )

    # The six oldest rows in the schema: one at 100 days, five at 50.
    admin = Admin(pool, schema=purge_schema)
    assert await admin.purge(queue="*", retention=timedelta(days=1), limit=6) == 6

    async with pool.acquire() as connection:
        remaining = await connection.fetch(
            f"SELECT queue, count(*) AS n FROM {purge_schema}.jobs GROUP BY queue"
        )
        assert {row["queue"]: row["n"] for row in remaining} == {mixed: 5}


async def test_a_bounded_purge_still_progresses_on_identically_aged_jobs(
    pool: asyncpg.Pool, purge_schema: str
) -> None:
    """Jobs finished in one transaction share a `finished_at`.

    The budget's boundary row then carries the same instant as the oldest one,
    and a strict `<` against it would delete nothing at all -- a purge that
    silently stops working. Rows of identical age have no oldest-first order to
    respect between them, so the caller's own cutoff is used instead.
    """
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {purge_schema}.jobs
                (queue, task, payload, state, max_attempts, finished_at)
            SELECT 'tied', 'prepare', '{{}}'::jsonb, 'succeeded', 3,
                   now() - interval '9 days'
            FROM generate_series(1, 10)
            """
        )

    admin = Admin(pool, schema=purge_schema)
    assert await admin.purge(queue="*", retention=timedelta(days=1), limit=3) == 3

    async with pool.acquire() as connection:
        assert (
            await connection.fetchval(f"SELECT count(*) FROM {purge_schema}.jobs") == 7
        )


async def test_purging_needs_a_queue_grant_not_just_a_privilege(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """0006 changed who may purge, and docs/migrations.md says so.

    Retention used to be a plain DELETE, so any role holding that privilege
    could run it. The routine authorizes the login role against
    `role_queue_grants` instead -- which is what lets a PURGE role delete with
    no DELETE grant, and what stops an operator identity that predates this
    from purging until it is listed. Pinned here so the trade cannot be
    reversed by accident, and so the documented remedy stays true.
    """
    role = f"rq_operator_{uuid.uuid4().hex[:8]}"
    password = "scoped-role-test-password"
    database = urlsplit(admin_dsn).path.lstrip("/")
    target = database_target(admin_dsn, database, user=role, password=password)
    await seed_terminal_job(
        pool, queue=queue_name, state="succeeded", age=timedelta(days=30)
    )

    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        # Deliberately not provision_role: this is the pre-existing operator
        # identity, holding privileges directly and no queue grant at all.
        await admin_connection.execute(
            f"CREATE ROLE {role} LOGIN PASSWORD '{password}'"
        )
        await admin_connection.execute(f"GRANT USAGE ON SCHEMA task_queue TO {role}")
        await admin_connection.execute(
            f"GRANT SELECT, DELETE ON task_queue.jobs TO {role}"
        )
        await admin_connection.execute(
            "GRANT EXECUTE ON FUNCTION task_queue.purge_terminal_jobs"
            f"(text, text[], timestamptz, integer) TO {role}"
        )

        operator_pool = await asyncpg.create_pool(
            target.database_url, min_size=1, max_size=2
        )
        assert operator_pool is not None
        try:
            operator = Admin(operator_pool)
            with pytest.raises(ConfigurationError) as refused:
                await operator.purge(queue=queue_name, retention=timedelta(days=1))
            assert f"not granted queue {queue_name}" in str(refused.value)

            # The documented remedy, and the only one needed.
            await grant_queues(admin_connection, role=role, queues=[QUEUE_WILDCARD])
            assert (
                await operator.purge(queue=queue_name, retention=timedelta(days=1)) == 1
            )
        finally:
            await operator_pool.close()
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
        finally:
            await admin_connection.close()


async def test_a_bounded_purge_takes_a_tie_group_that_straddles_the_budget(
    pool: asyncpg.Pool, purge_schema: str
) -> None:
    """The mixed case: some ages tied, some not, with the tie on the boundary.

    Jobs finished in one transaction share a `finished_at`, so a tie group
    routinely spans the point where the budget runs out. Dropping it deletes
    far fewer rows than asked for -- rows just as old as ones being deleted --
    and a cron sized to keep up silently falls behind. Named-queue and
    whole-schema purges have to agree here: same data, same answer.
    """
    for limit, tied in ((3, 4), (10, 40)):
        async with pool.acquire() as connection:
            await connection.execute(f"TRUNCATE {purge_schema}.jobs CASCADE")
        await seed_finished(
            pool, schema=purge_schema, queue="q", age=timedelta(days=10)
        )
        # One statement, so every row of this group shares an instant.
        async with pool.acquire() as connection:
            await connection.execute(
                f"""
                INSERT INTO {purge_schema}.jobs
                    (queue, task, payload, state, max_attempts, finished_at)
                SELECT 'q', 'prepare', '{{}}'::jsonb, 'succeeded', 3,
                       now() - interval '9 days'
                FROM generate_series(1, $1)
                """,
                tied,
            )

        admin = Admin(pool, schema=purge_schema)
        assert (
            await admin.purge(queue="*", retention=timedelta(days=1), limit=limit)
            == limit
        )
        async with pool.acquire() as connection:
            assert (
                await connection.fetchval(f"SELECT count(*) FROM {purge_schema}.jobs")
                == tied + 1 - limit
            )


async def test_purge_paths_agree_on_identical_data(
    pool: asyncpg.Pool, purge_schema: str
) -> None:
    """A whole-schema purge and a named-queue one must not diverge.

    They are the same documented operation with one argument different, and
    the bounding a fan-out needs is the place where they can drift apart.
    """
    admin = Admin(pool, schema=purge_schema)
    for named in (True, False):
        async with pool.acquire() as connection:
            await connection.execute(f"TRUNCATE {purge_schema}.jobs CASCADE")
            await connection.execute(
                f"""
                INSERT INTO {purge_schema}.jobs
                    (queue, task, payload, state, max_attempts, finished_at)
                SELECT 'q', 'prepare', '{{}}'::jsonb, 'succeeded', 3,
                       now() - interval '9 days'
                FROM generate_series(1, 200)
                """
            )
            await connection.execute(
                f"""
                INSERT INTO {purge_schema}.jobs
                    (queue, task, payload, state, max_attempts, finished_at)
                VALUES ('q', 'prepare', '{{}}'::jsonb, 'succeeded', 3,
                        now() - interval '10 days')
                """
            )
        removed = await admin.purge(
            queue="q" if named else "*", retention=timedelta(days=1), limit=150
        )
        assert removed == 150, "named" if named else "whole-schema"


async def test_a_purge_cutoff_is_judged_by_the_database_clock(
    pool: asyncpg.Pool, purge_schema: str
) -> None:
    """A client clock ahead of the database's must not leak a raw error.

    The routine compares the cutoff against PostgreSQL's `now()`. Checking it
    against the client's wall clock instead leaves a window the width of the
    skew, and a caller inside it -- `retention` of zero is the realistic way
    in -- passes the typed check and then takes an asyncpg error.

    `now()` is the transaction timestamp, so an open transaction reproduces
    exactly that skew without touching any clock: the database's reading of
    "now" stays at BEGIN while the client's moves on.
    """
    async with pool.acquire() as connection:
        transaction = connection.transaction()
        await transaction.start()
        try:
            frozen = await connection.fetchval("SELECT now()")
            await asyncio.sleep(0.25)
            # In the past by the client's wall clock, in the future by the
            # only clock that decides.
            cutoff = frozen + timedelta(milliseconds=100)
            assert cutoff < datetime.now(UTC)

            storage = Storage(purge_schema)
            with pytest.raises(ValidationError, match="cutoff in the past"):
                await storage.purge(connection, queue="q", older_than=cutoff, limit=10)
            with pytest.raises(ValidationError, match="cutoff in the past"):
                await storage.purge(connection, queue="*", older_than=cutoff, limit=10)

            # A cutoff the database agrees is past is accepted by both paths.
            past = frozen - timedelta(seconds=1)
            assert await storage.purge(connection, queue="q", older_than=past) == 0
            assert await storage.purge(connection, queue="*", older_than=past) == 0
        finally:
            await transaction.rollback()


async def test_provisioning_revokes_a_membership_someone_else_granted(
    admin_dsn: str,
) -> None:
    """A repaired role must not keep privileges through a group.

    Since PostgreSQL 16 one membership can be granted several times by
    different roles, and a bare `REVOKE role FROM member` removes only the
    grant the current connection made. A membership granted by anyone else
    survives it -- still inherited, still carrying whatever the group holds --
    while `provision_role` reports the role narrowed. A CONSUME-only role that
    comes back holding INSERT on `jobs` is exactly what this rule forbids.
    """
    role = f"rq_member_{uuid.uuid4().hex[:8]}"
    group = f"rq_group_{uuid.uuid4().hex[:8]}"
    granter = f"rq_granter_{uuid.uuid4().hex[:8]}"
    password = "scoped-role-test-password"
    database = urlsplit(admin_dsn).path.lstrip("/")
    target = database_target(admin_dsn, database, user=role, password=password)

    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        await admin_connection.execute(f"CREATE ROLE {group}")
        await admin_connection.execute(
            f"GRANT INSERT, DELETE ON task_queue.jobs TO {group}"
        )
        await admin_connection.execute(f"CREATE ROLE {granter} CREATEROLE")
        await admin_connection.execute(f"GRANT {group} TO {granter} WITH ADMIN OPTION")
        await admin_connection.execute(
            f"CREATE ROLE {role} LOGIN PASSWORD '{password}'"
        )
        # Granted by a third party, which is the case a bare REVOKE misses.
        granting = await asyncpg.connect(admin_dsn)
        try:
            await granting.execute(f"SET ROLE {granter}")
            await granting.execute(f"GRANT {group} TO {role}")
        finally:
            await granting.close()

        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.CONSUME],
            queues=["alpha"],
            password=password,
        )

        assert (
            await admin_connection.fetchval(
                """
                SELECT count(*) FROM pg_auth_members AS m
                JOIN pg_roles AS holder ON holder.oid = m.member
                WHERE holder.rolname = $1
                """,
                role,
            )
            == 0
        )

        # The grant table is only half the answer; what the role can actually
        # do is the other half.
        member = await asyncpg.connect(target.database_url)
        try:
            for statement in (
                "INSERT INTO task_queue.jobs (queue, task, payload, state, "
                "max_attempts) VALUES ('alpha', 'prepare', '{}'::jsonb, "
                "'pending', 3)",
                "DELETE FROM task_queue.jobs",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await member.execute(statement)
        finally:
            await member.close()
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
            for extra in (granter, group):
                await admin_connection.execute(f"DROP OWNED BY {extra}")
                await admin_connection.execute(f"DROP ROLE IF EXISTS {extra}")
        finally:
            await admin_connection.close()


async def test_a_superuser_can_run_retention_without_a_queue_grant(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    """0007: retention stopped being the one Admin operation a DBA cannot run.

    A superuser bypasses row-level security and every table grant in this
    schema, so refusing it here removed nothing it could not already do by
    hand -- it only broke the documented API for the identity most likely to
    be running a maintenance job.
    """
    if not await _is_superuser(pool):
        pytest.skip("needs a superuser connection to create one")

    role = f"rq_dba_{uuid.uuid4().hex[:8]}"
    password = "scoped-role-test-password"
    database = urlsplit(admin_dsn).path.lstrip("/")
    target = database_target(admin_dsn, database, user=role, password=password)
    await seed_terminal_job(
        pool, queue=queue_name, state="succeeded", age=timedelta(days=30)
    )

    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        await admin_connection.execute(
            f"CREATE ROLE {role} LOGIN SUPERUSER PASSWORD '{password}'"
        )
        assert (
            await admin_connection.fetchval(
                "SELECT count(*) FROM task_queue.role_queue_grants "
                "WHERE role_name = $1",
                role,
            )
            == 0
        )
        dba_pool = await asyncpg.create_pool(
            target.database_url, min_size=1, max_size=2
        )
        assert dba_pool is not None
        try:
            assert (
                await Admin(dba_pool).purge(
                    queue=queue_name, retention=timedelta(days=1)
                )
                == 1
            )
        finally:
            await dba_pool.close()
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
        finally:
            await admin_connection.close()


async def _is_superuser(pool: asyncpg.Pool) -> bool:
    async with pool.acquire() as connection:
        return bool(
            await connection.fetchval(
                "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
            )
        )
