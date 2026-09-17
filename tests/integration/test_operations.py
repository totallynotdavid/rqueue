"""§10.10: migration, upgrade, retention cleanup, and least-privilege roles."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import asyncpg
import pytest

from rqueue import Admin, MigrationError, Queue, check_readiness, migrations
from rqueue.models import JobState
from rqueue.roles import Capability, provision_role, revoke_role
from rqueue.storage import Storage
from scripts.database import database_target

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
    connection, schema = scratch_schema
    applied = await migrations.migrate(connection, schema=schema)
    assert [m.name for m in applied] == [
        "core",
        "scheduling",
        "queue_pause",
        "retry_policy",
    ]

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
        "queue_pauses",
    }


async def test_migrating_is_idempotent(
    scratch_schema: tuple[asyncpg.Connection, str],
) -> None:
    connection, schema = scratch_schema
    await migrations.migrate(connection, schema=schema)
    assert await migrations.migrate(connection, schema=schema) == ()


async def test_upgrading_from_the_previous_release_matches_a_fresh_install(
    admin_dsn: str,
) -> None:
    """§7: migrations are tested from empty *and* from the previous release."""
    stepwise = f"mig_step_{uuid.uuid4().hex[:8]}"
    fresh = f"mig_fresh_{uuid.uuid4().hex[:8]}"
    connection = await asyncpg.connect(admin_dsn)
    try:
        previous = max(m.version for m in migrations.load_migrations()) - 1
        applied = await migrations.migrate(connection, schema=stepwise, target=previous)
        assert [m.version for m in applied] == list(range(1, previous + 1))
        assert await migrations.current_version(connection, schema=stepwise) == previous

        # The previous release is a working queue on its own.
        await connection.execute(
            f"INSERT INTO {stepwise}.jobs (queue, task, payload, state, max_attempts) "
            "VALUES ('q', 't', '{}'::jsonb, 'pending', 3)"
        )

        upgraded = await migrations.migrate(connection, schema=stepwise)
        assert [m.version for m in upgraded] == [previous + 1]

        await migrations.migrate(connection, schema=fresh)
        assert await structure(connection, stepwise) == await structure(
            connection, fresh
        )
        # The row written under the previous release survived the upgrade.
        assert await connection.fetchval(f"SELECT count(*) FROM {stepwise}.jobs") == 1
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {stepwise} CASCADE")
        await connection.execute(f"DROP SCHEMA IF EXISTS {fresh} CASCADE")
        await connection.close()


async def test_a_failing_migration_leaves_no_partial_schema(
    scratch_schema: tuple[asyncpg.Connection, str],
) -> None:
    """§7: transactional where PostgreSQL permits, which for DDL is everywhere."""
    connection, schema = scratch_schema
    await connection.execute(f"CREATE SCHEMA {schema}")
    await connection.execute(f"CREATE TABLE {schema}.jobs (id int)")

    with pytest.raises(asyncpg.PostgresError):
        await migrations.migrate(connection, schema=schema)

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
async def produce_only_pool(admin_dsn: str, queue: str) -> AsyncIterator[asyncpg.Pool]:
    """A pool logged in as a role holding nothing but ``Capability.PRODUCE``."""
    role = f"rq_producer_{uuid.uuid4().hex[:8]}"
    password = "produce-only-test-password"
    database = urlsplit(admin_dsn).path.lstrip("/")
    target = database_target(admin_dsn, database, user=role, password=password)

    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        await provision_role(
            admin_connection,
            role=role,
            capabilities=[Capability.PRODUCE],
            queues=[queue],
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


async def test_a_produce_only_role_can_enqueue(admin_dsn: str, queue_name: str) -> None:
    """§10.10: PRODUCE alone has to be enough to enqueue, both insert paths.

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
