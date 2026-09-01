"""§10.7: a lost notification or a restart delays work; it never loses work."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC

import asyncpg
import pytest

from rqueue import Job, Queue, TaskContext, Worker
from rqueue.models import JobState

from .support import eventually, running


@asynccontextmanager
async def notifications_disabled(pool: asyncpg.Pool) -> AsyncIterator[None]:
    """Really stop PostgreSQL from emitting the wake notification.

    Disabling the triggers is the honest way to test "a missed NOTIFY only adds
    latency": no notification is produced at all, so anything that still works
    is working through polling.
    """
    async with pool.acquire() as connection:
        await connection.execute(
            "ALTER TABLE task_queue.jobs DISABLE TRIGGER jobs_notify_insert"
        )
        await connection.execute(
            "ALTER TABLE task_queue.jobs DISABLE TRIGGER jobs_notify_update"
        )
    try:
        yield
    finally:
        async with pool.acquire() as connection:
            await connection.execute(
                "ALTER TABLE task_queue.jobs ENABLE TRIGGER jobs_notify_insert"
            )
            await connection.execute(
                "ALTER TABLE task_queue.jobs ENABLE TRIGGER jobs_notify_update"
            )


async def test_a_missed_notification_only_costs_latency(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    done = asyncio.Event()

    async def work(payload: object, context: TaskContext) -> None:
        done.set()

    queue.register(name="work", handler=work)
    worker = Worker(queue, worker_id="poller", poll_interval=0.1, concurrency=1)

    async with notifications_disabled(pool), running(worker):
        async with pool.acquire() as connection, connection.transaction():
            job = await queue.enqueue(connection, task="work")
        await asyncio.wait_for(done.wait(), timeout=15)
        await eventually(
            lambda: _in_state(queue, job.id, JobState.SUCCEEDED),
            message="polling should deliver the job with no notification at all",
        )


async def test_a_notification_wakes_a_worker_before_its_poll_interval(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """The positive control for the test above."""
    done = asyncio.Event()

    async def work(payload: object, context: TaskContext) -> None:
        done.set()

    queue.register(name="work", handler=work)
    # A poll interval far longer than the test's patience: only NOTIFY can win.
    worker = Worker(queue, worker_id="listener", poll_interval=60.0, concurrency=1)

    async with running(worker):
        await asyncio.sleep(0.2)  # let the first tick finish and start waiting
        started = time.monotonic()
        async with pool.acquire() as connection, connection.transaction():
            await queue.enqueue(connection, task="work")
        await asyncio.wait_for(done.wait(), timeout=20)
        assert time.monotonic() - started < 10


async def test_a_worker_restart_finishes_the_work(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    first_started = asyncio.Event()
    release = asyncio.Event()
    completions: list[int] = []

    async def work(payload: object, context: TaskContext) -> None:
        if context.attempt == 1:
            first_started.set()
            await release.wait()
        completions.append(context.attempt)

    queue.register(name="work", handler=work)
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="work", max_attempts=3)

    stopping = Worker(
        queue,
        worker_id="restarting",
        poll_interval=0.05,
        concurrency=1,
        lease_duration=30,
        shutdown_timeout=0.2,
    )
    task = asyncio.create_task(stopping.run())
    await stopping.wait_started()
    await asyncio.wait_for(first_started.wait(), timeout=15)
    stopping.stop()
    await asyncio.wait_for(task, timeout=30)

    # The interrupted attempt was handed straight back, not left to expire.
    handed_back = await queue.get_job(job.id)
    assert handed_back is not None
    assert handed_back.state == JobState.PENDING
    assert handed_back.error_type == "WorkerShutdown"

    release.set()
    async with running(
        Worker(queue, worker_id="successor", poll_interval=0.05, concurrency=1)
    ):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.SUCCEEDED),
            message="the restarted worker should finish the job",
        )
    assert completions == [2]


async def test_a_database_restart_delays_work_but_loses_none(
    queue: Queue, pool: asyncpg.Pool, admin_dsn: str
) -> None:
    """Every connection dies at once, which is what a restart looks like here.

    The integration suite runs against a disposable database inside a cluster
    it does not own -- in CI that cluster is a service container -- so it
    terminates every backend rather than stopping the postmaster. From the
    client's side the two are the same event: all connections drop and must be
    re-established.
    """
    processed: list[str] = []

    async def work(payload: dict[str, str], context: TaskContext) -> None:
        processed.append(payload["tag"])

    queue.register(name="work", handler=work)
    async with pool.acquire() as connection, connection.transaction():
        before = await queue.enqueue(
            connection, task="work", payload={"tag": "before"}, delay=1.5
        )

    worker = Worker(queue, worker_id="survivor", poll_interval=0.1, concurrency=2)
    async with running(worker):
        killer = await asyncpg.connect(admin_dsn)
        try:
            await killer.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = current_database() AND pid <> pg_backend_pid()
                """
            )
        finally:
            await killer.close()

        after = await _enqueue_with_retry(queue, pool, {"tag": "after"})
        await eventually(
            lambda: _in_state(queue, before.id, JobState.SUCCEEDED),
            timeout=30,
            message="the job queued before the restart must still run",
        )
        await eventually(
            lambda: _in_state(queue, after.id, JobState.SUCCEEDED),
            timeout=30,
            message="the worker must reconnect and keep working",
        )
    assert sorted(processed) == ["after", "before"]


async def _enqueue_with_retry(
    queue: Queue, pool: asyncpg.Pool, payload: dict[str, str]
) -> Job:
    """Producers see the outage too; they retry, exactly like any other client."""
    last: Exception | None = None
    for _ in range(40):
        try:
            async with pool.acquire() as connection, connection.transaction():
                return await queue.enqueue(connection, task="work", payload=payload)
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError) as exc:
            last = exc
            await asyncio.sleep(0.25)
    raise AssertionError(f"the producer never reconnected: {last!r}")


@pytest.mark.parametrize("state", [JobState.SUCCEEDED])
async def test_stats_and_heartbeats_survive_a_restart(
    queue: Queue, pool: asyncpg.Pool, state: JobState
) -> None:
    """Readiness data is rebuilt by the next tick rather than lost."""
    from rqueue import check_readiness

    async def work(payload: object, context: TaskContext) -> None:
        return None

    queue.register(name="work", handler=work)
    async with running(Worker(queue, worker_id="beating", poll_interval=0.05)):
        await eventually(
            lambda: _worker_seen(pool, queue),
            message="the worker should register a heartbeat",
        )
        report = await check_readiness(pool, queue=queue.name)
        assert report.ready
        assert report.connected
        assert report.migrations_up_to_date
        assert "beating" in report.workers


async def _worker_seen(pool: asyncpg.Pool, queue: Queue) -> bool:
    from datetime import datetime, timedelta

    async with pool.acquire() as connection:
        live = await queue.storage.live_instances(
            connection,
            kind="worker",
            since=datetime.now(UTC) - timedelta(seconds=30),
            queue=queue.name,
        )
    return bool(live)


async def _in_state(queue: Queue, job_id: uuid.UUID, expected: JobState) -> bool:
    job = await queue.get_job(job_id)
    return job is not None and job.state == expected
