"""Durable, queue-wide pause: SQL-enforced, fleet-wide, and lease-safe."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import asyncpg
import pytest

from rqueue import Admin, Queue, TaskContext, Worker
from rqueue.errors import ValidationError

from .support import eventually, running


@pytest.fixture
def other_queue(pool: asyncpg.Pool) -> Queue:
    """A second queue, so "paused" can be told apart from "broken"."""
    return Queue(pool, name=f"q{uuid.uuid4().hex[:16]}")


async def enqueue(queue: Queue, pool: asyncpg.Pool, count: int = 1) -> None:
    async with pool.acquire() as connection, connection.transaction():
        for index in range(count):
            await queue.enqueue(connection, task="work", payload={"i": index})


async def claim(queue: Queue, pool: asyncpg.Pool, worker_id: str) -> list[str]:
    """Claim through the real claim path, returning the job ids leased."""
    async with pool.acquire() as connection:
        claimed = await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id=worker_id,
            tasks=["work"],
            limit=10,
            lease_seconds=30.0,
        )
    return [str(entry.job.id) for entry in claimed]


@pytest.fixture(autouse=True)
async def clear_pauses(pool: asyncpg.Pool) -> AsyncIterator[None]:
    """The pause table is global, so a leaked wildcard row would infect the suite."""
    yield
    async with pool.acquire() as connection:
        await connection.execute("DELETE FROM task_queue.queue_pauses")


async def test_pausing_one_queue_stops_its_claims_and_no_others(
    queue: Queue, other_queue: Queue, pool: asyncpg.Pool, admin: Admin, worker_id: str
) -> None:
    await enqueue(queue, pool, 3)
    await enqueue(other_queue, pool, 3)

    pause = await admin.pause_queue(queue.name)
    assert pause.is_paused and pause.paused_at is not None
    assert await admin.is_queue_paused(queue.name) is True
    assert await admin.is_queue_paused(other_queue.name) is False

    assert await claim(queue, pool, worker_id) == []
    assert len(await claim(other_queue, pool, worker_id)) == 3

    # Nothing was consumed, cancelled, or otherwise disturbed: the jobs are
    # simply not claimable while the pause row stands.
    stats = await queue.stats()
    assert stats.pending == 3 and stats.leased == 0

    resumed = await admin.resume_queue(queue.name)
    assert resumed == [queue.name]
    # Immediately, not eventually: the gate is the claim query itself, so there
    # is no worker-side state that has to catch up first.
    assert len(await claim(queue, pool, worker_id)) == 3


async def test_pausing_is_idempotent_and_keeps_the_first_instant(
    queue: Queue, admin: Admin
) -> None:
    first = await admin.pause_queue(queue.name)
    again = await admin.pause_queue(queue.name)
    assert again.paused_at == first.paused_at
    assert again.updated_at == first.updated_at

    assert [entry.queue for entry in await admin.paused_queues()] == [queue.name]
    assert await admin.resume_queue(queue.name) == [queue.name]
    # Resuming something already running changes nothing and says so.
    assert await admin.resume_queue(queue.name) == []
    assert await admin.paused_queues() == []


async def test_the_wildcard_pauses_and_resumes_every_queue(
    queue: Queue, other_queue: Queue, pool: asyncpg.Pool, admin: Admin, worker_id: str
) -> None:
    await enqueue(queue, pool, 2)
    await enqueue(other_queue, pool, 2)

    await admin.pause_queue("*")
    assert await admin.is_queue_paused(queue.name) is True
    assert await admin.is_queue_paused(other_queue.name) is True
    assert await claim(queue, pool, worker_id) == []
    assert await claim(other_queue, pool, worker_id) == []

    # A queue paused by name as well stays paused when only its own row is
    # cleared: the narrower call cannot punch a hole in the wildcard.
    await admin.pause_queue(queue.name)
    await admin.resume_queue(queue.name)
    assert await claim(queue, pool, worker_id) == []

    resumed = await admin.resume_queue("*")
    assert sorted(resumed) == sorted(["*"])
    assert len(await claim(queue, pool, worker_id)) == 2
    assert len(await claim(other_queue, pool, worker_id)) == 2


async def test_resuming_the_wildcard_also_clears_queues_paused_by_name(
    queue: Queue, other_queue: Queue, pool: asyncpg.Pool, admin: Admin, worker_id: str
) -> None:
    await enqueue(queue, pool, 1)
    await admin.pause_queue(queue.name)
    await admin.pause_queue("*")

    assert sorted(await admin.resume_queue("*")) == sorted(["*", queue.name])
    assert await admin.paused_queues() == []
    assert len(await claim(queue, pool, worker_id)) == 1


async def test_a_lease_taken_before_the_pause_runs_to_completion(
    queue: Queue, pool: asyncpg.Pool, admin: Admin
) -> None:
    """Pause stops new claims; it does not touch work already in flight."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def work(payload: dict[str, int], context: TaskContext) -> None:
        started.set()
        await release.wait()
        # The lease is still this worker's, mid-pause.
        await context.heartbeat()

    queue.register(name="work", handler=work)
    await enqueue(queue, pool, 1)

    worker = Worker(
        queue,
        worker_id="pause-inflight",
        concurrency=1,
        poll_interval=0.05,
        lease_duration=30.0,
    )
    async with running(worker):
        await asyncio.wait_for(started.wait(), timeout=10)
        await admin.pause_queue(queue.name)
        # A second job enqueued during the pause must not be picked up.
        await enqueue(queue, pool, 1)
        release.set()

        async def completed() -> bool:
            return (await queue.stats()).succeeded == 1

        await eventually(
            completed, message="the in-flight job never finished under a pause"
        )
        # The one enqueued during the pause is still waiting, unleased.
        stats = await queue.stats()
        assert stats.pending == 1 and stats.leased == 0


async def test_an_idle_worker_is_woken_by_a_resume_rather_than_its_poll(
    queue: Queue, pool: asyncpg.Pool, admin: Admin
) -> None:
    """NOTIFY is the latency optimization the durable row does not need."""
    done = asyncio.Event()

    async def work(payload: dict[str, int], context: TaskContext) -> None:
        done.set()

    queue.register(name="work", handler=work)
    await admin.pause_queue(queue.name)

    # Far longer than this test waits: only the resume's NOTIFY can wake it.
    worker = Worker(
        queue,
        worker_id="pause-wakeup",
        concurrency=1,
        poll_interval=30.0,
        lease_duration=30.0,
    )
    async with running(worker):
        await enqueue(queue, pool, 1)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(done.wait(), timeout=1.0)

        await admin.resume_queue(queue.name)
        await asyncio.wait_for(done.wait(), timeout=10.0)


async def test_the_wildcard_resume_wakes_a_worker_that_has_no_row_of_its_own(
    queue: Queue, pool: asyncpg.Pool, admin: Admin
) -> None:
    done = asyncio.Event()

    async def work(payload: dict[str, int], context: TaskContext) -> None:
        done.set()

    queue.register(name="work", handler=work)
    await admin.pause_queue("*")

    worker = Worker(
        queue,
        worker_id="pause-wakeup-all",
        concurrency=1,
        poll_interval=30.0,
        lease_duration=30.0,
    )
    async with running(worker):
        await enqueue(queue, pool, 1)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(done.wait(), timeout=1.0)

        await admin.resume_queue("*")
        await asyncio.wait_for(done.wait(), timeout=10.0)


async def test_a_pause_survives_the_workers_that_never_saw_it(
    queue: Queue, pool: asyncpg.Pool, admin: Admin
) -> None:
    """A pause is a row, so it outlives every process."""
    ran: list[uuid.UUID] = []

    async def work(payload: dict[str, int], context: TaskContext) -> None:
        ran.append(context.job_id)

    queue.register(name="work", handler=work)
    await admin.pause_queue(queue.name)
    await enqueue(queue, pool, 2)

    # A worker started from scratch, long after the pause, with no chance of
    # having received its NOTIFY, still claims nothing.
    fresh = Worker(queue, worker_id="pause-cold", concurrency=2, poll_interval=0.05)
    await fresh.drain(timeout=10)
    assert ran == []
    assert (await queue.stats()).pending == 2

    await admin.resume_queue(queue.name)
    await fresh.drain(timeout=10)
    assert len(ran) == 2


async def test_pause_targets_are_validated(admin: Admin) -> None:
    for bad in ("no spaces", "*x", "x" * 65):
        with pytest.raises(ValidationError):
            await admin.pause_queue(bad)
        with pytest.raises(ValidationError):
            await admin.resume_queue(bad)


async def test_a_scoped_role_cannot_pause_or_resume_a_queue(
    app_dsn: str | None, admin: Admin
) -> None:
    """SELECT stayed SELECT: obeying a pause is not the same as setting one.

    A blanket table grant on queue_pauses would leave every test above passing
    while handing any worker or producer replica the fleet-wide off switch, so
    this is the half that proves the grant is still least privilege -- asserted
    against the real scoped role rather than against the grant table rqueue
    builds it from.
    """
    if app_dsn is None:
        pytest.skip("needs the scoped role from scripts/integration.sh")

    scoped_pool = await asyncpg.create_pool(app_dsn, min_size=1, max_size=2)
    assert scoped_pool is not None
    try:
        scoped_admin = Admin(scoped_pool)

        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await scoped_admin.pause_queue("scoped")
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await scoped_admin.pause_queue("*")

        # A pause an operator did take is not the scoped role's to lift, and
        # the denial is a privilege error rather than a silent no-op: the row
        # is really there to be updated.
        await admin.pause_queue("scoped")
        try:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await scoped_admin.resume_queue("scoped")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await scoped_admin.resume_queue("*")
            assert await admin.is_queue_paused("scoped") is True

            # ... while reading the gate, which every claim it makes depends
            # on, still works.
            assert await scoped_admin.is_queue_paused("scoped") is True
            assert [entry.queue for entry in await scoped_admin.paused_queues()] == [
                "scoped"
            ]
        finally:
            await admin.resume_queue("scoped")
    finally:
        await scoped_pool.close()


async def test_a_scoped_worker_role_can_read_the_pause_it_must_obey(
    app_dsn: str | None, admin: Admin, pool: asyncpg.Pool
) -> None:
    """A least-privilege role still has to see the gate in the claim query."""
    if app_dsn is None:
        pytest.skip("needs the scoped role from scripts/integration.sh")

    scoped = await asyncpg.create_pool(app_dsn, min_size=1, max_size=2)
    assert scoped is not None
    try:
        queue = Queue(scoped, name="scoped")
        async with scoped.acquire() as connection, connection.transaction():
            job = await queue.enqueue(connection, task="work", payload={})

        await admin.pause_queue("scoped")
        assert await claim(queue, scoped, "scoped-worker") == []

        await admin.resume_queue("scoped")
        assert await claim(queue, scoped, "scoped-worker") == [str(job.id)]

        # Leave the shared "scoped" queue as it was found. Deleting takes the
        # owner connection; the scoped role deliberately cannot.
        async with pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM task_queue.jobs WHERE id = $1", job.id
            )
    finally:
        await scoped.close()
