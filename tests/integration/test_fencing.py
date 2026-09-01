"""§10.5: lease-token fencing.

This is the package's strongest guarantee over River and pgqueuer (§1, §3), so
these tests do the real thing: a lease is held past its expiry, another worker
takes the job over, and the stale holder then attempts every write it could
make. None of them may land.
"""

from __future__ import annotations

import asyncio
import uuid

import asyncpg
import pytest

from rqueue import LeaseLost, Queue, TaskContext, Worker
from rqueue.models import AttemptOutcome, JobState

from .support import eventually, running

LEASE = 0.4


async def test_a_stale_token_cannot_complete_the_newer_attempt(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            job = await queue.enqueue(
                connection, task="prepare", payload={"n": 1}, max_attempts=5
            )

        stale = (
            await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id="stalled-worker",
                tasks=["prepare"],
                limit=1,
                lease_seconds=LEASE,
            )
        )[0]
        assert stale.job.attempt == 1

        # Hold the lease past its expiry without heartbeating -- exactly what a
        # worker stuck in a GC pause or a stalled write looks like.
        await asyncio.sleep(LEASE + 0.3)
        recovered = await queue.storage.recover_expired_leases(
            connection, queue=queue.name
        )
        assert recovered == [job.id]

        fresh = (
            await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id="successor",
                tasks=["prepare"],
                limit=1,
                lease_seconds=60,
            )
        )[0]
        assert fresh.job.id == job.id
        assert fresh.job.attempt == 2
        assert fresh.lease_token != stale.lease_token

        # The stalled worker wakes up and tries to finish. Every write it can
        # make is fenced off by the token check.
        with pytest.raises(LeaseLost):
            await queue.storage.complete(
                connection, job_id=job.id, lease_token=stale.lease_token
            )
        with pytest.raises(LeaseLost):
            await queue.storage.fail_terminal(
                connection,
                job_id=job.id,
                lease_token=stale.lease_token,
                error_type="Boom",
                error_message="stale worker failing the job",
            )
        with pytest.raises(LeaseLost):
            await queue.storage.cancel_leased(
                connection,
                job_id=job.id,
                lease_token=stale.lease_token,
                reason="stale worker cancelling the job",
            )
        with pytest.raises(LeaseLost):
            await queue.storage.heartbeat(
                connection,
                job_id=job.id,
                lease_token=stale.lease_token,
                lease_seconds=60,
            )

        # The live attempt is untouched and can still finish.
        during = await queue.storage.get_job(connection, job.id)
        assert during is not None
        assert during.state == JobState.LEASED
        assert during.worker_id == "successor"
        assert during.lease_token == fresh.lease_token

        finished = await queue.storage.complete(
            connection, job_id=job.id, lease_token=fresh.lease_token
        )
        assert finished.state == JobState.SUCCEEDED

        attempts = await queue.storage.attempts(connection, job.id)
    assert [attempt.outcome for attempt in attempts] == [
        AttemptOutcome.LEASE_EXPIRED,
        AttemptOutcome.SUCCEEDED,
    ]
    assert attempts[0].lease_token == stale.lease_token
    assert attempts[1].lease_token == fresh.lease_token


async def test_a_stale_token_cannot_reopen_a_finished_job(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """The fence also holds after the successor has finished the work."""
    async with pool.acquire() as connection:
        async with connection.transaction():
            job = await queue.enqueue(connection, task="prepare", max_attempts=5)
        stale = (
            await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id="stalled",
                tasks=["prepare"],
                limit=1,
                lease_seconds=LEASE,
            )
        )[0]
        await asyncio.sleep(LEASE + 0.3)
        await queue.storage.recover_expired_leases(connection, queue=queue.name)
        fresh = (
            await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id="successor",
                tasks=["prepare"],
                limit=1,
                lease_seconds=60,
            )
        )[0]
        await queue.storage.complete(
            connection, job_id=job.id, lease_token=fresh.lease_token
        )

        with pytest.raises(LeaseLost):
            await queue.storage.reschedule(
                connection,
                job_id=job.id,
                lease_token=stale.lease_token,
                retry_at=fresh.leased_until,
                error_type="Boom",
                error_message="stale worker retrying a finished job",
            )
        final = await queue.storage.get_job(connection, job.id)
    assert final is not None
    assert final.state == JobState.SUCCEEDED
    assert final.error_type is None


async def test_lease_expiry_gives_a_bounded_number_of_retries(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """A worker that keeps crashing exhausts the budget and fails durably."""
    async with pool.acquire() as connection:
        async with connection.transaction():
            job = await queue.enqueue(connection, task="prepare", max_attempts=3)

        for expected_attempt in (1, 2, 3):
            claimed = await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id=f"crasher-{expected_attempt}",
                tasks=["prepare"],
                limit=1,
                lease_seconds=LEASE,
            )
            assert claimed[0].job.attempt == expected_attempt
            await asyncio.sleep(LEASE + 0.2)
            await queue.storage.recover_expired_leases(connection, queue=queue.name)

        final = await queue.storage.get_job(connection, job.id)
        assert final is not None
        assert final.state == JobState.FAILED
        assert final.error_type == "LeaseExpired"
        assert final.attempt == 3

        # Terminal means terminal: nothing claims it again.
        assert not await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id="hopeful",
            tasks=["prepare"],
            limit=1,
            lease_seconds=60,
        )
        attempts = await queue.storage.attempts(connection, job.id)
    assert len(attempts) == 3
    assert all(a.outcome == AttemptOutcome.LEASE_EXPIRED for a in attempts)


async def test_a_crashed_worker_is_recovered_by_another_worker(
    queue: Queue, admin_dsn: str, pool: asyncpg.Pool
) -> None:
    """A worker process dies mid-handler; the job is retried, not lost.

    The crash is real rather than a graceful stop: the doomed worker's pool is
    terminated under it, so its shutdown hand-back cannot reach PostgreSQL and
    the lease is simply left dangling -- what a killed process leaves behind.
    """
    started = asyncio.Event()
    finished = asyncio.Event()

    async def slow(payload: object, context: TaskContext) -> None:
        started.set()
        await asyncio.sleep(30)

    async def quick(payload: object, context: TaskContext) -> None:
        finished.set()

    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="work", max_attempts=3)

    doomed_pool = await asyncpg.create_pool(admin_dsn, min_size=1, max_size=4)
    assert doomed_pool is not None
    doomed_queue = Queue(doomed_pool, name=queue.name)
    doomed_queue.register(name="work", handler=slow)
    doomed = Worker(
        doomed_queue,
        worker_id="doomed",
        concurrency=1,
        poll_interval=0.05,
        lease_duration=LEASE,
    )
    doomed_task = asyncio.create_task(doomed.run())
    await asyncio.wait_for(started.wait(), timeout=15)

    doomed_pool.terminate()
    doomed_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await doomed_task

    leased = await queue.get_job(job.id)
    assert leased is not None and leased.state == JobState.LEASED

    queue.register(name="work", handler=quick)
    survivor = Worker(
        queue,
        worker_id="survivor",
        concurrency=1,
        poll_interval=0.05,
        lease_duration=30,
    )
    async with running(survivor):
        await asyncio.wait_for(finished.wait(), timeout=20)
        await eventually(
            lambda: _succeeded(queue, job.id),
            message="the recovered job should complete on its second attempt",
        )

    final = await queue.get_job(job.id)
    assert final is not None
    assert final.attempt == 2
    async with pool.acquire() as connection:
        attempts = await queue.storage.attempts(connection, job.id)
    assert [attempt.outcome for attempt in attempts] == [
        AttemptOutcome.LEASE_EXPIRED,
        AttemptOutcome.SUCCEEDED,
    ]


async def _succeeded(queue: Queue, job_id: uuid.UUID) -> bool:
    job = await queue.get_job(job_id)
    return job is not None and job.state == JobState.SUCCEEDED
