"""§10.4: many workers, at most one execution per successful lease attempt."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from rqueue import Queue, TaskContext, UnknownTask, Worker
from rqueue.models import JobState

from .support import eventually, running


async def test_multiple_workers_run_each_job_once(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    executions: list[str] = []
    lock = asyncio.Lock()

    async def record(payload: dict[str, str], context: TaskContext) -> None:
        async with lock:
            executions.append(payload["id"])
        await asyncio.sleep(0.01)

    queue.register(name="record", handler=record)
    ids = [str(uuid.uuid4()) for _ in range(40)]
    async with pool.acquire() as connection, connection.transaction():
        for job_id in ids:
            await queue.enqueue(connection, task="record", payload={"id": job_id})

    workers = [
        Worker(
            queue,
            worker_id=f"claimer-{index}",
            concurrency=4,
            poll_interval=0.05,
            lease_duration=30,
        )
        for index in range(4)
    ]
    tasks = [asyncio.create_task(worker.drain(timeout=60)) for worker in workers]
    await asyncio.gather(*tasks)

    assert sorted(executions) == sorted(ids)
    stats = await queue.stats()
    assert stats.succeeded == len(ids)
    assert stats.pending == 0 and stats.leased == 0


async def test_concurrent_claims_never_share_a_job(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async with pool.acquire() as connection, connection.transaction():
        for index in range(10):
            await queue.enqueue(connection, task="prepare", payload={"i": index})

    async def claim(worker: str) -> list[uuid.UUID]:
        async with pool.acquire() as connection:
            claimed = await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id=worker,
                tasks=["prepare"],
                limit=10,
                lease_seconds=30,
            )
        return [entry.job.id for entry in claimed]

    batches = await asyncio.gather(*(claim(f"c{index}") for index in range(5)))
    everything = [job_id for batch in batches for job_id in batch]
    assert len(everything) == len(set(everything)) == 10


async def test_a_named_concurrency_key_serializes_execution(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """§6: a concurrency key bounds concurrent *execution* of a shared resource."""
    async with pool.acquire() as connection, connection.transaction():
        for index in range(3):
            await queue.enqueue(
                connection,
                task="prepare",
                payload={"i": index},
                concurrency_key="compute-job:42",
            )
        await queue.enqueue(connection, task="prepare", payload={"free": True})

    async with pool.acquire() as connection:
        claimed = await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id="w1",
            tasks=["prepare"],
            limit=10,
            lease_seconds=30,
        )
        # One of the three keyed jobs, plus the unkeyed one.
        assert len(claimed) == 2
        keyed = [entry for entry in claimed if entry.job.concurrency_key]
        assert len(keyed) == 1

        # A second worker cannot take another job for the same key.
        assert not await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id="w2",
            tasks=["prepare"],
            limit=10,
            lease_seconds=30,
        )

        await queue.storage.complete(
            connection, job_id=keyed[0].job.id, lease_token=keyed[0].lease_token
        )
        after = await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id="w2",
            tasks=["prepare"],
            limit=10,
            lease_seconds=30,
        )
    assert len(after) == 1
    assert after[0].job.concurrency_key == "compute-job:42"


async def test_a_concurrency_slot_is_a_lease_not_a_flag(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """A crashed holder must not hold the resource forever (§6)."""
    async with pool.acquire() as connection, connection.transaction():
        for index in range(2):
            await queue.enqueue(
                connection,
                task="prepare",
                payload={"i": index},
                concurrency_key="resource:crash",
            )

    async with pool.acquire() as connection:
        first = await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id="doomed",
            tasks=["prepare"],
            limit=5,
            lease_seconds=0.3,
        )
        assert len(first) == 1
        # The holder dies: no heartbeat, no completion.
        await asyncio.sleep(0.6)
        recovered = await queue.storage.recover_expired_leases(
            connection, queue=queue.name
        )
        assert recovered == [first[0].job.id]

        second = await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id="survivor",
            tasks=["prepare"],
            limit=5,
            lease_seconds=30,
        )
    assert len(second) == 1


async def test_delayed_jobs_are_not_claimed_before_they_are_due(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            later = await queue.enqueue(
                connection, task="prepare", delay=3600, payload={}
            )
            now = await queue.enqueue(connection, task="prepare", payload={})
        claimed = await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id="w",
            tasks=["prepare"],
            limit=10,
            lease_seconds=30,
        )
    assert [entry.job.id for entry in claimed] == [now.id]
    stored = await queue.get_job(later.id)
    assert stored is not None and stored.state == JobState.PENDING


async def test_priority_then_schedule_then_insertion_order(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    past = datetime.now(UTC) - timedelta(minutes=5)
    async with pool.acquire() as connection:
        async with connection.transaction():
            low = await queue.enqueue(connection, task="prepare", priority=0)
            high = await queue.enqueue(connection, task="prepare", priority=9)
            old = await queue.enqueue(
                connection, task="prepare", priority=0, scheduled_at=past
            )
        claimed = await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id="w",
            tasks=["prepare"],
            limit=3,
            lease_seconds=30,
        )
    assert [entry.job.id for entry in claimed] == [high.id, old.id, low.id]


async def test_jobs_enqueued_in_one_transaction_keep_their_order(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """FIFO within a priority band, even inside a single transaction.

    Every row inserted in one transaction shares a ``created_at``, since that
    default is ``now()``; the claim order is broken by the insertion sequence
    instead, so "first enqueued, first claimed" survives batching.
    """
    async with pool.acquire() as connection:
        async with connection.transaction():
            expected = [
                (await queue.enqueue(connection, task="prepare", payload={"i": i})).id
                for i in range(8)
            ]
        claimed = await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id="w",
            tasks=["prepare"],
            limit=8,
            lease_seconds=30,
        )
    assert [entry.job.id for entry in claimed] == expected


async def test_a_worker_only_claims_task_names_it_registered(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async def known(payload: object, context: TaskContext) -> None:
        return None

    queue.register(name="known", handler=known)
    async with pool.acquire() as connection, connection.transaction():
        await queue.enqueue(connection, task="known")
        stranger = await queue.enqueue(connection, task="stranger")

    worker = Worker(
        queue,
        worker_id="picky",
        concurrency=2,
        poll_interval=0.05,
        strict_tasks=False,
    )
    async with running(worker):
        await eventually(
            lambda: _state(queue, stranger.id, JobState.PENDING),
            message="the unknown task should stay pending",
        )
        await eventually(
            lambda: _count(queue, JobState.SUCCEEDED),
            message="the known task should still run",
        )


async def test_strict_startup_refuses_a_queue_it_cannot_serve(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """§4: worker startup fails when queued work has no registered handler."""

    async def known(payload: object, context: TaskContext) -> None:
        return None

    queue.register(name="known", handler=known)
    async with pool.acquire() as connection, connection.transaction():
        await queue.enqueue(connection, task="stranger")

    strict = Worker(queue, worker_id="strict", poll_interval=0.05)
    with pytest.raises(UnknownTask, match="stranger"):
        await strict.run()

    # ... unless this deployment is configured to leave it for another one.
    lenient = Worker(queue, worker_id="lenient", poll_interval=0.05, strict_tasks=False)
    await lenient.drain(timeout=15)


async def _state(queue: Queue, job_id: uuid.UUID, expected: JobState) -> bool:
    job = await queue.get_job(job_id)
    return job is not None and job.state == expected


async def _count(queue: Queue, state: JobState) -> int:
    return len(await queue.list_jobs(states=[state]))
