"""Concurrent producers cannot create duplicate active jobs for a key."""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from rqueue import AlreadyEnqueued, Job, Queue
from rqueue.models import JobState


async def test_second_producer_waits_and_receives_the_existing_job(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """The losing producer gets the winner's job, not silence.

    ``ON CONFLICT DO NOTHING`` would return nothing here, because it does not
    wait for the in-flight inserter, so the loser would get silence. The
    ``DO UPDATE`` arbiter blocks until the first producer commits and then
    returns the row that won.
    """
    first = await pool.acquire()
    second = await pool.acquire()
    try:
        transaction = first.transaction()
        await transaction.start()
        winner = await queue.enqueue(
            first, task="prepare", dedupe_key="sim:1", on_conflict="return_existing"
        )

        async def losing_producer() -> Job:
            async with second.transaction():
                return await queue.enqueue(
                    second,
                    task="prepare",
                    dedupe_key="sim:1",
                    on_conflict="return_existing",
                )

        pending = asyncio.create_task(losing_producer())
        await asyncio.sleep(0.3)
        assert not pending.done(), "the second producer should block on the arbiter"

        await transaction.commit()
        loser = await asyncio.wait_for(pending, timeout=10)
        assert loser.id == winner.id
    finally:
        await pool.release(first)
        await pool.release(second)

    assert len(await queue.list_jobs()) == 1


async def test_raise_mode_reports_the_conflict(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            existing = await queue.enqueue(
                connection, task="prepare", dedupe_key="sim:2", on_conflict="raise"
            )
        with pytest.raises(AlreadyEnqueued) as caught:
            await queue.enqueue(
                connection, task="prepare", dedupe_key="sim:2", on_conflict="raise"
            )
    assert caught.value.existing_job_id == existing.id
    assert caught.value.dedupe_key == "sim:2"
    assert len(await queue.list_jobs()) == 1


async def test_many_racing_producers_yield_exactly_one_active_job(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async def produce(index: int) -> Job:
        async with pool.acquire() as connection, connection.transaction():
            return await queue.enqueue(
                connection,
                task="prepare",
                payload={"index": index},
                dedupe_key="sim:race",
                on_conflict="return_existing",
            )

    jobs = await asyncio.gather(*(produce(index) for index in range(8)))
    assert len({job.id for job in jobs}) == 1
    assert len(await queue.list_jobs()) == 1


async def test_the_key_is_released_when_the_job_becomes_terminal(
    queue: Queue, pool: asyncpg.Pool, worker_id: str
) -> None:
    """The documented lifecycle: a dedupe key is held only while a job is active."""
    async with pool.acquire() as connection:
        async with connection.transaction():
            first = await queue.enqueue(
                connection, task="prepare", dedupe_key="sim:3", on_conflict="raise"
            )
        claimed = await queue.storage.claim(
            connection,
            queue=queue.name,
            worker_id=worker_id,
            tasks=["prepare"],
            limit=1,
            lease_seconds=30,
        )
        # Still held while leased.
        with pytest.raises(AlreadyEnqueued):
            await queue.enqueue(
                connection, task="prepare", dedupe_key="sim:3", on_conflict="raise"
            )
        await queue.storage.complete(
            connection, job_id=claimed[0].job.id, lease_token=claimed[0].lease_token
        )
        async with connection.transaction():
            second = await queue.enqueue(
                connection, task="prepare", dedupe_key="sim:3", on_conflict="raise"
            )
    assert second.id != first.id
    states = {job.id: job.state for job in await queue.list_jobs()}
    assert states[first.id] == JobState.SUCCEEDED
    assert states[second.id] == JobState.PENDING


async def test_the_key_is_scoped_to_one_queue(
    pool: asyncpg.Pool, queue: Queue, queue_name: str
) -> None:
    other = Queue(pool, name=f"{queue_name[:12]}other")
    async with pool.acquire() as connection, connection.transaction():
        here = await queue.enqueue(
            connection, task="prepare", dedupe_key="shared", on_conflict="raise"
        )
        there = await other.enqueue(
            connection, task="prepare", dedupe_key="shared", on_conflict="raise"
        )
    assert here.id != there.id
