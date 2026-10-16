"""High-contention claims against one queue."""

from __future__ import annotations

import asyncio
import uuid

import asyncpg

from rqueue import Job, Queue
from rqueue.storage import ClaimedJob


async def test_many_direct_claimers_do_not_duplicate_or_starve(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    count = 240
    async with pool.acquire() as connection, connection.transaction():
        for index in range(count):
            await queue.enqueue(connection, task="prepare", payload={"i": index})

    claimed: list[uuid.UUID] = []
    claimed_lock = asyncio.Lock()

    async def claimer(worker: str) -> None:
        idle_rounds = 0
        while idle_rounds < 12:
            async with pool.acquire() as connection:
                batch = await queue.storage.claim(
                    connection,
                    queue=queue.name,
                    worker_id=worker,
                    tasks=["prepare"],
                    limit=4,
                    lease_seconds=30,
                )
                for entry in batch:
                    await queue.storage.complete(
                        connection,
                        job_id=entry.job.id,
                        lease_token=entry.lease_token,
                    )
            if batch:
                async with claimed_lock:
                    claimed.extend(entry.job.id for entry in batch)
                idle_rounds = 0
            else:
                idle_rounds += 1
                await asyncio.sleep(0.005)

    await asyncio.gather(*(claimer(f"stress-{i}") for i in range(20)))
    assert len(claimed) == count
    assert len(set(claimed)) == count
    stats = await queue.stats()
    assert stats.succeeded == count
    assert stats.pending == stats.leased == 0


async def test_burst_enqueue_and_claim_keeps_one_concurrency_key_active(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async def produce(index: int) -> Job:
        async with pool.acquire() as connection, connection.transaction():
            return await queue.enqueue(
                connection,
                task="prepare",
                payload={"producer": index},
                concurrency_key="stress-resource",
            )

    jobs = await asyncio.gather(*(produce(i) for i in range(40)))

    async def claim(worker: str) -> list[ClaimedJob]:
        async with pool.acquire() as connection:
            return await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id=worker,
                tasks=["prepare"],
                limit=10,
                lease_seconds=30,
            )

    batches = await asyncio.gather(*(claim(f"slot-{i}") for i in range(12)))
    active = {entry.job.id for batch in batches for entry in batch}
    assert len(active) == 1
    assert active <= {job.id for job in jobs}
