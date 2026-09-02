"""Independent high-contention checks for the rqueue review contract."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import asyncpg
import pytest

from rqueue import Job, LeaseLost, Queue, Scheduler, ScheduleSpec
from rqueue.models import JobState
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


async def test_stale_holder_write_is_rejected_after_reclaim(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async with pool.acquire() as connection:
        job = await queue.enqueue(
            connection, task="prepare", payload={}, max_attempts=4
        )
        stale = (
            await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id="stale",
                tasks=["prepare"],
                limit=1,
                lease_seconds=0.15,
            )
        )[0]
        await asyncio.sleep(0.35)
        await queue.storage.recover_expired_leases(connection, queue=queue.name)
        fresh = (
            await queue.storage.claim(
                connection,
                queue=queue.name,
                worker_id="fresh",
                tasks=["prepare"],
                limit=1,
                lease_seconds=30,
            )
        )[0]
        assert fresh.lease_token != stale.lease_token
        with pytest.raises(LeaseLost):
            await queue.storage.complete(
                connection, job_id=job.id, lease_token=stale.lease_token
            )
        current = await queue.storage.get_job(connection, job.id)
        assert current is not None
        assert current.state == JobState.LEASED
        assert current.lease_token == fresh.lease_token
        await queue.storage.complete(
            connection, job_id=job.id, lease_token=fresh.lease_token
        )


async def test_concurrent_producers_return_one_dedupe_job(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    barrier = asyncio.Barrier(32)

    async def produce(index: int) -> Job:
        await barrier.wait()
        async with pool.acquire() as connection, connection.transaction():
            return await queue.enqueue(
                connection,
                task="prepare",
                payload={"producer": index},
                dedupe_key="stress-dedupe",
                on_conflict="return_existing",
            )

    jobs = await asyncio.gather(*(produce(i) for i in range(32)))
    assert len({job.id for job in jobs}) == 1
    assert len(await queue.list_jobs()) == 1


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


async def test_parallel_scheduler_ticks_fire_one_occurrence(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    name = f"stress-schedule-{uuid.uuid4().hex[:10]}"
    spec = ScheduleSpec(name=name, task="maintenance", cron="* * * * *")
    replicas = [
        Scheduler(queue, scheduler_id=f"sched-{i}", schedules=[spec]) for i in range(24)
    ]
    (stored,) = await replicas[0].sync()
    moment = stored.created_at + timedelta(minutes=3)
    results = await asyncio.gather(*(r.tick(now=moment) for r in replicas))
    assert sum(map(len, results)) == 1
    async with pool.acquire() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.schedule_occurrences "
                "WHERE schedule_id = $1",
                stored.id,
            )
            == 1
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_queue.jobs "
                "WHERE metadata->>'rqueue.schedule' = $1",
                name,
            )
            == 1
        )
