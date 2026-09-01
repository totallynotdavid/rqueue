"""The master pipeline test from REQUIREMENTS.md §11.

One test that walks the whole lifecycle in a single run and asserts on durable
state after every step. It is the outer-loop signal during development -- run
it after any change to claiming, leases, or the scheduler and see the whole
pipeline pass or fail in one shot::

    mise run test-pipeline

It supplements the focused §10 tests rather than replacing them: those stay
granular so a failure points at one mechanism, while this one catches the
interactions between them.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import asyncpg
import pytest

from rqueue import (
    Admin,
    AlreadyEnqueued,
    LeaseLost,
    Queue,
    Retry,
    RetryPolicy,
    Scheduler,
    ScheduleSpec,
    TaskContext,
    Worker,
)
from rqueue.models import AttemptOutcome, JobState

from .support import eventually, running

LEASE = 0.4


async def test_master_pipeline(
    queue: Queue, pool: asyncpg.Pool, admin: Admin, widgets: str
) -> None:
    storage = queue.storage
    heartbeats: list[uuid.UUID] = []
    retried_once: set[uuid.UUID] = set()

    async def prepare(payload: dict[str, str], context: TaskContext) -> None:
        await context.heartbeat()
        heartbeats.append(context.job_id)

    async def flaky(payload: dict[str, str], context: TaskContext) -> None:
        if context.job_id not in retried_once:
            retried_once.add(context.job_id)
            raise Retry(delay=0.05, reason="forced retry")

    async def maintenance(payload: dict[str, str], context: TaskContext) -> None:
        return None

    queue.register(name="prepare", handler=prepare)
    queue.register(
        name="flaky",
        handler=flaky,
        retry=RetryPolicy(max_attempts=3, initial_backoff=0.05, jitter=0.0),
    )
    queue.register(name="maintenance", handler=maintenance)

    # -- 1. transactional enqueue, rolled back -----------------------------
    widget_id = uuid.uuid4()
    async with pool.acquire() as connection:
        with pytest.raises(RuntimeError):
            async with connection.transaction():
                await connection.execute(
                    f"INSERT INTO public.{widgets} (id, label) VALUES ($1, 'draft')",
                    widget_id,
                )
                abandoned = await queue.enqueue(
                    connection, task="prepare", payload={"widget": str(widget_id)}
                )
                raise RuntimeError("the producer rolled back")
    assert await queue.get_job(abandoned.id) is None
    async with pool.acquire() as connection:
        assert (
            await connection.fetchval(
                f"SELECT count(*) FROM public.{widgets} WHERE id = $1", widget_id
            )
            == 0
        )

    # -- 2. transactional enqueue, committed -------------------------------
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute(
            f"INSERT INTO public.{widgets} (id, label) VALUES ($1, 'committed')",
            widget_id,
        )
        job = await queue.enqueue(
            connection,
            task="prepare",
            payload={"widget": str(widget_id)},
            dedupe_key=f"widget:{widget_id}",
            on_conflict="raise",
        )
        await connection.execute(
            f"UPDATE public.{widgets} SET queue_job_id = $2 WHERE id = $1",
            widget_id,
            job.id,
        )
    stored = await queue.get_job(job.id)
    assert stored is not None and stored.state == JobState.PENDING

    # -- 3. dedupe_key conflict --------------------------------------------
    async with pool.acquire() as connection:
        with pytest.raises(AlreadyEnqueued) as conflict:
            await queue.enqueue(
                connection,
                task="prepare",
                dedupe_key=f"widget:{widget_id}",
                on_conflict="raise",
            )
        assert conflict.value.existing_job_id == job.id
        same = await queue.enqueue(
            connection,
            task="prepare",
            dedupe_key=f"widget:{widget_id}",
            on_conflict="return_existing",
        )
    assert same.id == job.id
    assert len(await queue.list_jobs()) == 1

    # -- 4. claim, 5. heartbeat, 6. successful completion -------------------
    async with running(
        Worker(
            queue,
            worker_id="pipeline-worker",
            concurrency=2,
            poll_interval=0.05,
            lease_duration=5.0,
            heartbeat_interval=0.1,
        )
    ):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.SUCCEEDED),
            message="the committed job should be claimed and completed",
        )
    assert heartbeats == [job.id]

    finished = await queue.get_job(job.id)
    assert finished is not None
    assert finished.attempt == 1
    assert finished.lease_token is None
    assert finished.finished_at is not None
    history = await admin.attempts(job.id)
    assert [record.outcome for record in history] == [AttemptOutcome.SUCCEEDED]
    assert history[0].worker_id == "pipeline-worker"

    # The dedupe key is released once the job is terminal.
    async with pool.acquire() as connection, connection.transaction():
        reused = await queue.enqueue(
            connection,
            task="prepare",
            dedupe_key=f"widget:{widget_id}",
            on_conflict="raise",
        )
    assert reused.id != job.id
    await admin.cancel_job(reused.id)

    # -- 7. a forced retry --------------------------------------------------
    async with pool.acquire() as connection, connection.transaction():
        retryable = await queue.enqueue(connection, task="flaky")
    async with running(
        Worker(queue, worker_id="retry-worker", poll_interval=0.05, concurrency=1)
    ):
        await eventually(
            lambda: _in_state(queue, retryable.id, JobState.SUCCEEDED),
            message="the forced retry should be followed by a success",
        )
    retried = await queue.get_job(retryable.id)
    assert retried is not None and retried.attempt == 2
    assert [record.outcome for record in await admin.attempts(retryable.id)] == [
        AttemptOutcome.RETRY,
        AttemptOutcome.SUCCEEDED,
    ]

    # -- 8. a named-concurrency-key conflict --------------------------------
    async with pool.acquire() as connection:
        async with connection.transaction():
            first = await queue.enqueue(
                connection, task="prepare", concurrency_key="widget-lock"
            )
            second = await queue.enqueue(
                connection, task="prepare", concurrency_key="widget-lock"
            )
        claimed = await storage.claim(
            connection,
            queue=queue.name,
            worker_id="lock-holder",
            tasks=["prepare"],
            limit=5,
            lease_seconds=30,
        )
        assert [entry.job.id for entry in claimed] == [first.id]
        assert not await storage.claim(
            connection,
            queue=queue.name,
            worker_id="lock-waiter",
            tasks=["prepare"],
            limit=5,
            lease_seconds=30,
        )
        blocked = await storage.get_job(connection, second.id)
        assert blocked is not None and blocked.state == JobState.PENDING

        await storage.complete(
            connection, job_id=first.id, lease_token=claimed[0].lease_token
        )
        released = await storage.claim(
            connection,
            queue=queue.name,
            worker_id="lock-waiter",
            tasks=["prepare"],
            limit=5,
            lease_seconds=30,
        )
        assert [entry.job.id for entry in released] == [second.id]
        await storage.complete(
            connection, job_id=second.id, lease_token=released[0].lease_token
        )

    # -- 9. lease expiry, then a fencing rejection --------------------------
    async with pool.acquire() as connection:
        async with connection.transaction():
            fenced = await queue.enqueue(connection, task="prepare", max_attempts=4)
        stale = (
            await storage.claim(
                connection,
                queue=queue.name,
                worker_id="stalled",
                tasks=["prepare"],
                limit=1,
                lease_seconds=LEASE,
            )
        )[0]
        await asyncio.sleep(LEASE + 0.3)
        assert await storage.recover_expired_leases(connection, queue=queue.name) == [
            fenced.id
        ]

        reopened = await storage.get_job(connection, fenced.id)
        assert reopened is not None
        assert reopened.state == JobState.PENDING
        assert reopened.lease_token is None

        successor = (
            await storage.claim(
                connection,
                queue=queue.name,
                worker_id="successor",
                tasks=["prepare"],
                limit=1,
                lease_seconds=60,
            )
        )[0]
        assert successor.lease_token != stale.lease_token

        with pytest.raises(LeaseLost):
            await storage.complete(
                connection, job_id=fenced.id, lease_token=stale.lease_token
            )
        still_leased = await storage.get_job(connection, fenced.id)
        assert still_leased is not None
        assert still_leased.state == JobState.LEASED
        assert still_leased.worker_id == "successor"

        await storage.complete(
            connection, job_id=fenced.id, lease_token=successor.lease_token
        )
        assert [
            record.outcome for record in await storage.attempts(connection, fenced.id)
        ] == [AttemptOutcome.LEASE_EXPIRED, AttemptOutcome.SUCCEEDED]

    # -- 10. one periodic occurrence firing ---------------------------------
    schedule_name = f"pipeline-{uuid.uuid4().hex[:8]}"
    scheduler = Scheduler(
        queue,
        scheduler_id="pipeline-scheduler",
        schedules=[
            ScheduleSpec(
                name=schedule_name,
                task="maintenance",
                cron="* * * * *",
                payload={"kind": "vacuum"},
            )
        ],
    )
    (schedule,) = await scheduler.sync()
    moment = schedule.created_at + timedelta(minutes=2)
    fired = await scheduler.tick(now=moment)
    assert len(fired) == 1
    assert await scheduler.tick(now=moment) == [], "one occurrence, one job"
    assert await scheduler.occurrence_count(schedule.id) == 1

    async with running(
        Worker(queue, worker_id="schedule-worker", poll_interval=0.05, concurrency=1)
    ):
        await eventually(
            lambda: _in_state(queue, fired[0].id, JobState.SUCCEEDED),
            message="the periodic job should be picked up and completed",
        )

    # -- the ledger at the end ----------------------------------------------
    stats = await queue.stats()
    assert stats.pending == 0
    assert stats.leased == 0
    assert stats.cancelled == 1
    assert stats.succeeded == 6
    assert stats.failed == 0


async def _in_state(queue: Queue, job_id: uuid.UUID, expected: JobState) -> bool:
    job = await queue.get_job(job_id)
    return job is not None and job.state == expected
