"""A consume-only role runs the whole worker, and nothing a worker must not do."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence

import asyncpg
import pytest

from rqueue import (
    Admin,
    PermanentFailure,
    Queue,
    RetryPolicy,
    TaskContext,
    Worker,
)
from rqueue.models import AttemptOutcome, JobState
from rqueue.roles import Capability

from .support import eventually, running, scoped_role_pool

RETRY = RetryPolicy(max_attempts=3, initial_backoff=0.05, jitter=0.0)


def register_tasks(queue: Queue, *, started: asyncio.Event, ran: list[str]) -> None:
    """The same tasks on the producer's queue and the worker's.

    A retry policy is written into the job at enqueue from the enqueuing
    queue's registry, so both sides have to know the tasks.
    """

    async def succeeds(payload: object, context: TaskContext) -> None:
        ran.append("succeeds")

    async def fails(payload: object, context: TaskContext) -> None:
        raise PermanentFailure("this job is meant to fail")

    async def flaky(payload: object, context: TaskContext) -> None:
        if context.attempt < 2:
            raise RuntimeError("transient")

    async def watchful(payload: object, context: TaskContext) -> None:
        started.set()
        await asyncio.sleep(30)

    queue.register(name="succeeds", handler=succeeds)
    queue.register(name="fails", handler=fails)
    queue.register(name="flaky", handler=flaky, retry=RETRY)
    queue.register(name="watchful", handler=watchful)


async def state_of(queue: Queue, job_id: uuid.UUID) -> JobState | None:
    job = await queue.get_job(job_id)
    return job.state if job is not None else None


async def test_a_consume_only_worker_runs_claim_heartbeat_finish_fail_retry_and_cancel(
    admin_dsn: str, pool: asyncpg.Pool, admin: Admin, queue_name: str
) -> None:
    """Every statement the worker issues works on CONSUME alone.

    The grants were derived from those statements; this is the proof they were
    derived completely. A missing column or table privilege would not show in
    a grant listing but stops a path here: the worker raises a permission
    error, or a job never leaves ``leased``.
    """
    started = asyncio.Event()
    ran: list[str] = []
    producer = Queue(pool, name=queue_name)
    register_tasks(producer, started=started, ran=ran)

    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.CONSUME],
        queues=[queue_name],
        prefix="rq_consumer",
    ) as consumer_pool:
        consumer = Queue(consumer_pool, name=queue_name)
        register_tasks(consumer, started=started, ran=ran)

        async with pool.acquire() as connection, connection.transaction():
            good = await producer.enqueue(connection, task="succeeds")
            bad = await producer.enqueue(connection, task="fails")
            flaky = await producer.enqueue(connection, task="flaky")
            keyed = [
                await producer.enqueue(
                    connection, task="succeeds", concurrency_key="shared"
                )
                for _ in range(2)
            ]
            watched = await producer.enqueue(connection, task="watchful")

        worker = Worker(
            consumer,
            worker_id=f"w-{uuid.uuid4().hex[:8]}",
            concurrency=3,
            poll_interval=0.05,
            lease_duration=5.0,
            heartbeat_interval=0.1,
        )
        async with running(worker):
            await asyncio.wait_for(started.wait(), timeout=15)
            # The cancel request reaches the worker through its heartbeat.
            await admin.cancel_job(watched.id)
            outcomes = {
                good.id: JobState.SUCCEEDED,
                bad.id: JobState.FAILED,
                flaky.id: JobState.SUCCEEDED,
                keyed[0].id: JobState.SUCCEEDED,
                keyed[1].id: JobState.SUCCEEDED,
                watched.id: JobState.CANCELLED,
            }

            async def settled() -> bool:
                return {
                    job_id: await state_of(producer, job_id) for job_id in outcomes
                } == outcomes

            await eventually(settled, message="every job should reach its outcome")

        async with pool.acquire() as connection:
            history = await producer.storage.attempts(connection, flaky.id)
            assert [record.outcome for record in history] == [
                AttemptOutcome.RETRY,
                AttemptOutcome.SUCCEEDED,
            ]
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM task_queue.concurrency_slots "
                    "WHERE queue = $1",
                    queue_name,
                )
                == 0
            )


async def test_a_consume_only_role_recovers_an_expired_lease(
    admin_dsn: str, pool: asyncpg.Pool, queue_name: str
) -> None:
    producer = Queue(pool, name=queue_name)
    async with pool.acquire() as connection:
        job = await producer.enqueue(connection, task="succeeds")

    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.CONSUME],
        queues=[queue_name],
        prefix="rq_consumer",
    ) as consumer_pool:
        storage = Queue(consumer_pool, name=queue_name).storage
        async with consumer_pool.acquire() as connection:
            claimed = await storage.claim(
                connection,
                queue=queue_name,
                worker_id="gone",
                tasks=["succeeds"],
                limit=1,
                lease_seconds=0.05,
            )
            assert [entry.job.id for entry in claimed] == [job.id]
            await asyncio.sleep(0.2)
            recovered = await storage.recover_expired_leases(
                connection, queue=queue_name
            )
    assert recovered == [job.id]
    assert await state_of(producer, job.id) == JobState.PENDING


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO task_queue.jobs (queue, task, payload, state, max_attempts) "
        "VALUES ('{queue}', 'evil', '{{}}'::jsonb, 'pending', 3)",
        "UPDATE task_queue.jobs SET task = 'evil' WHERE id = $1",
        "UPDATE task_queue.jobs SET payload = '{{\"x\": 1}}'::jsonb WHERE id = $1",
        "UPDATE task_queue.jobs SET queue = 'elsewhere' WHERE id = $1",
        "UPDATE task_queue.jobs SET max_attempts = 1000 WHERE id = $1",
        "UPDATE task_queue.jobs SET cancel_requested = true WHERE id = $1",
        "UPDATE task_queue.job_attempts SET worker_id = 'forged' WHERE job_id = $1",
        "DELETE FROM task_queue.jobs WHERE id = $1",
    ],
)
async def test_a_consume_only_role_cannot_create_or_redefine_a_job(
    admin_dsn: str,
    pool: asyncpg.Pool,
    queue_name: str,
    statement: str,
) -> None:
    """Enqueueing by INSERT is refused, and so is every UPDATE that would do it.

    Rewriting a pending job's ``task`` or ``payload`` hands the fleet work the
    same as an enqueue does, so the worker's ``UPDATE`` stops at the lease and
    outcome columns.
    """
    producer = Queue(pool, name=queue_name)
    async with pool.acquire() as connection:
        job = await producer.enqueue(connection, task="succeeds")

    async with scoped_role_pool(
        admin_dsn,
        capabilities=[Capability.CONSUME],
        queues=[queue_name],
        prefix="rq_consumer",
    ) as consumer_pool:
        sql = statement.format(queue=queue_name)
        arguments: Sequence[object] = (job.id,) if "$1" in sql else ()
        async with consumer_pool.acquire() as connection:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(sql, *arguments)
