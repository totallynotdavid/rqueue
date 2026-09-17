"""§10.1 and §10.2: the producer transaction is the unit of durability."""

from __future__ import annotations

import uuid

import asyncpg
import pytest

from rqueue import AlreadyEnqueued, Queue
from rqueue.models import JobRequest
from rqueue.retry import RetryPolicy


async def test_declared_defaults_are_persisted_without_a_handler(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """A producer can enqueue with metadata and no worker registration."""
    queue.declare_task(
        name="prepare",
        retry=RetryPolicy(max_attempts=7),
        timeout=42.0,
    )
    assert queue.tasks == {}

    async with pool.acquire() as connection:
        job = await queue.enqueue(connection, task="prepare")

    stored = await queue.get_job(job.id)
    assert stored is not None
    assert stored.max_attempts == 7
    assert stored.timeout_seconds == 42.0
    assert stored.retry_policy is not None
    assert stored.retry_policy.max_attempts == 7


async def test_rolled_back_producer_leaves_no_business_row_and_no_job(
    queue: Queue, pool: asyncpg.Pool, widgets: str
) -> None:
    """§10.1"""
    widget_id = uuid.uuid4()
    async with pool.acquire() as connection:
        with pytest.raises(RuntimeError):
            async with connection.transaction():
                await connection.execute(
                    f"INSERT INTO public.{widgets} (id, label) VALUES ($1, $2)",
                    widget_id,
                    "rolled back",
                )
                job = await queue.enqueue(
                    connection, task="prepare", payload={"widget": str(widget_id)}
                )
                await connection.execute(
                    f"UPDATE public.{widgets} SET queue_job_id = $2 WHERE id = $1",
                    widget_id,
                    job.id,
                )
                raise RuntimeError("the producer changed its mind")

        rows = await connection.fetchval(
            f"SELECT count(*) FROM public.{widgets} WHERE id = $1", widget_id
        )
        assert rows == 0
    assert await queue.get_job(job.id) is None
    assert await queue.list_jobs() == []


async def test_committed_producer_exposes_business_row_and_job(
    queue: Queue, pool: asyncpg.Pool, widgets: str
) -> None:
    """§10.2"""
    widget_id = uuid.uuid4()
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute(
            f"INSERT INTO public.{widgets} (id, label) VALUES ($1, $2)",
            widget_id,
            "committed",
        )
        job = await queue.enqueue(
            connection, task="prepare", payload={"widget": str(widget_id)}
        )
        await connection.execute(
            f"UPDATE public.{widgets} SET queue_job_id = $2 WHERE id = $1",
            widget_id,
            job.id,
        )

    # A *different* pooled connection sees both, which is the point: the worker
    # will not be reading the producer's connection.
    stored = await queue.get_job(job.id)
    assert stored is not None
    assert stored.state == "pending"
    assert stored.payload == {"widget": str(widget_id)}
    async with pool.acquire() as connection:
        linked = await connection.fetchval(
            f"SELECT queue_job_id FROM public.{widgets} WHERE id = $1", widget_id
        )
    assert linked == job.id


async def test_enqueue_uses_only_the_caller_connection(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """The job must be invisible outside the caller's open transaction."""
    async with pool.acquire() as producer, producer.transaction():
        job = await queue.enqueue(producer, task="prepare", payload={})
        async with pool.acquire() as observer:
            assert await queue.storage.get_job(observer, job.id) is None
        assert await queue.storage.get_job(producer, job.id) is not None
    assert await queue.get_job(job.id) is not None


async def test_enqueue_many_is_atomic(queue: Queue, pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        jobs = await queue.enqueue_many(
            connection,
            [JobRequest(task="prepare", payload={"n": n}) for n in range(5)],
        )
    assert len(jobs) == 5
    assert {job.payload["n"] for job in jobs} == set(range(5))


async def test_enqueue_many_validates_the_whole_batch_before_writing(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    from rqueue.errors import ValidationError

    async with pool.acquire() as connection:
        with pytest.raises(ValidationError):
            await queue.enqueue_many(
                connection,
                [
                    JobRequest(task="prepare", payload={"n": 0}),
                    JobRequest(task="prepare", payload=object()),
                ],
            )
    assert await queue.list_jobs() == []


async def test_enqueue_many_rolls_back_as_a_unit_on_a_conflict(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await queue.enqueue(
                connection,
                task="prepare",
                dedupe_key="batch-key",
                on_conflict="raise",
            )
        with pytest.raises(AlreadyEnqueued):
            await queue.enqueue_many(
                connection,
                [
                    JobRequest(task="prepare", payload={"n": 1}),
                    JobRequest(
                        task="prepare", dedupe_key="batch-key", on_conflict="raise"
                    ),
                ],
            )
    # The first entry of the failed batch must not survive.
    assert len(await queue.list_jobs()) == 1


async def test_a_failed_dedupe_does_not_poison_the_callers_transaction(
    queue: Queue, pool: asyncpg.Pool, widgets: str
) -> None:
    """AlreadyEnqueued is raised on a savepoint, so the caller can carry on."""
    widget_id = uuid.uuid4()
    async with pool.acquire() as connection:
        async with connection.transaction():
            first = await queue.enqueue(
                connection, task="prepare", dedupe_key="k", on_conflict="raise"
            )
        async with connection.transaction():
            await connection.execute(
                f"INSERT INTO public.{widgets} (id, label) VALUES ($1, $2)",
                widget_id,
                "still fine",
            )
            with pytest.raises(AlreadyEnqueued) as caught:
                await queue.enqueue(
                    connection, task="prepare", dedupe_key="k", on_conflict="raise"
                )
            assert caught.value.existing_job_id == first.id
            # The outer transaction is still usable.
            await connection.execute(
                f"UPDATE public.{widgets} SET label = $2 WHERE id = $1",
                widget_id,
                "committed anyway",
            )
        label = await connection.fetchval(
            f"SELECT label FROM public.{widgets} WHERE id = $1", widget_id
        )
    assert label == "committed anyway"
