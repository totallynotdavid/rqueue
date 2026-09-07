"""§10.6: retry, timeout, cancellation, and terminal failure are durable."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

import asyncpg

from rqueue import (
    Admin,
    CancelJob,
    PermanentFailure,
    Queue,
    Retry,
    RetryPolicy,
    TaskContext,
    Worker,
)
from rqueue.limits import MAX_ERROR_MESSAGE_LENGTH
from rqueue.models import AttemptOutcome, JobState

from .support import eventually, running


def make_worker(queue: Queue, **kwargs: object) -> Worker:
    defaults: dict[str, object] = {
        "worker_id": f"w-{uuid.uuid4().hex[:8]}",
        "concurrency": 2,
        "poll_interval": 0.05,
        "lease_duration": 5.0,
        "heartbeat_interval": 0.1,
    }
    defaults.update(kwargs)
    return Worker(queue, **defaults)  # type: ignore[arg-type]


async def test_a_failing_handler_retries_then_succeeds(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    attempts: list[int] = []

    async def flaky(payload: object, context: TaskContext) -> None:
        attempts.append(context.attempt)
        if context.attempt < 3:
            raise RuntimeError(f"transient failure {context.attempt}")

    queue.register(
        name="flaky",
        handler=flaky,
        retry=RetryPolicy(max_attempts=3, initial_backoff=0.05, jitter=0.0),
    )
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="flaky")

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.SUCCEEDED),
            message="the third attempt should succeed",
        )

    assert attempts == [1, 2, 3]
    final = await queue.get_job(job.id)
    assert final is not None and final.attempt == 3
    async with pool.acquire() as connection:
        history = await queue.storage.attempts(connection, job.id)
    assert [record.outcome for record in history] == [
        AttemptOutcome.RETRY,
        AttemptOutcome.RETRY,
        AttemptOutcome.SUCCEEDED,
    ]
    assert history[0].error_type == "RuntimeError"
    assert "transient failure 1" in (history[0].error_message or "")


async def test_handler_and_worker_share_the_retry_decision(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    observed: list[bool] = []
    predicate_calls = 0

    def retry_if(exc: BaseException) -> bool:
        nonlocal predicate_calls
        predicate_calls += 1
        return True

    async def doomed(payload: object, context: TaskContext) -> None:
        exc = RuntimeError(f"failure {context.attempt}")
        observed.append(context.will_retry(exc))
        raise exc

    queue.register(
        name="shared_decision",
        handler=doomed,
        retry=RetryPolicy(max_attempts=5, retry_if=retry_if, jitter=0.0),
    )
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(
            connection,
            task="shared_decision",
            max_attempts=2,
        )

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.FAILED),
            message="the persisted attempt limit should win over the registration",
        )

    assert observed == [True, False]
    assert predicate_calls == 1
    final = await queue.get_job(job.id)
    assert final is not None and final.attempt == 2


async def test_exhausting_the_budget_is_a_durable_terminal_failure(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async def doomed(payload: object, context: TaskContext) -> None:
        raise RuntimeError("always broken")

    queue.register(
        name="doomed",
        handler=doomed,
        retry=RetryPolicy(max_attempts=2, initial_backoff=0.05, jitter=0.0),
    )
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="doomed")

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.FAILED),
            message="the job should fail terminally after two attempts",
        )

    final = await queue.get_job(job.id)
    assert final is not None
    assert final.attempt == 2
    assert final.error_type == "RuntimeError"
    assert final.finished_at is not None
    assert final.lease_token is None


async def test_permanent_failure_skips_the_remaining_attempts(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    calls: list[int] = []

    async def refuses(payload: object, context: TaskContext) -> None:
        calls.append(context.attempt)
        raise PermanentFailure("this payload can never work")

    queue.register(name="refuses", handler=refuses, retry=RetryPolicy(max_attempts=5))
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="refuses")

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.FAILED),
            message="a permanent failure should be terminal at once",
        )

    assert calls == [1]
    final = await queue.get_job(job.id)
    assert final is not None and final.error_type == "PermanentFailure"


async def test_a_handler_can_choose_its_own_retry_instant(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async def defers(payload: object, context: TaskContext) -> None:
        exc = Retry(delay=3600, reason="upstream is rate limiting us")
        assert context.will_retry(exc)
        raise exc

    queue.register(name="defers", handler=defers)
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="defers")

    async with running(make_worker(queue)):
        await eventually(
            lambda: _rescheduled(queue, job.id),
            message="the job should be waiting for its chosen instant",
        )

    final = await queue.get_job(job.id)
    assert final is not None
    assert final.state == JobState.PENDING
    assert final.scheduled_at > datetime.now(UTC)
    assert final.error_type == "Retry"
    assert "rate limiting" in (final.error_message or "")


async def test_a_timeout_is_recorded_and_retried(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async def slow(payload: object, context: TaskContext) -> None:
        await asyncio.sleep(30)

    queue.register(
        name="slow",
        handler=slow,
        timeout=0.2,
        retry=RetryPolicy(max_attempts=2, initial_backoff=0.05, jitter=0.0),
    )
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="slow")

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.FAILED),
            message="a timing-out handler should exhaust its attempts",
        )

    final = await queue.get_job(job.id)
    assert final is not None
    assert final.error_type == "TimeoutError"
    async with pool.acquire() as connection:
        history = await queue.storage.attempts(connection, job.id)
    assert [record.outcome for record in history] == [
        AttemptOutcome.RETRY,
        AttemptOutcome.FAILED,
    ]


async def test_cancelling_a_pending_job_prevents_execution(
    queue: Queue, pool: asyncpg.Pool, admin: Admin
) -> None:
    ran = asyncio.Event()

    async def never(payload: object, context: TaskContext) -> None:
        ran.set()

    queue.register(name="never", handler=never)
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="never")

    cancelled = await admin.cancel_job(job.id)
    assert cancelled.state == JobState.CANCELLED
    assert cancelled.finished_at is not None

    async with running(make_worker(queue)):
        await asyncio.sleep(0.4)
    assert not ran.is_set()
    stored = await queue.get_job(job.id)
    assert stored is not None and stored.state == JobState.CANCELLED


async def test_cancelling_a_leased_job_is_cooperative(
    queue: Queue, pool: asyncpg.Pool, admin: Admin
) -> None:
    started = asyncio.Event()
    observed_request = asyncio.Event()

    async def watchful(payload: object, context: TaskContext) -> None:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            if context.cancel_requested:
                observed_request.set()
            raise

    queue.register(name="watchful", handler=watchful)
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="watchful")

    async with running(make_worker(queue)):
        await asyncio.wait_for(started.wait(), timeout=15)
        requested = await admin.cancel_job(job.id)
        # A leased job is only *asked* to stop; it stays leased until its
        # holder finalizes it.
        assert requested.state == JobState.LEASED
        assert requested.cancel_requested is True

        await eventually(
            lambda: _in_state(queue, job.id, JobState.CANCELLED),
            message="the lease holder should finalize the cancellation",
        )

    assert observed_request.is_set()
    final = await queue.get_job(job.id)
    assert final is not None
    assert final.error_type == "Cancelled"
    assert final.lease_token is None


async def test_a_handler_can_cancel_its_own_job(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    async def gives_up(payload: object, context: TaskContext) -> None:
        exc = CancelJob("the upstream record disappeared")
        assert not context.will_retry(exc)
        raise exc

    queue.register(name="gives_up", handler=gives_up)
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="gives_up")

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.CANCELLED),
            message="CancelJob should finalize the job as cancelled",
        )
    final = await queue.get_job(job.id)
    assert final is not None and "disappeared" in (final.error_message or "")


async def test_a_decode_failure_is_a_non_retryable_durable_failure(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    calls: list[int] = []

    def strict_decoder(payload: object) -> dict[str, int]:
        if not isinstance(payload, dict) or "count" not in payload:
            raise ValueError("payload needs a 'count'")
        return {"count": int(payload["count"])}

    async def counted(payload: dict[str, int], context: TaskContext) -> None:
        calls.append(payload["count"])

    queue.register(
        name="counted",
        handler=counted,
        decoder=strict_decoder,
        retry=RetryPolicy(max_attempts=5, initial_backoff=0.05),
    )
    async with pool.acquire() as connection, connection.transaction():
        bad = await queue.enqueue(connection, task="counted", payload={"nope": 1})
        good = await queue.enqueue(connection, task="counted", payload={"count": 7})

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, bad.id, JobState.FAILED),
            message="an undecodable payload should fail durably",
        )
        await eventually(
            lambda: _in_state(queue, good.id, JobState.SUCCEEDED),
            message="a decodable payload should still run",
        )

    assert calls == [7]
    failed = await queue.get_job(bad.id)
    assert failed is not None
    assert failed.attempt == 1, "a decode failure must not consume the retry budget"
    assert failed.error_type == "PayloadDecodeError"


async def test_error_text_is_bounded(queue: Queue, pool: asyncpg.Pool) -> None:
    async def verbose(payload: object, context: TaskContext) -> None:
        raise RuntimeError("x" * 50_000)

    queue.register(name="verbose", handler=verbose, retry=RetryPolicy(max_attempts=1))
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="verbose")

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.FAILED),
            message="the job should fail",
        )

    final = await queue.get_job(job.id)
    assert final is not None
    assert final.error_message is not None
    assert len(final.error_message) <= MAX_ERROR_MESSAGE_LENGTH


async def test_operator_retry_is_the_only_way_out_of_a_terminal_state(
    queue: Queue, pool: asyncpg.Pool, admin: Admin
) -> None:
    calls: list[int] = []
    should_fail = True

    async def sometimes(payload: object, context: TaskContext) -> None:
        calls.append(context.attempt)
        if should_fail:
            raise RuntimeError("not yet")

    queue.register(
        name="sometimes",
        handler=sometimes,
        retry=RetryPolicy(max_attempts=1),
    )
    async with pool.acquire() as connection, connection.transaction():
        job = await queue.enqueue(connection, task="sometimes")

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.FAILED),
            message="the job should fail first",
        )

    should_fail = False
    revived = await admin.retry_job(job.id)
    assert revived.state == JobState.PENDING
    # The attempt counter keeps counting; the budget is raised instead, so the
    # immutable attempt history is never overwritten.
    assert revived.attempt == 1
    assert revived.max_attempts == 2
    assert revived.error_type is None

    async with running(make_worker(queue)):
        await eventually(
            lambda: _in_state(queue, job.id, JobState.SUCCEEDED),
            message="the retried job should run again",
        )
    assert calls == [1, 2]


async def _in_state(queue: Queue, job_id: uuid.UUID, expected: JobState) -> bool:
    job = await queue.get_job(job_id)
    return job is not None and job.state == expected


async def _rescheduled(queue: Queue, job_id: uuid.UUID) -> bool:
    job = await queue.get_job(job_id)
    return (
        job is not None
        and job.state == JobState.PENDING
        and job.scheduled_at > datetime.now(UTC)
    )
