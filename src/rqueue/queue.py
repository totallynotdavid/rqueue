"""The producer-side API: task registration and transactional enqueueing."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from rqueue.errors import AlreadyEnqueued, UnknownTask, ValidationError
from rqueue.limits import (
    MAX_CONCURRENCY_KEY_LENGTH,
    MAX_DEDUPE_KEY_LENGTH,
    MAX_ENQUEUE_BATCH,
    MAX_QUEUE_NAME_LENGTH,
    MAX_TASK_NAME_LENGTH,
    validate_key,
    validate_max_attempts,
    validate_metadata,
    validate_name,
    validate_payload,
    validate_priority,
    validate_scheduled_at,
    validate_timeout,
)
from rqueue.models import Job, JobRequest, JobState, QueueStats
from rqueue.retry import RetryPolicy
from rqueue.storage import JobInsert, Storage
from rqueue.tasks import PayloadDecoder, TaskHandler, TaskRegistration, identity_decoder

if TYPE_CHECKING:
    import asyncpg

__all__ = ["ConflictMode", "Queue"]

#: What to do when a dedupe key is already held by an active job. §3 requires
#: the choice to be explicit per call, so there is no default.
ConflictMode = Literal["return_existing", "raise"]


class Queue:
    """One named queue, bound to an application-owned asyncpg pool.

    The pool is only used for inspection and administration. Enqueueing always
    takes a caller-supplied connection so the job lands in the caller's own
    transaction (§3):

    >>> async with pool.acquire() as connection:          # doctest: +SKIP
    ...     async with connection.transaction():
    ...         await app_db.execute(create_compute_job(...))
    ...         job = await queue.enqueue(
    ...             connection,
    ...             task="prepare_simulation",
    ...             payload={"compute_job_id": str(compute_job_id)},
    ...             dedupe_key=f"simulation:{external_id}",
    ...             on_conflict="return_existing",
    ...         )

    Business code should not import rqueue to do this. Pass a thin callback,
    the way picv-2025's ``repository.create_or_get_job(..., defer=...)`` does::

        async def enqueue_simulation(connection, compute_job_id):
            return await queue.enqueue(
                connection,
                task="prepare_simulation",
                payload={"compute_job_id": str(compute_job_id)},
                dedupe_key=f"simulation:{compute_job_id}",
                on_conflict="return_existing",
            )

    Fairness: a queue is served by the workers configured for it, and rqueue
    does not arbitrate between queues -- run one :class:`~rqueue.Worker` per
    queue and size each one deliberately. Within a queue the order is
    ``priority`` descending, then ``scheduled_at``, then insertion order.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        name: str = "default",
        schema: str = "task_queue",
        default_retry: RetryPolicy | None = None,
    ) -> None:
        self.name = validate_name(
            name, kind="queue name", max_length=MAX_QUEUE_NAME_LENGTH
        )
        self.pool = pool
        self.storage = Storage(schema)
        self.default_retry = default_retry or RetryPolicy()
        self._tasks: dict[str, TaskRegistration] = {}

    @property
    def schema(self) -> str:
        return self.storage.schema

    @property
    def notify_channel(self) -> str:
        return self.storage.notify_channel

    # ------------------------------------------------------------- registry

    def task(
        self,
        *,
        name: str,
        decoder: PayloadDecoder = identity_decoder,
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
    ) -> Callable[[TaskHandler], TaskHandler]:
        """Register an ``async def`` handler under an explicit task name."""

        def decorate(handler: TaskHandler) -> TaskHandler:
            self.register(
                name=name,
                handler=handler,
                decoder=decoder,
                retry=retry,
                timeout=timeout,
            )
            return handler

        return decorate

    def register(
        self,
        *,
        name: str,
        handler: TaskHandler,
        decoder: PayloadDecoder = identity_decoder,
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
    ) -> TaskRegistration:
        if name in self._tasks:
            raise ValidationError(f"task {name!r} is already registered")
        registration = TaskRegistration(
            name=name,
            handler=handler,
            decoder=decoder,
            retry=retry or self.default_retry,
            timeout=validate_timeout(timeout),
        )
        self._tasks[name] = registration
        return registration

    @property
    def tasks(self) -> Mapping[str, TaskRegistration]:
        return dict(self._tasks)

    def get_task(self, name: str) -> TaskRegistration:
        try:
            return self._tasks[name]
        except KeyError:
            raise UnknownTask(f"no handler registered for task {name!r}") from None

    # -------------------------------------------------------------- enqueue

    async def enqueue(
        self,
        connection: asyncpg.Connection,
        *,
        task: str,
        payload: Any = None,
        scheduled_at: datetime | None = None,
        delay: float | timedelta | None = None,
        priority: int = 0,
        max_attempts: int | None = None,
        dedupe_key: str | None = None,
        on_conflict: ConflictMode | None = None,
        concurrency_key: str | None = None,
        timeout: float | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Job:
        """Insert one job on the caller's connection and transaction.

        Never opens a second connection. If the caller's transaction rolls
        back, the job is gone with everything else it wrote.
        """
        spec = self.build_insert(
            JobRequest(
                task=task,
                payload=payload,
                scheduled_at=scheduled_at,
                delay=_as_seconds(delay),
                priority=priority,
                max_attempts=max_attempts,
                dedupe_key=dedupe_key,
                on_conflict=on_conflict,
                concurrency_key=concurrency_key,
                timeout=timeout,
                metadata=metadata,
            )
        )
        # A savepoint, when the caller is already in a transaction: it keeps an
        # AlreadyEnqueued from poisoning the caller's transaction, and rolls
        # back the no-op conflict UPDATE that produced it.
        async with connection.transaction():
            job, inserted = await self.storage.insert_job(connection, spec)
            if not inserted and spec.raise_on_conflict:
                raise AlreadyEnqueued(spec.dedupe_key or "", self.name, job.id)
        return job

    async def enqueue_many(
        self,
        connection: asyncpg.Connection,
        jobs: Sequence[JobRequest],
        /,
    ) -> list[Job]:
        """Insert a batch atomically, validating every entry before writing.

        Validation of the whole batch happens first, so a bad entry at index 40
        does not leave 39 rows written and then raise. The writes themselves
        run inside a transaction (a savepoint when the caller already has one),
        so a database-side failure rolls the batch back as a unit.
        """
        if len(jobs) > MAX_ENQUEUE_BATCH:
            raise ValidationError(
                f"enqueue_many accepts at most {MAX_ENQUEUE_BATCH} jobs per call "
                f"(got {len(jobs)})"
            )
        specs = [self.build_insert(request) for request in jobs]
        results: list[Job] = []
        async with connection.transaction():
            for spec in specs:
                job, inserted = await self.storage.insert_job(connection, spec)
                if not inserted and spec.raise_on_conflict:
                    raise AlreadyEnqueued(spec.dedupe_key or "", self.name, job.id)
                results.append(job)
        return results

    def build_insert(
        self, request: JobRequest, *, queue_name: str | None = None
    ) -> JobInsert:
        """Validate one enqueue request into a row that is ready to write.

        ``queue_name`` overrides the bound queue, which the scheduler needs
        when a periodic schedule targets a different queue than the one its
        :class:`Queue` handle is named for.
        """
        task = validate_name(
            request.task, kind="task name", max_length=MAX_TASK_NAME_LENGTH
        )
        if request.scheduled_at is not None and request.delay is not None:
            raise ValidationError("pass scheduled_at or delay, not both")
        if request.dedupe_key is not None and request.on_conflict is None:
            raise ValidationError(
                "dedupe_key requires an explicit on_conflict of "
                "'return_existing' or 'raise'"
            )
        if request.on_conflict not in (None, "return_existing", "raise"):
            raise ValidationError(
                f"on_conflict must be 'return_existing' or 'raise' "
                f"(got {request.on_conflict!r})"
            )

        when: datetime | None = None
        if request.scheduled_at is not None:
            when = validate_scheduled_at(request.scheduled_at)
        elif request.delay is not None:
            if request.delay < 0:
                raise ValidationError("delay must not be negative")
            when = validate_scheduled_at(
                datetime.now(UTC) + timedelta(seconds=request.delay)
            )

        registration = self._tasks.get(task)
        max_attempts = request.max_attempts
        if max_attempts is None:
            policy = registration.retry if registration else self.default_retry
            max_attempts = policy.max_attempts
        timeout = request.timeout
        if timeout is None and registration is not None:
            timeout = registration.timeout

        return JobInsert(
            id=uuid.uuid4(),
            queue=validate_name(
                queue_name or self.name,
                kind="queue name",
                max_length=MAX_QUEUE_NAME_LENGTH,
            ),
            task=task,
            payload_json=validate_payload(request.payload),
            priority=validate_priority(request.priority),
            max_attempts=validate_max_attempts(max_attempts),
            scheduled_at=when,
            dedupe_key=validate_key(
                request.dedupe_key,
                kind="dedupe_key",
                max_length=MAX_DEDUPE_KEY_LENGTH,
            ),
            concurrency_key=validate_key(
                request.concurrency_key,
                kind="concurrency_key",
                max_length=MAX_CONCURRENCY_KEY_LENGTH,
            ),
            timeout_seconds=validate_timeout(timeout),
            metadata_json=validate_metadata(request.metadata),
            raise_on_conflict=request.on_conflict == "raise",
        )

    # ----------------------------------------------------------- inspection

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[asyncpg.Connection]:
        """Borrow a pooled connection for an inspection or admin call."""
        async with self.pool.acquire() as connection:
            yield connection

    async def get_job(self, job_id: uuid.UUID) -> Job | None:
        async with self.connection() as connection:
            return await self.storage.get_job(connection, job_id)

    async def list_jobs(
        self,
        *,
        states: Sequence[JobState | str] | None = None,
        task: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        async with self.connection() as connection:
            return await self.storage.list_jobs(
                connection,
                queue=self.name,
                states=states,
                task=task,
                limit=limit,
                offset=offset,
            )

    async def stats(self) -> QueueStats:
        async with self.connection() as connection:
            return await self.storage.stats(connection, queue=self.name)


def _as_seconds(delay: float | timedelta | None) -> float | None:
    if delay is None:
        return None
    if isinstance(delay, timedelta):
        return delay.total_seconds()
    return float(delay)
