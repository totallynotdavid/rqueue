"""A non-durable, recording stand-in for :class:`rqueue.Queue`, for unit tests.

It is not exported from :mod:`rqueue`. It runs the production validation in
:meth:`rqueue.Queue.build_insert` and records the row instead of writing it. It
does not simulate transactions, dedupe conflicts, claiming or leases.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from rqueue.errors import ConfigurationError, UnknownTask, ValidationError
from rqueue.limits import MAX_ENQUEUE_BATCH
from rqueue.models import Job, JobRequest, JobState
from rqueue.queue import ConflictMode, Queue, _as_seconds
from rqueue.retry import RetryPolicy, RetryPolicyData
from rqueue.storage import JobInsert

if TYPE_CHECKING:
    import asyncpg

__all__ = ["RecordedEnqueue", "RecordingQueue"]


@dataclass(frozen=True, slots=True)
class RecordedEnqueue:
    """One enqueue call that passed the real validation and was recorded."""

    #: The request exactly as the caller expressed it, before validation.
    request: JobRequest
    #: The validated row production code would have written.
    spec: JobInsert
    #: The synthesized job handed back to the caller.
    job: Job
    connection: object = None

    @property
    def task(self) -> str:
        return self.spec.task

    @property
    def queue(self) -> str:
        return self.spec.queue

    @property
    def payload(self) -> Any:
        """The payload after a JSON round trip, as a worker would decode it."""
        return self.job.payload

    @property
    def dedupe_key(self) -> str | None:
        return self.spec.dedupe_key

    @property
    def concurrency_key(self) -> str | None:
        return self.spec.concurrency_key

    @property
    def metadata(self) -> Mapping[str, Any]:
        return self.job.metadata


class RecordingQueue(Queue):
    """A :class:`~rqueue.Queue` that validates enqueue calls and records them."""

    def __init__(
        self,
        *,
        name: str = "default",
        schema: str = "task_queue",
        default_retry: RetryPolicy | None = None,
        require_registered_tasks: bool = True,
    ) -> None:
        """Build a recording queue."""
        super().__init__(
            cast("asyncpg.Pool", None),
            name=name,
            schema=schema,
            default_retry=default_retry,
        )
        self.require_registered_tasks = require_registered_tasks
        #: Every call recorded so far, in call order.
        self.recorded: list[RecordedEnqueue] = []

    async def enqueue(
        self,
        connection: object = None,
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
        """Validate one call the way production does, then record it.

        ``connection`` is ignored, and defaults to ``None`` so a test can call
        this directly without inventing one.
        """
        request = JobRequest(
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
        return self._record(connection, request, self._validate(request))

    async def enqueue_many(
        self,
        connection: object,
        jobs: Sequence[JobRequest],
        /,
    ) -> list[Job]:
        """Validate a whole batch, then record it, or record none of it."""
        if len(jobs) > MAX_ENQUEUE_BATCH:
            raise ValidationError(
                f"enqueue_many accepts at most {MAX_ENQUEUE_BATCH} jobs per call "
                f"(got {len(jobs)})"
            )
        specs = [self._validate(request) for request in jobs]
        return [
            self._record(connection, request, spec)
            for request, spec in zip(jobs, specs, strict=True)
        ]

    def enqueued(self, task: str | None = None) -> list[RecordedEnqueue]:
        """The recorded calls, optionally narrowed to one task name."""
        if task is None:
            return list(self.recorded)
        return [recorded for recorded in self.recorded if recorded.task == task]

    def reset(self) -> None:
        """Forget every recorded call. Registered tasks are kept."""
        self.recorded.clear()

    def _validate(self, request: JobRequest) -> JobInsert:
        """Run the production validation path and the registry check."""
        spec = self.build_insert(request)
        if (
            self.require_registered_tasks
            and spec.task not in self._tasks
            and spec.task not in self._declarations
        ):
            raise UnknownTask(
                f"no declaration or handler registered for task {spec.task!r}; "
                "declare or register it on this RecordingQueue, or construct it "
                "with "
                "require_registered_tasks=False"
            )
        return spec

    def _record(self, connection: object, request: JobRequest, spec: JobInsert) -> Job:
        """Stand in for ``Storage.insert_job``: synthesize a job and keep it."""
        now = datetime.now(UTC)
        job = Job(
            id=spec.id,
            queue=spec.queue,
            task=spec.task,
            payload=json.loads(spec.payload_json),
            state=JobState.PENDING,
            priority=spec.priority,
            attempt=0,
            max_attempts=spec.max_attempts,
            scheduled_at=spec.scheduled_at or now,
            created_at=now,
            updated_at=now,
            dedupe_key=spec.dedupe_key,
            concurrency_key=spec.concurrency_key,
            timeout_seconds=spec.timeout_seconds,
            metadata=json.loads(spec.metadata_json),
            retry_policy=(
                RetryPolicyData.from_json(spec.retry_policy_json)
                if spec.retry_policy_json is not None
                else None
            ),
        )
        self.recorded.append(
            RecordedEnqueue(request=request, spec=spec, job=job, connection=connection)
        )
        return job

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[asyncpg.Connection]:
        """Always raises: there is no database behind a RecordingQueue."""
        raise ConfigurationError(
            "RecordingQueue has no pool, so inspection and administration "
            "(get_job, list_jobs, stats) are unavailable. Assert on "
            "RecordingQueue.recorded instead, or write an integration test "
            "against real PostgreSQL."
        )
        # Unreachable; @asynccontextmanager requires a generator.
        yield cast("asyncpg.Connection", None)
