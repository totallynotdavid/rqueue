"""A non-durable, recording stand-in for :class:`rqueue.Queue`, for unit tests.

Deliberately **not** exported from the :mod:`rqueue` package. Import it by its
full path::

    from rqueue.testing import RecordingQueue

so that "this is a test tool, not production wiring" is unambiguous from the
import line alone.

What it is for
--------------

A consumer application wants to unit-test the seam where its business code
asks for a job, such as a repository function that takes a ``defer`` callback,
without starting PostgreSQL. Substituting a bare
``AsyncMock`` proves only that *a* callback ran with *some* arguments.
:class:`RecordingQueue` proves more: it runs the *real* validation path,
:meth:`rqueue.Queue.build_insert`, which is a pure function with no database
access, and then records the validated row instead of writing it.

What a recorded call proves
---------------------------

* The task name is well-formed and, unless ``require_registered_tasks=False``,
  declared or registered -- so a typo is a test failure, not a runtime
  surprise.
* The payload is JSON-serializable and within :data:`rqueue.limits
  .MAX_PAYLOAD_BYTES`; the same for ``metadata``.
* ``dedupe_key`` is paired with an explicit ``on_conflict``, and
  ``scheduled_at``/``delay`` are not both set.
* ``priority``, ``max_attempts``, ``timeout``, and the two keys are within
  their documented bounds, and the registered task's retry/timeout defaults
  were applied.

In short: the call is well-formed and the real queue would accept it.

What it does **not** prove
--------------------------

This is a recorder, not a simulator. It does not model:

* PostgreSQL transactionality -- nothing is written, so nothing rolls back.
  A recorded call inside a transaction that later aborts is still recorded.
* Dedupe conflict resolution. Two calls sharing a ``dedupe_key`` are recorded
  as two independent jobs; ``on_conflict="return_existing"`` never returns an
  earlier job and ``on_conflict="raise"`` never raises
  :class:`~rqueue.AlreadyEnqueued`.
* Concurrency slots, claiming, leases, retries, state transitions, scheduling,
  or ``NOTIFY``.

Returned :class:`~rqueue.Job` values are synthesized locally: a fresh id,
``state=JobState.PENDING``, ``attempt=0``, and local timestamps. They are a
plausible return value for code that inspects what ``enqueue`` gave back (a
job id to store alongside a business row, say), not a durable row.

For any of the behaviour in that second list, use the real integration suite
against real PostgreSQL -- ``tests/integration/`` here, and ``mise run
test-integration`` to run it.
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
    #: Whatever was passed as the connection argument. Never used; recorded so
    #: a test can assert its code enqueued on the connection it was given.
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
    """A :class:`~rqueue.Queue` that validates enqueue calls and records them.

    It subclasses the real queue on purpose: call sites stay byte-identical
    between test and production code, and a consumer function annotated
    ``queue: Queue`` accepts one without an ``if TESTING:`` branch. Only the
    persistence step is replaced -- validation still runs through
    :meth:`~rqueue.Queue.build_insert`, and task registration
    (:meth:`~rqueue.Queue.task`, :meth:`~rqueue.Queue.register`) is inherited
    unchanged, so a test can declare the same enqueue metadata as a producer
    or register the same task names the worker does.

    There is no pool and no connection. The connection argument of
    :meth:`enqueue` and :meth:`enqueue_many` is accepted, recorded, and
    ignored, so the code under test can pass the connection it already has --
    real, faked, or ``None``::

        queue = RecordingQueue(name="compute")
        queue.register(name="prepare_simulation", handler=prepare_simulation)

        await create_or_get_job(data=..., simulation_id=..., defer=enqueue_simulation)

        (recorded,) = queue.enqueued("prepare_simulation")
        assert recorded.payload == {"compute_job_id": str(compute_job_id)}
        assert recorded.dedupe_key == f"simulation:{compute_job_id}"

    Read this module's docstring for what a recorded call does and does not
    prove. The inspection and administration methods inherited from
    :class:`~rqueue.Queue` (:meth:`~rqueue.Queue.get_job`,
    :meth:`~rqueue.Queue.list_jobs`, :meth:`~rqueue.Queue.stats`) need a real
    database and raise :class:`~rqueue.ConfigurationError` here.
    """

    def __init__(
        self,
        *,
        name: str = "default",
        schema: str = "task_queue",
        default_retry: RetryPolicy | None = None,
        require_registered_tasks: bool = True,
    ) -> None:
        """Build a recording queue.

        ``require_registered_tasks`` (default on) rejects an enqueue for a
        task name this queue has neither declared nor registered, which is how
        a typo'd name becomes a unit-test failure. Turn it off if the
        application under test legitimately produces for a task whose
        metadata and handler live in another deployment.
        """
        super().__init__(
            cast("asyncpg.Pool", None),
            name=name,
            schema=schema,
            default_retry=default_retry,
        )
        self.require_registered_tasks = require_registered_tasks
        #: Every call recorded so far, in call order.
        self.recorded: list[RecordedEnqueue] = []

    # -------------------------------------------------------------- enqueue

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
        """Validate a whole batch, then record it -- or record none of it.

        Production validates every entry before writing any row; this mirrors
        that, so a bad entry at index 40 leaves nothing in :attr:`recorded`.
        """
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

    # ------------------------------------------------------------ assertions

    def enqueued(self, task: str | None = None) -> list[RecordedEnqueue]:
        """The recorded calls, optionally narrowed to one task name."""
        if task is None:
            return list(self.recorded)
        return [recorded for recorded in self.recorded if recorded.task == task]

    def reset(self) -> None:
        """Forget every recorded call. Registered tasks are kept."""
        self.recorded.clear()

    # ------------------------------------------------------------- internals

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
            # The column defaults to now() when the request named no instant.
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
        # Unreachable. It is what makes this an async generator, which
        # @asynccontextmanager requires.
        yield cast("asyncpg.Connection", None)
