"""The producer-side API: task registration and transactional enqueueing."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from rqueue.errors import (
    AlreadyEnqueued,
    ConfigurationError,
    UnknownTask,
    ValidationError,
)
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
from rqueue.retry import RetryPolicy, RetryPolicyData
from rqueue.storage import JobInsert, Storage
from rqueue.tasks import (
    PayloadDecoder,
    TaskDeclaration,
    TaskHandler,
    TaskRegistration,
    _require_retry_policy,
    identity_decoder,
)

if TYPE_CHECKING:
    import asyncpg

__all__ = ["ConflictMode", "Queue"]

#: What to do when a dedupe key is already held by an active job. The choice
#: is explicit per call, so there is no default.
ConflictMode = Literal["return_existing", "raise"]


@dataclass(frozen=True, slots=True)
class _RegistrationOptions:
    """Options explicitly supplied to one worker registration."""

    retry: RetryPolicy | None
    timeout: float | None


class Queue:
    """One named queue, bound to an application-owned asyncpg pool.

    The pool is only used for inspection and administration. Enqueueing always
    takes a caller-supplied connection so the job lands in the caller's own
    transaction:

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

    Business code should not import rqueue to do this. Pass it a thin callback,
    for example as the ``defer`` argument of a repository function::

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
        self._declarations: dict[str, TaskDeclaration] = {}
        self._tasks: dict[str, TaskRegistration] = {}
        self._registration_options: dict[str, _RegistrationOptions] = {}

    @property
    def schema(self) -> str:
        return self.storage.schema

    @property
    def notify_channel(self) -> str:
        return self.storage.notify_channel

    # ------------------------------------------------------------- registry

    def declare_task(
        self,
        *,
        name: str,
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
    ) -> TaskDeclaration:
        """Declare enqueue defaults without registering a worker handler.

        A declaration is canonical for a task name. Repeating a declaration
        with an explicitly different retry policy or timeout raises
        :class:`~rqueue.errors.ValidationError`; retry_on and retry_if are
        worker-registration hooks and are rejected here. Otherwise the
        existing declaration is returned.
        """
        declaration = self._declarations.get(name)
        if declaration is None:
            registration = self._tasks.get(name)
            if registration is None:
                declaration = self._new_declaration(
                    name=name,
                    retry=retry,
                    timeout=timeout,
                )
            else:
                options = self._registration_options[name]
                if retry is not None and options.retry is not None:
                    self._check_declaration_options(
                        registration.declaration,
                        retry=retry,
                        timeout=None,
                        mismatch_subject="the existing registration",
                    )
                if timeout is not None and options.timeout is not None:
                    self._check_declaration_options(
                        registration.declaration,
                        retry=None,
                        timeout=timeout,
                        mismatch_subject="the existing registration",
                    )
                resolved_retry = (
                    retry
                    if retry is not None
                    else RetryPolicyData.from_policy(registration.retry).to_policy()
                )
                resolved_timeout = (
                    timeout if timeout is not None else registration.timeout
                )
                declaration = (
                    self._new_declaration(
                        name=name,
                        retry=retry,
                        timeout=resolved_timeout,
                    )
                    if retry is not None
                    else TaskDeclaration(
                        name=name,
                        retry=resolved_retry,
                        timeout=resolved_timeout,
                    )
                )
                self._adopt_declaration_in_registration(name, declaration)
            self._declarations[name] = declaration
            return declaration

        if retry is not None:
            self._validate_declaration_retry(retry, name=name)
        self._check_declaration_options(
            declaration,
            retry=retry,
            timeout=timeout,
        )
        return declaration

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
        validated_timeout = validate_timeout(timeout)
        declaration = self._declarations.get(name)
        if declaration is None:
            registration_retry = self.default_retry if retry is None else retry
            registration_declaration = TaskDeclaration(
                name=name,
                retry=registration_retry,
                timeout=validated_timeout,
            )
        else:
            self._check_declaration_options(
                declaration,
                retry=retry,
                timeout=timeout,
            )
            # Numeric settings belong to the canonical declaration, while
            # retry_on/retry_if are registration-time hooks owned by this
            # worker. Omitted retry settings inherit only the producer's
            # numeric defaults; an explicit policy is wholly worker-local.
            registration_retry = (
                retry
                if retry is not None
                else RetryPolicyData.from_policy(declaration.retry).apply_to(
                    self.default_retry
                )
            )
            registration_declaration = TaskDeclaration(
                name=declaration.name,
                retry=registration_retry,
                timeout=declaration.timeout,
            )
        registration = TaskRegistration.from_declaration(
            declaration=registration_declaration,
            handler=handler,
            decoder=decoder,
        )
        self._tasks[name] = registration
        self._registration_options[name] = _RegistrationOptions(
            retry=retry,
            timeout=validated_timeout if timeout is not None else None,
        )
        return registration

    def _new_declaration(
        self,
        *,
        name: str,
        retry: RetryPolicy | None,
        timeout: float | None,
    ) -> TaskDeclaration:
        if retry is None:
            resolved = RetryPolicyData.from_policy(self.default_retry).to_policy()
        else:
            resolved = self._validate_declaration_retry(retry, name=name)
        return TaskDeclaration(
            name=name,
            retry=resolved,
            timeout=validate_timeout(timeout),
        )

    def _validate_declaration_retry(
        self, retry: RetryPolicy | None, *, name: str
    ) -> RetryPolicy:
        if retry is None:
            return self.default_retry
        resolved = _require_retry_policy(retry, task_name=name)
        if resolved.retry_if is not None or tuple(resolved.retry_on) != (Exception,):
            raise ConfigurationError(
                "declare_task retry may only configure numeric settings; "
                "retry_on and retry_if belong to register()"
            )
        return resolved

    @staticmethod
    def _check_declaration_options(
        declaration: TaskDeclaration,
        *,
        retry: RetryPolicy | None,
        timeout: float | None,
        mismatch_subject: str = "the existing declaration",
    ) -> None:
        if retry is not None:
            retry = _require_retry_policy(retry, task_name=declaration.name)
            if RetryPolicyData.from_policy(retry) != RetryPolicyData.from_policy(
                declaration.retry
            ):
                raise ValidationError(
                    f"task {declaration.name!r} retry policy does not match "
                    f"{mismatch_subject}"
                )
        if timeout is not None:
            validated_timeout = validate_timeout(timeout)
            if validated_timeout != declaration.timeout:
                raise ValidationError(
                    f"task {declaration.name!r} timeout does not match "
                    f"{mismatch_subject}"
                )

    def _adopt_declaration_in_registration(
        self, name: str, declaration: TaskDeclaration
    ) -> None:
        registration = self._tasks[name]
        options = self._registration_options[name]
        retry = registration.retry
        if options.retry is None:
            retry = RetryPolicyData.from_policy(declaration.retry).apply_to(retry)
        timeout = (
            registration.timeout if options.timeout is not None else declaration.timeout
        )
        if retry == registration.retry and timeout == registration.timeout:
            return
        self._tasks[name] = TaskRegistration.from_declaration(
            declaration=TaskDeclaration(
                name=name,
                retry=retry,
                timeout=timeout,
            ),
            handler=registration.handler,
            decoder=registration.decoder,
        )

    @property
    def tasks(self) -> Mapping[str, TaskRegistration]:
        return dict(self._tasks)

    @property
    def declarations(self) -> Mapping[str, TaskDeclaration]:
        """The enqueue metadata declared for this queue's task names."""
        return dict(self._declarations)

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

        declaration = self._declarations.get(task)
        registration = self._tasks.get(task)
        policy = declaration.retry if declaration else None
        max_attempts = request.max_attempts
        if policy is not None:
            if max_attempts is not None:
                policy = replace(policy, max_attempts=max_attempts)
            else:
                max_attempts = policy.max_attempts
        elif max_attempts is None:
            # The database column is NOT NULL. A plain registration preserves
            # the old local max-attempts default, while its retry policy stays
            # NULL so a worker can apply its current local backoff/jitter
            # settings when it claims the job. A producer-only queue has only
            # its queue default available here.
            max_attempts = (
                registration.retry.max_attempts
                if registration is not None
                else self.default_retry.max_attempts
            )
        timeout = request.timeout
        if timeout is None:
            if declaration is not None:
                timeout = declaration.timeout
            elif registration is not None:
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
            retry_policy_json=(
                RetryPolicyData.from_policy(policy).to_json()
                if policy is not None
                else None
            ),
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
