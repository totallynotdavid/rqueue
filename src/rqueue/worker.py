"""The consumer side: claim, run, and finalize jobs under a lease."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import asyncpg

from rqueue.context import TaskContext
from rqueue.errors import (
    CancelJob,
    ConfigurationError,
    LeaseLost,
    PermanentFailure,
    Retry,
    UnknownTask,
    ValidationError,
)
from rqueue.executor import bounded_default_executor
from rqueue.limits import (
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_ERROR_TYPE_LENGTH,
    MAX_WORKER_ID_LENGTH,
    QUEUE_WILDCARD,
    truncate,
    validate_batch_size,
    validate_concurrency,
    validate_lease_seconds,
    validate_name,
)
from rqueue.metrics import MetricsSink, NullMetricsSink
from rqueue.models import Job
from rqueue.queue import Queue
from rqueue.storage import ClaimedJob
from rqueue.tasks import TaskRegistration

__all__ = ["Worker"]

_LOGGER: Final = logging.getLogger("rqueue.worker")

#: How long the loop waits after a database error before trying again. A
#: PostgreSQL restart shows up here as a burst of connection errors; the worker
#: reconnects through the pool and resumes, so a restart costs latency and
#: never a job (§10.7).
_ERROR_BACKOFF_SECONDS: Final = 1.0

_DB_ERRORS: Final = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError)


class Worker:
    """Runs registered handlers for one queue with bounded concurrency.

    The worker never holds a database transaction open across user code (§3):
    claiming, heartbeating, and finalizing each borrow a pooled connection for
    the duration of one short statement and give it straight back.
    """

    def __init__(
        self,
        queue: Queue,
        *,
        worker_id: str,
        concurrency: int = 4,
        batch_size: int | None = None,
        lease_duration: float = 30.0,
        heartbeat_interval: float | None = None,
        poll_interval: float = 1.0,
        shutdown_timeout: float = 30.0,
        strict_tasks: bool = True,
        install_default_executor: bool = True,
        executor_max_workers: int | None = None,
        recovery_batch: int = 100,
        metrics: MetricsSink | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.queue = queue
        self.worker_id = validate_name(
            worker_id, kind="worker_id", max_length=MAX_WORKER_ID_LENGTH
        )
        self.concurrency = validate_concurrency(concurrency)
        self.batch_size = validate_batch_size(batch_size or self.concurrency)
        self.lease_duration = validate_lease_seconds(lease_duration)
        # Three heartbeats per lease: one may be lost to a slow query or a GC
        # pause without a healthy worker's lease lapsing.
        self.heartbeat_interval = float(
            heartbeat_interval
            if heartbeat_interval is not None
            else max(self.lease_duration / 3.0, 0.05)
        )
        if self.heartbeat_interval >= self.lease_duration:
            raise ConfigurationError(
                "heartbeat_interval must be shorter than lease_duration"
            )
        if poll_interval <= 0:
            raise ValidationError("poll_interval must be positive")
        self.poll_interval = float(poll_interval)
        self.shutdown_timeout = float(shutdown_timeout)
        self.strict_tasks = strict_tasks
        self.install_default_executor = install_default_executor
        self.executor_max_workers = executor_max_workers or self.concurrency
        self.recovery_batch = recovery_batch
        self.metrics: MetricsSink = metrics or NullMetricsSink()
        self.logger = logger or _LOGGER

        self._storage = queue.storage
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()
        self._running: dict[uuid.UUID, asyncio.Task[None]] = {}
        #: Leases this worker still owns. An entry survives its task only when
        #: the task was cancelled from outside, which is how shutdown knows
        #: which attempts still need handing back.
        self._leases: dict[uuid.UUID, ClaimedJob] = {}
        self._started = asyncio.Event()

    # ------------------------------------------------------------- lifecycle

    async def run(self, *, until_idle: bool = False) -> None:
        """Serve the queue until :meth:`stop` is called.

        With ``until_idle`` the loop returns once a poll finds no claimable
        work and nothing is in flight -- what :meth:`drain` exposes.
        """
        self._stop.clear()
        await self._check_registry()
        async with self._runtime():
            self._started.set()
            try:
                await self._loop(until_idle=until_idle)
            finally:
                self._started.clear()
                await self._shutdown_inflight()

    async def drain(self, *, timeout: float | None = None) -> None:
        """Run until the queue has no immediately claimable work left."""
        if timeout is None:
            await self.run(until_idle=True)
            return
        async with asyncio.timeout(timeout):
            await self.run(until_idle=True)

    def stop(self) -> None:
        """Ask the loop to finish its in-flight jobs and return."""
        self._stop.set()
        self._wake.set()

    async def wait_started(self) -> None:
        await self._started.wait()

    @asynccontextmanager
    async def _runtime(self) -> AsyncIterator[None]:
        """Install the bounded executor and the NOTIFY listener for this run."""
        with contextlib.ExitStack() as stack:
            if self.install_default_executor:
                stack.enter_context(
                    bounded_default_executor(
                        self.executor_max_workers,
                        thread_name_prefix=f"rqueue-{self.worker_id}",
                    )
                )
            async with self._listener():
                yield

    @asynccontextmanager
    async def _listener(self) -> AsyncIterator[None]:
        """Subscribe to the wake channel, if a connection can be spared.

        Polling is the source of truth (§3). A listener that cannot be
        established -- an exhausted pool, a role without LISTEN rights -- costs
        latency and nothing else, so it is logged and skipped.
        """
        channel = self._storage.notify_channel
        connection: asyncpg.Connection | None = None
        try:
            connection = await self.queue.pool.acquire()
            await connection.add_listener(channel, self._on_notify)
        except Exception:
            self.logger.warning(
                "rqueue: could not LISTEN on %s; falling back to polling only",
                channel,
                exc_info=True,
            )
            if connection is not None:
                with contextlib.suppress(Exception):
                    await self.queue.pool.release(connection)
            yield
            return
        try:
            yield
        finally:
            with contextlib.suppress(Exception):
                await connection.remove_listener(channel, self._on_notify)
            with contextlib.suppress(Exception):
                await self.queue.pool.release(connection)

    def _on_notify(
        self, connection: object, pid: int, channel: str, payload: str
    ) -> None:
        # A job wake-up carries the queue it landed in; resuming the '*'
        # wildcard carries the wildcard, since it concerns every queue. No real
        # queue can be named '*' (rqueue.limits), so the two never collide.
        if payload not in (self.queue.name, QUEUE_WILDCARD):
            return
        self.metrics.counter("rqueue.wakeup", source="notify", queue=self.queue.name)
        self._wake.set()

    async def _check_registry(self) -> None:
        """Refuse to start if the queue holds work this deployment cannot run.

        With ``strict_tasks=False`` the unknown names are logged and left
        pending for whichever deployment does register them. Either way the
        claim query filters on this worker's registered names, so an unknown
        task is never leased and never lost.
        """
        if not self.queue.tasks:
            raise ConfigurationError(
                f"worker {self.worker_id!r} has no registered tasks"
            )
        async with self.queue.pool.acquire() as connection:
            queued = await self._storage.distinct_pending_tasks(
                connection, queue=self.queue.name
            )
        unknown = sorted(set(queued) - set(self.queue.tasks))
        if not unknown:
            return
        message = (
            f"queue {self.queue.name!r} has active jobs for unregistered tasks: "
            f"{', '.join(unknown)}"
        )
        if self.strict_tasks:
            raise UnknownTask(message)
        self.logger.warning("rqueue: %s (leaving them pending)", message)

    # ------------------------------------------------------------- main loop

    async def _loop(self, *, until_idle: bool) -> None:
        while not self._stop.is_set():
            try:
                claimed = await self._tick()
            except _DB_ERRORS:
                self.logger.warning(
                    "rqueue: worker %s could not reach PostgreSQL; retrying",
                    self.worker_id,
                    exc_info=True,
                )
                await self._sleep(_ERROR_BACKOFF_SECONDS)
                continue
            if until_idle and claimed == 0 and not self._running:
                return
            await self._await_work()

    async def _tick(self) -> int:
        async with self.queue.pool.acquire() as connection:
            await self._storage.record_runtime_heartbeat(
                connection,
                kind="worker",
                instance=self.worker_id,
                queue=self.queue.name,
            )
            recovered = await self._storage.recover_expired_leases(
                connection, queue=self.queue.name, limit=self.recovery_batch
            )
            if recovered:
                self.metrics.counter(
                    "rqueue.lease.expired",
                    len(recovered),
                    queue=self.queue.name,
                    worker_id=self.worker_id,
                )
                self.logger.info(
                    "rqueue: recovered %d expired lease(s) on queue %s",
                    len(recovered),
                    self.queue.name,
                )

            stats = await self._storage.stats(connection, queue=self.queue.name)
            self.metrics.gauge("rqueue.queue.depth", stats.depth, queue=self.queue.name)
            self.metrics.gauge("rqueue.queue.ready", stats.ready, queue=self.queue.name)
            if stats.oldest_ready_age_seconds is not None:
                self.metrics.gauge(
                    "rqueue.queue.oldest_ready_age",
                    stats.oldest_ready_age_seconds,
                    queue=self.queue.name,
                )

            capacity = self.concurrency - len(self._running)
            if capacity <= 0 or self._stop.is_set():
                return 0

            started = time.monotonic()
            try:
                claimed = await self._storage.claim(
                    connection,
                    queue=self.queue.name,
                    worker_id=self.worker_id,
                    tasks=sorted(self.queue.tasks),
                    limit=min(capacity, self.batch_size),
                    lease_seconds=self.lease_duration,
                )
            except asyncpg.DeadlockDetectedError:
                # Two workers reached for the same pair of concurrency slots in
                # opposite orders. PostgreSQL broke the tie; nothing was
                # claimed, so the next poll simply tries again.
                self.logger.info(
                    "rqueue: claim deadlocked on concurrency slots; retrying"
                )
                return 0
            finally:
                self.metrics.timing(
                    "rqueue.claim.latency",
                    time.monotonic() - started,
                    queue=self.queue.name,
                    worker_id=self.worker_id,
                )

        if claimed:
            self.metrics.counter(
                "rqueue.claim.jobs",
                len(claimed),
                queue=self.queue.name,
                worker_id=self.worker_id,
            )
        for entry in claimed:
            job_id = entry.job.id
            self._leases[job_id] = entry
            task = asyncio.create_task(
                self._execute(entry), name=f"rqueue-job-{job_id}"
            )
            self._running[job_id] = task
            task.add_done_callback(
                lambda _task, job_id=job_id: self._forget(job_id)  # type: ignore[misc]
            )
        return len(claimed)

    def _forget(self, job_id: uuid.UUID) -> None:
        self._running.pop(job_id, None)
        self._wake.set()

    async def _await_work(self) -> None:
        """Block until something plausibly changed, or the poll interval passed."""
        waiters = [
            asyncio.create_task(self._wake.wait()),
            asyncio.create_task(self._stop.wait()),
        ]
        try:
            await asyncio.wait(
                waiters,
                timeout=self.poll_interval,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for waiter in waiters:
                waiter.cancel()
            for waiter in waiters:
                with contextlib.suppress(asyncio.CancelledError):
                    await waiter
        if not self._wake.is_set():
            self.metrics.counter("rqueue.wakeup", source="poll", queue=self.queue.name)
        self._wake.clear()

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(seconds):
                await self._stop.wait()

    async def _shutdown_inflight(self) -> None:
        """Give running jobs a bounded chance to finish, then hand them back.

        Anything still leased after the cancel is returned to ``pending``
        immediately rather than left for lease expiry, so a rolling restart
        costs a round trip instead of a lease duration. If that write cannot be
        made, expiry recovery is still the backstop.
        """
        # A hard `task.cancel()` on run() means "stop now", not "stop in
        # shutdown_timeout seconds", so the grace period collapses.
        current = asyncio.current_task()
        grace = (
            0.0
            if current is not None and current.cancelling()
            else self.shutdown_timeout
        )
        if self._running:
            _, pending = await asyncio.wait(list(self._running.values()), timeout=grace)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=self.shutdown_timeout)

        for entry in list(self._leases.values()):
            self._leases.pop(entry.job.id, None)
            with contextlib.suppress(*_DB_ERRORS):
                await self._safe_finalize(
                    self._retry_now(
                        entry.job,
                        entry.lease_token,
                        "WorkerShutdown",
                        "worker stopped mid-attempt",
                    )
                )

    # ---------------------------------------------------------------- runner

    async def _execute(self, claimed: ClaimedJob) -> None:
        job = claimed.job
        token = claimed.lease_token
        cancel_event = asyncio.Event()
        lease_lost = asyncio.Event()

        try:
            registration = self.queue.get_task(job.task)
        except UnknownTask as exc:
            # Only reachable if a handler is unregistered between the claim
            # filter and here; fail durably rather than sit on the lease.
            await self._safe_finalize(
                self._fail_terminal(job, token, "UnknownTask", str(exc))
            )
            self._leases.pop(job.id, None)
            return

        try:
            payload = registration.decoder(job.payload)
        except Exception as exc:
            # §4: decode failures are non-retryable and become a durable failed
            # job. The same payload cannot become valid on a later attempt.
            self.logger.warning(
                "rqueue: payload for job %s failed to decode", job.id, exc_info=True
            )
            await self._safe_finalize(
                self._fail_terminal(job, token, "PayloadDecodeError", _describe(exc))
            )
            self.metrics.counter(
                "rqueue.job.failed", queue=self.queue.name, task=job.task
            )
            self._leases.pop(job.id, None)
            return

        context = TaskContext(
            job_id=job.id,
            queue=job.queue,
            task=job.task,
            attempt=job.attempt,
            max_attempts=job.max_attempts,
            metadata=job.metadata,
            heartbeat=lambda: self._beat(job, token, cancel_event),
            cancel_event=cancel_event,
            logger=self.logger,
        )

        handler_task = asyncio.create_task(
            self._invoke(registration, payload, context, job),
            name=f"rqueue-handler-{job.id}",
        )
        beat_task = asyncio.create_task(
            self._heartbeat_loop(job, token, cancel_event, lease_lost, handler_task),
            name=f"rqueue-heartbeat-{job.id}",
        )

        started = time.monotonic()
        handed_back = False
        try:
            try:
                await handler_task
            except asyncio.CancelledError:
                if lease_lost.is_set():
                    self.logger.warning(
                        "rqueue: abandoning job %s; another attempt owns its lease",
                        job.id,
                    )
                elif cancel_event.is_set():
                    await self._safe_finalize(
                        self._cancel(job, token, "cancelled by request")
                    )
                    self.metrics.counter(
                        "rqueue.job.cancelled", queue=self.queue.name, task=job.task
                    )
                else:
                    # This worker's own task was cancelled -- a shutdown past
                    # the grace period. Leave the lease in _leases so
                    # _shutdown_inflight hands the attempt back.
                    handler_task.cancel()
                    handed_back = True
            except Exception as exc:
                await self._on_exception(job, token, registration, exc)
            else:
                await self._safe_finalize(self._succeed(job, token))
        finally:
            beat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await beat_task
            self.metrics.timing(
                "rqueue.handler.duration",
                time.monotonic() - started,
                queue=self.queue.name,
                task=job.task,
            )
            if not handed_back:
                self._leases.pop(job.id, None)
        if handed_back:
            # Catching CancelledError above ran cleanup, but a caught
            # CancelledError that is not re-raised lets this task complete
            # normally. Re-raise so it still reports as cancelled to whatever
            # is holding it, matching the cancellation _shutdown_inflight asked for.
            raise asyncio.CancelledError

    async def _invoke(
        self,
        registration: TaskRegistration,
        payload: Any,
        context: TaskContext,
        job: Job,
    ) -> None:
        timeout = job.timeout_seconds or registration.timeout
        if timeout is None:
            await registration.handler(payload, context)
            return
        # §5: a timeout cancels the handler, but proves nothing about whether
        # an external side effect already happened. The job retries under the
        # normal policy; handlers are required to be idempotent (§3).
        async with asyncio.timeout(timeout):
            await registration.handler(payload, context)

    async def _on_exception(
        self,
        job: Job,
        token: uuid.UUID,
        registration: TaskRegistration,
        exc: BaseException,
    ) -> None:
        if isinstance(exc, Retry):
            await self._safe_finalize(
                self._reschedule(
                    job,
                    token,
                    _retry_instant(exc, registration, job),
                    "Retry",
                    _describe(exc),
                )
            )
            self.metrics.counter(
                "rqueue.job.retried", queue=self.queue.name, task=job.task
            )
            return
        if isinstance(exc, CancelJob):
            await self._safe_finalize(self._cancel(job, token, _describe(exc)))
            self.metrics.counter(
                "rqueue.job.cancelled", queue=self.queue.name, task=job.task
            )
            return

        self.logger.warning(
            "rqueue: job %s attempt %d raised %s",
            job.id,
            job.attempt,
            type(exc).__name__,
            exc_info=exc,
        )
        if isinstance(exc, PermanentFailure) or not registration.retry.should_retry(
            exc, attempt=job.attempt
        ):
            await self._safe_finalize(
                self._fail_terminal(job, token, type(exc).__name__, _describe(exc))
            )
            self.metrics.counter(
                "rqueue.job.failed", queue=self.queue.name, task=job.task
            )
            return

        await self._safe_finalize(
            self._reschedule(
                job,
                token,
                registration.retry.next_attempt_at(job.attempt),
                type(exc).__name__,
                _describe(exc),
            )
        )
        self.metrics.counter("rqueue.job.retried", queue=self.queue.name, task=job.task)

    # ------------------------------------------------------ lease bookkeeping

    async def _beat(
        self, job: Job, token: uuid.UUID, cancel_event: asyncio.Event
    ) -> bool:
        async with self.queue.pool.acquire() as connection:
            requested = await self._storage.heartbeat(
                connection,
                job_id=job.id,
                lease_token=token,
                lease_seconds=self.lease_duration,
            )
        if requested:
            cancel_event.set()
        return requested

    async def _heartbeat_loop(
        self,
        job: Job,
        token: uuid.UUID,
        cancel_event: asyncio.Event,
        lease_lost: asyncio.Event,
        handler_task: asyncio.Task[None],
    ) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                requested = await self._beat(job, token, cancel_event)
            except LeaseLost:
                # Another attempt owns this job now. Stop burning capacity on
                # work whose result can no longer be written.
                lease_lost.set()
                handler_task.cancel()
                return
            except _DB_ERRORS:
                # A transient database problem must not cancel a healthy
                # handler; the lease may well outlive the outage.
                self.logger.warning(
                    "rqueue: heartbeat for job %s failed; will retry",
                    job.id,
                    exc_info=True,
                )
                continue
            if requested:
                handler_task.cancel()
                return

    # ---------------------------------------------------------- finalization

    async def _succeed(self, job: Job, token: uuid.UUID) -> None:
        async with self.queue.pool.acquire() as connection:
            await self._storage.complete(connection, job_id=job.id, lease_token=token)
        self.metrics.counter(
            "rqueue.job.succeeded", queue=self.queue.name, task=job.task
        )

    async def _fail_terminal(
        self, job: Job, token: uuid.UUID, error_type: str, message: str
    ) -> None:
        async with self.queue.pool.acquire() as connection:
            await self._storage.fail_terminal(
                connection,
                job_id=job.id,
                lease_token=token,
                error_type=_bound_type(error_type),
                error_message=_bound_message(message) or error_type,
            )

    async def _cancel(self, job: Job, token: uuid.UUID, reason: str) -> None:
        async with self.queue.pool.acquire() as connection:
            await self._storage.cancel_leased(
                connection,
                job_id=job.id,
                lease_token=token,
                reason=_bound_message(reason) or "cancelled",
            )

    async def _reschedule(
        self,
        job: Job,
        token: uuid.UUID,
        retry_at: datetime,
        error_type: str,
        message: str,
    ) -> None:
        async with self.queue.pool.acquire() as connection:
            await self._storage.reschedule(
                connection,
                job_id=job.id,
                lease_token=token,
                retry_at=retry_at,
                error_type=_bound_type(error_type),
                error_message=_bound_message(message) or error_type,
            )

    async def _retry_now(
        self, job: Job, token: uuid.UUID, error_type: str, message: str
    ) -> None:
        if job.attempt >= job.max_attempts:
            await self._fail_terminal(job, token, error_type, message)
            return
        await self._reschedule(job, token, datetime.now(UTC), error_type, message)

    async def _safe_finalize(self, coro: Awaitable[None]) -> None:
        """Run one finalizing write, tolerating a lease we no longer hold.

        Losing that race is not an error condition: it is the fencing
        guarantee working, and the newer attempt owns the outcome.
        """
        try:
            await coro
        except LeaseLost:
            self.logger.warning(
                "rqueue: worker %s could not finalize a job it no longer leases",
                self.worker_id,
            )


def _describe(exc: BaseException) -> str:
    """One bounded, sanitized line about a failure.

    Full tracebacks go to the structured worker log (§5); the row keeps only
    enough for an operator to recognize the failure.
    """
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _bound_message(message: str | None) -> str | None:
    return truncate(message, MAX_ERROR_MESSAGE_LENGTH)


def _bound_type(error_type: str) -> str:
    return truncate(error_type, MAX_ERROR_TYPE_LENGTH) or "Error"


def _retry_instant(exc: Retry, registration: TaskRegistration, job: Job) -> datetime:
    if exc.at is not None:
        return exc.at
    if exc.delay is not None:
        seconds = (
            exc.delay.total_seconds()
            if isinstance(exc.delay, timedelta)
            else float(exc.delay)
        )
        return datetime.now(UTC) + timedelta(seconds=max(seconds, 0.0))
    return registration.retry.next_attempt_at(job.attempt)
