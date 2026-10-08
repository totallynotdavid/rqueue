"""Periodic schedules, fired through the occurrence-key table.

There is no leader election and no schedule-row claim. Each tick works out
which occurrences are due, then tries to insert ``(schedule_id,
occurrence_at)`` and the job it produces *in one transaction*. The unique
constraint decides the winner between replicas, and a scheduler that dies
mid-transaction leaves nothing behind, so the next tick retries the same
occurrence. That is the whole mechanism.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import asyncpg

from rqueue.cron import CronExpression, resolve_timezone
from rqueue.database_errors import raise_if_permanent
from rqueue.errors import ValidationError
from rqueue.limits import (
    MAX_CONCURRENCY_KEY_LENGTH,
    MAX_QUEUE_NAME_LENGTH,
    MAX_SCHEDULE_NAME_LENGTH,
    MAX_TASK_NAME_LENGTH,
    MAX_WORKER_ID_LENGTH,
    validate_key,
    validate_max_attempts,
    validate_name,
    validate_payload,
    validate_priority,
)
from rqueue.metrics import MetricsSink, NullMetricsSink
from rqueue.models import Job, JobRequest, Schedule
from rqueue.queue import Queue

__all__ = ["ScheduleSpec", "Scheduler"]

_LOGGER: Final = logging.getLogger("rqueue.scheduler")

_DB_ERRORS: Final = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError)


@dataclass(frozen=True, slots=True)
class ScheduleSpec:
    """A periodic schedule declared in code and synced to the database."""

    name: str
    task: str
    cron: str
    payload: Any = None
    timezone: str = "UTC"
    queue: str | None = None
    enabled: bool = True
    priority: int = 0
    max_attempts: int = 3
    concurrency_key: str | None = None

    def __post_init__(self) -> None:
        validate_name(
            self.name, kind="schedule name", max_length=MAX_SCHEDULE_NAME_LENGTH
        )
        validate_name(self.task, kind="task name", max_length=MAX_TASK_NAME_LENGTH)
        CronExpression.parse(self.cron)
        resolve_timezone(self.timezone)
        validate_priority(self.priority)
        validate_max_attempts(self.max_attempts)
        validate_key(
            self.concurrency_key,
            kind="concurrency_key",
            max_length=MAX_CONCURRENCY_KEY_LENGTH,
        )
        if self.queue is not None:
            validate_name(
                self.queue, kind="queue name", max_length=MAX_QUEUE_NAME_LENGTH
            )


class Scheduler:
    """Emits durable job occurrences for the enabled schedules.

    Safe to run on several replicas at once: they contend on the occurrence key
    and exactly one wins each occurrence.

    A tick considers *every* enabled schedule in the schema, not only those
    targeting this handle's queue -- a scheduler only enqueues, so one
    deployment can serve several queues. The :class:`~rqueue.Queue` it is built
    from supplies the pool, the schema, and the default queue for schedules
    that do not name one.
    """

    def __init__(
        self,
        queue: Queue,
        *,
        scheduler_id: str,
        schedules: Sequence[ScheduleSpec] = (),
        interval: float = 10.0,
        catchup: int = 1,
        metrics: MetricsSink | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.queue = queue
        self.scheduler_id = validate_name(
            scheduler_id, kind="scheduler_id", max_length=MAX_WORKER_ID_LENGTH
        )
        self.schedules = tuple(schedules)
        if interval <= 0:
            raise ValidationError("scheduler interval must be positive")
        self.interval = float(interval)
        if catchup < 1:
            raise ValidationError("catchup must be at least 1")
        #: How many missed occurrences one tick will fire after an outage. The
        #: default of 1 collapses a long outage into a single catch-up run,
        #: which is what a maintenance job almost always wants; raise it when
        #: every individual occurrence genuinely matters.
        self.catchup = catchup
        self.metrics: MetricsSink = metrics or NullMetricsSink()
        self.logger = logger or _LOGGER
        self._storage = queue.storage
        self._stop = asyncio.Event()
        #: Queues this scheduler has already reported it cannot retract, so the
        #: warning is not repeated on every tick. See `_report_unretractable`.
        self._unretractable: frozenset[str] = frozenset()

    # ------------------------------------------------------------- lifecycle

    async def run(self) -> None:
        """Sync declared schedules, then fire due occurrences until stopped."""
        self._stop.clear()
        await self.sync()
        while not self._stop.is_set():
            try:
                await self.tick()
            except _DB_ERRORS as exc:
                raise_if_permanent(exc)
                self.logger.warning(
                    "rqueue: scheduler %s could not reach PostgreSQL; retrying",
                    self.scheduler_id,
                    exc_info=True,
                )
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(self.interval):
                    await self._stop.wait()

    def stop(self) -> None:
        self._stop.set()

    async def sync(self) -> list[Schedule]:
        """Write the code-declared schedules into the database."""
        stored: list[Schedule] = []
        async with self.queue.pool.acquire() as connection:
            for spec in self.schedules:
                stored.append(
                    await self._storage.upsert_schedule(
                        connection,
                        name=spec.name,
                        queue=spec.queue or self.queue.name,
                        task=spec.task,
                        payload_json=validate_payload(spec.payload),
                        cron=spec.cron,
                        timezone=spec.timezone,
                        enabled=spec.enabled,
                        priority=spec.priority,
                        max_attempts=spec.max_attempts,
                        concurrency_key=spec.concurrency_key,
                    )
                )
        return stored

    # ------------------------------------------------------------------ tick

    async def tick(self, *, now: datetime | None = None) -> list[Job]:
        """Fire every occurrence that is due, and return the jobs created.

        ``now`` is injectable so a test can advance the clock instead of
        waiting a real cron period; production leaves it unset.

        The heartbeat is written per queue this tick actually serves, and for
        no others. One scheduler deployment can serve several queues (see the
        class docstring), so its liveness is a fact about each of them, and a
        readiness probe for a queue this scheduler feeds has to find it.

        A tick with nothing enabled to fire writes no heartbeat at all. There
        is no queue to claim liveness for -- this handle's own queue is a
        default for schedules that do not name one, not a queue this scheduler
        is feeding -- and claiming it would be both untrue and, for a role not
        granted that queue, an ``InsufficientPrivilegeError`` on every tick,
        which would stop :meth:`run`. So a scheduler with no enabled
        schedules reports as unavailable, because it is: nothing is scheduling
        anything.

        The heartbeats for queues this tick does *not* serve are dropped in the
        same breath, which is what makes the two statements above one answer
        rather than two. Disabling the last schedule on a queue is an ordinary
        operator action taken against a scheduler that keeps running, so the
        row it leaves behind is not a dead process's -- nothing will overwrite
        it, and until the staleness window expires a readiness probe for that
        queue answers "live, and this is the instance feeding it" about a
        scheduler that is feeding it nothing. The window exists for a process
        that died and cannot speak; a process that is still ticking says so
        itself.
        """
        moment = now or datetime.now(UTC)
        created: list[Job] = []
        async with self.queue.pool.acquire() as connection:
            schedules = await self._storage.list_schedules(
                connection, enabled_only=True
            )
            serving = sorted({schedule.queue for schedule in schedules})
            await self._reconcile_heartbeats(connection, serving)
            for schedule in schedules:
                created.extend(
                    await self.fire_due(schedule, now=moment, connection=connection)
                )
        if created:
            self.metrics.counter("rqueue.schedule.fired", len(created))
        return created

    async def _reconcile_heartbeats(
        self, connection: asyncpg.Connection, serving: list[str]
    ) -> None:
        """Claim the queues this tick serves, and retract the ones it does not.

        One transaction, and the writes before the retraction: a readiness
        probe landing mid-reconciliation must never see a queue this scheduler
        still serves without a heartbeat.

        The retraction is asked for only when there is something to retract,
        which is what keeps it off the ordinary tick. It needs ``DELETE`` on
        ``runtime_heartbeats``, and the ``SCHEDULE`` capability only started
        granting that alongside 0008 -- a schema migration cannot re-grant
        anything to a role that already exists, so a scheduler role provisioned
        before it has the old grant set until an operator re-provisions it.
        Issuing the statement unconditionally would fail such a role on *every*
        tick, forever, over a row it had no reason to touch.

        When it genuinely cannot retract, the claim is left to go stale rather
        than the tick to fail: scheduling work is the scheduler's job and a
        missing grant is not a reason to stop doing it. That is the behaviour
        from before the retraction existed, so the cost is a readiness answer
        that lags by the staleness window -- said once per distinct set, with
        the remedy, instead of once per tick.
        """
        async with connection.transaction():
            for name in serving:
                await self._storage.record_runtime_heartbeat(
                    connection,
                    kind="scheduler",
                    instance=self.scheduler_id,
                    queue=name,
                )
            stale = await self._storage.stale_heartbeat_queues(
                connection,
                kind="scheduler",
                instance=self.scheduler_id,
                keep=serving,
            )
            if not stale:
                self._unretractable = frozenset()
                return
            try:
                # A savepoint, so a refusal costs the retraction and not the
                # heartbeats written above it.
                async with connection.transaction():
                    await self._storage.clear_runtime_heartbeats(
                        connection,
                        kind="scheduler",
                        instance=self.scheduler_id,
                        keep=serving,
                    )
            except asyncpg.InsufficientPrivilegeError:
                self._report_unretractable(stale)
                return
        self._unretractable = frozenset()

    def _report_unretractable(self, stale: list[str]) -> None:
        """Say it once per distinct set, not once per tick.

        The rows stay until something can delete them, so the condition does
        not clear on its own and an unconditional log would repeat for as long
        as the process runs.
        """
        if frozenset(stale) == self._unretractable:
            return
        self._unretractable = frozenset(stale)
        self.logger.warning(
            "rqueue: scheduler %s cannot retract its heartbeat for %s: no DELETE "
            "on runtime_heartbeats. Readiness will report this scheduler as "
            "feeding those queues until the staleness window expires. Re-run "
            "provision_role for this scheduler's role to grant it.",
            self.scheduler_id,
            ", ".join(stale),
        )

    async def fire_due(
        self,
        schedule: Schedule,
        *,
        now: datetime | None = None,
        connection: asyncpg.Connection | None = None,
    ) -> list[Job]:
        """Fire the due occurrences of one schedule."""
        if connection is None:
            async with self.queue.pool.acquire() as borrowed:
                return await self.fire_due(schedule, now=now, connection=borrowed)

        moment = now or datetime.now(UTC)
        last = await self._storage.last_occurrence(connection, schedule.id)
        created: list[Job] = []
        for occurrence_at in self.due_occurrences(schedule, now=moment, last=last):
            job = await self.fire_occurrence(
                connection, schedule=schedule, occurrence_at=occurrence_at
            )
            if job is None:
                self.logger.debug(
                    "rqueue: occurrence %s of schedule %s was already fired",
                    occurrence_at,
                    schedule.name,
                )
                continue
            created.append(job)
        return created

    def due_occurrences(
        self,
        schedule: Schedule,
        *,
        now: datetime,
        last: datetime | None,
    ) -> list[datetime]:
        """Occurrence instants that are due and not yet recorded, oldest first.

        The lower bound is the last recorded occurrence, or -- for a schedule
        that has never fired -- the moment the schedule row was created. Without
        that anchor, deploying a new ``@daily`` schedule at noon would
        immediately fire that morning's midnight run.
        """
        cron = CronExpression.parse(schedule.cron)
        tz = resolve_timezone(schedule.timezone)
        floor = last if last is not None else schedule.created_at
        found: list[datetime] = []
        cursor = now
        while len(found) < self.catchup:
            occurrence = cron.previous(cursor, tz=tz)
            if occurrence is None or occurrence <= floor:
                break
            found.append(occurrence)
            cursor = occurrence - timedelta(minutes=1)
        found.reverse()
        return found

    async def fire_occurrence(
        self,
        connection: asyncpg.Connection,
        *,
        schedule: Schedule,
        occurrence_at: datetime,
    ) -> Job | None:
        """Record one occurrence and its job atomically, or lose the race.

        Runs on the caller's connection so a test -- or an application that
        wants the occurrence inside a wider transaction -- controls the commit.
        """
        spec = self.queue.build_insert(
            JobRequest(
                task=schedule.task,
                payload=schedule.payload,
                priority=schedule.priority,
                max_attempts=schedule.max_attempts,
                concurrency_key=schedule.concurrency_key,
                metadata={
                    "rqueue.schedule": schedule.name,
                    "rqueue.occurrence_at": occurrence_at.isoformat(),
                },
            ),
            queue_name=schedule.queue,
        )
        return await self._storage.fire_occurrence(
            connection,
            schedule_id=schedule.id,
            occurrence_at=occurrence_at,
            spec=spec,
            fired_by=self.scheduler_id,
        )

    # ------------------------------------------------------------ inspection

    async def occurrence_count(self, schedule_id: uuid.UUID) -> int:
        async with self.queue.pool.acquire() as connection:
            return await self._storage.occurrence_count(connection, schedule_id)

    async def stored_schedules(self, *, enabled_only: bool = False) -> list[Schedule]:
        async with self.queue.pool.acquire() as connection:
            return await self._storage.list_schedules(
                connection, enabled_only=enabled_only
            )
