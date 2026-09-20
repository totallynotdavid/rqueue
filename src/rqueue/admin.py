"""Inspection and administration operations (docs/requirements.md §4, §7).

These are deliberately outside :class:`~rqueue.context.TaskContext`: a handler
cannot retry, cancel, or purge anything. They are for operators, admin
endpoints, and maintenance jobs.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from rqueue.limits import validate_priority, validate_queue_target
from rqueue.models import Attempt, Job, JobState, QueuePause, QueueStats, Schedule
from rqueue.storage import Storage

if TYPE_CHECKING:
    import asyncpg

__all__ = ["Admin"]


class Admin:
    """Administration operations against one schema, over a pool."""

    def __init__(self, pool: asyncpg.Pool, *, schema: str = "task_queue") -> None:
        self.pool = pool
        self.storage = Storage(schema)

    @property
    def schema(self) -> str:
        return self.storage.schema

    # ----------------------------------------------------------- inspection

    async def get_job(self, job_id: uuid.UUID) -> Job | None:
        async with self.pool.acquire() as connection:
            return await self.storage.get_job(connection, job_id)

    async def list_jobs(
        self,
        *,
        queue: str | None = None,
        states: Sequence[JobState | str] | None = None,
        task: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        async with self.pool.acquire() as connection:
            return await self.storage.list_jobs(
                connection,
                queue=queue,
                states=states,
                task=task,
                limit=limit,
                offset=offset,
            )

    async def attempts(self, job_id: uuid.UUID) -> list[Attempt]:
        """The immutable attempt history for one job."""
        async with self.pool.acquire() as connection:
            return await self.storage.attempts(connection, job_id)

    async def stats(self, queue: str) -> QueueStats:
        async with self.pool.acquire() as connection:
            return await self.storage.stats(connection, queue=queue)

    # --------------------------------------------------------- state changes

    async def cancel_job(self, job_id: uuid.UUID) -> Job:
        """Cancel a job.

        A pending job is cancelled outright and will never run. A leased job is
        only *asked* to stop: §5 makes cancellation cooperative, so the lease
        holder finalizes it, or lease expiry does.
        """
        async with self.pool.acquire() as connection:
            return await self.storage.cancel(connection, job_id)

    async def retry_job(
        self,
        job_id: uuid.UUID,
        *,
        scheduled_at: datetime | None = None,
        additional_attempts: int = 1,
    ) -> Job:
        """Make a terminal job runnable again -- the operator retry from §3.

        This is the only path out of a terminal state. The attempt counter keeps
        counting rather than resetting, so the immutable attempt history stays
        coherent; the fresh budget comes from raising ``max_attempts`` by
        ``additional_attempts``.
        """
        async with self.pool.acquire() as connection:
            return await self.storage.retry_terminal(
                connection,
                job_id,
                scheduled_at=scheduled_at,
                additional_attempts=additional_attempts,
            )

    async def purge(
        self,
        *,
        queue: str | None = None,
        retention: timedelta,
        states: Sequence[JobState | str] | None = None,
        limit: int = 10000,
        now: datetime | None = None,
    ) -> int:
        """Delete terminal jobs that finished longer ago than ``retention``.

        Attempt records and occurrence rows are removed with their job by
        ``ON DELETE CASCADE``, so retention has one knob rather than three.

        The delete itself is done by the ``purge_terminal_jobs`` routine, which
        re-derives every bound for itself, so this is the operation
        :attr:`~rqueue.Capability.PURGE` authorizes -- a role holding it has no
        ``DELETE`` on ``jobs`` and does not need one. Naming a queue is the
        cheaper call; omitting it purges each queue that has anything to purge,
        spending ``limit`` across them as a single budget.

        Only terminal states may be named. Asking for a live one raises rather
        than quietly matching nothing, because a caller that asked for the
        wrong thing should learn that it did.
        """
        cutoff = (now or datetime.now(UTC)) - retention
        state_values = (
            tuple(str(state) for state in states) if states is not None else None
        )
        async with self.pool.acquire() as connection:
            if state_values is None:
                return await self.storage.purge(
                    connection, queue=queue, older_than=cutoff, limit=limit
                )
            return await self.storage.purge(
                connection,
                queue=queue,
                older_than=cutoff,
                states=state_values,
                limit=limit,
            )

    # ------------------------------------------------------------ queue pause

    async def pause_queue(self, name: str) -> QueuePause:
        """Stop a queue admitting new work, durably and fleet-wide.

        ``name='*'`` pauses every queue. Unlike :meth:`rqueue.Worker.stop`,
        which ends one worker instance, this is a row every worker's claim
        query reads, so it applies to replicas that have never heard of this
        call and survives a restart of all of them. It does not touch work
        already leased: an in-flight attempt runs to its normal end.
        """
        target = validate_queue_target(name)
        async with self.pool.acquire() as connection:
            return await self.storage.pause_queue(connection, queue=target)

    async def resume_queue(self, name: str) -> list[str]:
        """Let a queue admit work again, and return the queues resumed.

        Resuming takes effect on the next claim, not on the next poll: the
        pause lives in the claim query, so there is no per-worker state to
        catch up. The NOTIFY this write fires only saves an *idle* worker the
        rest of its poll interval.

        ``name='*'`` resumes everything, including queues paused by name --
        a "resume all" that silently left some queues paused would be a trap.
        Resuming one queue by name, in contrast, does not lift a wildcard
        pause: the narrower call cannot punch a hole in the broader one.
        """
        target = validate_queue_target(name)
        async with self.pool.acquire() as connection:
            return await self.storage.resume_queue(connection, queue=target)

    async def paused_queues(self) -> list[QueuePause]:
        """Every queue currently paused, with the instant it was paused."""
        async with self.pool.acquire() as connection:
            return await self.storage.paused_queues(connection)

    async def is_queue_paused(self, name: str) -> bool:
        """Whether ``name`` is paused, by its own row or by the wildcard."""
        target = validate_queue_target(name)
        async with self.pool.acquire() as connection:
            return await self.storage.is_queue_paused(connection, queue=target)

    # -------------------------------------------------------------- schedules

    async def list_schedules(self, *, enabled_only: bool = False) -> list[Schedule]:
        async with self.pool.acquire() as connection:
            return await self.storage.list_schedules(
                connection, enabled_only=enabled_only
            )

    async def get_schedule(self, name: str) -> Schedule:
        async with self.pool.acquire() as connection:
            return await self.storage.get_schedule(connection, name)

    async def set_schedule_enabled(self, name: str, enabled: bool) -> Schedule:
        async with self.pool.acquire() as connection:
            schedule = await self.storage.get_schedule(connection, name)
            return await self.storage.upsert_schedule(
                connection,
                name=schedule.name,
                queue=schedule.queue,
                task=schedule.task,
                payload_json=_dumps(schedule.payload),
                cron=schedule.cron,
                timezone=schedule.timezone,
                enabled=enabled,
                priority=validate_priority(schedule.priority),
                max_attempts=schedule.max_attempts,
                concurrency_key=schedule.concurrency_key,
            )

    async def delete_schedule(self, name: str) -> bool:
        async with self.pool.acquire() as connection:
            return await self.storage.delete_schedule(connection, name)


def _dumps(value: Any) -> str:
    import json

    return json.dumps(value, allow_nan=False, separators=(",", ":"))
