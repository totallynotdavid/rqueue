"""Inspection and administration for operators and maintenance jobs.

:class:`~rqueue.context.TaskContext` does not expose these to a handler.
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
        """List jobs, newest first. ``None`` and ``'*'`` both mean every queue."""
        async with self.pool.acquire() as connection:
            return await self.storage.list_jobs(
                connection,
                queue=None if queue is None else validate_queue_target(queue),
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
        """Counts for one queue, or summed over every queue for ``'*'``."""
        target = validate_queue_target(queue)
        async with self.pool.acquire() as connection:
            return await self.storage.stats(connection, queue=target)

    async def cancel_job(self, job_id: uuid.UUID) -> Job:
        """Cancel a pending job, or ask a leased job's worker to stop it."""
        async with self.pool.acquire() as connection:
            return await self.storage.cancel(connection, job_id)

    async def retry_job(
        self,
        job_id: uuid.UUID,
        *,
        scheduled_at: datetime | None = None,
        additional_attempts: int = 1,
    ) -> Job:
        """Make a terminal job runnable again by raising ``max_attempts``."""
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
        queue: str,
        retention: timedelta,
        states: Sequence[JobState | str] | None = None,
        limit: int = 10000,
        now: datetime | None = None,
    ) -> int:
        """Delete terminal jobs that finished longer ago than ``retention``.

        ``queue`` is required, and ``None`` raises
        :class:`~rqueue.ValidationError`. Naming a live state raises too.
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

    async def pause_queue(self, name: str) -> QueuePause:
        """Stop a queue admitting new work on every worker. ``'*'`` is every queue."""
        target = validate_queue_target(name)
        async with self.pool.acquire() as connection:
            return await self.storage.pause_queue(connection, queue=target)

    async def resume_queue(self, name: str) -> list[str]:
        """Let a queue admit work again, and return the queues resumed.

        ``'*'`` also resumes queues paused by name. Resuming one queue by name
        does not lift a wildcard pause.
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
