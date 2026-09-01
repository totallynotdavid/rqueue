"""What a handler is given (REQUIREMENTS.md §4).

:class:`TaskContext` exposes job identity, the attempt number, a lease-aware
heartbeat, a logger with structured fields, and cooperative cancellation state
-- and nothing else. There is no connection, no SQL, and no way to move the job
between states from inside a handler; outcomes are expressed by returning or by
raising one of the signals in :mod:`rqueue.errors`.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final

__all__ = ["TaskContext"]

_LOGGER: Final = logging.getLogger("rqueue.task")


class TaskContext:
    """Per-attempt handler context."""

    __slots__ = (
        "_cancel_event",
        "_heartbeat",
        "_log_fields",
        "attempt",
        "job_id",
        "logger",
        "max_attempts",
        "metadata",
        "queue",
        "task",
    )

    def __init__(
        self,
        *,
        job_id: uuid.UUID,
        queue: str,
        task: str,
        attempt: int,
        max_attempts: int,
        metadata: Mapping[str, Any],
        heartbeat: Callable[[], Awaitable[bool]],
        cancel_event: asyncio.Event,
        logger: logging.Logger | None = None,
    ) -> None:
        self.job_id = job_id
        self.queue = queue
        self.task = task
        self.attempt = attempt
        self.max_attempts = max_attempts
        self.metadata = dict(metadata)
        self._heartbeat = heartbeat
        self._cancel_event = cancel_event
        self._log_fields: dict[str, Any] = {
            "job_id": str(job_id),
            "queue": queue,
            "task": task,
            "attempt": attempt,
        }
        self.logger = logging.LoggerAdapter(logger or _LOGGER, self._log_fields)

    @property
    def log_fields(self) -> Mapping[str, Any]:
        """The structured fields identifying this attempt, for the app's logger."""
        return dict(self._log_fields)

    @property
    def cancel_requested(self) -> bool:
        """Whether someone has asked this job to stop.

        Cancellation is cooperative (§5): a handler that ignores this keeps
        running until its lease expires or it finishes on its own.
        """
        return self._cancel_event.is_set()

    @property
    def is_last_attempt(self) -> bool:
        return self.attempt >= self.max_attempts

    async def heartbeat(self) -> bool:
        """Extend the lease, and report whether cancellation was requested.

        Raises :class:`~rqueue.errors.LeaseLost` if this attempt no longer owns
        the job -- a long-running handler can use that to abandon work another
        worker has already taken over.
        """
        return await self._heartbeat()

    async def wait_for_cancel(self, timeout: float | None = None) -> bool:
        """Sleep until cancellation is requested, or until ``timeout``."""
        if timeout is None:
            await self._cancel_event.wait()
            return True
        try:
            async with asyncio.timeout(timeout):
                await self._cancel_event.wait()
        except TimeoutError:
            return False
        return True
