"""Exception hierarchy for rqueue.

Two families live here. `RqueueError` and its subclasses report failures of the
queue itself. `Retry`, `PermanentFailure`, and `CancelJob` are control-flow
signals a task handler raises to choose its own outcome (docs/requirements.md §5).
"""

from __future__ import annotations

from datetime import datetime, timedelta

__all__ = [
    "AlreadyEnqueued",
    "CancelJob",
    "ConfigurationError",
    "JobNotFound",
    "LeaseLost",
    "MigrationError",
    "PermanentFailure",
    "Retry",
    "RqueueError",
    "ScheduleNotFound",
    "UnknownTask",
    "ValidationError",
]


class RqueueError(Exception):
    """Base class for every error raised by the queue itself."""


class ConfigurationError(RqueueError):
    """A queue, worker, or scheduler was constructed with invalid settings."""


class ValidationError(RqueueError):
    """A caller-supplied value violated a documented bound or shape (§8)."""


class AlreadyEnqueued(RqueueError):
    """An active job already holds the requested dedupe key.

    Raised only when the caller asked for ``on_conflict="raise"``; the
    alternative is ``on_conflict="return_existing"`` (§3).
    """

    def __init__(self, dedupe_key: str, queue: str, existing_job_id: object) -> None:
        super().__init__(
            f"queue {queue!r} already has an active job for dedupe key "
            f"{dedupe_key!r} (job {existing_job_id})"
        )
        self.dedupe_key = dedupe_key
        self.queue = queue
        self.existing_job_id = existing_job_id


class JobNotFound(RqueueError):
    """No job exists with the requested id."""


class ScheduleNotFound(RqueueError):
    """No periodic schedule exists with the requested name."""


class UnknownTask(RqueueError):
    """A task name has no registered handler on this worker (§4)."""


class LeaseLost(RqueueError):
    """The lease token presented for a write is no longer the current one.

    This is the fencing rejection from §3: a worker that stalled past its lease
    expiry cannot overwrite the attempt that replaced it.
    """


class InvalidStateTransition(RqueueError):
    """A job was not in a state that permits the requested transition."""


class MigrationError(RqueueError):
    """The migration runner refused to apply or verify a migration (§7)."""


class Retry(Exception):
    """Raised by a handler to request another attempt at a chosen time (§5)."""

    def __init__(
        self,
        *,
        delay: float | timedelta | None = None,
        at: datetime | None = None,
        reason: str | None = None,
    ) -> None:
        if delay is not None and at is not None:
            raise ValueError("pass delay or at, not both")
        super().__init__(reason or "retry requested by handler")
        self.delay = delay
        self.at = at
        self.reason = reason


class PermanentFailure(Exception):
    """Raised by a handler to fail a job terminally without further retries."""


class CancelJob(Exception):
    """Raised by a handler to finalize its job as cancelled (§5)."""
