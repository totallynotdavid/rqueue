"""Durable value objects: job state, jobs, attempts, schedules, statistics."""

from __future__ import annotations

import enum
import json
import uuid
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from rqueue.errors import ConfigurationError
from rqueue.retry import RetryPolicyData

__all__ = [
    "Attempt",
    "Job",
    "JobRequest",
    "JobState",
    "QueuePause",
    "QueueStats",
    "Schedule",
]


class JobState(enum.StrEnum):
    """The durable lifecycle states of a job.

    ``PENDING`` and ``LEASED`` are active; the rest are terminal. A terminal
    job only becomes runnable again through the explicit operator retry
    operation in :mod:`rqueue.admin`.
    """

    PENDING = "pending"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_STATES


_TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED})
ACTIVE_STATES = (JobState.PENDING.value, JobState.LEASED.value)
TERMINAL_STATES = tuple(sorted(state.value for state in _TERMINAL_STATES))


class AttemptOutcome(enum.StrEnum):
    """How one attempt at a job ended."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRY = "retry"
    CANCELLED = "cancelled"
    LEASE_EXPIRED = "lease_expired"


@dataclass(frozen=True, slots=True)
class Job:
    """A durable job row, as returned by enqueue, claim, and inspection."""

    id: uuid.UUID
    queue: str
    task: str
    payload: Any
    state: JobState
    priority: int
    attempt: int
    max_attempts: int
    scheduled_at: datetime
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    dedupe_key: str | None = None
    concurrency_key: str | None = None
    worker_id: str | None = None
    lease_token: uuid.UUID | None = None
    leased_until: datetime | None = None
    heartbeat_at: datetime | None = None
    cancel_requested: bool = False
    timeout_seconds: float | None = None
    error_type: str | None = None
    error_message: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    retry_policy: RetryPolicyData | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Job:
        retry_policy = None
        if row.get("retry_policy") is not None:
            with suppress(ConfigurationError):
                retry_policy = RetryPolicyData.from_json(row["retry_policy"])
        return cls(
            id=row["id"],
            queue=row["queue"],
            task=row["task"],
            payload=json.loads(row["payload"]),
            state=JobState(row["state"]),
            priority=row["priority"],
            attempt=row["attempt"],
            max_attempts=row["max_attempts"],
            scheduled_at=row["scheduled_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            dedupe_key=row["dedupe_key"],
            concurrency_key=row["concurrency_key"],
            worker_id=row["worker_id"],
            lease_token=row["lease_token"],
            leased_until=row["leased_until"],
            heartbeat_at=row["heartbeat_at"],
            cancel_requested=row["cancel_requested"],
            timeout_seconds=row["timeout_seconds"],
            error_type=row["error_type"],
            error_message=row["error_message"],
            metadata=json.loads(row["metadata"]),
            retry_policy=retry_policy,
        )


@dataclass(frozen=True, slots=True)
class Attempt:
    """One immutable attempt record."""

    id: int
    job_id: uuid.UUID
    attempt: int
    worker_id: str
    lease_token: uuid.UUID
    started_at: datetime
    finished_at: datetime | None
    outcome: AttemptOutcome | None
    error_type: str | None
    error_message: str | None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Attempt:
        outcome = row["outcome"]
        return cls(
            id=row["id"],
            job_id=row["job_id"],
            attempt=row["attempt"],
            worker_id=row["worker_id"],
            lease_token=row["lease_token"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            outcome=AttemptOutcome(outcome) if outcome is not None else None,
            error_type=row["error_type"],
            error_message=row["error_message"],
        )


@dataclass(frozen=True, slots=True)
class Schedule:
    """A periodic schedule row."""

    id: uuid.UUID
    name: str
    queue: str
    task: str
    payload: Any
    cron: str
    timezone: str
    enabled: bool
    priority: int
    max_attempts: int
    concurrency_key: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Schedule:
        return cls(
            id=row["id"],
            name=row["name"],
            queue=row["queue"],
            task=row["task"],
            payload=json.loads(row["payload"]),
            cron=row["cron"],
            timezone=row["timezone"],
            enabled=row["enabled"],
            priority=row["priority"],
            max_attempts=row["max_attempts"],
            concurrency_key=row["concurrency_key"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


@dataclass(frozen=True, slots=True)
class JobRequest:
    """One job in an ``enqueue_many`` batch.

    Mirrors the keyword arguments of :meth:`rqueue.Queue.enqueue` so a caller
    can build a batch without learning a second vocabulary.
    """

    task: str
    payload: Any = None
    scheduled_at: datetime | None = None
    delay: float | None = None
    priority: int = 0
    max_attempts: int | None = None
    dedupe_key: str | None = None
    on_conflict: str | None = None
    concurrency_key: str | None = None
    timeout: float | None = None
    metadata: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class QueuePause:
    """The durable pause state of one queue, or of the ``'*'`` wildcard.

    ``paused_at`` is the timestamp the pause was taken, not a boolean: an
    operator looking at a stalled queue wants "paused since 03:14", and the
    column costs nothing extra to carry. ``None`` means the queue is running.
    """

    queue: str
    paused_at: datetime | None
    updated_at: datetime

    @property
    def is_paused(self) -> bool:
        return self.paused_at is not None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> QueuePause:
        return cls(
            queue=row["queue"],
            paused_at=row["paused_at"],
            updated_at=row["updated_at"],
        )


@dataclass(frozen=True, slots=True)
class QueueStats:
    """A point-in-time snapshot of one queue, for metrics and readiness."""

    queue: str
    pending: int
    leased: int
    succeeded: int
    failed: int
    cancelled: int
    ready: int
    oldest_ready_age_seconds: float | None
    expired_leases: int

    @property
    def depth(self) -> int:
        """Jobs that still have work left to do."""
        return self.pending + self.leased
