"""rqueue -- a durable PostgreSQL task queue for applications that use asyncpg.

Its defining feature is transactional enqueueing: an application inserts its own
rows and enqueues a job through the *same* ``asyncpg.Connection`` and the same
PostgreSQL transaction, so a rollback takes the job with it.

Importing this package opens no connection and creates no schema. Migrations
run only from the ``rqueue`` CLI or an explicit call to
:func:`rqueue.migrations.migrate`.

Delivery is **at least once**. A handler may run again after a crash, a lease
expiry, or an ambiguous network failure, so handlers and their external side
effects must be idempotent. rqueue does not offer exactly-once execution.
"""

from __future__ import annotations

from rqueue.admin import Admin
from rqueue.context import TaskContext
from rqueue.cron import CronExpression
from rqueue.errors import (
    AlreadyEnqueued,
    CancelJob,
    ConfigurationError,
    JobNotFound,
    LeaseLost,
    MigrationError,
    PermanentFailure,
    Retry,
    RqueueError,
    ScheduleNotFound,
    UnknownTask,
    ValidationError,
)
from rqueue.health import Readiness, check_readiness
from rqueue.metrics import LoggingMetricsSink, MetricsSink, NullMetricsSink
from rqueue.models import (
    Attempt,
    Job,
    JobRequest,
    JobState,
    QueuePause,
    QueueStats,
    Schedule,
)
from rqueue.queue import Queue
from rqueue.retry import RetryPolicy
from rqueue.scheduler import Scheduler, ScheduleSpec
from rqueue.tasks import PayloadDecoder, TaskDeclaration, TaskHandler, TaskRegistration
from rqueue.worker import Worker

__version__ = "0.2.0"

__all__ = [
    "Admin",
    "AlreadyEnqueued",
    "Attempt",
    "CancelJob",
    "ConfigurationError",
    "CronExpression",
    "Job",
    "JobNotFound",
    "JobRequest",
    "JobState",
    "LeaseLost",
    "LoggingMetricsSink",
    "MetricsSink",
    "MigrationError",
    "NullMetricsSink",
    "PayloadDecoder",
    "PermanentFailure",
    "Queue",
    "QueuePause",
    "QueueStats",
    "Readiness",
    "Retry",
    "RetryPolicy",
    "RqueueError",
    "Schedule",
    "ScheduleNotFound",
    "ScheduleSpec",
    "Scheduler",
    "TaskContext",
    "TaskDeclaration",
    "TaskHandler",
    "TaskRegistration",
    "UnknownTask",
    "ValidationError",
    "Worker",
    "__version__",
    "check_readiness",
]
