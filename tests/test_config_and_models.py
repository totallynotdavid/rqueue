"""Configuration validation, value objects, and the metrics/readiness shapes."""

from __future__ import annotations

import json
import logging
import pathlib
import uuid
from datetime import datetime, timedelta

import pytest

from rqueue import (
    LoggingMetricsSink,
    MetricsSink,
    NullMetricsSink,
    Queue,
    Readiness,
    RetryPolicy,
    ScheduleSpec,
    Worker,
)
from rqueue.errors import ConfigurationError, ValidationError
from rqueue.models import Job, JobState, QueueStats
from rqueue.storage import Storage


async def handler(payload: object, context: object) -> None:
    return None


def test_terminal_states_are_exactly_the_finished_ones() -> None:
    assert JobState.SUCCEEDED.is_terminal
    assert JobState.FAILED.is_terminal
    assert JobState.CANCELLED.is_terminal
    assert not JobState.PENDING.is_terminal
    assert not JobState.LEASED.is_terminal


def test_queue_stats_depth_counts_unfinished_work() -> None:
    stats = QueueStats(
        queue="q",
        pending=3,
        leased=2,
        succeeded=100,
        failed=1,
        cancelled=1,
        ready=3,
        oldest_ready_age_seconds=12.5,
        expired_leases=0,
    )
    assert stats.depth == 5


def test_job_is_built_from_a_row_with_json_text_columns() -> None:
    now = datetime.now().astimezone()
    row = {
        "id": uuid.uuid4(),
        "queue": "q",
        "task": "t",
        "payload": json.dumps({"a": 1}),
        "state": "pending",
        "priority": 0,
        "attempt": 0,
        "max_attempts": 3,
        "scheduled_at": now,
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "finished_at": None,
        "dedupe_key": None,
        "concurrency_key": None,
        "worker_id": None,
        "lease_token": None,
        "leased_until": None,
        "heartbeat_at": None,
        "cancel_requested": False,
        "timeout_seconds": None,
        "error_type": None,
        "error_message": None,
        "metadata": "{}",
    }
    job = Job.from_row(row)
    assert job.payload == {"a": 1}
    assert job.metadata == {}
    assert job.state is JobState.PENDING


def test_job_ignores_a_malformed_persisted_retry_policy_on_read() -> None:
    now = datetime.now().astimezone()
    row = {
        "id": uuid.uuid4(),
        "queue": "q",
        "task": "t",
        "payload": "{}",
        "state": "pending",
        "priority": 0,
        "attempt": 0,
        "max_attempts": 3,
        "scheduled_at": now,
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "finished_at": None,
        "dedupe_key": None,
        "concurrency_key": None,
        "worker_id": None,
        "lease_token": None,
        "leased_until": None,
        "heartbeat_at": None,
        "cancel_requested": False,
        "timeout_seconds": None,
        "error_type": None,
        "error_message": None,
        "metadata": "{}",
        "retry_policy": "{}",
    }

    assert Job.from_row(row).retry_policy is None


def test_worker_rejects_impossible_settings(offline_queue: Queue) -> None:
    offline_queue.register(name="t", handler=handler)
    with pytest.raises(ValidationError):
        Worker(offline_queue, worker_id="w", concurrency=0)
    with pytest.raises(ValidationError):
        Worker(offline_queue, worker_id="w", poll_interval=0)
    with pytest.raises(ValidationError):
        Worker(offline_queue, worker_id="w", lease_duration=0.0)
    with pytest.raises(ConfigurationError, match="heartbeat_interval"):
        Worker(
            offline_queue,
            worker_id="w",
            lease_duration=1.0,
            heartbeat_interval=5.0,
        )


def test_worker_defaults_are_derived_from_concurrency(offline_queue: Queue) -> None:
    offline_queue.register(name="t", handler=handler)
    worker = Worker(offline_queue, worker_id="w", concurrency=6, lease_duration=30.0)
    assert worker.batch_size == 6
    assert worker.executor_max_workers == 6
    assert worker.heartbeat_interval == pytest.approx(10.0)


def test_schedule_specs_are_validated_on_construction() -> None:
    assert ScheduleSpec(name="ok", task="t", cron="@daily")
    with pytest.raises(ValidationError):
        ScheduleSpec(name="ok", task="t", cron="not a cron")
    with pytest.raises(ValidationError):
        ScheduleSpec(name="ok", task="t", cron="@daily", timezone="Mars/Phobos")
    with pytest.raises(ValidationError):
        ScheduleSpec(name="bad name", task="t", cron="@daily")
    with pytest.raises(ValidationError):
        ScheduleSpec(name="ok", task="t", cron="@daily", max_attempts=0)


def test_retry_policy_is_hashable_and_shareable() -> None:
    policy = RetryPolicy(max_attempts=4)
    assert policy.max_attempts == 4
    assert RetryPolicy(max_attempts=4) == policy


def test_null_and_logging_sinks_satisfy_the_protocol(
    caplog: pytest.LogCaptureFixture,
) -> None:
    assert isinstance(NullMetricsSink(), MetricsSink)
    assert isinstance(LoggingMetricsSink(), MetricsSink)

    NullMetricsSink().counter("rqueue.job.succeeded", 1, queue="q")
    with caplog.at_level(logging.DEBUG, logger="rqueue.metrics"):
        sink = LoggingMetricsSink()
        sink.counter("rqueue.job.succeeded", 2, queue="q")
        sink.gauge("rqueue.queue.depth", 7.0, queue="q")
        sink.timing("rqueue.handler.duration", 0.25, task="t")
    assert [record.metric for record in caplog.records] == [  # type: ignore[attr-defined]
        "rqueue.job.succeeded",
        "rqueue.queue.depth",
        "rqueue.handler.duration",
    ]


def test_readiness_separates_its_failure_modes() -> None:
    down = Readiness(connected=False, schema_version=None, expected_schema_version=2)
    assert not down.ready

    behind = Readiness(
        connected=True,
        schema_version=1,
        expected_schema_version=2,
        workers=("w1",),
    )
    assert not behind.migrations_up_to_date
    assert behind.worker_available
    assert not behind.ready

    idle = Readiness(connected=True, schema_version=2, expected_schema_version=2)
    assert idle.migrations_up_to_date
    assert not idle.worker_available
    assert not idle.ready

    relaxed = Readiness(
        connected=True,
        schema_version=2,
        expected_schema_version=2,
        require_worker=False,
    )
    assert relaxed.ready

    needs_scheduler = Readiness(
        connected=True,
        schema_version=2,
        expected_schema_version=2,
        workers=("w1",),
        require_scheduler=True,
    )
    assert not needs_scheduler.ready


def test_queue_exposes_its_schema_and_wake_channel() -> None:
    storage = Storage("other_schema")
    assert storage.schema == "other_schema"
    assert storage.notify_channel == "rqueue_other_schema"


def test_a_worker_needs_at_least_one_registered_task(offline_queue: Queue) -> None:
    worker = Worker(offline_queue, worker_id="empty")
    with pytest.raises(ConfigurationError, match="no registered tasks"):
        import asyncio

        asyncio.run(worker._check_registry())


def test_retry_backoff_saturates_rather_than_overflowing() -> None:
    policy = RetryPolicy(
        initial_backoff=1.0, multiplier=2.0, max_backoff=60.0, jitter=0.0
    )
    assert policy.backoff_seconds(500) == 60.0
    assert policy.next_attempt_at(1) > datetime.now().astimezone() - timedelta(
        seconds=1
    )


def test_limits_declares_everything_the_package_imports_from_it() -> None:
    """`__all__` is a claim about the module, and it has to survive additions.

    Every bound and validator here is reached by name from elsewhere in the
    package, so an `__all__` that lists a subset is not a narrower public API --
    it is a stale one that the package's own modules already reach around. This
    checks both directions: nothing imported is missing from it, and nothing in
    it fails to resolve.
    """
    import ast

    from rqueue import limits

    source = pathlib.Path(limits.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)

    defined: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            defined.append(node.target.id)
        elif isinstance(node, ast.Assign):
            defined += [t.id for t in node.targets if isinstance(t, ast.Name)]
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            defined.append(node.name)
    public = {n for n in defined if not n.startswith("_") and n != "__all__"}
    assert set(limits.__all__) == public, public.symmetric_difference(limits.__all__)
    assert all(hasattr(limits, name) for name in limits.__all__)

    # And what the rest of the package actually asks for by name.
    wanted: set[str] = set()
    for path in pathlib.Path(limits.__file__).parent.rglob("*.py"):
        for found in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(found, ast.ImportFrom) and found.module == "rqueue.limits":
                wanted |= {alias.name for alias in found.names}
    assert wanted, "no module imports from rqueue.limits; this test is not testing"
    assert wanted <= set(limits.__all__), sorted(wanted - set(limits.__all__))


def test_purge_limit_is_bounded_before_a_statement_is_sent() -> None:
    """The routine enforces this bound too; this makes it a typed error."""
    from rqueue.limits import MAX_PURGE_LIMIT, validate_purge_limit

    assert validate_purge_limit(1) == 1
    assert validate_purge_limit(MAX_PURGE_LIMIT) == MAX_PURGE_LIMIT
    for bad in (0, -1, MAX_PURGE_LIMIT + 1):
        with pytest.raises(ValidationError):
            validate_purge_limit(bad)


def test_purge_states_are_checked_before_any_statement() -> None:
    """The routine is the boundary, but a fan-out over zero queues never
    reaches it -- so an invalid state must not come back as 0."""
    from rqueue.storage import _validate_purge_states

    assert _validate_purge_states(["succeeded", "cancelled"]) == [
        "succeeded",
        "cancelled",
    ]
    with pytest.raises(ValidationError, match="at least one terminal state"):
        _validate_purge_states([])
    with pytest.raises(ValidationError, match="pending is not terminal"):
        _validate_purge_states(["succeeded", "pending"])
