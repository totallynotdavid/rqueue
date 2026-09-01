"""Task registration and enqueue validation, without a database."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from rqueue import Queue, TaskContext
from rqueue.errors import ConfigurationError, UnknownTask, ValidationError
from rqueue.models import JobRequest
from rqueue.retry import RetryPolicy


async def handler(payload: object, context: TaskContext) -> None:
    return None


def test_registration_is_explicit_and_unique(offline_queue: Queue) -> None:
    offline_queue.register(name="a", handler=handler)
    assert "a" in offline_queue.tasks
    with pytest.raises(ValidationError, match="already registered"):
        offline_queue.register(name="a", handler=handler)


def test_sync_handlers_are_rejected(offline_queue: Queue) -> None:
    def blocking(payload: object, context: TaskContext) -> None:
        return None

    with pytest.raises(ConfigurationError, match=r"asyncio\.to_thread"):
        offline_queue.register(name="blocking", handler=blocking)  # type: ignore[arg-type]


def test_unregistered_task_lookup_raises(offline_queue: Queue) -> None:
    with pytest.raises(UnknownTask):
        offline_queue.get_task("missing")


def test_decorator_returns_the_original_function(offline_queue: Queue) -> None:
    decorated = offline_queue.task(name="decorated")(handler)
    assert decorated is handler


def test_dedupe_key_requires_an_explicit_conflict_mode(offline_queue: Queue) -> None:
    with pytest.raises(ValidationError, match="explicit on_conflict"):
        offline_queue.build_insert(JobRequest(task="t", dedupe_key="k"))
    spec = offline_queue.build_insert(
        JobRequest(task="t", dedupe_key="k", on_conflict="raise")
    )
    assert spec.raise_on_conflict is True
    assert not offline_queue.build_insert(
        JobRequest(task="t", dedupe_key="k", on_conflict="return_existing")
    ).raise_on_conflict


def test_unknown_conflict_mode_is_rejected(offline_queue: Queue) -> None:
    with pytest.raises(ValidationError, match="on_conflict"):
        offline_queue.build_insert(
            JobRequest(task="t", dedupe_key="k", on_conflict="ignore")
        )


def test_scheduled_at_and_delay_are_mutually_exclusive(offline_queue: Queue) -> None:
    with pytest.raises(ValidationError, match="not both"):
        offline_queue.build_insert(
            JobRequest(
                task="t",
                scheduled_at=datetime.now(UTC),
                delay=5,
            )
        )


def test_delay_becomes_a_scheduled_instant(offline_queue: Queue) -> None:
    spec = offline_queue.build_insert(JobRequest(task="t", delay=60))
    assert spec.scheduled_at is not None
    assert spec.scheduled_at > datetime.now(UTC) + timedelta(seconds=50)


def test_max_attempts_defaults_to_the_registered_policy(offline_queue: Queue) -> None:
    offline_queue.register(
        name="picky", handler=handler, retry=RetryPolicy(max_attempts=7)
    )
    assert offline_queue.build_insert(JobRequest(task="picky")).max_attempts == 7
    assert offline_queue.build_insert(JobRequest(task="other")).max_attempts == 3


def test_timeout_defaults_to_the_registered_timeout(offline_queue: Queue) -> None:
    offline_queue.register(name="slow", handler=handler, timeout=12.5)
    assert offline_queue.build_insert(JobRequest(task="slow")).timeout_seconds == 12.5


def test_queue_name_can_be_overridden_for_the_scheduler(offline_queue: Queue) -> None:
    spec = offline_queue.build_insert(JobRequest(task="t"), queue_name="other")
    assert spec.queue == "other"


def test_payload_is_serialized_at_validation_time(offline_queue: Queue) -> None:
    with pytest.raises(ValidationError):
        offline_queue.build_insert(JobRequest(task="t", payload={"fn": handler}))


def test_a_batch_is_validated_before_any_row_is_written(offline_queue: Queue) -> None:
    # enqueue_many builds every spec first; a bad entry raises before the first
    # insert, so nothing is written. Exercised here through the same helper.
    requests = [JobRequest(task="t"), JobRequest(task="t", payload=object())]
    with pytest.raises(ValidationError):
        [offline_queue.build_insert(request) for request in requests]
