"""The non-durable recording queue, exercised without a database."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import rqueue
from rqueue import Queue, TaskContext
from rqueue.errors import ConfigurationError, UnknownTask, ValidationError
from rqueue.models import Job, JobRequest, JobState
from rqueue.retry import RetryPolicy
from rqueue.testing import RecordedEnqueue, RecordingQueue


async def prepare_simulation(payload: object, context: TaskContext) -> None:
    return None


@pytest.fixture
def queue() -> RecordingQueue:
    recording = RecordingQueue(name="compute")
    recording.register(
        name="prepare_simulation",
        handler=prepare_simulation,
        retry=RetryPolicy(max_attempts=7),
        timeout=42.0,
    )
    return recording


def test_it_is_not_part_of_the_rqueue_package_surface() -> None:
    assert "RecordingQueue" not in rqueue.__all__
    assert not hasattr(rqueue, "RecordingQueue")


def test_it_is_a_queue_so_call_sites_do_not_branch(queue: RecordingQueue) -> None:
    assert isinstance(queue, Queue)


async def test_a_recorded_call_carries_the_validated_row_and_a_job(
    queue: RecordingQueue,
) -> None:
    compute_job_id = uuid.uuid4()
    sentinel = object()

    job = await queue.enqueue(
        sentinel,
        task="prepare_simulation",
        payload={"compute_job_id": str(compute_job_id)},
        dedupe_key=f"simulation:{compute_job_id}",
        on_conflict="return_existing",
        concurrency_key="tsdhn",
        metadata={"trace": "abc"},
    )

    (recorded,) = queue.recorded
    assert isinstance(recorded, RecordedEnqueue)
    assert recorded.connection is sentinel
    assert recorded.job is job
    assert recorded.task == "prepare_simulation"
    assert recorded.queue == "compute"
    assert recorded.payload == {"compute_job_id": str(compute_job_id)}
    assert recorded.dedupe_key == f"simulation:{compute_job_id}"
    assert recorded.concurrency_key == "tsdhn"
    assert recorded.metadata == {"trace": "abc"}
    assert recorded.request.on_conflict == "return_existing"


async def test_the_returned_job_is_plausible_pending_shape(
    queue: RecordingQueue,
) -> None:
    before = datetime.now(UTC)
    job = await queue.enqueue(task="prepare_simulation", payload=[1, 2])

    assert isinstance(job, Job)
    assert isinstance(job.id, uuid.UUID)
    assert job.state is JobState.PENDING
    assert job.attempt == 0
    assert job.queue == "compute"
    assert job.payload == [1, 2]
    assert job.metadata == {}
    assert before <= job.created_at <= datetime.now(UTC)
    assert job.created_at == job.updated_at
    assert job.scheduled_at >= before
    assert job.started_at is None
    assert job.finished_at is None
    assert job.lease_token is None


async def test_ids_are_unique_per_call(queue: RecordingQueue) -> None:
    first = await queue.enqueue(task="prepare_simulation")
    second = await queue.enqueue(task="prepare_simulation")
    assert first.id != second.id


async def test_the_connection_argument_is_optional_and_ignored(
    queue: RecordingQueue,
) -> None:
    await queue.enqueue(task="prepare_simulation")
    await queue.enqueue(None, task="prepare_simulation")
    assert [recorded.connection for recorded in queue.recorded] == [None, None]


# --------------------------------------------------------------- validation


async def test_registered_defaults_are_applied(queue: RecordingQueue) -> None:
    """Proof that the call really goes through Queue.build_insert."""
    job = await queue.enqueue(task="prepare_simulation")
    assert job.max_attempts == 7
    assert job.timeout_seconds == 42.0


async def test_declared_defaults_are_applied_without_a_handler() -> None:
    queue = RecordingQueue(name="compute")
    queue.declare_task(
        name="prepare_simulation",
        retry=RetryPolicy(max_attempts=7),
        timeout=42.0,
    )

    job = await queue.enqueue(task="prepare_simulation")

    assert job.max_attempts == 7
    assert job.timeout_seconds == 42.0
    assert queue.tasks == {}


async def test_declaration_and_registration_share_one_metadata_source() -> None:
    queue = RecordingQueue(name="compute")
    declaration = queue.declare_task(
        name="prepare_simulation",
        retry=RetryPolicy(max_attempts=7),
        timeout=42.0,
    )
    registration = queue.register(name="prepare_simulation", handler=prepare_simulation)

    assert registration.declaration is not declaration
    assert registration.retry.max_attempts == declaration.retry.max_attempts
    assert registration.retry.retry_on == queue.default_retry.retry_on
    assert registration.retry.retry_if is queue.default_retry.retry_if
    job = await queue.enqueue(task="prepare_simulation")
    assert job.max_attempts == 7
    assert job.timeout_seconds == 42.0


async def test_an_unregistered_task_name_is_rejected(queue: RecordingQueue) -> None:
    with pytest.raises(UnknownTask, match="prepare_simulaton"):
        await queue.enqueue(task="prepare_simulaton")
    assert queue.recorded == []


async def test_the_registry_check_can_be_turned_off() -> None:
    queue = RecordingQueue(require_registered_tasks=False)
    job = await queue.enqueue(task="handled_elsewhere")
    assert job.task == "handled_elsewhere"


async def test_a_dedupe_key_still_needs_an_explicit_conflict_mode(
    queue: RecordingQueue,
) -> None:
    with pytest.raises(ValidationError, match="explicit on_conflict"):
        await queue.enqueue(task="prepare_simulation", dedupe_key="k")
    assert queue.recorded == []


async def test_a_non_serializable_payload_is_rejected(queue: RecordingQueue) -> None:
    with pytest.raises(ValidationError):
        await queue.enqueue(task="prepare_simulation", payload={"when": object()})
    assert queue.recorded == []


async def test_an_oversized_payload_is_rejected(queue: RecordingQueue) -> None:
    with pytest.raises(ValidationError):
        await queue.enqueue(task="prepare_simulation", payload="x" * (256 * 1024))
    assert queue.recorded == []


async def test_scheduled_at_and_delay_remain_mutually_exclusive(
    queue: RecordingQueue,
) -> None:
    with pytest.raises(ValidationError, match="not both"):
        await queue.enqueue(
            task="prepare_simulation", scheduled_at=datetime.now(UTC), delay=5
        )


async def test_delay_becomes_a_scheduled_instant(queue: RecordingQueue) -> None:
    job = await queue.enqueue(task="prepare_simulation", delay=timedelta(minutes=5))
    assert job.scheduled_at > datetime.now(UTC) + timedelta(minutes=4)


async def test_an_out_of_range_priority_is_rejected(queue: RecordingQueue) -> None:
    with pytest.raises(ValidationError):
        await queue.enqueue(task="prepare_simulation", priority=10**9)


# ------------------------------------------------------------- enqueue_many


async def test_a_batch_is_validated_before_anything_is_recorded(
    queue: RecordingQueue,
) -> None:
    batch = [
        JobRequest(task="prepare_simulation", payload={"n": 1}),
        JobRequest(task="prepare_simulation", dedupe_key="k"),
    ]
    with pytest.raises(ValidationError, match="explicit on_conflict"):
        await queue.enqueue_many(None, batch)
    assert queue.recorded == []


async def test_a_valid_batch_is_recorded_in_order(queue: RecordingQueue) -> None:
    batch = [
        JobRequest(task="prepare_simulation", payload={"n": index})
        for index in range(3)
    ]
    jobs = await queue.enqueue_many(None, batch)
    assert [job.payload for job in jobs] == [{"n": 0}, {"n": 1}, {"n": 2}]
    assert [recorded.job for recorded in queue.recorded] == jobs
    assert [recorded.request for recorded in queue.recorded] == batch


async def test_an_oversized_batch_is_rejected(queue: RecordingQueue) -> None:
    batch = [JobRequest(task="prepare_simulation")] * 1001
    with pytest.raises(ValidationError, match="at most 1000"):
        await queue.enqueue_many(None, batch)
    assert queue.recorded == []


# ----------------------------------------------------- recording, not simulating


async def test_dedupe_conflicts_are_not_simulated(queue: RecordingQueue) -> None:
    """Documented non-goal: two calls, two records, no AlreadyEnqueued."""
    first = await queue.enqueue(
        task="prepare_simulation", dedupe_key="k", on_conflict="raise"
    )
    second = await queue.enqueue(
        task="prepare_simulation", dedupe_key="k", on_conflict="raise"
    )
    assert first.id != second.id
    assert len(queue.recorded) == 2


async def test_inspection_needs_a_real_database(queue: RecordingQueue) -> None:
    with pytest.raises(ConfigurationError, match="no pool"):
        await queue.get_job(uuid.uuid4())
    with pytest.raises(ConfigurationError, match="no pool"):
        await queue.list_jobs()
    with pytest.raises(ConfigurationError, match="no pool"):
        await queue.stats()


# ---------------------------------------------------------------- helpers


async def test_enqueued_filters_by_task_name() -> None:
    queue = RecordingQueue(require_registered_tasks=False)
    await queue.enqueue(task="a")
    await queue.enqueue(task="b")
    await queue.enqueue(task="a")
    assert len(queue.enqueued()) == 3
    assert [recorded.task for recorded in queue.enqueued("a")] == ["a", "a"]
    assert queue.enqueued("missing") == []


async def test_reset_clears_records_but_keeps_registrations(
    queue: RecordingQueue,
) -> None:
    await queue.enqueue(task="prepare_simulation")
    queue.reset()
    assert queue.recorded == []
    assert "prepare_simulation" in queue.tasks
    await queue.enqueue(task="prepare_simulation")
    assert len(queue.recorded) == 1


# ---------------------------------------------------- the consumer's seam


async def test_it_substitutes_for_a_real_queue_at_the_defer_seam(
    queue: RecordingQueue,
) -> None:
    """picv-2025's shape: repository code takes a defer callback, unchanged."""

    async def enqueue_simulation(connection: Any, compute_job_id: uuid.UUID) -> Job:
        return await queue.enqueue(
            connection,
            task="prepare_simulation",
            payload={"compute_job_id": str(compute_job_id)},
            dedupe_key=f"simulation:{compute_job_id}",
            on_conflict="return_existing",
        )

    async def create_or_get_job(
        *, simulation_id: uuid.UUID, defer: Callable[[Any, uuid.UUID], Awaitable[Job]]
    ) -> Job:
        # Stands in for repository code that knows nothing about the queue.
        return await defer(None, simulation_id)

    simulation_id = uuid.uuid4()
    job = await create_or_get_job(simulation_id=simulation_id, defer=enqueue_simulation)

    (recorded,) = queue.enqueued("prepare_simulation")
    assert recorded.payload == {"compute_job_id": str(simulation_id)}
    assert recorded.dedupe_key == f"simulation:{simulation_id}"
    assert recorded.job.id == job.id
