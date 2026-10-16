"""The non-durable recording queue, exercised without a database."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import pytest

from rqueue import TaskContext
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


async def test_the_returned_job_is_a_pending_job_with_the_registered_defaults(
    queue: RecordingQueue,
) -> None:
    before = datetime.now(UTC)
    job = await queue.enqueue(task="prepare_simulation", payload=[1, 2])
    other = await queue.enqueue(task="prepare_simulation")

    assert isinstance(job.id, uuid.UUID)
    assert job.id != other.id
    assert job.state is JobState.PENDING
    assert job.attempt == 0
    assert job.queue == "compute"
    assert job.payload == [1, 2]
    assert job.metadata == {}
    assert job.max_attempts == 7
    assert job.timeout_seconds == 42.0
    assert before <= job.created_at <= datetime.now(UTC)
    assert job.created_at == job.updated_at
    assert job.scheduled_at >= before
    assert job.started_at is None
    assert job.finished_at is None
    assert job.lease_token is None


async def test_the_connection_argument_is_recorded_when_given_and_optional(
    queue: RecordingQueue,
) -> None:
    await queue.enqueue(task="prepare_simulation")
    await queue.enqueue(None, task="prepare_simulation")
    assert [recorded.connection for recorded in queue.recorded] == [None, None]


@pytest.mark.parametrize(
    ("error", "call"),
    [
        pytest.param(
            UnknownTask,
            {"task": "prepare_simulaton"},
            id="an unregistered task name",
        ),
        pytest.param(
            ValidationError,
            {"task": "prepare_simulation", "priority": 10**9},
            id="a priority out of range",
        ),
    ],
)
async def test_an_invalid_call_raises_and_records_nothing(
    queue: RecordingQueue, error: type[Exception], call: dict[str, Any]
) -> None:
    with pytest.raises(error):
        await queue.enqueue(**call)
    assert queue.recorded == []


async def test_the_registry_check_can_be_turned_off() -> None:
    queue = RecordingQueue(require_registered_tasks=False)
    job = await queue.enqueue(task="handled_elsewhere")
    assert job.task == "handled_elsewhere"


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


async def test_dedupe_conflicts_are_not_simulated(queue: RecordingQueue) -> None:
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


async def test_it_substitutes_for_a_real_queue_at_the_defer_seam(
    queue: RecordingQueue,
) -> None:
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
        return await defer(None, simulation_id)

    simulation_id = uuid.uuid4()
    job = await create_or_get_job(simulation_id=simulation_id, defer=enqueue_simulation)

    (recorded,) = queue.enqueued("prepare_simulation")
    assert recorded.payload == {"compute_job_id": str(simulation_id)}
    assert recorded.dedupe_key == f"simulation:{simulation_id}"
    assert recorded.job.id == job.id
