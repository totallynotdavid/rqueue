"""§10.8: periodic schedules fire once per occurrence, across replicas."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import asyncpg

from rqueue import Queue, Schedule, Scheduler, ScheduleSpec, TaskContext, Worker
from rqueue.models import JobState

from .support import eventually, running


def spec(name: str, **kwargs: object) -> ScheduleSpec:
    defaults: dict[str, object] = {
        "name": name,
        "task": "maintenance",
        "cron": "* * * * *",
        "payload": {"kind": "vacuum"},
    }
    defaults.update(kwargs)
    return ScheduleSpec(**defaults)  # type: ignore[arg-type]


async def make_schedule(queue: Queue, **kwargs: object) -> tuple[Scheduler, Schedule]:
    name = f"sched-{uuid.uuid4().hex[:12]}"
    scheduler = Scheduler(
        queue,
        scheduler_id=f"s-{uuid.uuid4().hex[:8]}",
        schedules=[spec(name, **kwargs)],
    )
    (stored,) = await scheduler.sync()
    return scheduler, stored


async def test_one_occurrence_produces_one_job(queue: Queue) -> None:
    scheduler, stored = await make_schedule(queue)
    moment = stored.created_at + timedelta(minutes=2)

    created = await scheduler.tick(now=moment)
    assert len(created) == 1
    assert created[0].task == "maintenance"
    assert created[0].payload == {"kind": "vacuum"}
    assert created[0].metadata["rqueue.schedule"] == stored.name

    # Ticking again for the same instant must not fire it twice.
    assert await scheduler.tick(now=moment) == []
    assert len(await queue.list_jobs()) == 1
    assert await scheduler.occurrence_count(stored.id) == 1


async def test_replicas_racing_for_one_occurrence_fire_it_once(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    name = f"sched-{uuid.uuid4().hex[:12]}"
    replicas = [
        Scheduler(queue, scheduler_id=f"replica-{index}", schedules=[spec(name)])
        for index in range(5)
    ]
    (stored,) = await replicas[0].sync()
    moment = stored.created_at + timedelta(minutes=3)

    results = await asyncio.gather(*(replica.tick(now=moment) for replica in replicas))
    fired = [job for batch in results for job in batch]
    assert len(fired) == 1
    assert len(await queue.list_jobs()) == 1
    assert await replicas[0].occurrence_count(stored.id) == 1

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            "SELECT fired_by FROM task_queue.schedule_occurrences "
            "WHERE schedule_id = $1",
            stored.id,
        )
    assert len(rows) == 1


async def test_a_crash_between_occurrence_and_job_loses_neither(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """§10.8's crash case.

    The occurrence row and its job are written in one transaction, so a
    scheduler that dies before commit leaves nothing behind -- not a
    half-recorded occurrence, and not an advanced ``next_run`` that would skip
    the firing entirely (the gap in the claim-the-schedule-row design §6
    rejects).
    """
    scheduler, stored = await make_schedule(queue)
    moment = stored.created_at + timedelta(minutes=4)
    occurrence_at = scheduler.due_occurrences(
        stored,
        now=moment,
        last=None,
    )[0]

    async with pool.acquire() as connection:
        transaction = connection.transaction()
        await transaction.start()
        job = await scheduler.fire_occurrence(
            connection,
            schedule=stored,
            occurrence_at=occurrence_at,
        )
        assert job is not None
        # The scheduler process dies here, before the commit.
        await transaction.rollback()

    assert await queue.list_jobs() == []
    assert await scheduler.occurrence_count(stored.id) == 0

    # The next tick simply tries the same occurrence again.
    created = await scheduler.tick(now=moment)
    assert len(created) == 1
    assert await scheduler.occurrence_count(stored.id) == 1
    assert await scheduler.tick(now=moment) == []


async def test_a_new_schedule_does_not_fire_for_the_past(queue: Queue) -> None:
    scheduler, stored = await make_schedule(queue, cron="@daily")
    # Midnight today is before the schedule existed, so it must not fire.
    assert await scheduler.tick(now=stored.created_at + timedelta(minutes=1)) == []


async def test_catch_up_after_an_outage_is_bounded(queue: Queue) -> None:
    name = f"sched-{uuid.uuid4().hex[:12]}"
    scheduler = Scheduler(
        queue,
        scheduler_id="catchup",
        schedules=[spec(name)],
        catchup=3,
    )
    (stored,) = await scheduler.sync()
    # Ten minutes of downtime on a per-minute schedule; catchup=3 collapses it.
    created = await scheduler.tick(now=stored.created_at + timedelta(minutes=10))
    assert len(created) == 3
    instants = [job.metadata["rqueue.occurrence_at"] for job in created]
    assert instants == sorted(instants), "catch-up runs oldest first"


async def test_a_restarted_scheduler_does_not_refire(queue: Queue) -> None:
    """§10.7: a scheduler restart delays work at most."""
    name = f"sched-{uuid.uuid4().hex[:12]}"
    first = Scheduler(queue, scheduler_id="sched-a", schedules=[spec(name)])
    (stored,) = await first.sync()
    moment = stored.created_at + timedelta(minutes=2)
    assert len(await first.tick(now=moment)) == 1

    # The process restarts: a brand new object, same identity, no memory.
    second = Scheduler(queue, scheduler_id="sched-a", schedules=[spec(name)])
    await second.sync()
    assert await second.tick(now=moment) == []
    later = await second.tick(now=moment + timedelta(minutes=1))
    assert len(later) == 1
    assert len(await queue.list_jobs()) == 2


async def test_a_scheduled_job_is_picked_up_by_a_worker(queue: Queue) -> None:
    ran = asyncio.Event()

    async def maintenance(payload: dict[str, str], context: TaskContext) -> None:
        assert payload == {"kind": "vacuum"}
        ran.set()

    queue.register(name="maintenance", handler=maintenance)
    scheduler, stored = await make_schedule(queue)
    created = await scheduler.tick(now=stored.created_at + timedelta(minutes=2))

    worker = Worker(queue, worker_id="sched-worker", poll_interval=0.05)
    async with running(worker):
        await asyncio.wait_for(ran.wait(), timeout=15)
        await eventually(
            lambda: _in_state(queue, created[0].id, JobState.SUCCEEDED),
            message="the scheduled job should complete",
        )


async def test_a_disabled_schedule_stops_firing(queue: Queue) -> None:
    scheduler, stored = await make_schedule(queue)
    moment = stored.created_at + timedelta(minutes=2)
    assert len(await scheduler.tick(now=moment)) == 1

    disabled = Scheduler(
        queue,
        scheduler_id="off",
        schedules=[spec(stored.name, enabled=False)],
    )
    await disabled.sync()
    assert await disabled.tick(now=moment + timedelta(minutes=5)) == []


async def _in_state(queue: Queue, job_id: uuid.UUID, expected: JobState) -> bool:
    job = await queue.get_job(job_id)
    return job is not None and job.state == expected
