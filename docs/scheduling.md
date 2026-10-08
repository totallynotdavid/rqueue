# Periodic schedules

A `Scheduler` creates jobs for cron schedules.

```python
from rqueue import Queue, ScheduleSpec, Scheduler


async def run_scheduler(queue: Queue) -> None:
    scheduler = Scheduler(
        queue,
        scheduler_id="scheduler-1",
        schedules=[
            ScheduleSpec(
                name="nightly-vacuum",
                task="maintenance",
                cron="0 3 * * *",
                timezone="America/Lima",
            )
        ],
    )
    await scheduler.run()
```

`run()` first writes the specs to the `schedules` table, then ticks every
`interval` seconds (10 by default). A tick considers every enabled schedule in
the schema, not only those for the scheduler's queue. A schedule with no `queue`
uses the queue the scheduler was built from.

`ScheduleSpec` takes `name`, `task`, `cron`, and optionally `payload`,
`timezone` (default `UTC`), `queue`, `enabled`, `priority`, `max_attempts` (3),
and `concurrency_key`.

## One run per occurrence

Each tick writes the occurrence key `(schedule_id, occurrence_at)` and the job
in one transaction. Several scheduler replicas are safe: they collide on that
unique key, and exactly one wins. A scheduler that dies mid-tick leaves nothing
behind, so the next tick retries the same occurrence. There is no leader
election. The occurrence table records what fired and when. The transaction is
`fire_occurrence` in [`src/rqueue/storage.py`](../src/rqueue/storage.py).

After an outage, `catchup` (default 1) bounds how many missed occurrences one
tick fires. A new schedule never fires for occurrences that predate it.

## Manage schedules

`Admin` lists and changes schedules:

```python
from rqueue import Admin


async def manage_schedules(pool) -> None:
    admin = Admin(pool)
    await admin.list_schedules(enabled_only=True)
    await admin.set_schedule_enabled("nightly-vacuum", False)
    await admin.delete_schedule("nightly-vacuum")
```

`get_schedule(name)` returns one schedule and raises `ScheduleNotFound` if there
is none.
