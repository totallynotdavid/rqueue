# Pausing a queue

Pausing stops a queue from admitting new work on every worker replica. It does
not stop a process, and it does not touch jobs that are already leased.

```python
import asyncpg
from rqueue import Admin


async def pause_everything(database_url: str) -> None:
    pool = await asyncpg.create_pool(database_url)
    admin = Admin(pool)

    await admin.pause_queue("compute")  # every worker on `compute`
    await admin.resume_queue("compute")
    await admin.pause_queue("*")  # every queue
```

The pause is a row in `queue_pauses`, and the claim query reads it. The pause
therefore holds for a replica that never heard of the call, and it survives a
restart of every worker. A paused queue returns no claimable rows from the
database. The check is the `NOT EXISTS` predicate in the `claim_candidates`
statement in [`src/rqueue/storage.py`](../src/rqueue/storage.py).

Resuming takes effect on the next claim. A `NOTIFY` only saves an idle worker
the rest of its poll interval, as it does for new jobs. Nothing depends on it
arriving.

## What pause does not do

A job leased before the pause runs, heartbeats, and finalizes as usual. Pause
controls admission. `Worker.stop()` and `Worker.drain()` control one worker, and
`Admin.cancel_job` controls one job.

## Wildcard and named pauses

`resume_queue("*")` resumes every queue, including queues paused by name.
Resuming one queue by name does not lift a wildcard pause.

`"*"` means every queue wherever a queue is named: pause, resume, role grants,
`Admin.purge`, `Admin.stats`, and the `queue` filter of `Admin.list_jobs`. No
queue can be named `"*"`.

`Admin.paused_queues()` lists the paused queues and when each was paused.
`Admin.is_queue_paused(name)` answers for one queue, wildcard included.
