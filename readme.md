# rqueue

A durable task queue for Python applications that use
[asyncpg](https://github.com/MagicStack/asyncpg). Jobs live in PostgreSQL, so
you enqueue one in the same transaction as your own writes. If the transaction
rolls back, the job rolls back with it.

rqueue needs Python 3.13+ and PostgreSQL 14+. `asyncpg` is its only runtime
dependency. Handlers are `async def`, payloads are JSON, and delivery is at
least once.

## Get started

From a checkout of this repository:

```console
$ pip install .
$ rqueue --database-url "$DATABASE_URL" migrate
```

`migrate` creates the `task_queue` schema. It is the only thing that changes the
schema. Importing rqueue or starting a worker never does.

Register a task, enqueue a job inside a transaction, and run a worker:

```python
import asyncio
import os

import asyncpg
from rqueue import Queue, Worker


async def main():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    queue = Queue(pool, name="compute")

    @queue.task(name="greet")
    async def greet(payload, context):
        print("hello,", payload["name"])

    async with pool.acquire() as connection:
        async with connection.transaction():
            await queue.enqueue(connection, task="greet", payload={"name": "world"})

    await Worker(queue, worker_id="worker-1").drain()


asyncio.run(main())
```

`drain()` runs until no claimable job is left. A long-running service calls
`run()` instead.

## Features

- Transactional enqueue on your own `asyncpg.Connection`
- At-least-once delivery. Leases carry a fencing token, so a worker that stalls
  past its lease cannot overwrite the attempt that replaced it
- Deduplication of queued jobs with `dedupe_key`
- Mutual exclusion of running jobs on a shared resource with `concurrency_key`
- Priorities and delayed jobs
- Retries with exponential backoff and jitter, per-task timeouts, and
  cancellation of running jobs
- Periodic schedules from cron expressions with time zones. Several scheduler
  replicas are safe
- Pausing a queue for every worker replica at once
- Least-privilege PostgreSQL roles for producing, consuming, scheduling,
  inspecting, and purging, scoped to queues with row-level security
- A command line and an `Admin` API for inspecting, cancelling, retrying, and
  purging jobs
- Readiness checks and a metrics hook that needs no metrics vendor
- `RecordingQueue`, which unit-tests enqueue calls without a database

## Documentation

The [manual](docs/readme.md) covers each feature.
[docs/architecture.md](docs/architecture.md) is the map of the code. To change
rqueue, read [.github/contributing.md](.github/contributing.md).
