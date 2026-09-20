# rqueue

A durable PostgreSQL task queue for Python applications that already speak
[asyncpg](https://github.com/MagicStack/asyncpg).

Its defining feature is **transactional enqueueing**. Your application writes
its own rows and enqueues a job through the *same* `asyncpg.Connection` and the
same PostgreSQL transaction. If that transaction rolls back, the job rolls back
with it.

`asyncpg` is the only required runtime dependency and the only database driver
in the process. [docs/requirements.md](docs/requirements.md) is the full
specification this package implements, including why it exists rather than
adopting Procrastinate or pgqueuer.

## Guarantees

* **Delivery is at least once.** A handler may run again after a crash, a lease
  expiry, or an ambiguous network failure. Handlers and their external side
  effects must be idempotent. rqueue does not offer exactly-once execution and
  never will.
* **Leases are fenced.** A claim mints a fresh `lease_token`. Every write a
  worker makes carries it, and a SQL predicate on it gates every such write. A
  worker that stalls past its lease cannot overwrite the attempt that replaced
  it. River and pgqueuer do not provide this guarantee.
* **State transitions are enforced in SQL**, not only in Python. A terminal job
  becomes runnable again through exactly one path: the explicit operator retry.

## Requirements

* Python 3.13+
* PostgreSQL 14+

## Quickstart

Install the schema. Migrations never run implicitly, not at import and not at
worker startup:

```console
$ rqueue --database-url "$DATABASE_URL" migrate
```

Register a task, enqueue a job inside your own transaction, and run a worker:

```python
import asyncpg
from rqueue import Queue, Worker

pool = await asyncpg.create_pool(DATABASE_URL)
queue = Queue(pool, name="compute")


@queue.task(name="prepare_simulation")
async def prepare_simulation(payload, context):
    ...


async with pool.acquire() as connection:
    async with connection.transaction():
        await app_db.execute(create_compute_job(...))
        await queue.enqueue(
            connection,
            task="prepare_simulation",
            payload={"compute_job_id": str(compute_job_id)},
            dedupe_key=f"simulation:{external_id}",
            on_conflict="return_existing",
        )

worker = Worker(queue, worker_id="api-worker-1", concurrency=4)
await worker.run()
```

## Documentation

Setup and usage:

* [Migrations](docs/migrations.md): install the schema and upgrade between releases.
* [Tasks](docs/tasks.md): register handlers, declare tasks for producer-only
  processes, and decide what a failed attempt records.
* [Enqueueing](docs/enqueueing.md): enqueue inside your own transaction.
* [Running a worker](docs/worker.md): start, stop, and drain a worker, and run
  blocking work.
* [Testing](docs/testing.md): unit-test enqueue calls without PostgreSQL.

Behavior:

* [Keys](docs/keys.md): `dedupe_key` and `concurrency_key`.
* [Ordering and fairness](docs/ordering.md)
* [Retries, timeouts, and cancellation](docs/retries.md)
* [Pausing a queue](docs/pausing.md)
* [Periodic schedules](docs/scheduling.md)

Operating rqueue:

* [Operations](docs/operations.md): the CLI, `Admin`, readiness, and metrics.
* [Least-privilege roles](docs/roles.md): capabilities, row-level security, and
  `provision_role`.
* [Storage](docs/storage.md): the tables and the module layout.

Project:

* [Development](docs/development.md): build, test, and run the integration suite.
* [Requirements](docs/requirements.md): the design specification and its revision history.
* [Independent review](docs/review-report.md): findings from a review of the implementation.
