# Enqueueing

## Inside your own transaction

`Queue.enqueue` inserts the job on the connection you pass in. It opens no other
connection, so the job commits or rolls back with your transaction.

```python
async def create_compute_job(pool, queue, compute_job_id):
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                "INSERT INTO compute_jobs (id) VALUES ($1)", compute_job_id
            )
            job = await queue.enqueue(
                connection,
                task="prepare_simulation",
                payload={"compute_job_id": str(compute_job_id)},
                dedupe_key=f"simulation:{compute_job_id}",
                on_conflict="return_existing",
            )
            await connection.execute(
                "UPDATE compute_jobs SET queue_job_id = $2 WHERE id = $1",
                compute_job_id,
                job.id,
            )
```

`pool` is your `asyncpg` pool and `queue` is a `Queue` built on it.
`compute_jobs` stands for one of your own tables.

`enqueue_many(connection, [JobRequest(...), ...])` inserts a batch. It validates
every entry before it writes any row, and it accepts at most 1000 jobs per call.

## Arguments

| Argument          | Meaning                                                                             |
| ----------------- | ----------------------------------------------------------------------------------- |
| `task`            | the registered or declared task name                                                |
| `payload`         | JSON, at most 256 KiB serialized                                                    |
| `scheduled_at`    | when the job becomes claimable. At most 365 days ahead                              |
| `delay`           | seconds or a `timedelta` from now. Pass `scheduled_at` or `delay`, not both         |
| `priority`        | an integer from -32768 to 32767. The default is 0. See [Ordering](ordering.md)      |
| `max_attempts`    | an integer from 1 to 1000. The default comes from the task's retry policy           |
| `timeout`         | seconds. The default comes from the task                                            |
| `dedupe_key`      | see [Keys](keys.md). Requires `on_conflict`                                         |
| `on_conflict`     | `"return_existing"` or `"raise"`                                                    |
| `concurrency_key` | see [Keys](keys.md)                                                                 |
| `metadata`        | a JSON object, at most 8 KiB serialized. The handler reads it as `context.metadata` |

An argument outside its bound raises `ValidationError`.

## Keep business code free of the queue import

Repository code does not need to import rqueue. Pass it a `defer` callback and
let the composition layer supply the wrapper:

```python
# app/queueing.py, the only module that imports rqueue
def make_enqueue_simulation(queue):
    async def enqueue_simulation(connection, compute_job_id):
        return await queue.enqueue(
            connection,
            task="prepare_simulation",
            payload={"compute_job_id": str(compute_job_id)},
            dedupe_key=f"simulation:{compute_job_id}",
            on_conflict="return_existing",
        )

    return enqueue_simulation


# app/repository.py, knows nothing about the queue
async def create_or_get_job(connection, *, data, simulation_id, defer):
    async with connection.transaction():
        record = await insert_compute_job(connection, data, simulation_id)
        job = await defer(connection, record.id)
        await link_queue_job(connection, record.id, job.id)
        return record
```

`insert_compute_job` and `link_queue_job` are your own query functions. The
composition layer calls
`create_or_get_job(connection, ..., defer=make_enqueue_simulation(queue))`.

[Testing](testing.md) shows how to unit-test this callback without a database.
