# Enqueueing

## Inside your own transaction

```python
async with pool.acquire() as connection:
    async with connection.transaction():
        await app_db.execute(create_compute_job(...))
        job = await queue.enqueue(
            connection,
            task="prepare_simulation",
            payload={"compute_job_id": str(compute_job_id)},
            dedupe_key=f"simulation:{external_id}",
            on_conflict="return_existing",
        )
        await app_db.execute(record_queue_job_id(compute_job_id, job.id))
```

`enqueue` never opens a second connection. `enqueue_many(connection, [...])`
validates an entire batch before writing any row.

`dedupe_key` and `on_conflict` are described in [Keys](keys.md).

## Keeping business logic decoupled from the queue import

Repository code should not import rqueue at all. Pass it a `defer` callback and
let the composition layer supply the wrapper:

```python
# app/queueing.py, the only module that imports rqueue
async def enqueue_simulation(connection, compute_job_id):
    return await queue.enqueue(
        connection,
        task="prepare_simulation",
        payload={"compute_job_id": str(compute_job_id)},
        dedupe_key=f"simulation:{compute_job_id}",
        on_conflict="return_existing",
    )


# app/repository.py, knows nothing about the queue
async def create_or_get_job(*, data, simulation_id, defer):
    async with connection.transaction():
        record = await insert_compute_job(connection, data, simulation_id)
        job = await defer(connection, record.id)
        await link_queue_job(connection, record.id, job.id)
        return record
```

[Testing](testing.md) shows how to unit-test this callback without a database.
