# Tasks

## Register a handler

A handler is an `async def` function registered under an explicit name. Payloads
are JSON. You choose the decoder: a plain function, a Pydantic
`TypeAdapter(...).validate_python`, or any other callable.

```python
import asyncpg
from pydantic import BaseModel, TypeAdapter
from rqueue import Queue, RetryPolicy, TaskContext


class PreparePayload(BaseModel):
    compute_job_id: str


async def build_queue(database_url: str) -> Queue:
    pool = await asyncpg.create_pool(database_url)
    queue = Queue(pool, name="compute")

    @queue.task(
        name="prepare_simulation",
        decoder=TypeAdapter(PreparePayload).validate_python,
        retry=RetryPolicy(max_attempts=5, initial_backoff=2.0),
        timeout=600.0,
    )
    async def prepare_simulation(
        payload: PreparePayload, context: TaskContext
    ) -> None:
        await context.heartbeat()
        ...

    return queue
```

A payload that fails to decode becomes a `failed` job. It does not use up the
retry budget, because the same payload fails to decode on every attempt.

`rqueue` does not depend on Pydantic. A decoder is any callable.

## What a handler receives

`TaskContext` has these members:

| Member                     | Value                                                                                                                                 |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `job_id`, `queue`, `task`  | the job's identity                                                                                                                    |
| `attempt`, `max_attempts`  | this attempt's number and the job's limit                                                                                             |
| `is_last_attempt`          | whether `attempt >= max_attempts`                                                                                                     |
| `metadata`                 | the metadata given at enqueue                                                                                                         |
| `logger`, `log_fields`     | a logger that carries the job's fields, and the fields                                                                                |
| `heartbeat()`              | extends the lease and returns whether cancellation was requested. Raises `LeaseLost` if another attempt owns the job                  |
| `cancel_requested`         | whether someone asked to cancel the job. It turns true when a heartbeat sees the request. See [Cancellation](retries.md#cancellation) |
| `wait_for_cancel(timeout)` | waits for a cancellation request. Returns `True` if one arrived, `False` on timeout                                                   |
| `will_retry(exc)`          | whether the worker will retry after `exc`                                                                                             |

A handler has no database connection from rqueue.

## Record state before a retry

To update application state before you re-raise an exception, ask
`context.will_retry(exc)`. It returns the answer the worker will act on, so the
state you record matches what happens next. It returns `False` for
`PermanentFailure` and `CancelJob`. It uses this job's own `max_attempts`, not
the registered policy's default.

`Retry` skips the policy's exception filter but not the attempt limit. On the
last attempt `will_retry` returns `False` and the worker fails the job. A
rescheduled job with no attempts left would sit in `pending`, and no worker
would claim it.

```python
async def prepare_simulation(payload: PreparePayload, context: TaskContext) -> None:
    try:
        await update_business_state(payload)
    except Exception as exc:
        if context.will_retry(exc):
            await mark_retrying(payload)
        else:
            await mark_failed(payload)
        raise
```

This is the handler from [Register a handler](#register-a-handler), with its
`PreparePayload` and `TaskContext` imports. `update_business_state`,
`mark_retrying`, and `mark_failed` are your own functions.

## Declare a task without its handler

A producer-only process does not need to import the handler. Declare the enqueue
metadata instead:

```python
import asyncpg
from rqueue import Queue, RetryPolicy


async def build_producer_queue(database_url: str) -> Queue:
    pool = await asyncpg.create_pool(database_url)
    queue = Queue(pool, name="compute")
    queue.declare_task(
        name="prepare_simulation",
        retry=RetryPolicy(max_attempts=5, initial_backoff=2.0),
        timeout=600.0,
    )
    return queue
```

The declaration sets the numeric retry and timeout defaults. rqueue stores them
on each job row, so a worker in another process uses them too.

`retry_on` and `retry_if` are chosen by the worker's registration and are never
read from job data. Passing either to `declare_task()` raises
`ConfigurationError`.

When a worker also registers the task, the numeric options you omit take the
declaration's values. Values you pass must match the declaration, or
registration raises `ValidationError`. The same applies in the other order: a
declaration that omits options takes the existing registration's values, and a
conflicting value raises `ValidationError`.

Without `declare_task()`, `register()` keeps the retry policy in the worker. The
job's retry policy is `NULL`, and the worker uses its current backoff settings.
