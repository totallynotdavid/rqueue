# Tasks

## Registering a handler

Handlers are `async def` only, registered under an explicit name. Payloads are
JSON. The decoder is yours to choose: a plain function, a Pydantic
`TypeAdapter(...).validate_python`, or anything else callable.

```python
import asyncpg
from pydantic import BaseModel, TypeAdapter
from rqueue import Queue, RetryPolicy, TaskContext, Worker

pool = await asyncpg.create_pool(DATABASE_URL)
queue = Queue(pool, name="compute")


class PreparePayload(BaseModel):
    compute_job_id: str


@queue.task(
    name="prepare_simulation",
    decoder=TypeAdapter(PreparePayload).validate_python,
    retry=RetryPolicy(max_attempts=5, initial_backoff=2.0),
    timeout=600.0,
)
async def prepare_simulation(payload: PreparePayload, context: TaskContext) -> None:
    await context.heartbeat()
    ...
```

A payload that fails to decode becomes a durable *failed* job without consuming
the retry budget, because the same bytes will not decode on a later attempt.

## Recording state before a retry

When a handler catches an exception to update application-owned state before
re-raising it, `context.will_retry(exc)` reports whether the worker will
schedule another attempt. It is the same decision the worker makes, so the state
you record cannot disagree with what happens next. It accounts for
`PermanentFailure`, reports `False` for `CancelJob`, and honours this job's own
persisted `max_attempts` instead of the registered policy's default.

That covers `Retry` too. Raising it skips the policy's exception filter but not
the attempt budget. On the last attempt `will_retry` reports `False` and the
worker fails the job terminally. Rescheduling it would return it to `pending`
with no attempts left, where no worker would ever claim it again.

```python
try:
    await update_business_state(payload)
except Exception as exc:
    if context.will_retry(exc):
        await mark_retrying(payload)
    else:
        await mark_failed(payload)
    raise
```

## Declaring a task without its handler

A producer-only process does not need to import the handler. Declare the enqueue
metadata instead:

```python
queue = Queue(pool, name="compute")
queue.declare_task(
    name="prepare_simulation",
    retry=RetryPolicy(max_attempts=5, initial_backoff=2.0),
    timeout=600.0,
)
```

The declaration is the single source of truth for the numeric retry and timeout
defaults. Those values are materialized on each job row, so an independent
worker process uses them too.

Retryable exception classes and `retry_if` are worker-registration choices and
are never loaded from job data. Passing either to `declare_task()` raises
`ConfigurationError`.

When a worker also registers the task, omitting the numeric options adopts the
declaration. Explicitly supplied values must match it or registration raises
`ValidationError`. The same rule applies in the other order: a declaration with
omitted options adopts the existing registration's values, and an explicit
conflict raises `ValidationError`.

Without `declare_task()`, a plain `register()` keeps its retry policy local to
the worker. The job's `NULL` retry policy lets the worker use its current
backoff settings.
