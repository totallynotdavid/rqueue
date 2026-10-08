# Running a worker

A `Worker` serves one queue.

```python
from rqueue import Queue, Worker


async def serve(queue: Queue) -> None:
    worker = Worker(queue, worker_id="api-worker-1", concurrency=4)
    await worker.run()
```

The queue must have at least one registered task, or `run()` raises
`ConfigurationError`. At start the worker also checks the queue for active jobs
whose task it does not know. By default that raises `UnknownTask`. With
`strict_tasks=False` the worker logs the names and leaves those jobs `pending`
for a deployment that registers them. A worker only claims jobs for the task
names it has registered.

## Settings

| Argument                                           | Default               | Meaning                                                    |
| -------------------------------------------------- | --------------------- | ---------------------------------------------------------- |
| `worker_id`                                        | none                  | a name for this worker, at most 128 characters             |
| `concurrency`                                      | 4                     | handlers that run at once, from 1 to 1024                  |
| `batch_size`                                       | `concurrency`         | jobs claimed per tick, at most 1000                        |
| `lease_duration`                                   | 30.0                  | seconds a lease lasts, from 0.1 to 86400                   |
| `heartbeat_interval`                               | `lease_duration / 3`  | seconds between heartbeats. Must be shorter than the lease |
| `poll_interval`                                    | 1.0                   | seconds between polls of the queue                         |
| `shutdown_timeout`                                 | 30.0                  | seconds that in-flight handlers get to finish on shutdown  |
| `install_default_executor`, `executor_max_workers` | `True`, `concurrency` | see [Blocking work](#blocking-work)                        |
| `metrics`, `logger`                                | none                  | see [Operations](operations.md#metrics)                    |

## Stopping

`Worker.stop()` asks the loop to finish its in-flight jobs and return.
`await worker.shutdown()` does the same for a worker that runs in a background
task, and waits for the bounded shutdown to complete. Pass `timeout=` to
override `shutdown_timeout` for one shutdown. Handlers still running after the
grace period are cancelled, and the worker hands their leases back so another
worker can claim the jobs.

`Worker.drain()` runs until the queue has no claimable work left. Use it for
one-shot batch jobs and tests.

Shutdown also waits for any handler that is still inside `asyncio.to_thread`,
because Python cannot stop a running thread. A blocking handler therefore has to
return on its own; keep it short or give it its own timeout.

## Database errors

`Worker.run()` and `Scheduler.run()` retry a tick when PostgreSQL is
unreachable, and log `could not reach PostgreSQL; retrying`. A missing privilege
(SQLSTATE 42501) is the same on every tick, so `run()` raises
`ConfigurationError` and names `provision_role` as the fix. Every other database
error is retried, including a missing table or column, so a worker that starts
on a schema that is behind recovers once `rqueue migrate` has run. A schema that
does not exist at all fails the start-up check instead.

## Blocking work

A handler is always `async`. Wrap blocking calls in `asyncio.to_thread`:

```python
import asyncio

from rqueue import Queue


def register_simulate(queue: Queue) -> None:
    @queue.task(name="simulate")
    async def simulate(payload, context):
        await asyncio.to_thread(run_numba_kernel, payload["grid"])
```

`run_numba_kernel` stands for your own blocking function.

`Worker` installs a `ThreadPoolExecutor` with `concurrency` threads as the event
loop's default executor ([`src/rqueue/executor.py`](../src/rqueue/executor.py)).
A bare `asyncio.to_thread(...)` is therefore limited to the worker's capacity.
Set `executor_max_workers` to size the pool separately, or
`install_default_executor=False` to leave the loop alone.

Shutdown waits for these threads. Cancelling a handler does not make `run()`
return while its thread still runs.
