# Running a worker

```python
worker = Worker(queue, worker_id="api-worker-1", concurrency=4)
await worker.run()
```

A `Worker` serves exactly one queue.

## Stopping

`Worker.stop()` asks the loop to finish its in-flight jobs and return.
`await worker.shutdown()` is the async form for a worker running in a background
task. It waits for that bounded shutdown to complete.

`Worker.drain()` runs until the queue has no claimable work left, which is what
one-shot batch jobs and tests want.

The default shutdown mode is `executor_shutdown="wait"`. For a deployment that
must hand leases back and return control to its process supervisor even when a
handler is still inside `asyncio.to_thread`, select the detach mode:

```python
worker = Worker(
    queue,
    worker_id="api-worker-1",
    executor_shutdown="detach",
)
await worker.shutdown(timeout=10)
```

Detach does not kill or interrupt the Python thread, because Python cannot safely
kill an arbitrary thread. It only stops waiting for the worker-owned executor
after the shutdown grace period. **After a detached
shutdown, the process must be terminated by its supervisor. Do not reuse the
Worker or its detached executor, and do not start new queue work in that
process.** `shutdown(wait_for_blocking_threads=...)` can override the
constructor mode for one shutdown.

## Blocking work

There is no sync-handler code path. Wrap blocking work explicitly:

```python
@queue.task(name="simulate")
async def simulate(payload, context):
    await asyncio.to_thread(run_numba_kernel, payload.grid)
```

`Worker` installs a `ThreadPoolExecutor` sized from `concurrency` as the event
loop's default executor (see [`src/rqueue/executor.py`](../src/rqueue/executor.py)). A bare `asyncio.to_thread(...)` is therefore
capacity-limited without every task author building an executor of their own.
Set `executor_max_workers` to size it independently, or
`install_default_executor=False` to leave the loop alone.

Shutdown waits for these blocking threads by default. Cancelling the handler
coroutine does not make `run()` return while its thread is still running.
[Stopping](#stopping) describes the detach mode for deployments that cannot wait.
