# Testing

## Test enqueue calls without PostgreSQL

`rqueue.testing.RecordingQueue` is a `Queue` that runs the real validation and
records the result instead of writing it. Use it to test that your code enqueues
the right job, with no database.

`rqueue` does not export it. Import it by its full path, so test code is not
mistaken for production wiring:

```python
import uuid

from rqueue.testing import RecordingQueue

from app.queueing import make_enqueue_simulation
from app.repository import create_or_get_job
from app.tasks import prepare_simulation

queue = RecordingQueue(name="compute")
queue.register(name="prepare_simulation", handler=prepare_simulation)


async def test_creating_a_compute_job_enqueues_one(connection):
    record = await create_or_get_job(
        connection,
        data=...,
        simulation_id=uuid.uuid4(),
        defer=make_enqueue_simulation(queue),
    )

    (recorded,) = queue.enqueued("prepare_simulation")
    assert recorded.payload == {"compute_job_id": str(record.id)}
    assert recorded.dedupe_key == f"simulation:{record.id}"
```

`make_enqueue_simulation` and `create_or_get_job` are the functions from
[Enqueueing](enqueueing.md#keep-business-code-free-of-the-queue-import),
unchanged. `connection` is whatever your repository code needs for its own
tables, such as a fixture for your test database. The recording queue only
replaces rqueue's tables.

Call sites are the same in tests and in production. `RecordingQueue` subclasses
`Queue`. It accepts the connection argument, records it, and ignores it, so the
argument defaults to `None` and no `if TESTING:` branch is needed. See
[Enqueueing](enqueueing.md#keep-business-code-free-of-the-queue-import) for the
callback pattern.

## What a recorded call shows

A recorded call goes through the `Queue.build_insert` that production uses. It
shows that the call is well formed and that the real queue would accept it:

- The task name is valid and either declared or registered, so a typo fails the
  test.
- The payload and metadata are JSON and within their size limits.
- `dedupe_key` comes with an explicit `on_conflict`.
- `scheduled_at` and `delay` are not both set.
- The task's retry and timeout defaults were applied.

## What it does not show

`RecordingQueue` records. It does not simulate. It does not model transactions,
dedupe conflicts, concurrency slots, claiming, leases, state changes, or
scheduling. Two calls with the same `dedupe_key` record two jobs, and
`on_conflict="raise"` never raises. The returned `Job` is built locally, with a
new id, `state=pending`, and local timestamps.

To test any of that, write an integration test against PostgreSQL. See
[contributing](../.github/contributing.md#checks).

## Read the recording

`RecordingQueue.recorded` lists every `RecordedEnqueue`. Each has the original
`request`, the validated `spec`, and the built `job`. `enqueued(task=None)`
filters the list, and `reset()` clears it.
