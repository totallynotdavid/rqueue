# Testing your enqueue calls without PostgreSQL

`rqueue.testing.RecordingQueue` is a `Queue` that runs the real validation and
records the result instead of writing it. A consumer can unit-test "does my code
enqueue the right job" with no database.

It is deliberately **not** exported from the `rqueue` package. Import it by its
full path, so a test tool can never be mistaken for production wiring:

```python
from rqueue.testing import RecordingQueue

queue = RecordingQueue(name="compute")
queue.register(name="prepare_simulation", handler=prepare_simulation)


# The same app/queueing.py callback as in docs/enqueueing.md, unchanged.
async def enqueue_simulation(connection, compute_job_id):
    return await queue.enqueue(
        connection,
        task="prepare_simulation",
        payload={"compute_job_id": str(compute_job_id)},
        dedupe_key=f"simulation:{compute_job_id}",
        on_conflict="return_existing",
    )


async def test_creating_a_compute_job_enqueues_one():
    record = await create_or_get_job(
        data=..., simulation_id=sid, defer=enqueue_simulation
    )

    (recorded,) = queue.enqueued("prepare_simulation")
    assert recorded.payload == {"compute_job_id": str(record.id)}
    assert recorded.dedupe_key == f"simulation:{record.id}"
```

Call sites do not change between test and production. `RecordingQueue`
subclasses `Queue`, and its connection argument is accepted, recorded, and
ignored (it defaults to `None`), so no `if TESTING:` branch is needed anywhere.

## What a recorded call proves

A recorded call goes through the same `Queue.build_insert` production uses. It
proves the call is well-formed and the real queue would accept it:

* The task name is valid and either declared or registered, so a typo fails the
  test.
* The payload and metadata are JSON and within their size bounds.
* `dedupe_key` is paired with an explicit `on_conflict`.
* `scheduled_at` and `delay` are not both set.
* The task's retry and timeout defaults were applied.

## What it does not prove

`RecordingQueue` is a recorder, not a simulator. It does not model
transactionality, dedupe conflict resolution, concurrency slots, claiming,
leases, state transitions, or scheduling. Two calls sharing a `dedupe_key`
record two jobs, and `on_conflict="raise"` never raises. Returned `Job` values
are synthesized locally, with a fresh id, `state=pending`, and local timestamps.

For any of that, write an integration test against real PostgreSQL with
`mise run test-integration`.

## Reading the recording

`RecordingQueue.recorded` is the full list of `RecordedEnqueue` records. Each
has the original `request`, the validated `spec`, and the synthesized `job`.
`enqueued(task=None)` filters the list and `reset()` clears it.
