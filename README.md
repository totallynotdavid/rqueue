# rqueue

A durable PostgreSQL task queue for Python applications that already speak
[asyncpg](https://github.com/MagicStack/asyncpg).

Its defining feature is **transactional enqueueing**: your application writes
its own rows and enqueues a job through the *same* `asyncpg.Connection` and the
same PostgreSQL transaction. If that transaction rolls back, the job rolls back
with it.

`asyncpg` is the only required runtime dependency, and the only database driver
in the process. See [REQUIREMENTS.md](REQUIREMENTS.md) for the full
specification this package implements, including why it exists rather than
adopting Procrastinate or pgqueuer.

## Guarantees, plainly

* **Delivery is at least once.** A handler may run again after a crash, a lease
  expiry, or an ambiguous network failure. Handlers and their external side
  effects must be idempotent. rqueue does not offer exactly-once execution and
  never will.
* **Leases are fenced.** A claim mints a fresh `lease_token`; every write a
  worker makes carries it, and every such write is gated by a SQL predicate on
  it. A worker that stalls past its lease cannot overwrite the attempt that
  replaced it. This is the guarantee River and pgqueuer do not provide.
* **State transitions are enforced in SQL**, not only in Python. A terminal job
  becomes runnable again through exactly one path: the explicit operator retry.

## Requirements

* Python 3.13+
* PostgreSQL 14+

## Quickstart

### 1. Install the schema

Migrations never run implicitly -- not at import, not at worker startup. They
are an explicit command that takes an advisory lock and is forward-only:

```console
$ rqueue --database-url "$DATABASE_URL" migrate
applied 0001_core
applied 0002_scheduling
applied 0003_queue_pause
applied 0004_retry_policy
```

Queue tables live in their own schema, `task_queue` by default, never in your
application's business schema. Pass `--schema` to change it.

### 2. Register tasks

Handlers are `async def` only, registered under an explicit name. Payloads are
JSON; the decoder is yours to choose (a plain function, a Pydantic
`TypeAdapter(...).validate_python`, anything callable).

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

When a handler catches an exception to update application-owned state before
re-raising it, `context.will_retry(exc)` reports whether the worker's registered
retry policy will schedule another attempt. It accounts for the last attempt
and `PermanentFailure`, and reports `True` for `Retry` or `False` for
`CancelJob`, without calculating a backoff timestamp:

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

A payload that fails to decode becomes a durable *failed* job without
consuming the retry budget: the same bytes will not decode on a later attempt.

A producer-only process does not need to import the handler. Declare the
enqueue metadata instead:

```python
queue = Queue(pool, name="compute")
queue.declare_task(
    name="prepare_simulation",
    retry=RetryPolicy(max_attempts=5, initial_backoff=2.0),
    timeout=600.0,
)
```

The declaration is the single source of truth for the numeric retry and timeout
defaults; those values are materialized on each job row so an independent
worker process uses them too. Retryable exception classes and `retry_if` remain
worker-registration choices and are never loaded from job data; passing either
to `declare_task()` raises `ConfigurationError`. When a worker
also registers the task, omitting the numeric options adopts the declaration;
explicitly supplied values must match it or registration raises
`ValidationError`. The same rule applies in the other order: a declaration
with omitted options adopts the existing registration's values, while an
explicit conflict raises `ValidationError`.

Without `declare_task()`, a plain `register()` keeps its retry policy local to
the worker, preserving the pre-declaration behavior for existing applications;
the job's `NULL` retry policy lets the worker use its current backoff settings.

### 3. Enqueue inside your own transaction

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

**Keep business logic decoupled from the queue import.** Repository code should
not import rqueue at all -- pass it a `defer` callback and let the composition
layer supply the wrapper:

```python
# app/queueing.py -- the only module that imports rqueue
async def enqueue_simulation(connection, compute_job_id):
    return await queue.enqueue(
        connection,
        task="prepare_simulation",
        payload={"compute_job_id": str(compute_job_id)},
        dedupe_key=f"simulation:{compute_job_id}",
        on_conflict="return_existing",
    )


# app/repository.py -- knows nothing about the queue
async def create_or_get_job(*, data, simulation_id, defer):
    async with connection.transaction():
        record = await insert_compute_job(connection, data, simulation_id)
        job = await defer(connection, record.id)
        await link_queue_job(connection, record.id, job.id)
        return record
```

### 4. Run a worker

```python
worker = Worker(queue, worker_id="api-worker-1", concurrency=4)
await worker.run()
```

`Worker.stop()` asks the loop to finish its in-flight jobs and return;
`await worker.shutdown()` is the async form for a worker running in a
background task and waits for that bounded shutdown to complete.
`Worker.drain()` runs until the queue has no claimable work left, which is what
one-shot batch jobs and tests want. The default shutdown mode is
`executor_shutdown="wait"`.

For a deployment that must hand leases back and return control to its process
supervisor even when a handler is still inside `asyncio.to_thread`, select the
explicit detach mode:

```python
worker = Worker(
    queue,
    worker_id="api-worker-1",
    executor_shutdown="detach",
)
await worker.shutdown(timeout=10)
```

Detach does not kill or interrupt the Python thread. It only stops waiting for
the worker-owned executor after the shutdown grace period. **After a detached
shutdown, the process must be terminated by its supervisor. Do not reuse the
Worker or its detached executor, and do not start new queue work in that
process.** `shutdown(wait_for_blocking_threads=...)` can override the
constructor mode for one shutdown.

## Testing your enqueue calls without PostgreSQL

`rqueue.testing.RecordingQueue` is a `Queue` that runs the real validation and
records the result instead of writing it, so a consumer can unit-test "does my
code enqueue the right job" with no database. It is deliberately **not**
exported from the `rqueue` package -- import it by its full path, so a test
tool can never be mistaken for production wiring:

```python
from rqueue.testing import RecordingQueue

queue = RecordingQueue(name="compute")
queue.register(name="prepare_simulation", handler=prepare_simulation)


# The same app/queueing.py callback as above, unchanged.
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

Call sites do not change between test and production: `RecordingQueue`
subclasses `Queue`, and its connection argument is accepted, recorded, and
ignored (it defaults to `None`), so no `if TESTING:` branch is needed anywhere.

A recorded call goes through the same `Queue.build_insert` production uses, so
it proves the call is well-formed and the real queue would accept it: the task
name is valid and either declared or registered (a typo fails the test), the
payload and metadata are JSON and within their size bounds, `dedupe_key` is
paired with an explicit `on_conflict`, `scheduled_at`/`delay` are not both set,
and the task's retry and timeout defaults were applied.

It is a recorder, not a simulator. It does not model transactionality, dedupe
conflict resolution (two calls sharing a `dedupe_key` record two jobs;
`on_conflict="raise"` never raises), concurrency slots, claiming, leases, state
transitions, or scheduling. Returned `Job` values are synthesized locally --
a fresh id, `state=pending`, local timestamps. For any of that, write an
integration test against real PostgreSQL (`mise run test-integration`).

`RecordingQueue.recorded` is the full list of `RecordedEnqueue` records --
each with the original `request`, the validated `spec`, and the synthesized
`job`. `enqueued(task=None)` filters it and `reset()` clears it.

## The two keys: `dedupe_key` and `concurrency_key`

They are different primitives and rqueue keeps them apart (Procrastinate calls
them `queueing_lock` and `lock`).

**`dedupe_key`** prevents a duplicate job from being *queued*. It is scoped to
one queue and held for exactly as long as the job is active -- `pending` or
`leased`. The moment the job reaches a terminal state the key is free again, so
"one simulation queued per external id" does not block the next run of the same
simulation tomorrow.

Because the choice matters, `on_conflict` has no default and must be given
whenever `dedupe_key` is:

* `"return_existing"` -- get the job that already holds the key;
* `"raise"` -- get an `AlreadyEnqueued` carrying that job's id.

Either way the losing producer learns what happened. rqueue never silently
absorbs an enqueue.

**`concurrency_key`** prevents duplicate concurrent *execution* of a shared
resource -- "one simulation at a time per compute job". It is a lease, not a
flag: the holder's slot is released when the job finishes, and expires on its
own if the holder dies, so a crashed worker cannot hold a business resource
hostage.

## Ordering and fairness

Within one queue, jobs are claimed in this order:

1. `priority` descending (a small integer, default 0),
2. then `scheduled_at` ascending -- only jobs whose time has come,
3. then insertion order.

**Between queues there is no fairness mechanism, and rqueue does not pretend
otherwise.** A `Worker` serves exactly one queue. Run one worker per queue and
size each deliberately; a busy queue cannot starve a quiet one because they do
not share a worker.

## Retries, timeouts, cancellation

A handler expresses its outcome by returning or by raising:

| Outcome | How |
| --- | --- |
| success | return normally |
| retry at a chosen time | `raise Retry(delay=...)` or `Retry(at=...)` |
| terminal failure now | `raise PermanentFailure(...)` |
| cancel this job | `raise CancelJob(...)` |
| retry under the policy | raise anything else the policy retries |

The persisted `attempt` column is the sole authority on how many attempts a job
has had -- it survives crashes and worker restarts. Exception detail recorded in
PostgreSQL is bounded and sanitized; full tracebacks belong in the structured
worker log.

Timeouts are per task (`timeout=` on registration, or per job on enqueue). A
timeout cancels the handler; it proves nothing about whether an external side
effect already happened, which is the other reason handlers must be idempotent.

Cancellation is cooperative. `Admin.cancel_job` on a *pending* job cancels it
outright. On a *leased* job it sets `cancel_requested`; the lease holder sees it
on its next heartbeat and finalizes the job -- or, if that worker never comes
back, lease expiry does.

## Pausing a queue

Pausing stops a queue admitting **new** work across every worker replica,
without stopping a process and without touching anything already leased:

```python
admin = Admin(pool)

await admin.pause_queue("compute")   # every worker on `compute`, right now
await admin.resume_queue("compute")
await admin.pause_queue("*")         # every queue
```

The switch is a durable row (`queue_pauses.paused_at`), and the claim query
itself reads it -- so the pause holds for a worker replica that has never heard
of the call, and it survives a restart of all of them. That is deliberately
stronger than the references: Oban keeps the flag in each producer process, so
a restarted queue comes back running, and River stores the row but still checks
it in the client, so a worker that has not polled yet can still issue a claim.
Here a paused queue returns zero claimable rows at the database.

`NOTIFY` is a latency optimization only, exactly as it is for job wake-ups: it
saves an *idle* worker the rest of its poll interval when you resume. Nothing
depends on its arrival.

What pause does **not** do: it does not touch an in-flight attempt. A job leased
before the pause runs, heartbeats, and finalizes normally -- pause is about
admission, `Worker.stop()`/`drain()` are about a worker's own lifecycle, and
`Admin.cancel_job` is about one job.

`resume_queue("*")` resumes everything, including queues paused by name --
a "resume all" that quietly left some queues paused would be a trap. Resuming
one queue by name does not lift a wildcard pause; the narrower call cannot
punch a hole in the broader one. `Admin.paused_queues()` lists what is paused
and since when, and `Admin.is_queue_paused(name)` answers for one queue,
wildcard included.

## Blocking work

There is no sync-handler code path. Wrap blocking work explicitly:

```python
@queue.task(name="simulate")
async def simulate(payload, context):
    await asyncio.to_thread(run_numba_kernel, payload.grid)
```

`Worker` installs a `ThreadPoolExecutor` sized from `concurrency` as the event
loop's default executor, so a bare `asyncio.to_thread(...)` is capacity-limited
without every task author building an executor of their own. Set
`executor_max_workers` to size it independently, or
`install_default_executor=False` to leave the loop alone.

Shutdown waits for these blocking threads by default, so cancellation of the
handler coroutine does not make `run()` return while its thread is still
running. Use `executor_shutdown="detach"` only when the supervisor will
terminate the process after lease hand-back; Python cannot safely kill an
arbitrary thread.

## Periodic schedules

```python
scheduler = Scheduler(
    queue,
    scheduler_id="scheduler-1",
    schedules=[
        ScheduleSpec(
            name="nightly-vacuum",
            task="maintenance",
            cron="0 3 * * *",
            timezone="America/Lima",
        )
    ],
)
await scheduler.run()
```

Each tick writes the occurrence key `(schedule_id, occurrence_at)` and the job
it produces in **one transaction**. Several scheduler replicas are safe: they
collide on that unique key and exactly one wins. A scheduler that dies mid-tick
leaves nothing behind, so the next tick simply retries the same occurrence --
no leader election, no schedule-row reclaim, and a free audit trail of what
fired when.

After an outage, `catchup` (default 1) bounds how many missed occurrences one
tick fires; a new schedule never fires for occurrences that predate it.

## Operations

```console
$ rqueue status                     # applied and pending migrations, as JSON
$ rqueue readiness --queue compute  # connectivity / schema / worker / scheduler
$ rqueue purge --retention-days 30  # delete old terminal jobs
$ rqueue grant-role --role api --capability produce --queue compute
```

`Admin` exposes the same operations in-process: `get_job`, `list_jobs`,
`attempts`, `stats`, `cancel_job`, `retry_job`, `purge`, `pause_queue` /
`resume_queue`, and the schedule accessors. An operator retry keeps the attempt counter running -- attempt
records are immutable, so a reset would collide with the history it is meant to
preserve -- and grants a fresh budget by raising `max_attempts`.

`check_readiness` reports connectivity, migration version, worker availability,
and scheduler availability *separately*, because a single "ready: false" is
useless during an incident.

Metrics go to a `MetricsSink` you supply (`counter` / `gauge` / `timing`); no
vendor is assumed. `LoggingMetricsSink` turns them into structured log records.
The series emitted are listed on the protocol in `rqueue/metrics.py`.

### Least-privilege roles

```python
await provision_role(
    connection,
    role="api_producer",
    capabilities=[Capability.PRODUCE],
    queues=["compute"],
)
```

The migration role owns the schema and is the only role that runs DDL. A
producer may enqueue and read; a worker may claim and transition; neither can
change the schema. Queue scoping is enforced by row-level-security policies
driven by rows in `role_queue_grants`, so the queues a role may touch arrive as
bind parameters rather than interpolated SQL.

## Storage

| Table | Holds |
| --- | --- |
| `jobs` | one row per job, including its lease and terminal outcome |
| `job_attempts` | one immutable record per attempt, for audit and debugging |
| `concurrency_slots` | the lease behind each named concurrency key |
| `schedules` | periodic schedule definitions |
| `schedule_occurrences` | the occurrence keys that have fired, and their jobs |
| `runtime_heartbeats` | worker and scheduler liveness, for readiness checks |
| `queue_pauses` | which queues are paused, and since when |
| `role_queue_grants` | which queues a granted role may reach |
| `schema_migrations` | applied migration versions and their checksums |

Every index is declared in the migration next to the query it serves. All
internal SQL is static and parameterized. The runtime read/write path lives in
`rqueue/storage.py`; schema DDL and role grants are separate concerns in
`rqueue/migrations` and `rqueue/roles.py`.

## Development

PostgreSQL is managed by [mise](https://mise.jdx.dev), project-local, not
Docker -- the same shape as the sibling picv-2025 repository, so one mental
model covers both.

```console
$ mise run install           # sync the virtualenv
$ mise run db:start          # start .data/postgres (init on first run)
$ mise run test              # fast suite; no database, runs in ~1s
$ mise run test-integration  # disposable database + scoped role, then teardown
$ mise run test-pipeline     # just the §11 master pipeline test, for the inner loop
$ mise run lint              # ruff check, ruff format --check, mypy --strict
$ mise run db:reset          # delete the local cluster
```

`scripts/integration.sh` creates `rqueue_integration_<timestamp>_<pid>`, runs
rqueue's own migrations against it with an admin connection, provisions a scoped
least-privilege role, runs `pytest -m integration`, and drops both the database
and the role in a `trap ... EXIT`. It never runs against a shared database. Set
`RQUEUE_DATABASE_URL` and it skips `mise run db:start`, which is how CI supplies
its own service container.

The fast suite is database-free and stays that way. Integration tests are marked
`pytest.mark.integration` and excluded from the default run.
