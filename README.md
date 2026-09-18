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
applied 0005_queue_scoped_runtime
applied 0006_purge_function
applied 0007_retention_by_queue
applied 0008_heartbeat_ownership
applied 0009_retract_own_heartbeat
applied 0010_slot_ownership
applied 0011_scheduler_enqueue_grant
```

Queue tables live in their own schema, `task_queue` by default, never in your
application's business schema. Pass `--schema` to change it.

**Three migrations need the fleet stopped first.** 0005, 0008 and 0010 each
invert the usual ordering to "stop, clear, migrate, deploy" rather than
"migrate, deploy", and each checks that for itself rather than trusting the one
before it. 0005 is the worked example; the notes on 0008 and 0010 below say
what they add. It changes the heartbeat key, and the previous release writes
`ON CONFLICT (kind, instance)`, which stops matching any index the moment it
runs. That statement opens every
worker tick and its failure is retried, not fatal, so an old worker does not
crash -- it stalls, claiming nothing. The migration refuses to be the cause of
that:

```console
$ rqueue migrate
error: migration 0005_queue_scoped_runtime failed: rqueue: 1 runtime heartbeat row(s) present
HINT:  Stop every rqueue worker and scheduler and confirm the processes have
exited. Then DELETE FROM task_queue.runtime_heartbeats, wait one poll interval,
and check it is still empty -- a process that had not yet ticked refills it,
which is the only way to see one. Re-run the migration, then deploy.
```

(The schema in that hint is whichever one you are migrating, not necessarily
`task_queue`.)

The check is deliberately not "has anything beaten recently?" -- a worker with
a long poll interval is alive with an old heartbeat, and any window generous
enough for it means nothing. An *empty* table is checkable instead: rqueue
never deletes these rows, and a running process rewrites its own within one
poll interval. The migration takes `ACCESS EXCLUSIVE` on that table before
looking, so nothing can slip in between the check and the key change, and
nothing is applied when it refuses -- the running fleet keeps working while you
stop it.

Be exact about what an empty table proves: not "nothing is running", but
"nothing has ticked since it was cleared". A process that has started and not
yet reached its first tick has written nothing to see. So clear the table,
**wait a poll interval, and confirm it is still empty** -- that is what turns
such a process into a visible one, because ticking is the only thing it can do
next. A process started after that check fails from its first tick, having
never claimed a job, rather than degrading a fleet that was working. That
window is the drain, and it belongs to the deployment.

**Re-run `provision_role` for every role after upgrading rqueue.** The grant
table lives in `rqueue.roles`, which makes it part of the *release*, not part
of the schema: `migrate` moves the schema forward and leaves every existing
role holding whatever the version that provisioned it handed out. A role is
correct on the day it is provisioned and drifts from then on, so an upgrade is
two steps, not one:

```python
await provision_role(
    connection, role="cron_runner", capabilities=[Capability.SCHEDULE],
    queues=["compute"],
)
```

It is idempotent and it is the repair: the revokes run first, so the role ends
up holding exactly the capabilities named here and nothing else.

A migration can do some of this for you, and this release does what it safely
can -- 0011 below. What it cannot do is decide anything that depends on the
capability set, because the database has never recorded it. Grants can be
fingerprinted where a capability implies a privilege nothing else grants, and
that is enough to *add*; it is not enough to take away, since a privilege two
capabilities both confer looks identical either way.

What this release changes, and what you get by re-provisioning:

| Capability | Change | Until you re-provision |
| --- | --- | --- |
| `SCHEDULE` | `UPDATE (updated_at)` on `jobs` | Repaired by 0011; nothing to do |
| `SCHEDULE` | `DELETE` on `runtime_heartbeats` | Ticks and schedules normally, but cannot retract a heartbeat for a queue whose last schedule was disabled, so readiness reports it as still feeding that queue until the staleness window expires. Logged once, naming the queue and this remedy |
| `INSPECT` | `SELECT` on `concurrency_slots` and `runtime_heartbeats` | `check_readiness` raises rather than reporting, so a probe using an inspection role reports the database unreachable. This is a fix, not a regression -- the grant was missing before too |
| `CONSUME` | loses `INSERT` on `jobs` | The worker role stays able to enqueue, which is the separation this release adds. Only re-provisioning closes it |
| `PURGE` | new capability | Nothing; no existing role has it |

Nothing about 0008 *itself* needs a re-provision. The kinds a role may claim are
backfilled from the grants it already holds, and the policies read them through
a `SECURITY DEFINER` function rather than the table, so a role provisioned by
the previous release keeps writing its heartbeat with no new grant of any kind
-- which matters because a heartbeat is the liveness mechanism itself and has
nothing to fall back to. A row written before the migration is attributed to
whoever ran it and simply ages out.

**0008 changes the heartbeat key, so deploy after the whole migration, not
between.** The key gains the writing role, and the previous release's upsert
names `ON CONFLICT (kind, instance, queue)`, which stops matching any index the
moment 0008 lands:

```
ERROR:  there is no unique or exclusion constraint matching the ON CONFLICT
specification
```

That is the same failure 0005 describes, for the same reason, and 0008 refuses
in the same way: `ACCESS EXCLUSIVE` on the table, then the same demand that it
be empty. It does not lean on 0005 having asked. A migration body runs exactly
once, so a database that applied 0005 in an earlier release -- possibly months
ago -- never re-runs that check when it picks this one up; an upgrade in place
would otherwise walk straight into the new key with the fleet still running.
After the drain, applying every pending migration in one `rqueue migrate` is
safe, and it is the default.

Stopping in between -- `--target 7`, deploy, `--target 8` -- is the sequence
that puts a running previous-release worker in front of the new key, and it is
what the check catches: the redeployed fleet beats, and `--target 8` refuses
rather than stalling it. It has to be caught there, because nothing downstream
can help. The heartbeat upsert is the first statement of every tick, the error
is retried rather than fatal, and nothing in the new code runs inside that old
process to turn it into a better message.

Keeping a three-column unique index alongside the new key would make the old
statement resolve, and is not an option: it would forbid two roles from holding
separate rows for one component, which is the deadlock 0008's key change exists
to avoid. Grant compatibility and binary compatibility are separate questions,
and 0008 buys the first deliberately -- a role provisioned by the previous
release keeps working -- while the second is what this note is about.

**0010 needs the leases drained too, not just the fleet stopped.** It records
which role holds each concurrency slot, and there is nothing in a row written
before it that says who that was -- so every existing row is attributed to
whoever runs the migration. For a heartbeat that is harmless: 0008 leaves an
orphan that ages out while its component writes a fresh row on the next tick. A
slot is not a signal, it is the mutual exclusion for an attempt that is running
right now, and its real holder can neither extend nor release a row it no
longer owns. The lease would run out under the running attempt, and at that
moment the key becomes acquirable by any worker on the queue -- a second worker
starting the same logical job while the first is still inside it.

So 0010 refuses on either of two counts:

```console
$ rqueue migrate
error: migration 0010_slot_ownership failed: rqueue: 1 concurrency slot(s) still leased
HINT:  Stop every rqueue worker, confirm the processes have exited, and let the
outstanding leases expire -- bounded by the lease_seconds the workers ran with.
A slot outliving its worker is already stale, so DELETE FROM
task_queue.concurrency_slots is equally good once nothing is running. Re-run
the migration, then deploy.
```

and, separately, on a non-empty `runtime_heartbeats` -- because slots cannot
show you that the fleet is stopped. A worker between jobs holds no slot and is
invisible in that table, and it needs to be visible: the previous release's
takeover path never sets the owner column 0010 adds, so the first time it takes
over a slot another role left expired it fails with `new row violates row-level
security policy`. Inside a single `rqueue migrate` that second check costs
nothing, 0008 having just demanded the same thing; it is there for the staged
upgrade -- `--target 9`, deploy, `--target 10` -- which is the only way to
arrive at 0010 with a fleet running.

**0011 repairs the one grant a scheduler cannot do without.** Every enqueue is
an `INSERT ... ON CONFLICT ... DO UPDATE SET updated_at`, and PostgreSQL wants
`UPDATE` on every column a `DO UPDATE SET` names -- even one written back to
its own value. This release adds that column-scoped grant to `SCHEDULE`, and a
scheduler role from the previous release does not have it. There is no degraded
mode: it cannot enqueue at all, and `Scheduler.run` catches the error alongside
every other PostgreSQL failure, so what you see is `could not reach PostgreSQL;
retrying` once a tick while nothing fires.

So 0011 grants it, to every role in `role_queue_grants` that holds `INSERT` on
`schedules` -- the fingerprint 0008 already uses, and an exact one, since no
other capability grants that. Adding on a fingerprint is safe here because a
role it matches already holds `INSERT` on `jobs`: the repair widens nothing an
operator had not already agreed to. Taking away on a fingerprint would not be,
which is why the `CONSUME` row in the table above still needs you.

**0006 changes who may purge.** Retention used to be a plain `DELETE`, so any
role with that privilege could run it. It now goes through a `SECURITY
DEFINER` routine that authorizes the *login* role against `role_queue_grants`,
which is what lets a `PURGE` role delete without holding `DELETE` on `jobs`.
The trade is that privilege alone is no longer enough:

```console
ConfigurationError: role dba_ops is not granted queue alpha
```

Three identities are let through without a row: the schema owner, a superuser,
and any role `provision_role()` created -- it writes the row in the same call.
What is left is an operator identity that predates this and is neither: a
service account for a retention cron, say. Give it a row, or run the cron as
one of the three:

```python
await grant_queues(connection, role="retention_cron", queues=["*"])
```

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
re-raising it, `context.will_retry(exc)` reports whether the worker will
schedule another attempt -- the same decision the worker itself makes, so the
state you record cannot disagree with what happens next. It accounts for
`PermanentFailure`, reports `False` for `CancelJob`, and honours this job's own
persisted `max_attempts`, not the registered policy's default.

That last point covers `Retry` too. Raising it skips the policy's exception
filter, not the attempt budget: on the last attempt `will_retry` reports
`False` and the worker fails the job terminally, because rescheduling it would
return it to `pending` with no attempts left, where no worker would ever claim
it again.

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
hostage. Like `dedupe_key`, it is scoped to one queue: two queues using the
same key name do not exclude each other, because a queue is the boundary a
least-privilege role is scoped to and a lock that crossed it would be a lock a
scoped worker could neither see nor take.

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

The migration role owns the schema and is the only role that runs DDL. Queue
scoping is enforced by row-level-security policies driven by rows in
`role_queue_grants`, so the queues a role may touch arrive as bind parameters
rather than interpolated SQL. Six tables are under those policies: `jobs`,
`job_attempts`, `concurrency_slots`, `runtime_heartbeats`, `schedules`, and
`schedule_occurrences`.

A heartbeat is the one row whose queue scope is not the whole answer at all. On a
queue a worker and a scheduler share, "may this role touch this queue?" lets
either of them write the other's liveness row, so the row also records the role
that wrote it and only that role may refresh or retract it -- and the `kind` it
claims must be one that role's capabilities entitle it to, or a worker could
tell a readiness probe a scheduler is alive by inserting a row of its own.
Reading stays wide -- readiness has to see components it did not write -- and
retracting is narrower still: a role may delete a heartbeat it wrote whatever
it is currently granted, because taking a claim down asserts nothing. Requiring
the queue grant there would strand a scheduler's own row on a queue an operator
had just reassigned, unreadable and undeletable by anyone but the schema owner.

That is not every table carrying a queue name. Two are deliberately left out,
for different reasons and with different consequences -- one is readable by any
role that can reach the schema, the other is not directly readable at all:

- `queue_pauses`, because the wildcard pause is a row on the queue `'*'` and a
  policy scoped to a role's own queues would hide it. A worker that cannot see
  the global pause does not honour it, which is the one outcome the pause table
  exists to prevent. The cost is that a role granted one queue can see every
  queue's pause state -- when it was paused, and by implication that it exists.
- `role_runtime_kinds`, which records which component kinds a role may claim
  liveness as. Not directly readable at all, in fact: the heartbeat policies
  reach it through a `SECURITY DEFINER` function, so no runtime role needs a
  grant on it. It carries no queue column, so the count above is unaffected.
- `role_queue_grants`, because the policies on the six tables above are
  themselves subqueries against it, evaluated as the querying role. Under RLS
  it would filter itself and every other policy would match nothing. It is
  granted `SELECT` directly instead, so a scoped role can read every role's
  queue grants -- the shape of the deployment, not a way into it. Only the
  migration role may write it, which is what keeps a role from granting itself
  a queue.

A policy can only test the queue the writer wrote, and on a row that also
points at a job that is a label rather than a boundary. Those labels are tied
to their job by composite foreign key -- attempt records, concurrency slots,
and occurrences alike -- so a role granted one queue cannot attach its own
label to another queue's job and pass its own policy. That matters because the
keys those rows carry are global: an attempt number, a slot key, an occurrence
instant. Forging one consumes it, and the queue that really owns it is denied
service.

The anchor is always the job, because a job's queue never changes. A schedule's
can, so an occurrence takes its queue from the job it fired, not from the
schedule that fired it -- moving a schedule leaves its existing history where
those jobs actually live. The occurrence key itself stays queue-free: it is the
one-firing-per-instant guarantee and has to mean the same thing from every
queue. What protects it is the policy, which asks whether the writer can see
the schedule at all -- so a scheduler reads the whole history of a schedule it
owns, across a queue move, but can only write occurrences for schedules on
queues it holds.

A scheduler reports liveness for the queues it schedules. With no enabled
schedules it reports none, because nothing is being scheduled -- there is no
queue for it to be the live scheduler of.

| Capability | May |
| --- | --- |
| `PRODUCE` | enqueue and read jobs (`UPDATE` only on `updated_at`, for the enqueue conflict path) |
| `CONSUME` | claim, heartbeat, and transition jobs -- but **not** enqueue them |
| `SCHEDULE` | read schedules and enqueue their occurrences -- but not transition a job |
| `INSPECT` | read everything, write nothing, including the liveness a readiness probe needs |
| `PURGE` | delete terminal jobs, through `purge_terminal_jobs()` and only through it |

`CONSUME` has no `INSERT` on `jobs`: claiming work and creating it are separate
authorities, and a worker that can enqueue can hand the fleet any task it
likes. A process that legitimately does both -- a worker that fans out
follow-up jobs -- asks for `[Capability.PRODUCE, Capability.CONSUME]` and says
so in its provisioning call.

`PURGE` has no `DELETE` on `jobs` either. It gets `EXECUTE` on
`task_queue.purge_terminal_jobs(queue, states, finished_before, max_rows)`, a
`SECURITY DEFINER` routine installed by migration 0006 that re-derives every
limit for itself: one named queue the caller is granted, terminal states only,
a cutoff in the past, and a bounded batch. The routine is the safety boundary,
not the grant -- a crafted call cannot reach a `pending` job, another queue's
jobs, or the whole table.

`Admin.purge()` and `rqueue purge` go through that routine, so they are exactly
what `PURGE` authorizes -- there is no second, rawer path:

```python
removed = await Admin(pool).purge(queue="compute", retention=timedelta(days=30))
```

Naming a queue is the cheaper call. "Every queue" is spelled by omitting the
argument, not by the `"*"` that pause and resume take -- purge reads `"*"` as a
queue name, and there is no queue by that name. Omitting it purges every queue
that has anything to purge -- for a scoped role, every queue it can see --
spending `limit` across them as one budget, oldest job first regardless of
which queue it is on, and deleting that many when that many exist. None of that
follows from visiting the queues in a good order: the cutoff is first tightened
to the age of the budget's last row -- inclusive of every job finishing in the
same instant, since a transaction that finishes a hundred jobs gives them all
one `finished_at` -- so no queue can reach past it however much backlog it has.
Tightening it is also what keeps the work proportional to `limit` rather than
to the backlog behind it. Only terminal states may be named; asking for a live
one raises rather than quietly matching nothing. The cutoff must be
timezone-aware, as every caller-supplied instant in rqueue is.

A role that *owns* a table in the schema, the schema itself, or the database is
refused outright, with nothing written. Ownership sits above the ACL that
`provision_role()` edits: an owner can re-grant itself anything, and `ALTER` or
`DROP` the table -- including switching row-level security off. Reassign the
object to the migration role first.

Provisioning also repairs what lies outside the schema. A provisioned role is
set `NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION NOINHERIT`
and stripped of every role membership before its capability grants are applied,
so re-running `provision_role()` on a role someone quietly made a superuser
really does narrow it. There is no switch to ask for less: a connection that
cannot clear an attribute the role holds raises, rather than returning a role
that is only partly narrowed.

The whole call is one transaction, so that holds for every other failure too.
Granting `PURGE` against a schema whose migrations stop short of the purge
routine is the one you meet mid-deploy; it raises, and the role is left exactly
as the call found it -- absent if it did not exist, untouched if it did --
rather than existing, able to log in, and carrying half a capability set.

## Storage

| Table | Holds |
| --- | --- |
| `jobs` | one row per job, including its lease and terminal outcome |
| `job_attempts` | one immutable record per attempt, for audit and debugging |
| `concurrency_slots` | the lease behind each named concurrency key, per queue |
| `schedules` | periodic schedule definitions |
| `schedule_occurrences` | the occurrence keys that have fired, and their jobs |
| `runtime_heartbeats` | worker and scheduler liveness per queue, for readiness checks |
| `queue_pauses` | which queues are paused, and since when |
| `role_queue_grants` | which queues a granted role may reach |
| `schema_migrations` | applied migration versions and their checksums |

Plus one routine: `purge_terminal_jobs()`, the `SECURITY DEFINER` boundary the
`PURGE` capability is granted instead of a table-wide `DELETE`.

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
