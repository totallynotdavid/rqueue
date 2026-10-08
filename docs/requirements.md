# rqueue: package requirements for a PostgreSQL task queue on asyncpg

Revision 2. Supersedes the original draft. Changes from the draft are called
out inline as **Revised:** notes, each with the reasoning that produced them.
Keep those notes. They are the record of why the design is shaped this way,
not just what it says.

## 1. Purpose

Build a small, durable PostgreSQL task queue for Python applications that use
`asyncpg`. Its defining feature is *transactional enqueueing*: an application
can insert/update its own rows and enqueue a job through the **same**
`asyncpg.Connection` and PostgreSQL transaction.

**Revised:** the original draft framed this as filling a gap in Procrastinate
("Procrastinate doesn't support transactional enqueueing"). That's false.
Procrastinate's `PsycopgConnector` already supports deferring a job on a
caller-supplied connection inside the caller's transaction
(`configure(connection=conn).defer_async(...)`), and so does `pgqueuer`'s
`AsyncpgDriver`. The actual reason to build this package is narrower and more
honest: **avoid running two PostgreSQL drivers in one process.** The
consuming application (picv-2025) already speaks asyncpg end to end via
`relq`. Adding Procrastinate means a second driver, a second connection pool,
and a second set of type codecs to reason about. A queue that speaks asyncpg
natively lets the queue and the application share one pool and one
`Connection` type.

The package is intended to replace the queue boundary currently provided by
Procrastinate for asyncpg/relq applications. It must support the TSDHN shape:
durable chained jobs, delayed retries, periodic maintenance jobs, named
concurrency locks, worker-crash recovery, and observable job state.

It is a task queue, not a workflow engine, ORM, generic database layer, or
distributed transaction coordinator.

### Why not adopt `pgqueuer` instead of building this?

`pgqueuer` (janbjorge/pgqueuer, Python + asyncpg) is mature and well-engineered.
It has MVCC-safe concurrency-slot allocation and batch CTE-based claiming, and
transactional enqueue on a caller connection already works today. It was evaluated directly
(schema and query builder read in full) before starting this package. It has
three gaps against the requirements below that are correctness/feature gaps,
not style differences, and are the actual justification for building rather
than adopting:

1. **No fencing token.** Its crash recovery is heartbeat-only. It is the same
   coarse model River (Go) uses: a job is reclaimed when
   `heartbeat < NOW() - timeout`, with no token comparison. Nothing stops a
   worker that is merely slow (GC pause, stalled heartbeat writer, not
   actually dead) from completing a job after another worker has already
   reclaimed and finished it. §3's lease-token requirement is this package's
   most load-bearing guarantee and pgqueuer does not provide it.
2. **No named concurrency key on an arbitrary business resource.** pgqueuer
   limits concurrency per *entrypoint* (task type) only. It has no equivalent
   to Procrastinate's `lock=f"compute-job:{id}"`. That lock is mutual exclusion scoped to
   an application-chosen key, which picv-2025 needs ("one simulation per
   compute job").
3. **Dedup's skip mode drops silently.** `on_conflict="skip"` does
   `ON CONFLICT DO NOTHING` and returns nothing for the skipped row. §3's
   dedupe_key requirement is to return the existing job on conflict, not just
   silently absorb the call.

## 2. Platform and dependencies

- Python 3.13+.
- PostgreSQL 14+ only. Where a statement exists only in a later release, the
  server version decides which form is issued: role-membership `REVOKE ...
  GRANTED BY` is 16+, and is both unavailable and unnecessary before it, since
  14 and 15 record one grantor per membership and a bare `REVOKE` removes it.
- `asyncpg` is the sole database driver and the only required runtime
  dependency.
- The public API is asyncio-native. No synchronous API, psycopg adapter,
  SQLAlchemy integration, Redis, RabbitMQ, or broker abstraction.
- The package owns versioned PostgreSQL migrations. It must not perform
  implicit schema creation at import time or on normal worker startup.
- `relq` is optional from the queue package's perspective. The queue accepts
  an `asyncpg.Connection`. Applications may use that exact connection through
  `relq_postgres.PostgresDatabase` inside the same transaction.

**Revised:** the original draft pinned Python 3.15+. As of this writing
(2026-08-31), Python 3.15 has not reached GA (expected ~October 2026 per the
PEP 719 cadence) and no released `asyncpg` build supports it. The latest
(0.31.0) tops out at 3.14. Pinning 3.15+ today means the dependency cannot be
installed by anyone. 3.13+ was chosen instead: fully supported by current
asyncpg, and gives `asyncio.TaskGroup`/exception groups for the worker's
bounded-concurrency loop.

## 3. Core correctness model

### Delivery and idempotency

- Delivery is **at least once**. A worker may run a handler again after a
  crash, lease expiry, or ambiguous network failure.
- The package must never claim exactly-once execution. Task handlers and
  external side effects must be idempotent or use an application-level
  idempotency key/fencing check.
- A job has an immutable UUID id, task name, JSON payload, queue name,
  creation time, scheduling time, attempt count, max attempts, and durable
  terminal outcome.
- Payloads are JSON only. Callables, pickles, arbitrary Python objects, and
  import-path execution are prohibited. Workers execute only explicitly
  registered task names.

**Revised:** `pgqueuer` stores payload as opaque `BYTEA` and leaves
serialization entirely to the application. That's a legitimate but looser
tradeoff. This package deliberately keeps the stricter JSON-only requirement
from the original draft (no pickle, no arbitrary bytes).

### Atomic enqueueing

The enqueue API takes a caller-owned `asyncpg.Connection`. It never opens a
second connection:

```python
async with pool.acquire() as connection:
    async with connection.transaction():
        await app_db.execute(create_compute_job(...))
        job = await queue.enqueue(
            connection,
            task="prepare_simulation",
            payload={"compute_job_id": str(compute_job_id)},
            dedupe_key=f"simulation:{external_id}",
        )
        await app_db.execute(record_queue_job_id(compute_job_id, job.id))
```

If this transaction rolls back, no application row, queue row, or queue-job
reference may survive. The worker must use a different pooled connection only
after the transaction commits.

**Idiomatic usage: keep business logic decoupled from the queue import.**
picv-2025's `repository.py` never imports Procrastinate directly: its
`create_or_get_job(*, data, simulation_id, defer)` takes a `defer` callback,
and the caller passes a thin wrapper (`enqueue_simulation(conn, job_id)`) that
calls the queue. This package's `enqueue` should be trivially wrappable in
that shape. Document it as the intended pattern in the quickstart, not just
something that happens to work.

- `enqueue_many(connection, jobs)` is atomic and validates every job before
  writing any row.
- A durable `dedupe_key` is scoped to a queue and has a documented lifecycle.
  Concurrent enqueue attempts with the same active key return the existing job
  or raise `AlreadyEnqueued`. The choice must be explicit per call.
- PostgreSQL `NOTIFY` may wake workers after commit, but polling remains the
  source of truth. A missed notification must only add latency, never lose a
  job.

**Revised:** fire `NOTIFY` selectively, not on every write. River's trigger
only fires on transition into an "available" state, with a minimal payload
(queue name, not the row). pgqueuer's fires on every INSERT/UPDATE/DELETE.
Prefer River's tighter version: fewer wakeups, and no reason to notify on a
heartbeat-only UPDATE.

### Claiming, leases, and fencing

- A worker claims eligible jobs with one short transaction using
  `FOR UPDATE SKIP LOCKED`.
- A claim records `worker_id`, a fresh opaque `lease_token`, `leased_until`,
  and attempt number. Only the current lease token may heartbeat, complete,
  fail, or reschedule that attempt.
- Lease expiry makes an unfinished job eligible again. A stale worker must be
  unable to overwrite the later attempt's result.
- Job state transitions are validated by SQL predicates, not only Python
  checks. Terminal jobs cannot become runnable again except through an
  explicit operator retry operation.
- A worker uses a bounded-concurrency loop. It must not hold a database
  transaction open while user code runs.

**This is the package's strongest guarantee relative to prior art.** See §1.
Neither River nor pgqueuer implement lease-token fencing. Both rely on
heartbeat-timeout reclaim alone. Do not weaken this to match them.

## 4. Public API

The initial public surface should be intentionally small:

```python
queue = Queue(pool, name="compute")

@queue.task(name="prepare_simulation")
async def prepare_simulation(payload: PreparePayload, context: TaskContext) -> None:
    ...

await queue.enqueue(connection, task="prepare_simulation", payload={...})
await queue.enqueue_many(connection, [...])

worker = Worker(queue, worker_id="api-worker-1", concurrency=4)
await worker.run()
```

- `Worker.stop()` requests a graceful stop. `await worker.shutdown()` is the
  awaitable lifecycle operation for a worker running in a background task. Its
  `timeout` bounds the in-flight handler grace period.
- `executor_shutdown="wait"` is the safe default. After handing leases back,
  the worker waits for blocking work submitted through `asyncio.to_thread`.
  `executor_shutdown="detach"` returns without waiting for a still-running
  thread, but does not interrupt it. A process using detach must be terminated
  by its supervisor afterward and must not reuse the detached executor or start
  new work in that process. `shutdown(wait_for_blocking_threads=...)` may
  explicitly override the mode for one shutdown.
- Task registration is explicit and unique by task name. Worker startup fails
  if a queued task name has no registered handler, unless configured to leave
  it pending for a different worker deployment.
- Task payload validation is explicit. The package provides a JSON payload
  protocol. Applications may supply a Pydantic TypeAdapter or a plain decoder.
  Decode failures are non-retryable and become a durable failed job.
- `TaskContext` exposes only job id, attempt number, queue, lease-aware
  heartbeat, logger/structured fields, cooperative cancellation state, and the
  `will_retry()` decision hook. It must not expose raw SQL or permit mutation
  of queue internals.
- **Handlers are `async def` only, with no sync-handler code path.** If a task's
  work is blocking (CPU-bound simulation, subprocess, blocking I/O), the
  handler wraps it explicitly: `await asyncio.to_thread(blocking_fn, ...)`.
  `Worker` installs a bounded `ThreadPoolExecutor` (sized from `concurrency`)
  as the event loop's default executor at startup, so a bare
  `asyncio.to_thread(...)` call is automatically capacity-limited without
  every task author constructing their own executor.
- The package offers explicit inspection/administration operations: get job,
  list by state, retry terminal job, cancel pending job, purge completed jobs
  older than a retention period, and pause/resume a queue. These are not part
  of handler context.

**Revised:** the original draft already specified async-only handlers. An
intermediate revision of this document proposed adding first-class sync
handler support (justified by picv-2025's numba-based handler being fully
synchronous). That was reversed after reading `pgqueuer`'s executor code: it
used to support sync entrypoints and *removed the feature*, with the executor
now raising `TypeError("Sync entrypoints are no longer supported... wrap
blocking code with asyncio.to_thread()")`. That is direct evidence from a
project that tried the "auto-detect and route to a thread" design and walked
it back. Stay async-only. The one-line `asyncio.to_thread` wrapper is not
meaningful ceremony for a fully-synchronous handler, and it keeps the
worker's execution model to one path instead of two.

**Revised (2026-09-02):** this document never considered **durable,
queue-wide pause/resume**, and the omission was an oversight rather than a
decision. Both mature references have it: Oban's `pause_queue`/`resume_queue`
(with a `:*` wildcard for every queue) and River's `QueuePause`/`QueueResume`.
It is a different operation from anything specified above: it stops a
queue admitting *new* work across every worker replica at once, without
killing a process, where `Worker.stop()`/`Worker.drain()` only ever affect the
one worker instance they are called on and `cancel_job` only ever affects one
job. An operator draining a database, holding back a queue during an incident,
or gating a deploy has no way to express that with the API as specified. The
decision is to build it, as `Admin.pause_queue(name)` /
`Admin.resume_queue(name)`, with `'*'` meaning every queue.

The design, briefly: a durable `queue_pauses` table is the single source of
truth. Its `paused_at timestamptz` column is NULL when the queue is running, and
it holds the instant rather than a boolean so an admin view gets "paused since"
for free. The claim query itself carries the predicate, so a paused queue yields
zero claimable rows in SQL regardless of what any worker process currently
believes. `NOTIFY` on resume is a latency optimization for an idle worker and
nothing more. That follows §3's existing rule for job wake-ups ("polling is the
source of truth") and this document's stance that state transitions are
enforced in SQL, not only in Python. That is also why Oban's model was *not*
copied: Oban keeps `paused`
in each producer process and rebroadcasts it over PubSub, so a restarted queue
comes back running. It goes one step past River too: River stores the row but
still tests it client-side before fetching, so a worker that has not yet polled
can still issue a claim against a paused queue. Because rqueue already makes
every claim decision in one statement, the check costs one InitPlan evaluation
per claim and holds for workers that have never heard of the call.

Deliberately *not* adopted from Oban: starting a `Worker` already paused
(`Oban.start_queue(paused: true)`). In Oban the flag is per-producer state, so
a constructor argument is the only way to express it. Here the pause is durable
and queue-wide, so `await admin.pause_queue(name)` before starting a worker
already says it, exactly once, for the whole fleet. A per-`Worker` argument
would either write global state from one replica's constructor or gate
admission in Python, and both are worse than the call that already exists.

## 5. Retry, failure, and cancellation semantics

- A task result is one of: success, retry at a specified timestamp, terminal
  failure, or cancellation.
- Retry policy is declared per task: max attempts, exponential backoff,
  bounded jitter, and retryable exception classes/predicate. The persisted
  attempt count is the sole authority across crashes and worker restarts.
- Exception details recorded in PostgreSQL are bounded and sanitized. Full
  tracebacks belong in structured worker logs, not unbounded database fields
  or user-facing responses.
- Cancellation is cooperative. Cancelling a queued job prevents execution.
  Cancelling a leased job signals its handler and is finalized only by the
  lease-holder or lease expiry.
- Timeouts must be explicit per task. Timeout cancellation cannot be treated
  as proof that an external side effect did not occur.

## 6. Scheduling and concurrency controls

- Delayed jobs use `scheduled_at`. Workers claim only due jobs.
- Periodic jobs support a validated cron expression and timezone. A scheduler
  emits one durable job occurrence at a time via a unique **occurrence key**:
  one row per `(schedule_id, occurrence_time)` with a unique constraint,
  inserted in the same transaction as the job it produces. Multiple scheduler
  replicas must be safe.
- Optional named `dedupe_key` prevents duplicate active jobs. It is not a
  substitute for a general workflow/DAG system.
- Optional named concurrency keys limit an externally shared resource (for
  example, one simulation per compute job). Their semantics must be lease
  based and crash safe. Do not model them as a permanently held Boolean.
  These are a distinct primitive from `dedupe_key`. Dedupe prevents a
  duplicate *job from being queued*. A concurrency key prevents duplicate
  *concurrent execution*. Procrastinate models this split as `queueing_lock`
  vs. `lock`. Keep the two-key model and do not collapse them.
- Priority is a small integer and is resolved alongside `scheduled_at` and
  creation order. Fairness between queues must be documented, not implied.

**Revised:** the original draft offered "a PostgreSQL advisory lock or unique
occurrence key" as alternatives for periodic-job dedup. This revision commits
to occurrence-key only, and rules out both the advisory-lock approach and the
alternative seen in prior art. Four approaches were compared directly:

- **Session advisory lock** (originally proposed): fragile under any
  connection pooling. The lock is session-scoped, and a scheduler that ever
  borrows a pooled connection silently loses the guarantee.
- **Leader election** (River's approach: an `UNLOGGED river_leader` table,
  single elected instance runs the scheduler): works, but is real machinery
  (election, liveness, failover) to avoid double-firing.
- **Claim-the-schedule-row** (pgqueuer's approach: the schedule row itself
  is claimed via the same `SKIP LOCKED` + heartbeat-reclaim as jobs, and
  `next_run` is advanced optimistically at claim time): simpler than leader
  election, but has a real gap: a crash between claiming the schedule row
  and durably creating its job can advance `next_run` without ever producing
  the occurrence, silently skipping a firing.
- **Occurrence-key table** (kept): insert `(schedule_id, occurrence_time)`
  with a unique constraint, in the same transaction that creates the
  occurrence's job. No reclaim logic needed at all. If the transaction
  didn't commit, the occurrence was never marked fired, so the next
  scheduler tick just tries again. Durable, survives any pooling, and gives
  a free audit trail of what fired when.

## 7. Storage, migrations, and operations

- Queue tables live in a configurable schema (default `task_queue`), never in
  an application's business schema by default.
- Required tables cover jobs, attempts/lease history, periodic schedules and
  their fired occurrences, queue pause state (see §4), and schema migration
  state. Separate immutable
  attempt records are preferred for auditability and debugging.
- Indexes must support due-job claiming, active dedupe keys, lease expiry,
  queue/state inspection, and retention cleanup. Explain every index with its
  serving query.
- Migration execution is an explicit CLI command and takes an advisory lock.
  Migrations are forward-only, transactional where PostgreSQL permits, and
  tested from an empty database and the immediately previous release.
- A migration that the previous release's *running* code cannot survive must
  detect that code and refuse, rather than apply and break it. Runtime
  liveness is already recorded, so "is the old fleet still up?" is a question
  the database can answer. The refusal names the required order, in the
  deployment's own schema rather than the default one. What such a check can
  prove must be stated exactly: absence of a trace is not absence of a
  process. The gap it leaves is named as the drain window. Testing "from
  the previous release" therefore means starting at the previous migration and
  exercising the previous release's own statements across the transition, not
  starting at the new one.
- Each such migration carries its own check. A body runs exactly once, so a
  database that applied an earlier one in an earlier release never re-executes
  it, and a check inherited by comment holds only for a database installed
  from empty in a single `migrate`. Where a migration also strands rows the
  running fleet owns, the check covers those as well, and the liveness half
  of it reads the table written on a timer, not the one the migration is
  changing: a component idle between units of work leaves no trace in the
  latter.
- Retention is oldest-first across the whole schema, not only within one
  queue. A bounded purge reads work proportional to its limit rather than to
  the backlog behind it. It also delivers that limit when the rows exist. All
  three follow from bounding the delete by the age of the budget's last row,
  inclusive of every row sharing that instant. Jobs finished in one
  transaction share a `finished_at`, so a tie group routinely straddles the
  boundary. None of them follows from the order queues are visited in, since
  one queue can hold the oldest row and enough newer ones to exhaust the
  budget by itself. The cost bound needs its own limit as well as that one: a
  tie group has no size bound, so the age of the budget's last row does not
  bound how many rows share it, and every scan taken against that cutoff must
  carry the budget too.
- Purging by queue and purging the whole schema are the same operation with
  one argument different. Given identical data they delete identically.
- Expose structured metrics/logging hooks for queue depth, oldest ready job,
  claim latency, handler duration, retry/failure count, expired leases, and
  notification/poll wakeups. Do not require a metrics vendor.
- Provide readiness checks that distinguish PostgreSQL connectivity, migration
  version, worker availability, and scheduler availability.

## 8. Security and resource limits

- Never execute user-provided code, SQL, shell commands, or serialized Python
  objects from a payload.
- Bound payload size, task name length, error text, metadata, attempts,
  scheduling horizon, poll batch size, and worker concurrency.
- Use parameterized SQL exclusively. Internal SQL is static, reviewed, and
  isolated in the storage implementation.
- Support PostgreSQL roles with least privilege. API producers may enqueue and
  inspect only their allowed queues. Workers may claim and transition jobs on
  their queues. The migration role owns DDL. A worker capability grants no `INSERT` on `jobs`.
  Claiming work and creating it are separate authorities, and a role that needs
  both asks for both capabilities.
- Retention deletion is a capability of its own, and it is not a table-wide
  `DELETE`. The purge capability grants `EXECUTE` on a `SECURITY DEFINER`
  routine that re-derives queue, terminal state, age cutoff, and batch size
  from its own arguments. The routine is the safety boundary, so a crafted call
  cannot reach a non-terminal job, another queue's jobs, or an unbounded batch.
  The package's own retention API goes through that routine and no other path,
  so holding the capability is sufficient to run it. A capability whose only
  caller is hand-written SQL is not one. The corollary is a breaking change to
  document, not to soften: a privilege alone no longer authorizes retention,
  so an operator identity that is neither the schema owner nor queue-granted
  loses an ability it had.
- A bound duplicated between Python and a migration must be tested to agree.
  Migrations are checksummed and forward-only, so the SQL side can never be
  corrected afterwards. An unchecked pair silently degrades a typed error into
  a raw database one.
- Ownership is not a grant and cannot be pruned like one. Provisioning must
  refuse a role that owns the schema, the database, or an object in the
  schema: an owner re-grants itself anything the moment it is narrowed. "An
  object" means whatever catalog it lives in, not the handful a grant model
  thinks about: relations, routines, types, collations, conversions,
  operators and their classes and families, text-search dictionaries and
  configurations, extended statistics, extensions. A check written catalog by
  catalog is a list that falls behind the server, so the ownership record
  PostgreSQL keeps for every object alike is the one to read.
- The capability grant table belongs to the release, and the schema version
  does not track it. `migrate` moves the schema. The privileges an existing
  role holds only move when `provision_role` runs again, so every release that
  changes the table states what an un-reprovisioned role loses. A migration may
  repair what it can derive: a capability that implies a privilege nothing else
  grants is an exact fingerprint, and adding on one is safe because the roles
  it matches already hold the neighbouring privileges. Removing on one is not,
  because a privilege two capabilities both confer is indistinguishable, and
  the capability set is the caller's. It is never recorded, so never the
  database's to infer. A change the fingerprint cannot repair is documented
  with its failure mode. A change that leaves a component unable to work at all
  rather than degraded is repaired.
- Provisioning a role is one transaction. It issues a dozen statements across
  the role, the database, the schema, each table, each routine, and the queue
  grants. A failure part way through must leave the role exactly as it was
  rather than existing, able to log in, and holding whichever half of the
  capability set was applied first.
- Narrowing a role means every privilege class, not the ones a grant model
  happens to use. Tables, routines, and sequences are separate classes, and
  `REVOKE ... ON ALL TABLES` reaches none of the others. An identity column
  is a sequence object of its own, so a privilege on one outlives every repair
  that names only tables.
- Role provisioning is responsible for the privileges that live outside the
  schema as well as the grants inside it. Repairing a role sets `NOSUPERUSER
  NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION NOINHERIT` and revokes the
  role memberships it did not grant, before applying capability grants. There is
  no opt-out: a caller that cannot complete the repair gets an error rather than
  a partly narrowed role, because the next thing done with that role is handing
  out its credentials.
- Row-level security covers the six tables that hold queue-scoped *work*, not
  only `jobs` and `job_attempts`. Concurrency slots, runtime heartbeats,
  schedules, and schedule occurrences are queue-scoped by the same
  `role_queue_grants` policy.
- Two queue-carrying tables are outside the policies by design and readable in
  full. `queue_pauses` is outside because a policy scoped to a role's own
  queues would hide the `'*'` global pause from the workers that must honour
  it. `role_queue_grants` is outside because every policy above is a subquery
  against it evaluated as the querying role, so under RLS it would filter
  itself and every policy would match nothing. Both leak the shape of the
  deployment (which queues exist, which are paused, which roles hold what), and
  neither is writable by a scoped role.
- `role_runtime_kinds`, which records the kinds a role may claim liveness as, is
  outside the policies for the same reason as `role_queue_grants` and carries no
  queue. It is not directly readable either, since the policies reach it
  through a `SECURITY DEFINER` function.
- Where such a table's own key was global, it is narrowed to include the queue,
  so that the key an upsert resolves against and the policy that decides
  visibility agree on what identifies a row. A concurrency key is therefore
  scoped to its queue, as `dedupe_key` already was, and a heartbeat is one
  component's liveness on one queue.
- Where such a row also references a job or a schedule, its queue must be tied
  to that reference by constraint, not merely written by the caller. A policy
  can only test the label, so a label the writer chooses is not a boundary.
  This matters wherever such a row also carries a globally unique key (an
  attempt number, a slot key, an occurrence instant), because forging the label
  consumes that key and denies service to the queue that owns it.
- Liveness is recorded per queue, so a component that serves several records
  one heartbeat per queue served, and an unfiltered liveness query counts each
  instance once. A component serving no queue records nothing rather than
  claiming a queue it does not feed.
- Where a queue label is tied to a reference, the reference must be one whose
  queue cannot change. A job qualifies. A schedule does not.
- A uniqueness guarantee that spans queues must not be scoped by queue. The
  occurrence key is one: scoping it would let a schedule moved between queues
  fire its history again. Such a key is protected by a policy that asks what
  the writer can see, not by narrowing the key.

### Shared durable state: states, owners, and ordering

The tables above are written by several processes at once, and the bullets in
this section and in §6 and §7 constrain them one property at a time. This is
the same content stated once as a whole, because a concurrency invariant that
is only ever described in pieces is one nobody can check.

For each, the complete state set, who may cause each transition, and what is
ordered with respect to what. "Operator" means an `Admin` call or the CLI.
"Migration role" means the identity that runs DDL.

**`concurrency_slots`**: a row *is* the lease on a named key.

| State | Meaning |
| --- | --- |
| absent | the key is free |
| held | a row whose `leased_until` is in the future |
| expired | a row whose `leased_until` has passed, so it is free but not yet reaped |

- Worker, claiming: absent or expired → held, as one `INSERT ... ON CONFLICT
  (queue, key) DO UPDATE ... WHERE leased_until <= now()`. Two workers racing
  for one key serialize on the row. The loser claims no job.
- Worker, finalizing: held → absent, `DELETE` matched on `job_id` *and*
  `lease_token`. A worker whose lease expired cannot release the slot its
  successor now holds.
- Time: held → expired. No actor, no statement, no reaper.
- A slot records the role holding it, and only that role may extend or release
  it while the lease is live. Queue scope cannot express this: every worker on
  a queue shares the slot table, so a policy that stops at the queue lets any
  of them delete a slot another is holding, after which the key is held twice.
- The expiry branch is the exception, and it is the table's oldest rule: an
  expired slot may be taken over or cleared by any role granted the queue,
  because the holder it is cleaning up after is by definition not there to do
  it. Taking one over takes over the ownership with it.
- The slot is acquired inside the claim transaction and released inside the
  finalize transaction, so a job is never leased without its slot and never
  holds a slot after its attempt is durable.

**`runtime_heartbeats`**: one row per `(kind, instance, queue, role)`.

| State | Meaning |
| --- | --- |
| absent | that component is not serving that queue |
| fresh | `updated_at` within the caller's staleness window |
| stale | older than it, so the component is presumed dead |

- Worker or scheduler: absent or stale → fresh, upserting its own row once per
  tick, for each queue it serves and no other.
- Scheduler: fresh → absent, for a queue that has dropped out of its enabled
  set, on the same tick. Retraction is scoped by ownership alone: a role may
  always take down what it wrote, whatever it is currently entitled to serve.
  Every other condition on it is a way for a claim to outlive the thing it
  claims, and under row-level security it fails silently, because an excluded
  row is invisible rather than forbidden. A worker's queues are fixed at construction, so it has
  nothing to retract.
- A policy must not depend on a grant that only provisioning can make. A policy
  expression is evaluated as the querying role, so a table a policy reads is a
  table every role it governs must hold `SELECT` on, and a migration cannot
  grant that to roles that already exist. A table introduced alongside the
  policy that reads it is therefore reached through a `SECURITY DEFINER`
  function, not directly.
- A change to a key that a running process's statements name is a change that
  process cannot survive, and no grant or policy can soften it: an `ON CONFLICT`
  target resolves against an index, and one that no longer exists is an error
  on the next statement, inside a binary none of the new code runs in. Such a
  migration must either not be reachable while the previous release is running,
  or say plainly that it is not. Keeping the old key alongside the new one is
  not a third option when the two disagree about what a row is.
- A privilege a running role may lack is asked for only when it is needed. A
  schema migration cannot re-grant anything to an existing role, so a grant
  added by one is absent until an operator re-provisions. A statement issued
  unconditionally would fail every tick of every deployment, including the ones
  with nothing to do. When it is needed and refused, the claim is left to go
  stale rather than the tick to fail.
- Time: fresh → stale. This is the answer for a process that *died*. It cannot
  retract anything, so time must. It is not the answer for a running process
  whose responsibilities changed. That one says so itself.
- No component writes another's row, and no operator writes any of them. This
  is enforced, not merely intended: the row records the role that wrote it and
  a role may only write, refresh or retract its own. Queue scope alone cannot
  say it. On a queue a worker and a scheduler share, it lets either write the
  other's row.
- A role may only claim a `kind` its capabilities entitle it to, recorded at
  provisioning time. Ownership alone leaves the other half open: a worker role
  inserting a row that says `scheduler` owns that row legitimately, and tells a
  readiness probe the same lie as overwriting a real one.
- Ordering within a tick: the upserts commit with the retraction, so a probe
  never sees a served queue without its heartbeat.

**`schedules`**: declared in code, synced to the database.

| State | Meaning |
| --- | --- |
| enabled | `tick()` considers it |
| disabled | it is ignored, and its history is kept |

- Scheduler `sync()`, or operator: either direction. A schedule's queue may
  also change, which is why nothing else may be tied to it (see the bullet on
  references whose queue cannot change).
- Disabling is not deleting: occurrences already fired stay, and re-enabling
  does not re-fire them.

**`schedule_occurrences`**: one row per `(schedule_id, occurrence_at)`, and
the exactly-once guarantee for scheduling.

| State | Meaning |
| --- | --- |
| absent | that occurrence has not fired |
| fired | it has, in the same transaction as the job it created |

- Scheduler: absent → fired, insert-only. There is no update and no delete. The
  `SCHEDULE` capability grants neither.
- A plain `INSERT`, not `ON CONFLICT DO NOTHING`, so a second scheduler waits
  on the first's uncommitted row and proceeds only if that transaction aborted.
- Removed only by cascade, with the job or the schedule.
- The key is not queue-scoped: a schedule moved between queues must not fire
  its history again.

**`queue_pauses`**: admission control, not lifecycle.

| State | Meaning |
| --- | --- |
| absent | the queue admits work |
| paused | a row with `paused_at` set, so claims yield nothing |

- Operator only, either direction. Pausing twice keeps the first `paused_at`.
- `'*'` is a row like any other and means every queue. Resuming `'*'` clears
  the named pauses too. Resuming one queue does not lift the wildcard.
- The gate is in the claim SQL, so it binds every worker whatever a `Worker`
  believes. It never touches a job already leased.

**`role_queue_grants`**: the data the policies read.

- Migration role only. A scoped role holds `SELECT` and nothing else, which is
  what stops it granting itself a queue.

**`role_runtime_kinds`**: which component kinds a role may claim liveness as.

| State | Meaning |
| --- | --- |
| no rows | the role may write no heartbeat at all |
| `worker` | it holds `CONSUME`, and may claim liveness as a worker |
| `scheduler` | it holds `SCHEDULE`, and may claim liveness as a scheduler |
| both | it holds both capabilities, and may claim either |

- `provision_role` only. Not an operator, not a runtime component, and not the
  component whose kind it describes. A role that could write this table could
  authorize its own liveness claims, which is the whole thing the table is for.
- Replaced on every provisioning call, not merged into: the rows are deleted
  and re-inserted from the capability set in that call. So narrowing a role
  takes its kinds with it. Re-provisioning a `CONSUME`-and-`SCHEDULE` role
  with `CONSUME` alone leaves it `worker`, and with neither (`INSPECT`,
  `PRODUCE`, `PURGE`) leaves it no rows and no ability to heartbeat.
- `revoke_role` deletes them, like the queue grants.
- The migration that introduced the table backfilled a row for every role that
  already held the corresponding capability, derived from the grants it held at
  that moment. That is a one-time reconstruction of what provisioning would
  have written, and it is not re-derived afterwards: a backfilled row stays
  exactly as the migration left it until that role is next provisioned, at
  which point the replace above takes over and the grants stop being consulted.
  A capability changed by hand in between is therefore not reflected here,
  which is a reason to narrow a role by re-provisioning it rather than by
  revoking privileges directly.

**Retention**: the only path by which a durable job row disappears.

- Operator only, through `purge_terminal_jobs`, and only for jobs in a terminal
  state older than the cutoff. A live job is never a candidate.
- Deleting a job cascades to its attempts, its slots, and its occurrences, so
  retention has one knob rather than four.
- Ordering: oldest-first across the whole schema, bounded by the budget. See
  the retention bullets above for what that costs and why.

**Revised:** the original §8 bullet described the role model only in terms of
who may enqueue, claim, and run DDL. Auditing the first consuming application
showed that leaves three real gaps: a worker role that could enqueue, a
purger that could only be expressed as a raw `DELETE` grant, and a "repaired"
role that kept `SUPERUSER` or `BYPASSRLS` through provisioning. The
bullets above are therefore added rather than reworded. Breaking the capability grants
was accepted: a least-privilege model that has to be widened by hand at every
call site is not one.

## 9. Explicit non-goals for v1

- Exactly-once execution or distributed transactions.
- Arbitrary callable serialization, task discovery, pickle, or a generic
  plugin system.
- Workflow graphs, fan-out/fan-in orchestration, saga compensation, or a
  visual dashboard.
- Multi-database replication, cross-region queue semantics, or non-Postgres
  backends.
- A compatibility layer for Procrastinate, Celery, Dramatiq, or RQ.

## 10. Acceptance criteria

Before calling the package production-ready, automated integration tests
against real PostgreSQL must prove:

1. A rolled-back producer transaction leaves neither business data nor a job.
2. A committed producer transaction exposes both business data and its job.
3. Two concurrent producers cannot create duplicate active jobs for one
   dedupe key.
4. Multiple workers process a job at most once per successful lease attempt.
5. A worker crash during a handler causes bounded retry after lease expiry.
   The stale worker cannot complete the newer attempt. This is the fencing
   guarantee from §3 and needs a test that actually holds a lease past
   expiry and attempts to write with the stale token, not just a timing test.
6. Retry, timeout, cancellation, and terminal failure state transitions are
   durable and observable.
7. A missed `NOTIFY`, worker restart, scheduler restart, and database restart
   delay work at most. None loses work.
8. Periodic schedules fire once per occurrence across multiple schedulers,
   including a scheduler crash between claiming an occurrence and enqueueing
   its job (§6). The occurrence must not be lost or double-fired.
9. Blocking handlers cannot starve unrelated jobs beyond their configured
   worker concurrency. Verify the default-executor bound from §4 actually
   caps concurrent `asyncio.to_thread` work, not just async task concurrency.
10. Migration, upgrade, retention cleanup, and least-privilege role tests pass.

## 11. Development workflow

**New section.** picv-2025 (the initial consuming application) already has a
working pattern for exactly this. Mirror it rather than inventing a new one.

- **PostgreSQL is managed by `mise`, not Docker.** picv-2025's `mise.toml`
  runs a project-local cluster directly (`mise x postgres -- pg_ctl -D
  $PWD/.data/postgres ...`), with `db:start`/`db:stop`/`db:reset` tasks
  (`db:start` initializes the cluster). Reuse this exact structure so a contributor working across both
  repos has one mental model, not two.
- **Integration tests run against a disposable, per-run database and a
  least-privilege role.** picv-2025's `scripts/integration.sh` creates
  `tsdhn_integration_<timestamp>_<pid>`, runs migrations against it with an
  admin connection, provisions a scoped app role, runs
  `pytest -m integration`, then drops both. Mirror this for `rqueue`: a
  `scripts/integration.sh` (or equivalent `mise` task) that creates a fresh
  database, runs `rqueue`'s own migrations, runs the integration suite, and
  tears down, never against a shared or production-shaped database.
- **Fast tests stay database-free.** Integration tests are marked
  (`pytest.mark.integration`) and excluded from the default `mise run test`
  the same way picv-2025 splits `test` from `test-integration`.
- **A master pipeline test, runnable in a loop during development.** One
  integration test that exercises the full lifecycle end to end in a single
  run: transactional enqueue and rollback, claim, heartbeat, successful
  completion, a forced retry, a dedupe_key conflict, a named-concurrency-key
  conflict, a lease-expiry-then-fencing-rejection sequence, and one periodic
  occurrence firing. It asserts on durable state after each step rather than
  mocking any of it. This is the outer-loop feedback signal during
  development: run it after every change to the claim/lease/scheduler code
  to see pass/fail across the whole pipeline in one shot, instead of only
  learning about a regression in claiming from a scheduler test failure
  later. It supplements, not replaces, the focused acceptance-criteria tests
  in §10. Those stay granular so a failure points at one mechanism.

**Revised (2026-09-02):** neither this section nor §9 said anything about
*consumer*-side testing, and every comparable library ships something for it
(`procrastinate.testing.InMemoryConnector`, `PgQueuer.in_memory()`,
`Oban.Testing`), so a consumer such as picv-2025 had no way to unit-test
"does my code enqueue the right job" short of a real PostgreSQL round trip or
a hand-rolled fake that proves nothing about the call's shape. Added
`rqueue.testing.RecordingQueue`: it routes every call through the real,
database-free `Queue.build_insert` and records the validated request instead
of writing it. It is explicitly **non-durable and non-simulating**. It proves
a call is well-formed and would be accepted, and models no transactionality,
dedupe resolution, claiming, or execution. Those stay the job of the §10
integration suite. It lives outside `rqueue/__init__.py`'s exports, following
Procrastinate's precedent, so the production import surface is unchanged.
