# rqueue requirements

This document defines rqueue's current behavior and links to the document that
owns each detail. The test modules that prove the contract are listed in
[section 10](#10-acceptance-criteria).

## 1. Purpose

rqueue is a small, durable PostgreSQL task queue for Python applications that
use `asyncpg`. Its defining feature is _transactional enqueueing_: an
application writes its own rows and enqueues a job through the same
`asyncpg.Connection` and PostgreSQL transaction.

The queue speaks asyncpg natively, so it shares the application's pool and
connection type. A process runs one PostgreSQL driver, not two.

rqueue supports durable chained jobs, delayed retries, periodic jobs, named
concurrency locks, worker-crash recovery, and observable job state. It is a task
queue. It is not a workflow engine, an ORM, a generic database layer, or a
distributed transaction coordinator.

## 2. Platform and dependencies

- Python 3.13 or later.
- PostgreSQL 14 or later. Where a statement exists only in a later release, the
  server version decides which form rqueue issues. `provision_role` names the
  grantor in `REVOKE ... GRANTED BY` on 16 and later, and issues a bare `REVOKE`
  on 14 and 15, which record one grantor per membership.
- `asyncpg` is the only database driver and the only runtime dependency.
- The public API is asyncio-native. It has no synchronous API, no other driver
  adapter, and no broker abstraction.
- The package owns versioned PostgreSQL migrations. Importing rqueue and
  starting a worker never create or change the schema. See
  [Migrations](migrations.md).
- The queue accepts an `asyncpg.Connection`. Any code that runs on that same
  connection shares the transaction.

## 3. Core correctness model

### Delivery and idempotency

- Delivery is at least once. A handler can run again after a crash, a lease
  expiry, or a network failure that hides the outcome of an attempt. rqueue
  never claims exactly-once execution, so handlers and their external side
  effects must be idempotent. See [Delivery](delivery.md).
- A job has an immutable UUID id, a task name, a JSON payload, a queue name,
  creation and scheduling times, an attempt count, a maximum attempt count, a
  state, and a durable outcome.
- Payloads are JSON. Callables, pickles, and arbitrary objects are rejected at
  enqueue. A worker runs only task names registered with it.

### Atomic enqueueing

- `Queue.enqueue` writes on the caller's connection and opens no other. If the
  caller's transaction rolls back, no job survives. A worker sees the job only
  after the transaction commits. See [Enqueueing](enqueueing.md).
- `enqueue_many` is atomic. It validates every job before it writes any row.
- Business code can stay free of the queue import by taking a `defer` callback.
  [Enqueueing](enqueueing.md#keep-business-code-free-of-the-queue-import) shows
  the pattern.
- `dedupe_key` is scoped to a queue and held while the job is `pending` or
  `leased`. A concurrent enqueue of an active key returns the existing job or
  raises `AlreadyEnqueued`, as `on_conflict` says. The caller always chooses,
  and rqueue never absorbs an enqueue silently. See [Keys](keys.md).
- A `NOTIFY` wakes idle workers. A trigger sends it when a job becomes
  immediately claimable or a paused queue resumes, with the queue name as the
  payload. A heartbeat or any update that makes nothing claimable sends none.
  Polling is the source of truth, so a missed notification adds latency and
  loses no job.

### Claiming, leases, and fencing

- A worker claims due jobs in one short transaction with
  `FOR UPDATE SKIP LOCKED`.
- A claim records the worker id, a fresh opaque `lease_token`, `leased_until`,
  and the attempt number.
- Only the current lease token can heartbeat, complete, fail, reschedule, or
  cancel an attempt. A worker that stalls past its lease cannot overwrite the
  attempt that replaced it.
- An expired lease makes the job eligible again.
- SQL predicates and table constraints enforce state transitions, not only
  Python checks. A terminal job becomes runnable again only through
  `Admin.retry_job`.
- A worker runs handlers with bounded concurrency and holds no database
  transaction while a handler runs.

## 4. Public API

The public surface is small:

| Object            | Purpose                                       | Document                    |
| ----------------- | --------------------------------------------- | --------------------------- |
| `Queue`           | register tasks, enqueue jobs                  | [Tasks](tasks.md)           |
| `Worker`          | claim, run, and finalize jobs for one queue   | [Worker](worker.md)         |
| `Scheduler`       | fire periodic schedules                       | [Scheduling](scheduling.md) |
| `Admin`           | inspect and administer jobs, schedules, pause | [Operations](operations.md) |
| `check_readiness` | report database, schema, worker, scheduler    | [Operations](operations.md) |
| `provision_role`  | create or repair a least-privilege role       | [Roles](roles.md)           |

Behavior the API guarantees:

- Handlers are `async def` only. Blocking work goes through `asyncio.to_thread`.
  A `Worker` installs a bounded `ThreadPoolExecutor`, sized from `concurrency`,
  as the event loop's default executor, so a bare `asyncio.to_thread` call is
  capacity-limited. See [Blocking work](worker.md#blocking-work).
- `Worker.stop()` requests a graceful stop. `await worker.shutdown()` stops a
  worker that runs in a background task, and its `timeout` bounds the grace
  period for in-flight handlers. After the worker hands the leases back, `run()`
  returns without waiting for threads started with `asyncio.to_thread`. See
  [Stopping](worker.md#stopping).
- Task registration is explicit, and a task name is unique within a queue. At
  start, a `Worker` raises `UnknownTask` when the queue holds active jobs for a
  task it does not know. With `strict_tasks=False` it leaves those jobs
  `pending` for another deployment. See [Running a worker](worker.md).
- Payload decoding is explicit. A decoder is any callable, such as a Pydantic
  `TypeAdapter`. A decode failure is not retried and becomes a durable `failed`
  job.
- `TaskContext` gives a handler the job's identity, attempt numbers, metadata, a
  logger, a lease-aware `heartbeat()`, cancellation state, and `will_retry()`.
  It exposes no connection and no SQL, and it cannot change a job's state. See
  [Tasks](tasks.md#what-a-handler-receives).
- `Admin` gets a job, lists jobs by state, retries a terminal job, cancels a
  job, purges old terminal jobs, and pauses and resumes queues. None of these
  belong to the handler context.
- Pausing is durable and queue-wide. The pause is a row that the claim query
  reads, so a paused queue yields no claimable job for any worker, whatever that
  worker believes. A job already leased is unaffected. See
  [Pausing a queue](pausing.md).
- `rqueue.testing.RecordingQueue` records validated enqueue calls without a
  database. The `rqueue` package does not export it. See [Testing](testing.md).

## 5. Retry, failure, and cancellation

- A handler ends an attempt with success, a retry at a chosen time, a terminal
  failure, or a cancellation. See [Retries](retries.md#outcomes).
- Retry policy is declared per task: maximum attempts, exponential backoff,
  bounded jitter, and the exceptions to retry. The persisted `attempt` count is
  the only authority, and a crash or a restart does not reset it.
- Error text stored in PostgreSQL is bounded. Full tracebacks belong in the
  worker's log.
- Cancellation is cooperative. Cancelling a `pending` job prevents it from
  running. Cancelling a `leased` job signals its handler, and the lease holder
  or lease expiry finalizes it. See [Cancellation](retries.md#cancellation).
- A timeout is optional, set per task or per job. When it expires the handler is
  cancelled and the job retries under its policy. A timeout does not show
  whether an external side effect happened. See [Timeouts](retries.md#timeouts).

## 6. Scheduling and concurrency controls

- Delayed jobs use `scheduled_at`. Workers claim only due jobs.
- Periodic jobs use a validated cron expression and time zone. A scheduler fires
  an occurrence by inserting its occurrence key, `(schedule_id, occurrence_at)`,
  and the job in one transaction. The key is unique, so several scheduler
  replicas are safe and need no leader. A crash mid-tick leaves nothing behind,
  and the next tick fires the occurrence. See
  [Periodic schedules](scheduling.md).
- `dedupe_key` stops a duplicate job from being queued. `concurrency_key` stops
  duplicate concurrent execution. They are separate primitives. A concurrency
  key is a lease that expires if its holder dies, never a held flag. See
  [Keys](keys.md).
- Both keys are scoped to a queue.
- Claim order within a queue is priority, then `scheduled_at`, then insertion
  order. Between queues there is no ordering and no fairness mechanism. See
  [Ordering](ordering.md).

## 7. Storage, migrations, and operations

- Queue tables live in a configurable schema, `task_queue` by default, apart
  from the application's own schema. [Storage](storage.md) lists the tables.
- Attempt records are a separate table and are not updated after an attempt
  ends.
- Each index is declared in its migration beside the query it serves.
- `rqueue migrate` is the only command that changes the schema. It takes an
  advisory lock. Migrations are forward-only, run in a transaction, and are
  checksummed, so rqueue refuses an edited migration and a schema newer than the
  package. The integration suite migrates an empty database and upgrades from
  every shipped version.
- A migration that the previous release's running code cannot survive checks for
  that code and refuses to apply. Each such migration carries its own check
  because a migration body runs once. The check reads liveness from the table a
  running process writes on a timer. It sees a process only after that process
  has ticked, so the remaining gap is the drain window. See
  [Running processes](migrations.md#running-processes).
- Retention deletes the oldest terminal jobs first across the whole schema, and
  a purge reads work proportional to its limit, not to the backlog. Purging one
  queue and purging `"*"` are the same operation and delete the same rows from
  the same data. See [Purge](roles.md#purge).
- A worker and a scheduler report metrics through a `MetricsSink`: queue depth,
  oldest ready job, claim latency, handler duration, outcomes, expired leases,
  and wake-ups. No metrics vendor is required. See
  [Metrics](operations.md#metrics).
- `check_readiness` reports database connectivity, schema version, worker
  liveness, and scheduler liveness separately. See
  [Readiness](operations.md#readiness).

## 8. Security and resource limits

- rqueue never executes code, SQL, shell commands, or serialized Python objects
  from a payload.
- Payload size, metadata size, name and key lengths, error text, attempts,
  priority, scheduling horizon, batch sizes, lease duration, purge limit, and
  worker concurrency are bounded. The bounds and their validators are in
  [`src/rqueue/limits.py`](../src/rqueue/limits.py). A violation raises
  `ValidationError` before a statement is sent. A bound stored in a column is
  also a `CHECK` constraint, so a direct SQL writer cannot exceed it. A bound
  that Python and a migration both state is tested to agree. The purge limit has
  that test.
- SQL is static and parameterized. The only interpolated text is the schema
  name, after `validate_identifier` accepts it. All of it is in
  [`src/rqueue/storage.py`](../src/rqueue/storage.py) and the role code.

### Roles and row-level security

[Roles](roles.md) is the authority for the following.

- Five capabilities (`PRODUCE`, `CONSUME`, `SCHEDULE`, `INSPECT`, `PURGE`) scope
  a role to queues. The migration role owns DDL.
- `CONSUME` grants no `INSERT` on `jobs`. Claiming work and creating it are
  separate authorities, and a role that needs both asks for both.
- `PURGE` grants `EXECUTE` on the `SECURITY DEFINER` routine
  `purge_terminal_jobs`, not a table-wide `DELETE`. The routine re-derives
  queue, terminal states, cutoff, and batch size from its own arguments, and
  `Admin.purge` goes through it. A table privilege alone does not authorize
  retention.
- `provision_role` runs in one transaction. A failure leaves the role as it was.
  It refuses a role that owns the schema, the database, or any object in the
  schema, in any catalog. It narrows every privilege class, not only tables:
  routines, sequences, role attributes, and role memberships. It has no opt-out.
- Row-level security scopes six tables to a role's queues: `jobs`,
  `job_attempts`, `concurrency_slots`, `runtime_heartbeats`, `schedules`, and
  `schedule_occurrences`. `queue_pauses` and `role_queue_grants` are readable in
  full by design, and a scoped role cannot write them. `role_runtime_kinds` is
  read only through a `SECURITY DEFINER` function.
- A queue label on a row that points at a job is tied to that job's queue by a
  composite foreign key, so a role cannot use its own label on another queue's
  job. A schedule's queue can change, so nothing is tied to it, and the
  occurrence key `(schedule_id, occurrence_at)` has no queue.
- The capability grant table belongs to the release, not to the schema.
  `migrate` moves the schema, and a role's privileges move when `provision_role`
  runs again. A migration repairs only what a privilege fingerprint proves, and
  never removes a privilege. [Migrations](migrations.md#re-provision-roles)
  lists what an un-reprovisioned role lacks.
- A component asks for an optional privilege only when it needs it. A scheduler
  without `DELETE` on `runtime_heartbeats` keeps ticking and logs that it cannot
  retract.

### Shared durable state

Several processes write the same tables.
[Storage](storage.md#who-writes-each-table) lists each table's states and the
actor that may cause each transition. The rules are:

- Every transition is enforced in SQL, whatever a process believes.
- No component writes another's row, and no operator writes liveness rows. A
  heartbeat row records the role that wrote it, and a role may write, refresh,
  or delete only its own.
- A role may claim only a liveness `kind` its capabilities entitle it to.
- A scheduler retracts a heartbeat for a queue it stopped serving on the same
  tick. A process that dies cannot retract, so time makes its row stale.
- A concurrency slot records its holder. While the lease is live, only the
  holder extends or releases it. Any role granted the queue may take over an
  expired slot.
- Occurrence rows are insert-only and go away only by cascade.

## 9. Scope

rqueue does not provide:

- exactly-once execution or distributed transactions;
- arbitrary callable serialization, task discovery, pickle, or a plugin system;
- workflow graphs, fan-out and fan-in orchestration, saga compensation, or a
  dashboard;
- multi-database replication, cross-region queue semantics, or a backend other
  than PostgreSQL.

## 10. Acceptance criteria

Integration tests against real PostgreSQL prove these. Run them with
`mise run test-integration`.

| Property                                                                                                       | Test module                                                                           |
| -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| A rolled-back producer leaves no business row and no job. A committed one exposes both.                        | [`test_transactional_enqueue.py`](../tests/integration/test_transactional_enqueue.py) |
| Concurrent producers cannot create duplicate active jobs for one `dedupe_key`.                                 | [`test_dedupe.py`](../tests/integration/test_dedupe.py)                               |
| Workers never share a job, and a named concurrency key serializes execution.                                   | [`test_claiming.py`](../tests/integration/test_claiming.py)                           |
| A stale lease token cannot complete or reopen a job. A crashed worker's job retries a bounded number of times. | [`test_fencing.py`](../tests/integration/test_fencing.py)                             |
| Retry, timeout, cancellation, and terminal failure are durable.                                                | [`test_state_transitions.py`](../tests/integration/test_state_transitions.py)         |
| A missed `NOTIFY`, a worker restart, and a database restart delay work and lose none.                          | [`test_resilience.py`](../tests/integration/test_resilience.py)                       |
| A schedule fires once per occurrence across replicas, across a restart, and across a crash.                    | [`test_scheduling.py`](../tests/integration/test_scheduling.py)                       |
| A paused queue yields no claim, for every worker.                                                              | [`test_queue_pause.py`](../tests/integration/test_queue_pause.py)                     |
| Blocking handlers cannot exceed the worker's thread bound or starve unrelated jobs.                            | [`test_worker_concurrency.py`](../tests/integration/test_worker_concurrency.py)       |
| Migrations, upgrades, retention, readiness, and least-privilege roles behave as documented.                    | [`test_operations.py`](../tests/integration/test_operations.py)                       |
| One job lifecycle, end to end, with stored state checked after each step.                                      | [`test_master_pipeline.py`](../tests/integration/test_master_pipeline.py)             |

## 11. Development workflow

- The fast suite needs no database. Integration tests carry the `integration`
  marker and run on a disposable per-run database against a least-privilege
  role.
- PostgreSQL for development is a project-local cluster managed by `mise`. It
  does not use Docker.

[Contributing](../.github/contributing.md) has the commands.
