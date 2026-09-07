# rqueue — PostgreSQL task queue on asyncpg — package requirements

Revision 2. Supersedes the original draft. Changes from the draft are called
out inline as **Revised:** notes, each with the reasoning that produced them —
keep those notes; they're the record of why the design is shaped this way,
not just what it says.

## 1. Purpose

Build a small, durable PostgreSQL task queue for Python applications that use
`asyncpg`. Its defining feature is *transactional enqueueing*: an application
can insert/update its own rows and enqueue a job through the **same**
`asyncpg.Connection` and PostgreSQL transaction.

**Revised:** the original draft framed this as filling a gap in Procrastinate
("Procrastinate doesn't support transactional enqueueing"). That's false —
Procrastinate's `PsycopgConnector` already supports deferring a job on a
caller-supplied connection inside the caller's transaction
(`configure(connection=conn).defer_async(...)`), and so does `pgqueuer`'s
`AsyncpgDriver`. The actual reason to build this package is narrower and more
honest: **avoid running two PostgreSQL drivers in one process.** The
consuming application (picv-2025) already speaks asyncpg end to end via
`relq`; adding Procrastinate means a second driver, a second connection pool,
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

`pgqueuer` (janbjorge/pgqueuer, Python + asyncpg) is mature and well-engineered
— MVCC-safe concurrency-slot allocation, batch CTE-based claiming, transactional
enqueue on a caller connection already works today. It was evaluated directly
(schema and query builder read in full) before starting this package. It has
three gaps against the requirements below that are correctness/feature gaps,
not style differences, and are the actual justification for building rather
than adopting:

1. **No fencing token.** Its crash recovery is heartbeat-only — the same
   coarse model River (Go) uses: a job is reclaimed when
   `heartbeat < NOW() - timeout`, with no token comparison. Nothing stops a
   worker that is merely slow (GC pause, stalled heartbeat writer, not
   actually dead) from completing a job after another worker has already
   reclaimed and finished it. §3's lease-token requirement is this package's
   most load-bearing guarantee and pgqueuer does not provide it.
2. **No named concurrency key on an arbitrary business resource.** pgqueuer
   limits concurrency per *entrypoint* (task type) only. It has no equivalent
   to Procrastinate's `lock=f"compute-job:{id}"` — mutual exclusion scoped to
   an application-chosen key, which picv-2025 needs ("one simulation per
   compute job").
3. **Dedup's skip mode drops silently.** `on_conflict="skip"` does
   `ON CONFLICT DO NOTHING` and returns nothing for the skipped row. §3's
   dedupe_key requirement is to return the existing job on conflict, not just
   silently absorb the call.

## 2. Platform and dependencies

- Python 3.13+.
- PostgreSQL 14+ only.
- `asyncpg` is the sole database driver and the only required runtime
  dependency.
- The public API is asyncio-native. No synchronous API, psycopg adapter,
  SQLAlchemy integration, Redis, RabbitMQ, or broker abstraction.
- The package owns versioned PostgreSQL migrations. It must not perform
  implicit schema creation at import time or on normal worker startup.
- `relq` is optional from the queue package's perspective. The queue accepts
  an `asyncpg.Connection`; applications may use that exact connection through
  `relq_postgres.PostgresDatabase` inside the same transaction.

**Revised:** the original draft pinned Python 3.15+. As of this writing
(2026-08-31), Python 3.15 has not reached GA (expected ~October 2026 per the
PEP 719 cadence) and no released `asyncpg` build supports it — the latest
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
tradeoff; this package keeps the stricter JSON-only requirement from the
original draft (no pickle, no arbitrary bytes) deliberately.

### Atomic enqueueing

The enqueue API takes a caller-owned `asyncpg.Connection`; it never opens a
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

**Idiomatic usage — keep business logic decoupled from the queue import.**
picv-2025's `repository.py` never imports Procrastinate directly: its
`create_or_get_job(*, data, simulation_id, defer)` takes a `defer` callback,
and the caller passes a thin wrapper (`enqueue_simulation(conn, job_id)`) that
calls the queue. This package's `enqueue` should be trivially wrappable in
that shape — document it as the intended pattern in the quickstart, not just
something that happens to work.

- `enqueue_many(connection, jobs)` is atomic and validates every job before
  writing any row.
- A durable `dedupe_key` is scoped to a queue and has a documented lifecycle.
  Concurrent enqueue attempts with the same active key return the existing job
  or raise `AlreadyEnqueued`; the choice must be explicit per call.
- PostgreSQL `NOTIFY` may wake workers after commit, but polling remains the
  source of truth. A missed notification must only add latency, never lose a
  job.

**Revised:** fire `NOTIFY` selectively, not on every write. River's trigger
only fires on transition into an "available" state, with a minimal payload
(queue name, not the row). pgqueuer's fires on every INSERT/UPDATE/DELETE.
Prefer River's tighter version — fewer wakeups, no reason to notify on a
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

**This is the package's strongest guarantee relative to prior art** — see §1.
Neither River nor pgqueuer implement lease-token fencing; both rely on
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
  awaitable lifecycle operation for a worker running in a background task; its
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
  protocol; applications may supply a Pydantic TypeAdapter or a plain decoder.
  Decode failures are non-retryable and become a durable failed job.
- `TaskContext` exposes only job id, attempt number, queue, lease-aware
  heartbeat, logger/structured fields, cooperative cancellation state, and the
  `will_retry()` decision hook. It must not expose raw SQL or permit mutation
  of queue internals.
- **Handlers are `async def` only — no sync-handler code path.** If a task's
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

**Revised:** the original draft already specified async-only handlers; an
intermediate revision of this document proposed adding first-class sync
handler support (justified by picv-2025's numba-based handler being fully
synchronous). That was reversed after reading `pgqueuer`'s executor code: it
used to support sync entrypoints and *removed the feature*, with the executor
now raising `TypeError("Sync entrypoints are no longer supported... wrap
blocking code with asyncio.to_thread()")`. That is direct evidence from a
project that tried the "auto-detect and route to a thread" design and walked
it back. Stay async-only; the one-line `asyncio.to_thread` wrapper is not
meaningful ceremony for a fully-synchronous handler, and it keeps the
worker's execution model to one path instead of two.

**Revised (2026-09-02):** this document never considered **durable,
queue-wide pause/resume**, and the omission was an oversight rather than a
decision. Both mature references have it — Oban's `pause_queue`/`resume_queue`
(with a `:*` wildcard for every queue) and River's `QueuePause`/`QueueResume`
— and it is a different operation from anything specified above: it stops a
queue admitting *new* work across every worker replica at once, without
killing a process, where `Worker.stop()`/`Worker.drain()` only ever affect the
one worker instance they are called on and `cancel_job` only ever affects one
job. An operator draining a database, holding back a queue during an incident,
or gating a deploy has no way to express that with the API as specified. The
decision is to build it, as `Admin.pause_queue(name)` /
`Admin.resume_queue(name)`, with `'*'` meaning every queue.

The design, briefly: a durable `queue_pauses` table (`paused_at timestamptz`,
NULL meaning running — the instant, not a boolean, so an admin view gets
"paused since" for free) is the single source of truth; the claim query itself
carries the predicate, so a paused queue yields zero claimable rows in SQL
regardless of what any worker process currently believes; `NOTIFY` on resume is
a latency optimization for an idle worker and nothing more. That follows §3's
existing rule for job wake-ups ("polling is the source of truth") and this
document's stance that state transitions are enforced in SQL, not only in
Python — which is also why Oban's model was *not* copied: Oban keeps `paused`
in each producer process and rebroadcasts it over PubSub, so a restarted queue
comes back running. It goes one step past River too: River stores the row but
still tests it client-side before fetching, so a worker that has not yet polled
can still issue a claim against a paused queue. Because rqueue already makes
every claim decision in one statement, the check costs one InitPlan evaluation
per claim and holds for workers that have never heard of the call.

Deliberately *not* adopted from Oban: starting a `Worker` already paused
(`Oban.start_queue(paused: true)`). In Oban the flag is per-producer state, so
a constructor argument is the only way to express it; here the pause is durable
and queue-wide, so `await admin.pause_queue(name)` before starting a worker
already says it — exactly once, for the whole fleet. A per-`Worker` argument
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
- Cancellation is cooperative. Cancelling a queued job prevents execution;
  cancelling a leased job signals its handler and is finalized only by the
  lease-holder or lease expiry.
- Timeouts must be explicit per task. Timeout cancellation cannot be treated
  as proof that an external side effect did not occur.

## 6. Scheduling and concurrency controls

- Delayed jobs use `scheduled_at`; workers claim only due jobs.
- Periodic jobs support a validated cron expression and timezone. A scheduler
  emits one durable job occurrence at a time via a unique **occurrence key**:
  one row per `(schedule_id, occurrence_time)` with a unique constraint,
  inserted in the same transaction as the job it produces. Multiple scheduler
  replicas must be safe.
- Optional named `dedupe_key` prevents duplicate active jobs. It is not a
  substitute for a general workflow/DAG system.
- Optional named concurrency keys limit an externally shared resource (for
  example, one simulation per compute job). Their semantics must be lease
  based and crash safe; do not model them as a permanently held Boolean.
  These are a distinct primitive from `dedupe_key` — dedupe prevents a
  duplicate *job from being queued*, a concurrency key prevents duplicate
  *concurrent execution*. Procrastinate models this split as `queueing_lock`
  vs. `lock`; keep the two-key model, don't collapse them.
- Priority is a small integer and is resolved alongside `scheduled_at` and
  creation order. Fairness between queues must be documented, not implied.

**Revised:** the original draft offered "a PostgreSQL advisory lock or unique
occurrence key" as alternatives for periodic-job dedup; this revision commits
to occurrence-key only, and rules out both the advisory-lock approach and the
alternative seen in prior art. Three approaches were compared directly:

- **Session advisory lock** (originally proposed): fragile under any
  connection pooling — the lock is session-scoped, and a scheduler that ever
  borrows a pooled connection silently loses the guarantee.
- **Leader election** (River's approach — an `UNLOGGED river_leader` table,
  single elected instance runs the scheduler): works, but is real machinery
  (election, liveness, failover) to avoid double-firing.
- **Claim-the-schedule-row** (pgqueuer's approach — the schedule row itself
  is claimed via the same `SKIP LOCKED` + heartbeat-reclaim as jobs, and
  `next_run` is advanced optimistically at claim time): simpler than leader
  election, but has a real gap — a crash between claiming the schedule row
  and durably creating its job can advance `next_run` without ever producing
  the occurrence, silently skipping a firing.
- **Occurrence-key table** (kept): insert `(schedule_id, occurrence_time)`
  with a unique constraint, in the same transaction that creates the
  occurrence's job. No reclaim logic needed at all — if the transaction
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
- Support PostgreSQL roles with least privilege: API producers may enqueue and
  inspect only their allowed queues; workers may claim/transition their queues;
  migration role owns DDL.

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
5. A worker crash during a handler causes bounded retry after lease expiry;
   the stale worker cannot complete the newer attempt — this is the fencing
   guarantee from §3 and needs a test that actually holds a lease past
   expiry and attempts to write with the stale token, not just a timing test.
6. Retry, timeout, cancellation, and terminal failure state transitions are
   durable and observable.
7. A missed `NOTIFY`, worker restart, scheduler restart, and database restart
   delay work at most; none loses work.
8. Periodic schedules fire once per occurrence across multiple schedulers,
   including a scheduler crash between claiming an occurrence and enqueueing
   its job (§6) — the occurrence must not be lost or double-fired.
9. Blocking handlers cannot starve unrelated jobs beyond their configured
   worker concurrency — verify the default-executor bound from §4 actually
   caps concurrent `asyncio.to_thread` work, not just async task concurrency.
10. Migration, upgrade, retention cleanup, and least-privilege role tests pass.

## 11. Development workflow

**New section.** picv-2025 (the initial consuming application) already has a
working pattern for exactly this — mirror it rather than inventing a new one.

- **PostgreSQL is managed by `mise`, not Docker.** picv-2025's `mise.toml`
  runs a project-local cluster directly (`mise x postgres -- pg_ctl -D
  $PWD/.data/postgres ...`), with `db:init`/`db:start`/`db:stop`/`db:reset`
  tasks. Reuse this exact structure so a contributor working across both
  repos has one mental model, not two.
- **Integration tests run against a disposable, per-run database and a
  least-privilege role.** picv-2025's `scripts/integration.sh` creates
  `tsdhn_integration_<timestamp>_<pid>`, runs migrations against it with an
  admin connection, provisions a scoped app role, runs
  `pytest -m integration`, then drops both. Mirror this for `rqueue`: a
  `scripts/integration.sh` (or equivalent `mise` task) that creates a fresh
  database, runs `rqueue`'s own migrations, runs the integration suite, and
  tears down — never against a shared or production-shaped database.
- **Fast tests stay database-free**; integration tests are marked
  (`pytest.mark.integration`) and excluded from the default `mise run test`
  the same way picv-2025 splits `test` from `test-integration`.
- **A master pipeline test, runnable in a loop during development.** One
  integration test that exercises the full lifecycle end to end in a single
  run — transactional enqueue and rollback, claim, heartbeat, successful
  completion, a forced retry, a dedupe_key conflict, a named-concurrency-key
  conflict, a lease-expiry-then-fencing-rejection sequence, and one periodic
  occurrence firing — asserting on durable state after each step rather than
  mocking any of it. This is the outer-loop feedback signal during
  development: run it after every change to the claim/lease/scheduler code
  to see pass/fail across the whole pipeline in one shot, instead of only
  learning about a regression in claiming from a scheduler test failure
  later. It supplements, not replaces, the focused acceptance-criteria tests
  in §10 — those stay granular so a failure points at one mechanism.

**Revised (2026-09-02):** neither this section nor §9 said anything about
*consumer*-side testing, and every comparable library ships something for it
(`procrastinate.testing.InMemoryConnector`, `PgQueuer.in_memory()`,
`Oban.Testing`) — so a consumer such as picv-2025 had no way to unit-test
"does my code enqueue the right job" short of a real PostgreSQL round trip or
a hand-rolled fake that proves nothing about the call's shape. Added
`rqueue.testing.RecordingQueue`: it routes every call through the real,
database-free `Queue.build_insert` and records the validated request instead
of writing it. It is explicitly **non-durable and non-simulating** — it proves
a call is well-formed and would be accepted, and models no transactionality,
dedupe resolution, claiming, or execution; those stay the job of the §10
integration suite. It lives outside `rqueue/__init__.py`'s exports, following
Procrastinate's precedent, so the production import surface is unchanged.
