# rqueue independent review

This is a review of the implementation against [the requirements](requirements.md).
It was run on 2026-09-02 against revision `db5586d`. Test counts below are from
that run. Source citations point at the current tree, except where a sentence
names `db5586d` because the code it describes has since changed.

## Result

The implementation is substantially complete and the concurrency mechanisms
tested here behaved correctly. The review found one genuine gap: a role
provisioned with `Capability.PRODUCE` alone could not enqueue at all. That was a
§8 least-privilege bug, and it left the producer-only part of acceptance
criterion §10.10 unmet. The review itself changed no implementation files.

**The gap is now fixed.** `Capability.PRODUCE` grants
`SELECT, INSERT, UPDATE (updated_at)` on `jobs`. That is the column-scoped
`UPDATE` the enqueue statement's `ON CONFLICT ... DO UPDATE SET updated_at`
requires, and nothing wider. A producer still cannot write `state`, `payload`,
or `attempt`. §8 and §10.10 are met as of that change. See
[The producer-only enqueue gap](#the-producer-only-enqueue-gap).

## Verification performed

- `uv run pytest -q`: 95 passed, 69 skipped because no database URL was set.
- `bash scripts/integration.sh`: all 69 existing PostgreSQL integration tests
  passed.
- Added `tests/integration/test_review_stress.py` and ran it through the same
  disposable-database workflow. All 5 tests passed.
- The stress tests covered 240 jobs and 20 direct claimers, and stale-token
  completion after reclaim.
- They also covered 32 simultaneous dedupe producers, 40 simultaneous
  same-concurrency-key producers followed by 12 claimers, and 24 scheduler
  replicas racing on one occurrence.

## Requirements coverage

### §1 Purpose: met

`readme.md:3-9` documents the asyncpg-native, transactional-enqueue purpose. The
implementation exposes that model.

### §2 Platform and dependencies: met

`pyproject.toml:5` requires Python 3.13+. `pyproject.toml:9-11` lists asyncpg as
the only runtime dependency. `src/rqueue/__init__.py:6-8` states that importing
the package does no schema work. `src/rqueue/migrations/__init__.py:197-252`
makes migration an explicit call.

### §3 Correctness model: met by code and stress evidence

`src/rqueue/limits.py:174-198` serializes payloads as JSON only and enforces the
size bound. `src/rqueue/storage.py:1015-1031` claims with `FOR UPDATE SKIP
LOCKED`.

`src/rqueue/storage.py:1089-1096` heartbeats only under the lease token and the
`leased` state. `src/rqueue/storage.py:1104-1171` gates every finalizing write
the same way. `src/rqueue/storage.py:1190-1247` clears the token when a lease
expires.

`test_stale_holder_write_is_rejected_after_reclaim`
(`tests/integration/test_review_stress.py:62`) observed `LeaseLost` while the
successor remained leased.

### §4 Public API: met

`src/rqueue/tasks.py:205-216` rejects a handler that is not `async def`.
`src/rqueue/worker.py:277-303` refuses to start when the queue holds work for a
task with no registered handler. `src/rqueue/context.py:27-76` gives a handler
context no SQL connection. `src/rqueue/executor.py:23-61` installs the bounded
default executor that caps `asyncio.to_thread`.

### §5 Retry, failure, and cancellation: met

`src/rqueue/worker.py:604-674` records durable outcomes and dispatches on the
retry policy. `src/rqueue/worker.py:587-602` makes timeout cancellation
explicit.

`src/rqueue/storage.py:527-537` cancels a pending job outright and only requests
cancellation of a leased one. `src/rqueue/storage.py:1147-1157` finalizes a
leased cancellation under the lease token.

### §6 Scheduling and concurrency: met

`src/rqueue/storage.py:1028` orders due jobs by priority, `scheduled_at`, then
`seq`. `src/rqueue/storage.py:1045-1063` acquires a concurrency slot as a leased
row, atomically.

`src/rqueue/migrations/0002_scheduling.sql:36-47` makes the occurrence key a
primary key. `src/rqueue/storage.py:763-801` inserts the occurrence in the same
transaction as its job.

`test_parallel_scheduler_ticks_fire_one_occurrence`
(`tests/integration/test_review_stress.py:157`) found one job and one occurrence
across 24 replicas.

### §7 Storage, migrations, and operations: met

The required tables and indexes are in `src/rqueue/migrations/0001_core.sql` and
`src/rqueue/migrations/0002_scheduling.sql`. `src/rqueue/migrations/__init__.py:165-183`
rejects a missing or modified migration. `src/rqueue/migrations/__init__.py:197-252`
takes the advisory lock. Metrics calls are spread through
`src/rqueue/worker.py:274-672`. `src/rqueue/health.py:58-123` reports
connectivity, schema, worker, and scheduler readiness separately.

### §8 Security and resource limits: met, after the fix

`src/rqueue/limits.py:174-198` bounds payload size. `src/rqueue/storage.py:1-6`
documents that every value reaches PostgreSQL as a bind parameter.
`src/rqueue/migrations/0002_scheduling.sql:87-122` scopes `jobs` and
`job_attempts` by queue with row-level security.

The one gap is described in
[The producer-only enqueue gap](#the-producer-only-enqueue-gap).

### §9 Non-goals: met

The reviewed public surface has no callable, pickle, broker, or workflow
compatibility path.

### §10 Acceptance criteria: met

Criteria 1-6, 8-9, and the migration and retention parts of 10 passed the
existing tests. The additional race tests above strengthen 3, 5, and 8.

Criterion 7 had tests for missed notifications and for worker and scheduler
restart. The database-restart evidence gap is now closed by
`tests/integration/test_resilience.py::test_a_real_database_restart_delays_work_but_loses_none`.
It stops and starts the real postmaster, not just backends, and shows that no
work is lost across the `ConnectionRefusedError` window.

Criterion 10 was not fully met at review time, because the producer-only role
could not enqueue. The grant fix closes it.

### §11 Development workflow: met

`scripts/integration.sh` creates a fresh database, migrates it, provisions a
scoped role, runs the marked tests, and drops the database. `mise.toml:17-23`
separates the fast tests from the integration tests and provides the master
pipeline command.

## The producer-only enqueue gap

At `db5586d`, `src/rqueue/roles.py:50-56` granted `Capability.PRODUCE` only
`SELECT, INSERT` on `jobs`. Every enqueue is an `INSERT ... ON CONFLICT ... DO
UPDATE` (`src/rqueue/storage.py:989-1005`), and PostgreSQL requires `UPDATE`
privilege for that statement. A producer-only role therefore received
`permission denied for table jobs`, even for an enqueue with no dedupe conflict.

The existing role tests provisioned produce and consume together, which masked
the case. An isolated producer-only probe against the disposable database failed
at `src/rqueue/storage.py:156` (at `db5586d`) with this output:

```text
asyncpg.exceptions.InsufficientPrivilegeError: permission denied for table jobs
```

The row-level-security predicate was not the cause. The flaw was the privilege
the enqueue statement needs.

### The fix

Commit `cf7c1e4` changed `Capability.PRODUCE` to grant
`SELECT, INSERT, UPDATE (updated_at)` on `jobs`
(`src/rqueue/roles.py:76-80`). The grant is column-scoped, so the conflict path
works while `state`, `payload`, and `attempt` stay unwritable. The insert SQL and
its conflict strategy did not change.

A produce-and-consume role also collects a whole-table `UPDATE` from `CONSUME`.
`_drop_subsumed_column_privileges` (`src/rqueue/roles.py:177-191`) drops the
subsumed column entry before `provision_role` emits the `GRANT`.

### Permanent tests

- `tests/integration/test_operations.py::test_a_produce_only_role_can_enqueue`
  covers a plain insert and the dedupe `DO UPDATE` conflict path under a
  produce-only role.
- `tests/integration/test_operations.py::test_a_produce_only_role_cannot_rewrite_a_job`
  is the least-privilege negative case.
- `tests/integration/test_operations.py::test_produce_and_consume_merge_into_one_whole_table_update`
  covers the merged grant.
- `tests/test_roles.py` covers the grant-merging step without a database.

## Design decisions under review

The stress results support the key decisions:

- Dedupe is queue-scoped by the partial unique index, and simultaneous producers
  all received the same existing job.
- FIFO tie-breaking uses the identity `seq` column, not transaction timestamps.
  The direct-claimer run completed every job with no duplication and no
  indefinite starvation.
- Concurrency keys remain distinct from dedupe keys. The leased slot model
  allowed one active claimant for the shared key under burst load.
- Row-level-security queue scoping passed the existing cross-queue integration
  checks.
- Occurrence-key scheduling remained sound under 24 simultaneous scheduler
  ticks, with exactly one durable occurrence and job.
