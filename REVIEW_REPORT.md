# rqueue independent review

## Result

The implementation is substantially complete and the concurrency mechanisms
tested here behaved correctly. This review found one genuine gap: a role
provisioned with `Capability.PRODUCE` alone could not enqueue at all -- a §8
least-privilege role bug that left the producer-only portion of acceptance
criterion §10.10 unmet. No implementation files were changed by the review
itself.

**That gap is now fixed.** `Capability.PRODUCE` grants
`SELECT, INSERT, UPDATE (updated_at)` on `jobs`: the column-scoped UPDATE the
enqueue statement's `ON CONFLICT ... DO UPDATE SET updated_at` requires, and
nothing wider -- a producer still cannot write `state`, `payload`, or
`attempt`. §8 and §10.10 are met as of that change; the rest of this report
stands as written.

## Verification performed

- `uv run pytest -q`: 95 passed, 69 skipped because no database URL was set.
- `bash scripts/integration.sh`: all 69 existing PostgreSQL integration tests
  passed.
- Added `tests/integration/test_review_stress.py` and ran it through the same
  disposable-database workflow: all 5 tests passed.
- Stress coverage included 240 jobs and 20 direct claimers, stale-token
  completion after reclaim, 32 simultaneous dedupe producers, 40 simultaneous
  same-concurrency-key producers followed by 12 claimers, and 24 scheduler
  replicas racing on one occurrence.

## Requirements coverage

- §1 Purpose: **met**. `README.md:1-5` documents the asyncpg-native,
  transactional-enqueue purpose and the implementation exposes that model.
- §2 Platform/dependencies: **met**. `pyproject.toml:1-15` requires Python
  3.13+ and only runtime-depends on asyncpg; `src/rqueue/__init__.py:1-12`
  performs no schema work. Migrations are explicit in
  `src/rqueue/migrations/__init__.py:186-235`.
- §3 Correctness model: **met by code and stress evidence**. JSON-only
  serialization and bounds are in `src/rqueue/limits.py:116-159`; claim uses
  `FOR UPDATE SKIP LOCKED` in `src/rqueue/storage.py:664-676`; lease writes
  require the token and leased state in `src/rqueue/storage.py:715-783`.
  Expiry clears the token in `src/rqueue/storage.py:803-860`, and the stale
  completion stress test observed `LeaseLost` while the successor remained
  leased.
- §4 Public API: **met**. Explicit registration and async-only enforcement are
  in `src/rqueue/tasks.py:34-59`; startup task checking is in
  `src/rqueue/worker.py:210-235`; handler context has no SQL connection in
  `src/rqueue/context.py:23-66`; bounded `to_thread` capacity is installed by
  `src/rqueue/executor.py:22-47`.
- §5 Retry/failure/cancellation: **met**. Durable outcome handling and policy
  dispatch are in `src/rqueue/worker.py:521-576`; timeout cancellation is
  explicit at `src/rqueue/worker.py:504-519`; cancellation uses a request for
  leased jobs and finalizes under the lease token at
  `src/rqueue/storage.py:907-924` and `src/rqueue/storage.py:760-769`.
- §6 Scheduling/concurrency: **met**. Due-job ordering is
  `src/rqueue/storage.py:664-675`; concurrency slots are leased and atomically
  acquired in `src/rqueue/storage.py:678-689`; occurrence uniqueness and the
  same-transaction insert are in `src/rqueue/migrations/0002_scheduling.sql:38-53`
  and `src/rqueue/storage.py:560-597`. The scheduler stress test found one job
  and one occurrence across 24 replicas.
- §7 Storage/migrations/operations: **met**. Required tables and indexes are
  in `src/rqueue/migrations/0001_core.sql` and `0002_scheduling.sql`; migration
  locking/version checks are in `src/rqueue/migrations/__init__.py:186-235`;
  metrics calls are distributed through `src/rqueue/worker.py:255-361` and
  readiness distinguishes connectivity, schema, worker, and scheduler in
  `src/rqueue/health.py:24-123`.
- §8 Security/resource limits: **met (after the fix below)**. Payloads and SQL
  values are bounded/parameterized (`src/rqueue/limits.py:116-159`,
  `src/rqueue/storage.py:1-10`) and RLS queue scoping exists in
  `src/rqueue/migrations/0002_scheduling.sql:87-124`. As reviewed, the producer
  role granted only `SELECT, INSERT` on jobs in `src/rqueue/roles.py:50-56`,
  while every enqueue uses `ON CONFLICT ... DO UPDATE` in
  `src/rqueue/storage.py:649-661`. PostgreSQL requires UPDATE privilege for
  that statement, so a producer-only role received `permission denied for
  table jobs` even for an enqueue with no dedupe conflict. Existing role tests
  provision both produce and consume, masking this case. An isolated
  producer-only probe against the disposable database failed at
  `src/rqueue/storage.py:156` with the exact PostgreSQL output:
  `asyncpg.exceptions.InsufficientPrivilegeError: permission denied for table
  jobs`.
  **Fixed** in `src/rqueue/roles.py`: `Capability.PRODUCE` now grants
  `SELECT, INSERT, UPDATE (updated_at)` on `jobs` -- column-scoped, so the
  conflict path works while `state`, `payload`, and `attempt` stay unwritable.
  The insert SQL and its conflict strategy were not changed. Because a
  produce+consume role also collects a whole-table `UPDATE` from `CONSUME`,
  `provision_role` drops the subsumed column entry before emitting the `GRANT`
  (`_drop_subsumed_column_privileges`).
- §9 Non-goals: **met**. No callable/pickle/broker/workflow compatibility path
  is present in the reviewed public surface.
- §10 Acceptance criteria: **met, except the §10.7 evidence gap**. Criteria 1-6,
  8-9 and the migration/retention portions of 10 passed existing tests, with the
  additional race tests above strengthening 3, 5, and 8. Criterion 7 has tests
  for missed notifications and worker/scheduler restart, but I found no
  integration test that actually restarts PostgreSQL. Criterion 10 was not
  fully met at review time because the producer-only role path described above
  was unusable; the grant fix closes it, and permanent tests now cover it:
  `tests/integration/test_operations.py::test_a_produce_only_role_can_enqueue`
  (plain insert *and* the dedupe `DO UPDATE` conflict path under a
  PRODUCE-only role),
  `::test_a_produce_only_role_cannot_rewrite_a_job` (the least-privilege
  negative case), `::test_produce_and_consume_merge_into_one_whole_table_update`,
  and `tests/test_roles.py` for the grant-merging step.
- §11 Development workflow: **met**. `scripts/integration.sh` creates a fresh
  database, migrates it, provisions a scoped role, runs marked tests, and drops
  the database; `mise.toml` separates fast tests and integration tests and
  provides the master-pipeline command.

## Design decisions under review

The stress results support, rather than undermine, the key decisions:

- Dedupe is queue-scoped by the partial unique index and simultaneous producers
  all received the same existing job.
- FIFO tie-breaking uses the identity `seq` column, not transaction timestamps;
  the direct-claimer run completed every job without duplication or indefinite
  starvation.
- Concurrency keys remain distinct from dedupe keys. The leased slot model
  allowed only one active claimant for the shared key under burst load.
- RLS-driven queue scoping passed the existing cross-queue integration checks;
  the flaw is specifically the producer privilege required by the enqueue SQL,
  not the row predicate.
- Occurrence-key scheduling remained sound under 24 simultaneous scheduler
  ticks, with exactly one durable occurrence and job.

## Recommended follow-up

The producer-only enqueue authorization is resolved (see §8 above), with
permanent producer-only integration tests. A PostgreSQL restart test should
still be added to close the evidence gap in acceptance criterion §10.7.
