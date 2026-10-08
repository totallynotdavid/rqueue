# Architecture

rqueue stores jobs in PostgreSQL and moves them through their states with SQL
statements. The Python code validates input, runs handlers, and decides which
statement to issue. PostgreSQL holds the state.

## How a job moves

1. **Enqueue.** `Queue.enqueue` validates the request with `Queue.build_insert`
   and inserts the job on the connection the caller passes in. It opens no
   connection of its own, so the job commits or rolls back with the caller's
   transaction.
2. **Claim.** Each `Worker` tick records the worker's heartbeat, recovers
   expired leases, and claims due jobs for its queue. A claim runs in one short
   transaction and leases each job with a new `lease_token`.
3. **Run.** The worker runs the handler as an asyncio task. It keeps the lease
   alive with heartbeats. It holds no database transaction while the handler
   runs.
4. **Finalize.** The worker writes the outcome (success, retry, failure, or
   cancellation) with a statement that matches the job's `lease_token`. A stale
   worker's statement matches nothing.

`Scheduler.tick` creates jobs for due cron schedules. It inserts the occurrence
key and the job in one transaction. `Admin` and the `rqueue` command inspect and
change jobs from outside the worker loop.

[Delivery](delivery.md) describes states and leases. [Storage](storage.md)
describes the tables and is the authority on which actor may change each piece
of shared state.

## Modules

All paths are under [`src/rqueue/`](../src/rqueue/).

| Module         | Responsibility                                                                      |
| -------------- | ----------------------------------------------------------------------------------- |
| `queue.py`     | `Queue`: task registration and transactional enqueue                                |
| `worker.py`    | `Worker`: claim, run, and finalize jobs under a lease                               |
| `scheduler.py` | `Scheduler`: periodic schedules, fired through the occurrence-key table             |
| `admin.py`     | `Admin`: inspection and administration                                              |
| `cli.py`       | the `rqueue` command                                                                |
| `storage.py`   | every runtime SQL statement, and the only module that touches job and schedule rows |
| `migrations/`  | schema DDL and the migration runner                                                 |
| `roles.py`     | capability grants and `provision_role`                                              |
| `health.py`    | `check_readiness`                                                                   |
| `tasks.py`     | the task registry: explicit names, explicit decoders, async handlers                |
| `context.py`   | `TaskContext`, what a handler receives                                              |
| `retry.py`     | `RetryPolicy`                                                                       |
| `executor.py`  | the bounded default executor that `Worker` installs                                 |
| `metrics.py`   | `MetricsSink` and `LoggingMetricsSink`                                              |
| `limits.py`    | size and range bounds, and their validators                                         |
| `models.py`    | job state, jobs, attempts, schedules, and statistics                                |
| `cron.py`      | validated cron expressions                                                          |
| `errors.py`    | the exception hierarchy                                                             |
| `testing.py`   | `RecordingQueue`, which `rqueue` does not export                                    |

## Boundaries

- **`storage.py` owns the SQL.** Its statements are static strings. Values reach
  PostgreSQL as bind parameters. The only interpolated text is the schema name,
  after `limits.validate_identifier` accepts it. Role provisioning in `roles.py`
  builds statements with PostgreSQL's `format('%I', ...)`.
- **Migrations are the only DDL.** Nothing runs them at import or at worker
  startup. `rqueue migrate` and `rqueue.migrations.migrate` do. See
  [Migrations](migrations.md).
- **Handlers get no SQL access.** `TaskContext` carries the job id, queue, task
  name, attempt numbers, metadata, a logger, `heartbeat()`, cancellation state,
  and `will_retry()`.
- **State rules live in SQL.** Statements that change a job test its current
  state in the same statement, and table constraints reject impossible rows. A
  lease token exists on a row exactly when the row is `leased`.

## Repository layout

| Path                 | Holds                                                                |
| -------------------- | -------------------------------------------------------------------- |
| `src/rqueue/`        | the package                                                          |
| `tests/`             | the fast suite, which needs no database                              |
| `tests/integration/` | tests that run against PostgreSQL                                    |
| `scripts/`           | `integration.sh` and its database helper, see the contributing guide |
| `mise.toml`          | the toolchain versions and the development tasks                     |

See [.github/contributing.md](../.github/contributing.md) for how to run them.
