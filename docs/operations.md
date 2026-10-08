# Operations

## The command line

```console
$ rqueue status                     # applied and pending migrations, as JSON
$ rqueue readiness --queue compute  # connectivity, schema, worker, scheduler
$ rqueue purge --retention-days 30  # delete old terminal jobs
$ rqueue grant-role --role api --capability produce --queue compute
```

Every command takes the global options `--database-url` (or
`RQUEUE_DATABASE_URL`), `--schema` (or `RQUEUE_SCHEMA`, default `task_queue`),
and `-v` for debug logging. Pass them before the command name:
`rqueue --database-url "$DATABASE_URL" status`.

| Command      | Options                                                                                                    | Exit status                   |
| ------------ | ---------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `migrate`    | `--target N`                                                                                               | 0, or 1 on an error           |
| `status`     | none                                                                                                       | 1 when migrations are pending |
| `readiness`  | `--queue`, `--require-scheduler`, `--no-require-worker`                                                    | 1 when not ready              |
| `purge`      | `--retention-days` (required), `--queue`, `--limit` (default 10000)                                        | 0, or 1 on an error           |
| `grant-role` | `--role`, `--capability` (repeat), `--queue` (repeat, default `*`), `--password` or `RQUEUE_ROLE_PASSWORD` | 0, or 1 on an error           |

`--capability` takes `produce`, `consume`, `schedule`, `inspect`, or `purge`.
Without a database URL the command exits with status 2. See
[Migrations](migrations.md) for `migrate`.

## Admin

`Admin(pool)` offers the same operations in a process:

| Method                                                                      | Does                                            |
| --------------------------------------------------------------------------- | ----------------------------------------------- |
| `get_job(id)`, `list_jobs(queue, states, task, limit, offset)`              | read jobs. `list_jobs` returns 100 by default   |
| `attempts(id)`                                                              | the attempt history of one job                  |
| `stats(queue)`                                                              | counts for one queue                            |
| `cancel_job(id)`                                                            | see [Cancellation](retries.md#cancellation)     |
| `retry_job(id, scheduled_at, additional_attempts)`                          | make a terminal job runnable again              |
| `purge(queue, retention, states, limit)`                                    | see [Roles](roles.md#purge)                     |
| `pause_queue`, `resume_queue`, `paused_queues`, `is_queue_paused`           | see [Pausing](pausing.md)                       |
| `list_schedules`, `get_schedule`, `set_schedule_enabled`, `delete_schedule` | see [Schedules](scheduling.md#manage-schedules) |

`retry_job` keeps the attempt counter and adds `additional_attempts` (default 1)
to `max_attempts`. Attempt records are never rewritten, so a reset would collide
with the history. It is the only way out of a terminal state.

## Readiness

`check_readiness(pool, queue=...)` reports four things separately: whether the
database answers, whether the schema is at the version this release expects,
whether a worker is live, and whether a scheduler is live. The report lists the
live workers and schedulers and a `problems` list, so a probe can tell a
database outage from a schema that is behind or a queue nobody consumes.

A worker counts as live when it wrote a heartbeat in the last 60 seconds
(`worker_staleness`). A scheduler counts for 300 seconds
(`scheduler_staleness`). A worker is required by default and a scheduler is not.
`rqueue readiness` prints the report as JSON and exits 1 when `ready` is false.

## Metrics

Pass a `MetricsSink` to `Worker` or `Scheduler`. It has `counter`, `gauge`, and
`timing` methods, and rqueue assumes no vendor. `LoggingMetricsSink` writes each
call as a structured log record. The default sink drops everything.

| Series                                                      | Kind    | Meaning                                 |
| ----------------------------------------------------------- | ------- | --------------------------------------- |
| `rqueue.queue.depth`                                        | gauge   | jobs pending or leased                  |
| `rqueue.queue.ready`                                        | gauge   | jobs eligible now                       |
| `rqueue.queue.oldest_ready_age`                             | gauge   | seconds                                 |
| `rqueue.claim.latency`                                      | timing  | seconds for one claim round trip        |
| `rqueue.claim.jobs`                                         | counter | jobs leased                             |
| `rqueue.handler.duration`                                   | timing  | seconds of handler code                 |
| `rqueue.job.succeeded`, `.retried`, `.failed`, `.cancelled` | counter | job outcomes                            |
| `rqueue.lease.expired`                                      | counter | leases recovered                        |
| `rqueue.wakeup`                                             | counter | tagged `source=notify` or `source=poll` |
| `rqueue.schedule.fired`                                     | counter | occurrences fired                       |

## Roles and retention

[Roles](roles.md) covers `provision_role`, capabilities, and purging.
