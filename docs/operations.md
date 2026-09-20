# Operations

## The CLI

```console
$ rqueue status                     # applied and pending migrations, as JSON
$ rqueue readiness --queue compute  # connectivity / schema / worker / scheduler
$ rqueue purge --retention-days 30  # delete old terminal jobs
$ rqueue grant-role --role api --capability produce --queue compute
```

Every command takes `--database-url` (or `RQUEUE_DATABASE_URL`) and `--schema`.
`rqueue migrate` is covered in [Migrations](migrations.md).

## Admin

`Admin` exposes the same operations in-process: `get_job`, `list_jobs`,
`attempts`, `stats`, `cancel_job`, `retry_job`, `purge`,
`pause_queue` / `resume_queue`, and the schedule accessors.

An operator retry keeps the attempt counter running. Attempt records are
immutable, so a reset would collide with the history it is meant to preserve.
The retry grants a fresh budget by raising `max_attempts`.

## Readiness

`check_readiness` reports connectivity, migration version, worker availability,
and scheduler availability *separately*, because a single "ready: false" is
useless during an incident.

## Metrics

Metrics go to a `MetricsSink` you supply (`counter` / `gauge` / `timing`), and no
vendor is assumed. `LoggingMetricsSink` turns them into structured log records.
The series emitted are listed on the protocol in `src/rqueue/metrics.py`.

## Roles and retention

[Least-privilege roles](roles.md) covers `provision_role`, capabilities, and
`Admin.purge`.
