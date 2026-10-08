# Roles

`provision_role` creates or repairs a PostgreSQL role that holds only the
privileges its job needs.

```python
import asyncpg
from rqueue.roles import Capability, provision_role


async def provision_api_producer(migration_dsn: str) -> None:
    connection = await asyncpg.connect(migration_dsn)
    try:
        await provision_role(
            connection,
            role="api_producer",
            capabilities=[Capability.PRODUCE],
            queues=["compute"],
        )
    finally:
        await connection.close()
```

`migration_dsn` connects as the migration role.

The same from the command line:

```console
$ rqueue grant-role --role api_producer --capability produce --queue compute
```

The migration role owns the schema and is the only role that runs DDL. Row-level
security policies scope each role to its queues. The policies read rows in
`role_queue_grants`, so granting a queue inserts a row and never changes a
policy. `grant_queues` and `revoke_role` in `rqueue.roles` change those rows.

After you upgrade rqueue, run `provision_role` again for every role. See
[Re-provision roles](migrations.md#re-provision-roles).

## Capabilities

| Capability | Allows                                                                                 |
| ---------- | -------------------------------------------------------------------------------------- |
| `PRODUCE`  | enqueue and read jobs. `UPDATE` only on `updated_at`, for the enqueue conflict path    |
| `CONSUME`  | claim, heartbeat, and finish jobs. It does not allow enqueueing                        |
| `SCHEDULE` | read schedules and enqueue their occurrences. It does not allow changing a job's state |
| `INSPECT`  | read everything and write nothing, including the liveness rows that readiness reads    |
| `PURGE`    | delete terminal jobs through `purge_terminal_jobs()`, and only through it              |

`CONSUME` has no `INSERT` on `jobs`. Claiming work and creating it are separate
authorities, and a worker that can enqueue can give the fleet any task it likes.
A process that does both, such as a worker that enqueues follow-up jobs, asks
for `[Capability.PRODUCE, Capability.CONSUME]`.

`PURGE` has no `DELETE` on `jobs` either. It gets `EXECUTE` on
`task_queue.purge_terminal_jobs(queue, states, finished_before, max_rows)`, a
`SECURITY DEFINER` routine that migration 0006 installs. The routine checks its
own arguments: one named queue that the caller is granted, terminal states only,
a cutoff in the past, and a bounded batch. A crafted call cannot reach a
`pending` job, another queue's jobs, or the whole table.

## Purge

`Admin.purge()` and `rqueue purge` call that routine, so they are what `PURGE`
authorizes. There is no other path.

```python
from datetime import timedelta

from rqueue import Admin


async def purge_compute(pool) -> int:
    return await Admin(pool).purge(queue="compute", retention=timedelta(days=30))
```

`pool` is an `asyncpg` pool connected as a role with `PURGE`. The call returns
the number of deleted jobs.

- Name a queue to purge that queue. Omit `queue` to purge every queue the caller
  can see. `"*"`, which pause and resume accept, is not a queue name here and
  raises `ValidationError`, as does any other invalid name.
- `limit` (default 10000, at most 100000) is one budget across all queues. The
  oldest jobs go first, whichever queue holds them. The call first tightens the
  cutoff to the age of the last job in the budget, including every job that
  finished in the same instant, so no queue deletes past that cutoff however
  large its backlog is. This also keeps the work proportional to `limit`.
- Name only terminal states (`succeeded`, `failed`, `cancelled`). A live state
  raises.
- The cutoff must be timezone-aware. This holds for every instant that a caller
  passes to rqueue.
- Deleting a job also deletes its attempts, slots, and occurrences.

## Row-level security

Six tables are under the queue-scoping policies: `jobs`, `job_attempts`,
`concurrency_slots`, `runtime_heartbeats`, `schedules`, and
`schedule_occurrences`. See [Storage](storage.md) for who writes each.

### Heartbeats

On a queue that a worker and a scheduler share, queue scope alone would let
either of them write the other's liveness row. A heartbeat row therefore also
records the role that wrote it, and only that role may refresh or delete it. The
`kind` it claims (`worker` or `scheduler`) must be one that the role's
capabilities allow. Otherwise a worker could tell a readiness probe that a
scheduler is alive.

Any role with queue access can read heartbeats, because readiness has to see
components it did not write. A role may delete a heartbeat it wrote even after
its grants changed, so a scheduler can always take down its own row.

A scheduler records liveness for the queues it schedules. With no enabled
schedules it records none.

### Tables outside the policies

Two tables that carry a queue name are not under the policies, and any role that
can reach the schema can read them in full:

- `queue_pauses`. The wildcard pause is a row for the queue `'*'`, and a policy
  scoped to a role's queues would hide it from the workers that must honor it. A
  role with one queue can therefore see which queues are paused.
- `role_queue_grants`. Each policy is a subquery on this table, evaluated as the
  querying role, so a policy on the table would filter itself. A scoped role has
  `SELECT` on it, which shows it every role's queue grants. Only the migration
  role can write it, so a role cannot grant itself a queue.

`role_runtime_kinds` records which component kinds a role may claim liveness as.
It has no queue column. No runtime role can read it: the heartbeat policies
reach it through a `SECURITY DEFINER` function.

### Queue labels tied to jobs

A policy can test only the queue that the writer wrote. On a row that also
points at a job, a composite foreign key ties that queue to the job's queue.
This covers attempt records, concurrency slots, and occurrences. A role with one
queue cannot attach its own label to another queue's job.

A job's queue never changes, but a schedule's can. An occurrence therefore takes
its queue from the job it fired, not from the schedule. Moving a schedule leaves
its history where its jobs are.

The occurrence key `(schedule_id, occurrence_at)` has no queue, so one firing
per instant means the same thing from every queue. The policy asks whether the
writer can see the schedule. A scheduler can read the whole history of a
schedule it owns, across a queue move, and can write occurrences only for
schedules on queues it holds.

## What provisioning repairs

`provision_role` refuses a role that owns a table in the schema, the schema, or
the database, and writes nothing. An owner can grant itself anything and can
alter or drop the table. Reassign the object to the migration role first.

It also repairs privileges outside the schema. It sets the role to
`NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION NOINHERIT` and
removes every role membership before it applies the capability grants. Running
it on a role that someone made a superuser narrows that role. A connection that
cannot clear an attribute the role holds raises, so you never get a partly
narrowed role.

The call is one transaction. If it fails, the role stays as the call found it:
absent if it did not exist, unchanged if it did. For example, granting `PURGE`
on a schema whose migrations do not yet include the purge routine raises.
