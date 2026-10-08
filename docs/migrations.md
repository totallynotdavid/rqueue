# Migrations

## Install the schema

`rqueue migrate` applies every pending migration. It takes an advisory lock and
is forward-only. Nothing else changes the schema: importing rqueue or starting a
worker never does.

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

The queue tables live in their own schema, `task_queue` by default. Pass
`--schema` or set `RQUEUE_SCHEMA` to change it. `rqueue status` prints the
applied and pending migrations as JSON and exits 1 when any are pending.
`migrate --target N` stops after migration `N`, which is how you test a staged
upgrade.

From Python, call `rqueue.migrations.migrate(connection, schema=...)`.

## Upgrade

An upgrade has two steps:

1. `rqueue migrate` moves the schema forward.
2. [Re-provision every role](#re-provision-roles). Privileges move only when you
   run `provision_role` again.

| Migration | What it changes                          | What you do                                                   |
| --------- | ---------------------------------------- | ------------------------------------------------------------- |
| 0005      | the heartbeat key                        | [stop the fleet and clear the heartbeats](#running-processes) |
| 0006      | who may purge                            | [grant queues](#0006-who-may-purge) to purge-only identities  |
| 0008      | the heartbeat key gains the writing role | [stop the fleet and clear the heartbeats](#running-processes) |
| 0010      | concurrency slots record their holder    | [stop the fleet and let the leases end](#running-processes)   |
| 0011      | adds a grant to existing scheduler roles | nothing                                                       |

Apply all pending migrations in one `rqueue migrate`.

### Running processes

A worker or scheduler of the previous release cannot run against the schema
after 0005, 0008, or 0010. 0005 and 0008 change the heartbeat key, so its
heartbeat upsert fails and every tick is retried without claiming anything. 0010
records who holds each concurrency slot, and the previous release's slot
takeover never sets that column.

Each of these migrations checks for itself, under an `ACCESS EXCLUSIVE` lock on
the table it reads, and refuses when it finds a process of the previous release.
Nothing is applied when it refuses. It refuses when:

- `runtime_heartbeats` has any row (0005, 0008, 0010);
- a concurrency slot is still leased (0010).

```console
$ rqueue migrate
error: migration 0005_queue_scoped_runtime failed: rqueue: 1 runtime heartbeat row(s) present
HINT:  Stop every rqueue worker and scheduler and confirm the processes have
exited. Then DELETE FROM task_queue.runtime_heartbeats, wait one poll interval,
and check it is still empty -- a process that had not yet ticked refills it,
which is the only way to see one. Re-run the migration, then deploy.
```

To pass the check:

1. Stop every worker and scheduler.
2. Run `DELETE FROM task_queue.runtime_heartbeats`. Use your schema name if it
   is not `task_queue`.
3. Wait one poll interval and check that the table is still empty. A process
   that had started but not yet ticked writes a row now.
4. For 0010, wait for the leases to end, bounded by the `lease_duration` the
   workers ran with, or run `DELETE FROM task_queue.concurrency_slots` once
   nothing runs.
5. Run `rqueue migrate`, then deploy.

The check is an empty table, not a recent heartbeat, because a worker with a
long `poll_interval` is alive with an old one. rqueue never deletes these rows,
and a running process rewrites its own within one poll interval.

Each migration runs the check again. A migration body runs once, so a database
that applied 0005 in an earlier release never repeats that check when it picks
up 0008. Applying every pending migration in one `rqueue migrate` is safe after
the steps above.

#### Known limits

The check sees a process only once it has ticked after the table was cleared, so
a process that ticks less often than you waited, or is paused, goes unseen, and
an exact check is separate work.

0008 needs no re-provisioning. It fills in the component kinds that each role
may claim from the grants the role already holds, so a role that an earlier
release provisioned keeps writing its heartbeat.

### 0006: who may purge

Migration 0006 puts purging behind the `SECURITY DEFINER` routine
`purge_terminal_jobs`, which checks the _login_ role against
`role_queue_grants`. A table privilege alone does not authorize a purge. A role
that holds the privilege but has no grant row for the queue is refused:

```console
ConfigurationError: role dba_ops is not granted queue alpha
```

Three identities pass without a grant row: the schema owner, a superuser, and
any role that `provision_role` created, since it writes the row in the same
call. Any other identity, such as a service account that runs a retention cron,
needs a row:

```python
import asyncpg
from rqueue.roles import grant_queues


async def grant_retention_cron(migration_dsn: str) -> None:
    connection = await asyncpg.connect(migration_dsn)
    try:
        await grant_queues(connection, role="retention_cron", queues=["*"])
    finally:
        await connection.close()
```

`migration_dsn` connects as the schema owner, the role that ran `migrate`.

### 0011: scheduler enqueue grant

Every enqueue is an `INSERT ... ON CONFLICT ... DO UPDATE SET updated_at`.
PostgreSQL requires `UPDATE` on every column that `DO UPDATE SET` names. The
`SCHEDULE` capability now grants `UPDATE (updated_at)` on `jobs`. A scheduler
role provisioned by an earlier release lacks it and could not enqueue. The
scheduler would stop with a `ConfigurationError` that names the missing
privilege; a worker or scheduler stops the same way for any privilege it lacks.

0011 adds the grant to every role in `role_queue_grants` that holds `INSERT` on
`schedules`. Only the `SCHEDULE` capability grants that privilege, so the match
is exact.

## Re-provision roles

After upgrading rqueue, run `provision_role` again for every role. The grant
table belongs to the release in `rqueue.roles`, not to the schema, so `migrate`
leaves each role with what the release that provisioned it handed out.

```python
import asyncpg
from rqueue.roles import Capability, provision_role


async def reprovision_cron_runner(migration_dsn: str) -> None:
    connection = await asyncpg.connect(migration_dsn)
    try:
        await provision_role(
            connection,
            role="cron_runner",
            capabilities=[Capability.SCHEDULE],
            queues=["compute"],
        )
    finally:
        await connection.close()
```

`provision_role` is idempotent. It revokes first, so the role ends up with
exactly the capabilities in the call.

A migration can add a privilege when a capability implies one that no other
capability grants, as 0011 does. It cannot remove one, because the database does
not record a role's capabilities. This release changes these grants:

| Capability | Change                                                   | Until you re-provision                                                                                                                                                                                                                      |
| ---------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SCHEDULE` | `UPDATE (updated_at)` on `jobs`                          | Added by 0011. Nothing to do.                                                                                                                                                                                                               |
| `SCHEDULE` | `DELETE` on `runtime_heartbeats`                         | The scheduler ticks and fires schedules. It cannot retract its heartbeat for a queue whose last schedule you disabled, so readiness reports it as feeding that queue until the staleness window expires. It logs this once, with the queue. |
| `INSPECT`  | `SELECT` on `concurrency_slots` and `runtime_heartbeats` | `check_readiness` raises, so a probe that uses an inspection role reports the database unreachable.                                                                                                                                         |
| `CONSUME`  | loses `INSERT` on `jobs`                                 | The worker role can still enqueue. Only re-provisioning removes that.                                                                                                                                                                       |
| `PURGE`    | new capability                                           | Nothing. No existing role has it.                                                                                                                                                                                                           |
