# Storage

rqueue keeps all state in PostgreSQL, in one schema (`task_queue` by default).
Migrations create it. See [Migrations](migrations.md).

| Table                  | Holds                                                                  |
| ---------------------- | ---------------------------------------------------------------------- |
| `jobs`                 | one row per job, with its lease and its final outcome                  |
| `job_attempts`         | one record per attempt, never updated after the attempt ends           |
| `concurrency_slots`    | the lease behind each concurrency key, per queue                       |
| `schedules`            | periodic schedule definitions                                          |
| `schedule_occurrences` | the occurrence keys that have fired, and their jobs                    |
| `runtime_heartbeats`   | worker and scheduler liveness per queue, for readiness checks          |
| `queue_pauses`         | which queues are paused, and since when                                |
| `role_queue_grants`    | which queues a provisioned role may reach                              |
| `role_runtime_kinds`   | which component kinds (worker, scheduler) a role may claim liveness as |
| `schema_migrations`    | applied migration versions and their checksums                         |

One routine, `purge_terminal_jobs()`, is the `SECURITY DEFINER` boundary that
the `PURGE` capability receives instead of a table-wide `DELETE`.

Each index is declared in the migration, next to the query it serves. All
runtime SQL is static and takes its values as bind parameters.

## Who writes each table

Several processes write these tables at once. This section lists the states of
each table and which actor may change them. "Operator" means an `Admin` call or
the `rqueue` command.

### `concurrency_slots`

A row is the lease on a key. The key is free when no row exists or when the
row's `leased_until` has passed.

- A worker takes a free key in the claim transaction, with one
  `INSERT ... ON CONFLICT (queue, key) DO UPDATE ... WHERE leased_until <= now()`.
  Two workers that race for one key serialize on the row, and the loser claims
  no job.
- A worker releases the slot in the finalize transaction. The `DELETE` matches
  on the job id and the `lease_token`, so a worker whose lease expired cannot
  release the slot of the worker that replaced it.
- A slot expires by time. No statement and no reaper is involved. The one
  exception is an operator deleting the rows once no worker runs, before
  migration 0010 (see [Running processes](migrations.md#running-processes)).
- The slot records the role that holds it. While the lease is live, only that
  role may extend or release it. An expired slot may be taken over or cleared by
  any role that holds the queue, and the new holder becomes the recorded owner.
- A job is never leased without its slot, and never holds a slot after its
  attempt is recorded.

### `runtime_heartbeats`

One row for each `(kind, instance, queue, role)`. A row is fresh when
`updated_at` is within the caller's staleness window, and stale after that.

- A worker or scheduler writes its own row once per tick, for each queue it
  serves.
- A scheduler deletes its row for a queue that has left its set of enabled
  schedules, on the same tick. A worker's queues are fixed when it starts, so it
  deletes nothing.
- A process that died cannot delete its row. Time makes it stale.
- No component writes another's row. Only an operator writes one, by deleting
  the rows before migration 0005, 0008, or 0010 (see
  [Running processes](migrations.md#running-processes)). The row records the
  writing role, and a role may write, refresh, or delete only its own rows. A
  role may write only a `kind` that its capabilities allow.
- The upserts and the delete of one tick commit together, so a probe never sees
  a served queue without its heartbeat.

### `schedules`

A schedule is enabled or disabled. `tick()` considers only enabled schedules.

- `Scheduler.sync()` and operators can change either way. A schedule's queue can
  change too.
- Disabling does not delete. Occurrences that already fired stay, and enabling
  the schedule again does not fire them again.

### `schedule_occurrences`

One row for each `(schedule_id, occurrence_at)`.

- A scheduler inserts a row in the same transaction as the job it creates. Rows
  are never updated or deleted. The `SCHEDULE` capability grants neither.
- The insert is a plain `INSERT`, not `ON CONFLICT DO NOTHING`. A second
  scheduler waits for the first one's uncommitted row and continues only if that
  transaction aborted.
- Rows go away only by cascade, with the job or the schedule.

### `queue_pauses`

A queue is paused when it has a row with `paused_at` set.

- Only an operator changes it. Pausing twice keeps the first `paused_at`.
- `'*'` is a row like any other and means every queue.
- The claim statement reads the table, so the pause binds every worker. It never
  touches a job that is already leased. See [Pausing](pausing.md).

### `role_queue_grants` and `role_runtime_kinds`

Only `provision_role`, `grant_queues`, `revoke_role`, and the migration role
write these tables. A scoped role can read `role_queue_grants` and cannot write
it.

`provision_role` replaces a role's rows in `role_runtime_kinds` on every call. A
role with `CONSUME` gets `worker`, and a role with `SCHEDULE` gets `scheduler`.
A role with neither, such as one with only `INSPECT`, `PRODUCE`, or `PURGE`, has
no rows and cannot write a heartbeat. Narrowing a role by re-provisioning it
removes its kinds with its grants. `revoke_role` deletes them too.

Migration 0008 filled the table once from the capabilities that existing roles
held at that time. Those rows stay until the role is provisioned again. Narrow a
role by re-provisioning it, not by revoking its privileges by hand.

### Retention

Retention is the only way a job row disappears. Only an operator deletes, only
through `purge_terminal_jobs`, and only for terminal jobs older than the cutoff.
Deleting a job also deletes its attempts, slots, and occurrences, so retention
has one setting. See [Roles](roles.md#purge).
