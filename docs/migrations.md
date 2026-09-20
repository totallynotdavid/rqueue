# Migrations

## Applying migrations

Migrations never run implicitly, not at import and not at worker startup.
`rqueue migrate` is an explicit command that takes an advisory lock and is
forward-only:

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

Queue tables live in their own schema, `task_queue` by default, separate from
your application's business schema. Pass `--schema` to change it.
`rqueue status` lists applied and pending migrations. `--target N` stops after
migration `N`, which is how a staged upgrade is tested.

## Upgrading

An upgrade is two separate steps. `rqueue migrate` moves the schema forward. The
privileges of every existing role only move when you run `provision_role` again.
See [Re-provisioning roles](#re-provisioning-roles) below.

Three migrations need the fleet stopped first: 0005, 0008, and 0010. Each
inverts the usual "migrate, deploy" order to "stop, clear, migrate, deploy", and
each checks that for itself instead of trusting the one before it. A migration
body runs exactly once, so a database that applied 0005 in an earlier release
never re-runs that check when it picks up 0008.

### 0005: the heartbeat key changes

0005 changes the heartbeat key. The previous release writes
`ON CONFLICT (kind, instance)`, which stops matching any index the moment 0005
runs. That statement opens every worker tick and a failure is retried, not
fatal, so an old worker does not crash. It stalls and claims nothing. The
migration refuses to be the cause of that:

```console
$ rqueue migrate
error: migration 0005_queue_scoped_runtime failed: rqueue: 1 runtime heartbeat row(s) present
HINT:  Stop every rqueue worker and scheduler and confirm the processes have
exited. Then DELETE FROM task_queue.runtime_heartbeats, wait one poll interval,
and check it is still empty -- a process that had not yet ticked refills it,
which is the only way to see one. Re-run the migration, then deploy.
```

The schema in that hint is whichever one you are migrating, not necessarily
`task_queue`.

The check is an empty table, not "has anything beaten recently?". A worker with
a long poll interval is alive with an old heartbeat, so no window generous
enough for it means anything. An empty table is checkable. rqueue never deletes
these rows, and a running process rewrites its own within one poll interval. The
migration takes `ACCESS EXCLUSIVE` on the table before looking, so nothing can
slip in between the check and the key change. Nothing is applied when it
refuses, so the running fleet keeps working while you stop it.

An empty table proves only that nothing has ticked since it was cleared. A
process that has started and not yet reached its first tick has written nothing
to see. So clear the table, **wait a poll interval, and confirm it is still
empty**. Ticking is the only thing such a process can do next, so the wait turns
it into a visible one. A process started after that check fails from its first
tick, having never claimed a job, and does not degrade a working fleet. That
window is the drain, and it belongs to the deployment.

### 0008: the heartbeat key gains the writing role

**Deploy after the whole migration, not between migrations.** 0008 adds the
writing role to the heartbeat key. The previous release's upsert names
`ON CONFLICT (kind, instance, queue)`, which stops matching any index the moment
0008 lands:

```
ERROR:  there is no unique or exclusion constraint matching the ON CONFLICT
specification
```

This is the failure 0005 describes, for the same reason, and 0008 refuses in the
same way: `ACCESS EXCLUSIVE` on the table, then the same demand that it be empty.
It does not rely on 0005 having asked. After the drain, applying every pending
migration in one `rqueue migrate` is safe, and it is the default.

Stopping in between is the sequence that puts a running previous-release worker
in front of the new key: `--target 7`, deploy, `--target 8`. The redeployed
fleet beats, and `--target 8` refuses instead of stalling it. The check has to
catch it there because nothing downstream can help. The heartbeat upsert is the
first statement of every tick, the error is retried, and nothing in the new code
runs inside the old process to turn it into a better message.

Keeping a three-column unique index alongside the new key would make the old
statement resolve, and it is not an option. It would forbid two roles from
holding separate rows for one component, which is the deadlock 0008's key change
exists to avoid. Grant compatibility and binary compatibility are separate
questions. 0008 keeps grants compatible, so a role provisioned by the previous
release keeps working. The drain above is what handles binary compatibility.

0008 itself needs no re-provisioning. The kinds a role may claim are backfilled
from the grants it already holds, and the policies read them through a
`SECURITY DEFINER` function instead of the table. A role provisioned by the
previous release therefore keeps writing its heartbeat with no new grant, which
matters because the heartbeat is the liveness mechanism and has nothing to fall
back to. A row written before the migration is attributed to whoever ran it and
ages out.

### 0010: leases must be drained too

0010 needs the leases drained, not only the fleet stopped. It records which role
holds each concurrency slot, and a row written before it does not say who that
was, so every existing row is attributed to whoever runs the migration.

For a heartbeat that is harmless: 0008 leaves an orphan that ages out while its
component writes a fresh row on the next tick. A slot is different. It is the
mutual exclusion for an attempt that is running right now, and its real holder
can neither extend nor release a row it no longer owns. The lease runs out under
the running attempt, and at that moment any worker on the queue can acquire the
key. A second worker then starts the same logical job while the first is still
inside it.

So 0010 refuses on either of two counts. The first is a slot that is still
leased:

```console
$ rqueue migrate
error: migration 0010_slot_ownership failed: rqueue: 1 concurrency slot(s) still leased
HINT:  Stop every rqueue worker, confirm the processes have exited, and let the
outstanding leases expire -- bounded by the lease_seconds the workers ran with.
A slot outliving its worker is already stale, so DELETE FROM
task_queue.concurrency_slots is equally good once nothing is running. Re-run
the migration, then deploy.
```

The second is a non-empty `runtime_heartbeats`, because slots cannot show that
the fleet is stopped. A worker between jobs holds no slot and is invisible in
that table, and it has to be visible. The previous release's takeover path never
sets the owner column 0010 adds, so the first time it takes over a slot another
role left expired it fails with `new row violates row-level security policy`.

Inside a single `rqueue migrate` the second check costs nothing, since 0008 has
just demanded the same thing. It exists for the staged upgrade (`--target 9`,
deploy, `--target 10`), which is the only way to reach 0010 with a fleet
running.

### 0011: the scheduler enqueue grant

0011 repairs the one grant a scheduler cannot do without. Every enqueue is an
`INSERT ... ON CONFLICT ... DO UPDATE SET updated_at`, and PostgreSQL requires
`UPDATE` on every column a `DO UPDATE SET` names, even one written back to its
own value. This release adds that column-scoped grant to `SCHEDULE`, and a
scheduler role from the previous release does not have it.

There is no degraded mode. Without the grant the scheduler cannot enqueue at
all. `Scheduler.run` catches the error alongside every other PostgreSQL failure,
so the only symptom is `could not reach PostgreSQL; retrying` once a tick while
nothing fires.

So 0011 grants it to every role in `role_queue_grants` that holds `INSERT` on
`schedules`. That fingerprint is the one 0008 already uses, and it is exact,
because no other capability grants that privilege. Adding on a fingerprint is
safe here because a matching role already holds `INSERT` on `jobs`, so the
repair widens nothing an operator had not already agreed to. Taking away on a
fingerprint would not be safe, which is why the `CONSUME` row in the table below
still needs you.

### 0006: who may purge

0006 changes who may purge. Retention used to be a plain `DELETE`, so any role
with that privilege could run it. It now goes through a `SECURITY DEFINER`
routine that authorizes the *login* role against `role_queue_grants`. That lets a
`PURGE` role delete without holding `DELETE` on `jobs`. The trade is that
privilege alone is no longer enough:

```console
ConfigurationError: role dba_ops is not granted queue alpha
```

Three identities are let through without a row: the schema owner, a superuser,
and any role `provision_role()` created, since it writes the row in the same
call. What is left is an operator identity that predates this and is none of
those, such as a service account for a retention cron. Give it a row, or run the
cron as one of the three:

```python
from rqueue.roles import grant_queues

await grant_queues(connection, role="retention_cron", queues=["*"])
```

## Re-provisioning roles

**Re-run `provision_role` for every role after upgrading rqueue.** The grant
table lives in `rqueue.roles`, so it is part of the release and not part of the
schema. `migrate` moves the schema forward and leaves every existing role
holding whatever the version that provisioned it handed out. A role is correct
on the day it is provisioned and drifts from then on.

```python
from rqueue.roles import Capability, provision_role

await provision_role(
    connection, role="cron_runner", capabilities=[Capability.SCHEDULE],
    queues=["compute"],
)
```

`provision_role` is idempotent and it is the repair. The revokes run first, so
the role ends up holding exactly the capabilities named in the call and nothing
else.

A migration can do part of this for you, and this release does what it safely
can in 0011. It cannot decide anything that depends on the capability set,
because the database has never recorded it. A grant can be fingerprinted where a
capability implies a privilege nothing else grants, and that is enough to *add*.
It is not enough to take away, since a privilege two capabilities both confer
looks identical either way.

What this release changes, and what you get by re-provisioning:

| Capability | Change | Until you re-provision |
| --- | --- | --- |
| `SCHEDULE` | `UPDATE (updated_at)` on `jobs` | Repaired by 0011. Nothing to do |
| `SCHEDULE` | `DELETE` on `runtime_heartbeats` | Ticks and schedules normally, but cannot retract a heartbeat for a queue whose last schedule was disabled, so readiness reports it as still feeding that queue until the staleness window expires. Logged once, naming the queue and this remedy |
| `INSPECT` | `SELECT` on `concurrency_slots` and `runtime_heartbeats` | `check_readiness` raises instead of reporting, so a probe using an inspection role reports the database unreachable. This is a fix, since the grant was missing before too |
| `CONSUME` | loses `INSERT` on `jobs` | The worker role stays able to enqueue, which is the separation this release adds. Only re-provisioning closes it |
| `PURGE` | new capability | Nothing. No existing role has it |
