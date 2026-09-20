# Least-privilege roles

```python
from rqueue.roles import Capability, provision_role

await provision_role(
    connection,
    role="api_producer",
    capabilities=[Capability.PRODUCE],
    queues=["compute"],
)
```

The migration role owns the schema and is the only role that runs DDL. Queue
scoping is enforced by row-level-security policies driven by rows in
`role_queue_grants`, so the queues a role may touch arrive as bind parameters
instead of interpolated SQL.

After upgrading rqueue, re-run `provision_role` for every role. See
[Re-provisioning roles](migrations.md#re-provisioning-roles).

## Capabilities

| Capability | May |
| --- | --- |
| `PRODUCE` | enqueue and read jobs (`UPDATE` only on `updated_at`, for the enqueue conflict path) |
| `CONSUME` | claim, heartbeat, and transition jobs, but **not** enqueue them |
| `SCHEDULE` | read schedules and enqueue their occurrences, but not transition a job |
| `INSPECT` | read everything, write nothing, including the liveness a readiness probe needs |
| `PURGE` | delete terminal jobs, through `purge_terminal_jobs()` and only through it |

`CONSUME` has no `INSERT` on `jobs`. Claiming work and creating it are separate
authorities, and a worker that can enqueue can hand the fleet any task it likes.
A process that legitimately does both, such as a worker that fans out follow-up
jobs, asks for `[Capability.PRODUCE, Capability.CONSUME]` in its provisioning
call.

`PURGE` has no `DELETE` on `jobs` either. It gets `EXECUTE` on
`task_queue.purge_terminal_jobs(queue, states, finished_before, max_rows)`, a
`SECURITY DEFINER` routine installed by migration 0006 that re-derives every
limit for itself: one named queue the caller is granted, terminal states only, a
cutoff in the past, and a bounded batch. The routine is the safety boundary, not
the grant. A crafted call cannot reach a `pending` job, another queue's jobs, or
the whole table.

## Purging

`Admin.purge()` and `rqueue purge` go through that routine, so they are exactly
what `PURGE` authorizes. There is no second, rawer path:

```python
removed = await Admin(pool).purge(queue="compute", retention=timedelta(days=30))
```

Naming a queue is the cheaper call. "Every queue" is spelled by omitting the
argument, not by the `"*"` that pause and resume take. Purge reads `"*"` as a
queue name, and there is no queue by that name.

Omitting the queue purges every queue that has anything to purge, which for a
scoped role means every queue it can see. `limit` is spent across them as one
budget, oldest job first regardless of which queue it is on, and that many jobs
are deleted when that many exist. None of that follows from visiting the queues
in a good order. The cutoff is first tightened to the age of the budget's last
row, inclusive of every job finishing in the same instant, since a transaction
that finishes a hundred jobs gives them all one `finished_at`. No queue can
reach past that cutoff however much backlog it has. Tightening it also keeps the
work proportional to `limit` instead of to the backlog behind it.

Only terminal states may be named. Asking for a live one raises instead of
quietly matching nothing. The cutoff must be timezone-aware, as every
caller-supplied instant in rqueue is.

## Row-level security

Six tables are under the queue-scoping policies: `jobs`, `job_attempts`,
`concurrency_slots`, `runtime_heartbeats`, `schedules`, and
`schedule_occurrences`.

### Heartbeats

A heartbeat is the one row whose queue scope is not the whole answer. On a queue
a worker and a scheduler share, "may this role touch this queue?" lets either of
them write the other's liveness row. The row therefore also records the role that
wrote it, and only that role may refresh or retract it. The `kind` it claims must
be one that role's capabilities entitle it to. Otherwise a worker could tell a
readiness probe that a scheduler is alive by inserting a row of its own.

Reading stays wide, because readiness has to see components it did not write.
Retracting is narrower still. A role may delete a heartbeat it wrote whatever it
is currently granted, because taking a claim down asserts nothing. Requiring the
queue grant there would strand a scheduler's own row on a queue an operator had
just reassigned, unreadable and undeletable by anyone but the schema owner.

A scheduler reports liveness for the queues it schedules. With no enabled
schedules it reports none, because there is no queue for it to be the live
scheduler of.

### Tables outside the policies

Two tables that carry a queue name are left out of the policies, and both are
readable in full by any role that can reach the schema:

* `queue_pauses`, because the wildcard pause is a row on the queue `'*'` and a
  policy scoped to a role's own queues would hide it. A worker that cannot see
  the global pause does not honour it, which is the one outcome the pause table
  exists to prevent. The cost is that a role granted one queue can see every
  queue's pause state, meaning when it was paused and, by implication, that it
  exists.
* `role_queue_grants`, because the policies on the six tables above are
  themselves subqueries against it, evaluated as the querying role. Under RLS it
  would filter itself and every other policy would match nothing. It is granted
  `SELECT` directly instead, so a scoped role can read every role's queue
  grants. That reveals the shape of the deployment and is not a way into it.
  Only the migration role may write it, which is what keeps a role from granting
  itself a queue.

A third table, `role_runtime_kinds`, is also outside the policies. It records
which component kinds a role may claim liveness as. It carries no queue column,
so the count of six is unaffected. It is not directly readable at all: the
heartbeat policies reach it through a `SECURITY DEFINER` function, so no runtime
role needs a grant on it.

### Queue labels tied to jobs

A policy can only test the queue the writer wrote. On a row that also points at a
job, that queue is a label and not a boundary. Those labels are tied to their job
by composite foreign key, for attempt records, concurrency slots, and
occurrences alike. A role granted one queue therefore cannot attach its own label
to another queue's job and pass its own policy.

That matters because the keys those rows carry are global: an attempt number, a
slot key, an occurrence instant. Forging one consumes it, and the queue that
really owns it is denied service.

The anchor is always the job, because a job's queue never changes. A schedule's
can change, so an occurrence takes its queue from the job it fired and not from
the schedule that fired it. Moving a schedule leaves its existing history where
those jobs actually live.

The occurrence key itself stays queue-free. It is the one-firing-per-instant
guarantee and has to mean the same thing from every queue. What protects it is
the policy, which asks whether the writer can see the schedule at all. A
scheduler reads the whole history of a schedule it owns, across a queue move, but
can only write occurrences for schedules on queues it holds.

## What provisioning repairs

A role that *owns* a table in the schema, the schema itself, or the database is
refused outright, with nothing written. Ownership sits above the ACL that
`provision_role()` edits. An owner can re-grant itself anything and can `ALTER`
or `DROP` the table, including switching row-level security off. Reassign the
object to the migration role first.

Provisioning also repairs what lies outside the schema. A provisioned role is set
`NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION NOINHERIT` and
stripped of every role membership before its capability grants are applied.
Re-running `provision_role()` on a role someone quietly made a superuser
therefore narrows it. There is no switch to ask for less. A connection that
cannot clear an attribute the role holds raises, instead of returning a role that
is only partly narrowed.

The whole call is one transaction, so that holds for every other failure too.
Granting `PURGE` against a schema whose migrations stop short of the purge
routine is the one you meet mid-deploy. It raises, and the role is left exactly
as the call found it: absent if it did not exist, untouched if it did. It does
not end up existing, able to log in, and carrying half a capability set.
