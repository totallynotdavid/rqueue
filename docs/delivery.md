# Delivery

Delivery is at least once. A handler can run again after a worker crash, a lease
expiry, or a network failure that hides whether an attempt finished. Make
handlers and their external side effects idempotent. rqueue offers no
exactly-once execution.

## Job states

| State       | Meaning                                         | Terminal |
| ----------- | ----------------------------------------------- | -------- |
| `pending`   | waiting to be claimed, now or at `scheduled_at` | no       |
| `leased`    | claimed by a worker, which holds the lease      | no       |
| `succeeded` | the handler returned                            | yes      |
| `failed`    | no attempts left, or a permanent failure        | yes      |
| `cancelled` | cancelled before or during execution            | yes      |

A terminal job becomes `pending` again only through `Admin.retry_job`. See
[Operations](operations.md#admin).

Each attempt also writes a row to `job_attempts` that is never updated after the
attempt ends. `Admin.attempts(job_id)` returns them.

## Leases

A worker claims jobs in one short transaction that uses
`FOR UPDATE SKIP LOCKED`. The claim sets the job to `leased`, adds one to
`attempt`, and records the worker id, a new `lease_token`, and `leased_until`.
The worker holds no database transaction open while a handler runs.

While a handler runs, the worker extends the lease. A heartbeat runs every
`lease_duration / 3` seconds (30 seconds by default, so every 10).
`TaskContext.heartbeat()` extends it on demand.

Every write for an attempt names the job id, the `lease_token`, and the state
`leased` in its `WHERE` clause. The writes are the heartbeat, success, failure,
reschedule, and cancellation. These statements are in
[`src/rqueue/storage.py`](../src/rqueue/storage.py). A worker that stalled past
its lease holds a token that no longer matches, so its write changes no row and
raises `LeaseLost`. It cannot overwrite the attempt that replaced it.

## When a worker dies

Each worker tick recovers the expired leases on its queue. For every expired
lease, in one statement, rqueue:

- closes the attempt record with the outcome `lease_expired`,
- releases the job's concurrency slot, and
- moves the job to `pending` so another worker can claim it. If the job has no
  attempts left it becomes `failed` with the error type `LeaseExpired`. If
  cancellation was requested it becomes `cancelled`.

A queue with no running worker recovers nothing until one starts.

## Wake-ups

A worker polls its queue every `poll_interval` seconds (1 by default). It also
listens on a PostgreSQL `NOTIFY` channel, which PostgreSQL sends when a job
becomes claimable or a queue resumes. A missed notification adds latency and
loses nothing.
