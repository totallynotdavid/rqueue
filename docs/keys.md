# Keys

`dedupe_key` and `concurrency_key` are different primitives. One stops a
duplicate job from being queued. The other stops two jobs from running at the
same time.

## `dedupe_key`

`dedupe_key` prevents a duplicate job from being queued. It is scoped to one
queue and held while the job is active, which means `pending` or `leased`. When
the job reaches a terminal state the key is free again. "One simulation queued
per external id" therefore does not block the next run of the same simulation
tomorrow. The rule is the partial unique index `jobs_dedupe_active_uq` in
[`0001_core.sql`](../src/rqueue/migrations/0001_core.sql).

`on_conflict` has no default. Pass it whenever you pass `dedupe_key`:

- `"return_existing"` returns the job that already holds the key.
- `"raise"` raises `AlreadyEnqueued`, which carries the existing job's id.

Either way the producer learns what happened. rqueue never absorbs an enqueue
silently.

## `concurrency_key`

`concurrency_key` prevents two jobs from running at once on a shared resource,
such as "one simulation at a time per compute job". It is a lease, not a flag.
The slot is released when the job finishes. It expires on its own if the holder
dies, so a crashed worker cannot hold a business resource.

The claim transaction acquires the slot and the finalize transaction releases it
(`acquire_slot` and `release_slot` in
[`src/rqueue/storage.py`](../src/rqueue/storage.py)). A job whose key is held
stays `pending` until the slot is free.

Like `dedupe_key`, a concurrency key is scoped to one queue. Two queues that use
the same key name do not exclude each other. A queue is the boundary a
[role](roles.md) is scoped to, and a lock that crossed it would be one a scoped
worker could neither see nor take.
