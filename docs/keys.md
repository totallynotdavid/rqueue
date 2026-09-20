# Keys: `dedupe_key` and `concurrency_key`

They are different primitives and rqueue keeps them apart. Procrastinate calls
them `queueing_lock` and `lock`.

## `dedupe_key`

`dedupe_key` prevents a duplicate job from being *queued*. It is scoped to one
queue and held for exactly as long as the job is active, which means `pending`
or `leased`. The moment the job reaches a terminal state the key is free again.
"One simulation queued per external id" therefore does not block the next run of
the same simulation tomorrow.

`on_conflict` has no default and must be given whenever `dedupe_key` is:

* `"return_existing"` returns the job that already holds the key.
* `"raise"` raises an `AlreadyEnqueued` carrying that job's id.

Either way the losing producer learns what happened. rqueue never silently
absorbs an enqueue.

## `concurrency_key`

`concurrency_key` prevents duplicate concurrent *execution* of a shared
resource, such as "one simulation at a time per compute job". It is a lease, not
a flag. The holder's slot is released when the job finishes and expires on its
own if the holder dies, so a crashed worker cannot hold a business resource
hostage.

Like `dedupe_key`, it is scoped to one queue. Two queues using the same key name
do not exclude each other. A queue is the boundary a least-privilege role is
scoped to, and a lock that crossed it would be one a scoped worker could neither
see nor take.
