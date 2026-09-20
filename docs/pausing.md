# Pausing a queue

Pausing stops a queue admitting **new** work across every worker replica. It
does not stop a process and does not touch anything already leased:

```python
admin = Admin(pool)

await admin.pause_queue("compute")   # every worker on `compute`, right now
await admin.resume_queue("compute")
await admin.pause_queue("*")         # every queue
```

The switch is a durable row (`queue_pauses.paused_at`), and the claim query
itself reads it. The pause therefore holds for a worker replica that has never
heard of the call, and it survives a restart of all of them. Here a paused queue
returns zero claimable rows at the database.

That is stronger than the references. Oban keeps the flag in each producer
process, so a restarted queue comes back running. River stores the row but
checks it in the client, so a worker that has not polled yet can still issue a
claim.

`NOTIFY` is a latency optimization only, exactly as it is for job wake-ups. It
saves an *idle* worker the rest of its poll interval when you resume. Nothing
depends on its arrival.

## What pause does not do

Pause does not touch an in-flight attempt. A job leased before the pause runs,
heartbeats, and finalizes normally. Pause is about admission,
`Worker.stop()` and `Worker.drain()` are about a worker's own lifecycle, and
`Admin.cancel_job` is about one job.

## Wildcard and named pauses

`resume_queue("*")` resumes every queue, including queues paused by name.
Resuming one queue by name does not lift a wildcard pause, so the narrower call
cannot punch a hole in the broader one.

`Admin.paused_queues()` lists what is paused and since when.
`Admin.is_queue_paused(name)` answers for one queue, wildcard included.
