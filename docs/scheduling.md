# Periodic schedules

```python
scheduler = Scheduler(
    queue,
    scheduler_id="scheduler-1",
    schedules=[
        ScheduleSpec(
            name="nightly-vacuum",
            task="maintenance",
            cron="0 3 * * *",
            timezone="America/Lima",
        )
    ],
)
await scheduler.run()
```

Each tick writes the occurrence key `(schedule_id, occurrence_at)` and the job it
produces in **one transaction**. Several scheduler replicas are safe. They
collide on that unique key and exactly one wins. A scheduler that dies mid-tick
leaves nothing behind, so the next tick retries the same occurrence. There is no
leader election and no schedule-row reclaim, and the occurrence table is an
audit trail of what fired when.

After an outage, `catchup` (default 1) bounds how many missed occurrences one
tick fires. A new schedule never fires for occurrences that predate it.
