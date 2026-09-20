# Storage

| Table | Holds |
| --- | --- |
| `jobs` | one row per job, including its lease and terminal outcome |
| `job_attempts` | one immutable record per attempt, for audit and debugging |
| `concurrency_slots` | the lease behind each named concurrency key, per queue |
| `schedules` | periodic schedule definitions |
| `schedule_occurrences` | the occurrence keys that have fired, and their jobs |
| `runtime_heartbeats` | worker and scheduler liveness per queue, for readiness checks |
| `queue_pauses` | which queues are paused, and since when |
| `role_queue_grants` | which queues a granted role may reach |
| `role_runtime_kinds` | which component kinds (worker, scheduler) a role may claim liveness as |
| `schema_migrations` | applied migration versions and their checksums |

Plus one routine: `purge_terminal_jobs()`, the `SECURITY DEFINER` boundary the
`PURGE` capability is granted instead of a table-wide `DELETE`.

Every index is declared in the migration next to the query it serves. All
internal SQL is static and parameterized.

## Module layout

* `src/rqueue/storage.py`: the runtime read/write path.
* `src/rqueue/migrations/`: schema DDL.
* `src/rqueue/roles.py`: role grants.

[Requirements §8](requirements.md) states the invariants each table's writers
must keep.
