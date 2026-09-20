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

| Module | Holds |
| --- | --- |
| `queue.py` | `Queue`: task registration and transactional enqueue |
| `worker.py` | `Worker`: claim, run, and finalize jobs under a lease |
| `scheduler.py` | `Scheduler`: periodic schedules fired through the occurrence-key table |
| `storage.py` | the only module that reads or writes job and schedule rows, and where every runtime SQL statement lives |
| `migrations/` | schema DDL and the migration runner |
| `roles.py` | capability grants and `provision_role` |
| `admin.py` | `Admin`: inspection and administration |
| `health.py` | `check_readiness` |
| `tasks.py` | the task registry: explicit names, explicit decoders, async handlers only |
| `context.py` | `TaskContext`, what a handler is given |
| `retry.py` | `RetryPolicy` |
| `executor.py` | the bounded default executor the worker installs |
| `metrics.py` | `MetricsSink` and `LoggingMetricsSink` |
| `limits.py` | resource bounds and their validators |
| `models.py` | job state, jobs, attempts, schedules, and statistics |
| `cron.py` | validated cron expressions |
| `errors.py` | the exception hierarchy |
| `cli.py` | the `rqueue` command |
| `testing.py` | `RecordingQueue`, which the package does not export |

All paths are under `src/rqueue/`.

[Requirements §8](requirements.md) states the invariants each table's writers
must keep.
