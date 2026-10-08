# rqueue manual

Read the first three documents in order. The rest are reference for one subject
each.

1. [Migrations](migrations.md): install the schema and upgrade between releases.
2. [Tasks](tasks.md): register handlers, declare tasks for producer-only
   processes, and record state before a retry.
3. [Enqueueing](enqueueing.md): enqueue inside your own transaction and keep
   business code free of the queue import.
4. [Running a worker](worker.md): start, stop, and drain a worker, and run
   blocking work.
5. [Keys](keys.md): `dedupe_key` and `concurrency_key`.
6. [Ordering](ordering.md): the order in which jobs are claimed.
7. [Retries, timeouts, and cancellation](retries.md)
8. [Delivery](delivery.md): job states, leases, and what happens when a worker
   dies.
9. [Periodic schedules](scheduling.md)
10. [Pausing a queue](pausing.md)
11. [Testing](testing.md): unit-test enqueue calls without PostgreSQL.
12. [Operations](operations.md): the command line, `Admin`, readiness, and
    metrics.
13. [Roles](roles.md): capabilities, row-level security, and `provision_role`.
14. [Storage](storage.md): the tables and who writes them.

[architecture.md](architecture.md) maps the source code.
