# Retries, timeouts, and cancellation

A handler expresses its outcome by returning or by raising:

| Outcome | How |
| --- | --- |
| success | return normally |
| retry at a chosen time | `raise Retry(delay=...)` or `Retry(at=...)` |
| terminal failure now | `raise PermanentFailure(...)` |
| cancel this job | `raise CancelJob(...)` |
| retry under the policy | raise anything else the policy retries |

The persisted `attempt` column is the sole authority on how many attempts a job
has had. It survives crashes and worker restarts. Exception detail recorded in
PostgreSQL is bounded and sanitized. Full tracebacks belong in the structured
worker log.

To record application state that depends on the outcome, see
[Recording state before a retry](tasks.md#recording-state-before-a-retry).

## Timeouts

Timeouts are per task (`timeout=` on registration, or per job on enqueue). A
timeout cancels the handler. It proves nothing about whether an external side
effect already happened, which is the other reason handlers must be idempotent.

## Cancellation

Cancellation is cooperative. `Admin.cancel_job` on a *pending* job cancels it
outright. On a *leased* job it sets `cancel_requested`. The lease holder sees it
on its next heartbeat and finalizes the job. If that worker never comes back,
lease expiry finalizes it.
