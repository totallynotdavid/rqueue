# Retries, timeouts, and cancellation

## Outcomes

A handler chooses its outcome by returning or by raising:

| Outcome                    | How                                               |
| -------------------------- | ------------------------------------------------- |
| success                    | return normally                                   |
| retry at a chosen time     | `raise Retry(delay=...)` or `raise Retry(at=...)` |
| fail now, without retrying | `raise PermanentFailure(...)`                     |
| cancel this job            | `raise CancelJob(...)`                            |
| retry under the policy     | raise any other exception that the policy retries |

`Retry` takes `delay` (seconds or a `timedelta`) or `at` (a `datetime`), not
both. With neither, the job waits for the policy's backoff.

`Retry` skips the policy's exception filter but not the attempt limit. When no
attempts are left the job fails with the error type `Retry`.

The `attempt` column in PostgreSQL is the only count of how many attempts a job
has had, so a crash or a worker restart does not reset it. To record application
state that depends on the outcome, see
[Record state before a retry](tasks.md#record-state-before-a-retry).

## Retry policy

`RetryPolicy` sets the budget and the wait between attempts:

| Field             | Default        | Meaning                                                 |
| ----------------- | -------------- | ------------------------------------------------------- |
| `max_attempts`    | 3              | total attempts, from 1 to 1000                          |
| `initial_backoff` | 1.0            | seconds to wait after the first failure                 |
| `multiplier`      | 2.0            | growth of the wait for each further failure, at least 1 |
| `max_backoff`     | 3600.0         | upper bound on the wait, in seconds                     |
| `jitter`          | 0.1            | a fraction of the wait, added or subtracted at random   |
| `retry_on`        | `(Exception,)` | exception classes the policy retries                    |
| `retry_if`        | `None`         | a predicate that replaces `retry_on` when set           |

The wait after attempt `n` is `initial_backoff * multiplier ** (n - 1)`, capped
at `max_backoff`, then spread by `jitter` so that a fleet that fails together
does not retry together. `PermanentFailure` is never retried.

`max_attempts`, the backoff settings and the timeout are stored on the job when
it is enqueued. `retry_on` and `retry_if` stay in the worker's registration. See
[Declare a task without its handler](tasks.md#declare-a-task-without-its-handler).

rqueue stores error detail in PostgreSQL truncated to 4096 characters for the
message and 256 for the type. Full tracebacks belong in the worker's log.

## Timeouts

A timeout is set per task with `timeout=` on registration, or per job on
enqueue. When it expires the handler is cancelled and the job retries under the
policy. A timeout does not show whether an external side effect already
happened, which is another reason handlers must be idempotent.

## Cancellation

- `Admin.cancel_job` on a `pending` job cancels it at once. The job never runs.
- On a `leased` job it sets `cancel_requested`. The worker's next heartbeat sees
  the request, sets `TaskContext.cancel_requested`, and cancels the handler's
  asyncio task. The worker then finalizes the job as `cancelled`. The wait is at
  most one heartbeat interval, which is a third of the lease duration.
- A handler that catches `asyncio.CancelledError` must re-raise it. If it
  returns normally instead, the worker records the job as `succeeded`.
- If the worker never returns, lease expiry finalizes the job. See
  [Delivery](delivery.md#when-a-worker-dies).
