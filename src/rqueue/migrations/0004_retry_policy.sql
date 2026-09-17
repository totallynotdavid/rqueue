-- rqueue 0004: persist the data-only retry settings used to enqueue a job.
--
-- State and transition invariant:
--
-- * retry_policy IS NULL means that the job has no producer declaration for
--   these numeric retry settings. This is the state of pre-0004 rows and of
--   jobs enqueued by a queue that only calls register(). The NOT NULL
--   jobs.max_attempts column remains the attempt ceiling; a worker supplies
--   its registration's local backoff, multiplier, jitter, and retry hooks.
--   timeout_seconds follows the same fallback rule when it is NULL.
-- * A non-NULL retry_policy is exactly the version-1 object checked below.
--   It is created by a producer enqueue after declare_task() and is
--   authoritative for max_attempts, backoff, multiplier, and jitter across
--   worker processes. The worker reads it but never supplies executable
--   behavior from it; retry_on and retry_if remain registration-local.
-- * A producer may create the NULL -> valid-policy transition only by
--   inserting a new job. A worker may read either valid state, using its
--   local registration for omitted values and hooks. An operator retry keeps
--   the current state and raises both jobs.max_attempts and the persisted
--   policy's max_attempts together when the policy is non-NULL.
-- * Malformed non-NULL data is outside the valid state machine. Normal writes
--   cannot create it because this constraint is validated; the claim path
--   quarantines a legacy/directly-corrupted value by recording the error and
--   moving it to NULL before terminally failing that attempt.
--
-- Exception classes and retry predicates are deliberately not persisted:
-- they are executable registration-time choices owned by the worker.

ALTER TABLE {schema}.jobs
    ADD COLUMN retry_policy jsonb;

ALTER TABLE {schema}.jobs
    ADD CONSTRAINT jobs_retry_policy_shape CHECK (
        retry_policy IS NULL OR (
            jsonb_typeof(retry_policy) = 'object'
            AND retry_policy ?& ARRAY[
                'version', 'max_attempts', 'initial_backoff',
                'max_backoff', 'multiplier', 'jitter'
            ]
            AND retry_policy - 'version' - 'max_attempts'
                - 'initial_backoff' - 'max_backoff'
                - 'multiplier' - 'jitter' = '{}'::jsonb
            AND jsonb_typeof(retry_policy->'version') = 'number'
            AND retry_policy->>'version' = '1'
            AND jsonb_typeof(retry_policy->'max_attempts') = 'number'
            AND jsonb_typeof(retry_policy->'initial_backoff') = 'number'
            AND jsonb_typeof(retry_policy->'max_backoff') = 'number'
            AND jsonb_typeof(retry_policy->'multiplier') = 'number'
            AND jsonb_typeof(retry_policy->'jitter') = 'number'
            AND retry_policy->>'max_attempts' ~ '^[0-9]+$'
            AND (retry_policy->>'max_attempts')::numeric =
                trunc((retry_policy->>'max_attempts')::numeric)
            AND (retry_policy->>'max_attempts')::numeric BETWEEN 1 AND 1000
            AND (retry_policy->>'max_attempts')::numeric = max_attempts
            AND (retry_policy->>'initial_backoff')::numeric >= 0
            AND (retry_policy->>'initial_backoff')::numeric <=
                1.7976931348623157e308
            AND (retry_policy->>'max_backoff')::numeric >=
                (retry_policy->>'initial_backoff')::numeric
            AND (retry_policy->>'max_backoff')::numeric <=
                1.7976931348623157e308
            AND (retry_policy->>'multiplier')::numeric >= 1
            AND (retry_policy->>'multiplier')::numeric <=
                1.7976931348623157e308
            AND (retry_policy->>'jitter')::numeric BETWEEN 0 AND 1
        ) IS TRUE
    ) NOT VALID;

ALTER TABLE {schema}.jobs
    VALIDATE CONSTRAINT jobs_retry_policy_shape;
