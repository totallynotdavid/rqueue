-- rqueue 0001: jobs, attempt history, concurrency slots, and the wake trigger.
--
-- Every index below is listed with the query it serves, as REQUIREMENTS.md §7
-- requires. {schema} is substituted by the migration runner with a validated
-- lower-case identifier; it is the only interpolated value in this package.

CREATE TABLE {schema}.jobs (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Insertion order, and the tie-break the claim ordering needs.
    -- created_at cannot serve: it defaults to now(), which is the
    -- transaction timestamp, so every job enqueued in one transaction
    -- shares it and FIFO would collapse into UUID order.
    seq               bigint      GENERATED ALWAYS AS IDENTITY,
    queue             text        NOT NULL,
    task              text        NOT NULL,
    payload           jsonb       NOT NULL,
    state             text        NOT NULL,
    priority          smallint    NOT NULL DEFAULT 0,
    attempt           integer     NOT NULL DEFAULT 0,
    max_attempts      integer     NOT NULL,
    scheduled_at      timestamptz NOT NULL DEFAULT now(),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    started_at        timestamptz,
    finished_at       timestamptz,
    dedupe_key        text,
    concurrency_key   text,
    worker_id         text,
    lease_token       uuid,
    leased_until      timestamptz,
    heartbeat_at      timestamptz,
    cancel_requested  boolean     NOT NULL DEFAULT false,
    timeout_seconds   double precision,
    error_type        text,
    error_message     text,
    metadata          jsonb       NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT jobs_state_valid CHECK (
        state IN ('pending', 'leased', 'succeeded', 'failed', 'cancelled')
    ),
    -- The fencing invariant from §3: exactly the leased rows carry a token and
    -- an expiry. Clearing the token is therefore the same act as ending the
    -- lease, and no terminal row can be reopened by presenting an old token.
    CONSTRAINT jobs_lease_shape CHECK (
        (state = 'leased') = (lease_token IS NOT NULL)
        AND (state = 'leased') = (leased_until IS NOT NULL)
    ),
    CONSTRAINT jobs_terminal_finished CHECK (
        (state IN ('succeeded', 'failed', 'cancelled')) = (finished_at IS NOT NULL)
    ),
    CONSTRAINT jobs_attempt_bounds CHECK (
        attempt >= 0
        AND max_attempts BETWEEN 1 AND 1000
        AND attempt <= max_attempts
    ),
    CONSTRAINT jobs_timeout_positive CHECK (
        timeout_seconds IS NULL OR timeout_seconds > 0
    ),
    -- §8 resource bounds, restated in SQL so a direct writer cannot exceed
    -- them either. The Python validators in rqueue.limits raise earlier.
    CONSTRAINT jobs_queue_length CHECK (length(queue) BETWEEN 1 AND 64),
    CONSTRAINT jobs_task_length CHECK (length(task) BETWEEN 1 AND 128),
    CONSTRAINT jobs_worker_id_length CHECK (
        worker_id IS NULL OR length(worker_id) BETWEEN 1 AND 128
    ),
    CONSTRAINT jobs_dedupe_key_length CHECK (
        dedupe_key IS NULL OR length(dedupe_key) BETWEEN 1 AND 256
    ),
    CONSTRAINT jobs_concurrency_key_length CHECK (
        concurrency_key IS NULL OR length(concurrency_key) BETWEEN 1 AND 256
    ),
    CONSTRAINT jobs_error_type_length CHECK (
        error_type IS NULL OR length(error_type) <= 256
    ),
    CONSTRAINT jobs_error_message_length CHECK (
        error_message IS NULL OR length(error_message) <= 4096
    ),
    CONSTRAINT jobs_payload_size CHECK (octet_length(payload::text) <= 262144),
    CONSTRAINT jobs_metadata_size CHECK (octet_length(metadata::text) <= 8192),
    CONSTRAINT jobs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

-- Serves the claim candidate query:
--   SELECT ... WHERE queue = $1 AND state = 'pending' AND scheduled_at <= now()
--   ORDER BY priority DESC, scheduled_at, seq
--   FOR UPDATE SKIP LOCKED LIMIT $n
CREATE INDEX jobs_claim_idx
    ON {schema}.jobs (queue, priority DESC, scheduled_at, seq)
    WHERE state = 'pending';

-- Enforces "at most one active job per dedupe key per queue" (§3) and serves
-- the ON CONFLICT arbiter in the enqueue upsert. The predicate is repeated
-- verbatim in that statement so PostgreSQL can infer this index.
CREATE UNIQUE INDEX jobs_dedupe_active_uq
    ON {schema}.jobs (queue, dedupe_key)
    WHERE dedupe_key IS NOT NULL AND state IN ('pending', 'leased');

-- Serves the expired-lease sweep:
--   SELECT ... WHERE state = 'leased' AND leased_until <= now()
CREATE INDEX jobs_lease_expiry_idx
    ON {schema}.jobs (leased_until)
    WHERE state = 'leased';

-- Serves queue/state inspection and depth metrics:
--   SELECT ... WHERE queue = $1 AND state = ANY($2) ORDER BY seq DESC
CREATE INDEX jobs_queue_state_idx
    ON {schema}.jobs (queue, state, seq);

-- Serves retention cleanup:
--   DELETE FROM jobs WHERE state IN (terminal) AND finished_at < $1
CREATE INDEX jobs_retention_idx
    ON {schema}.jobs (finished_at)
    WHERE state IN ('succeeded', 'failed', 'cancelled');

-- Immutable-once-finalized attempt records: one row per claim, closed exactly
-- once with its outcome. Kept separate from jobs so a job's history survives
-- the next attempt overwriting the job row's lease columns (§7).
CREATE TABLE {schema}.job_attempts (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id        uuid        NOT NULL REFERENCES {schema}.jobs (id) ON DELETE CASCADE,
    queue         text        NOT NULL,
    task          text        NOT NULL,
    attempt       integer     NOT NULL,
    worker_id     text        NOT NULL,
    lease_token   uuid        NOT NULL,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    outcome       text,
    error_type    text,
    error_message text,

    CONSTRAINT job_attempts_outcome_valid CHECK (
        outcome IS NULL
        OR outcome IN ('succeeded', 'failed', 'retry', 'cancelled', 'lease_expired')
    ),
    CONSTRAINT job_attempts_finished_shape CHECK (
        (outcome IS NULL) = (finished_at IS NULL)
    ),
    CONSTRAINT job_attempts_error_message_length CHECK (
        error_message IS NULL OR length(error_message) <= 4096
    )
);

-- Serves attempt-history lookup (`SELECT ... WHERE job_id = $1 ORDER BY
-- attempt`) and enforces one record per attempt number.
CREATE UNIQUE INDEX job_attempts_job_attempt_uq
    ON {schema}.job_attempts (job_id, attempt);

-- Named concurrency keys (§6): mutual exclusion on an application-chosen
-- resource, held as a *lease* rather than a boolean flag. A crashed holder's
-- slot becomes acquirable the moment `leased_until` passes, with no reclaim
-- job needed. Acquisition is an INSERT ... ON CONFLICT DO UPDATE, so two
-- workers racing for one key serialize on this row instead of both passing a
-- NOT EXISTS check against a snapshot neither can see the other in.
CREATE TABLE {schema}.concurrency_slots (
    key          text        PRIMARY KEY,
    job_id       uuid        NOT NULL REFERENCES {schema}.jobs (id) ON DELETE CASCADE,
    lease_token  uuid        NOT NULL,
    worker_id    text        NOT NULL,
    acquired_at  timestamptz NOT NULL DEFAULT now(),
    leased_until timestamptz NOT NULL,

    CONSTRAINT concurrency_slots_key_length CHECK (length(key) BETWEEN 1 AND 256)
);

-- Serves the "release every slot this job holds" cleanup and slot inspection.
CREATE INDEX concurrency_slots_job_idx ON {schema}.concurrency_slots (job_id);

-- Wake trigger (§3). Following River rather than pgqueuer: fire only on the
-- transition *into* an immediately-runnable state, with the queue name as the
-- whole payload. A heartbeat-only UPDATE must not wake every worker, and a
-- job scheduled for the future has nothing to wake anyone for yet.
CREATE FUNCTION {schema}.notify_job_available() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(TG_ARGV[0], NEW.queue);
    RETURN NULL;
END;
$$;

CREATE TRIGGER jobs_notify_insert
    AFTER INSERT ON {schema}.jobs
    FOR EACH ROW
    WHEN (NEW.state = 'pending' AND NEW.scheduled_at <= now())
    EXECUTE FUNCTION {schema}.notify_job_available('rqueue_{schema}');

CREATE TRIGGER jobs_notify_update
    AFTER UPDATE ON {schema}.jobs
    FOR EACH ROW
    WHEN (
        NEW.state = 'pending'
        AND NEW.scheduled_at <= now()
        AND (OLD.state IS DISTINCT FROM 'pending' OR OLD.scheduled_at > now())
    )
    EXECUTE FUNCTION {schema}.notify_job_available('rqueue_{schema}');
