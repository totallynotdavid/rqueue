-- rqueue 0002: periodic schedules, their fired occurrences, and the runtime
-- heartbeats that readiness checks read.

CREATE TABLE {schema}.schedules (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text        NOT NULL UNIQUE,
    queue           text        NOT NULL,
    task            text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    cron            text        NOT NULL,
    timezone        text        NOT NULL DEFAULT 'UTC',
    enabled         boolean     NOT NULL DEFAULT true,
    priority        smallint    NOT NULL DEFAULT 0,
    max_attempts    integer     NOT NULL DEFAULT 3,
    concurrency_key text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT schedules_name_length CHECK (length(name) BETWEEN 1 AND 128),
    CONSTRAINT schedules_queue_length CHECK (length(queue) BETWEEN 1 AND 64),
    CONSTRAINT schedules_task_length CHECK (length(task) BETWEEN 1 AND 128),
    CONSTRAINT schedules_cron_length CHECK (length(cron) BETWEEN 1 AND 256),
    CONSTRAINT schedules_max_attempts CHECK (max_attempts BETWEEN 1 AND 1000),
    CONSTRAINT schedules_payload_size CHECK (octet_length(payload::text) <= 262144)
);

-- Serves the scheduler tick: `SELECT ... WHERE enabled ORDER BY name`.
CREATE INDEX schedules_enabled_idx ON {schema}.schedules (name) WHERE enabled;

-- The occurrence key from §6. One row per (schedule, occurrence instant),
-- inserted in the *same transaction* as the job it produces. Two schedulers
-- racing for one occurrence collide on this primary key and exactly one wins;
-- a scheduler that dies mid-transaction leaves no row at all, so the next tick
-- simply tries the same occurrence again. No leader election, no reclaim, and
-- the table doubles as an audit trail of what fired when.
CREATE TABLE {schema}.schedule_occurrences (
    schedule_id   uuid        NOT NULL REFERENCES {schema}.schedules (id) ON DELETE CASCADE,
    occurrence_at timestamptz NOT NULL,
    job_id        uuid        NOT NULL REFERENCES {schema}.jobs (id) ON DELETE CASCADE,
    fired_at      timestamptz NOT NULL DEFAULT now(),
    fired_by      text,

    PRIMARY KEY (schedule_id, occurrence_at)
);

-- Serves "what was the most recent occurrence of this schedule?", the query
-- that bounds catch-up after a scheduler outage. The primary key is ascending
-- on occurrence_at, so a descending scan needs its own index.
CREATE INDEX schedule_occurrences_recent_idx
    ON {schema}.schedule_occurrences (schedule_id, occurrence_at DESC);

-- Liveness rows for readiness checks (§7). Workers and schedulers upsert their
-- own row every tick; a readiness probe reports the component available when a
-- row is newer than the caller's staleness bound. Deliberately not part of the
-- claim path -- a stale row delays a readiness answer, never a job.
CREATE TABLE {schema}.runtime_heartbeats (
    kind       text        NOT NULL,
    instance   text        NOT NULL,
    queue      text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    metadata   jsonb       NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (kind, instance),
    CONSTRAINT runtime_heartbeats_kind_valid CHECK (kind IN ('worker', 'scheduler')),
    CONSTRAINT runtime_heartbeats_instance_length CHECK (
        length(instance) BETWEEN 1 AND 128
    )
);

-- Serves "is any worker for this queue alive?":
--   SELECT ... WHERE kind = $1 AND updated_at > $2
CREATE INDEX runtime_heartbeats_liveness_idx
    ON {schema}.runtime_heartbeats (kind, updated_at DESC);

-- Queue scoping for least-privilege roles (§8). A granted role's reach is
-- data, not DDL: one row per (role, queue) here, and one policy that reads it.
-- That keeps role provisioning free of interpolated queue names and lets an
-- operator widen or narrow a role with an INSERT or a DELETE.
CREATE TABLE {schema}.role_queue_grants (
    role_name  text        NOT NULL,
    queue      text        NOT NULL,
    granted_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (role_name, queue)
);

-- Row-level security is enabled but not forced, so the schema owner -- the
-- migration role -- still sees everything. Any *other* role reaches only the
-- queues it has been granted; '*' grants every queue.
ALTER TABLE {schema}.jobs ENABLE ROW LEVEL SECURITY;

CREATE POLICY jobs_queue_scope ON {schema}.jobs
    FOR ALL
    USING (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user AND g.queue IN (jobs.queue, '*')
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user AND g.queue IN (jobs.queue, '*')
        )
    );

ALTER TABLE {schema}.job_attempts ENABLE ROW LEVEL SECURITY;

CREATE POLICY job_attempts_queue_scope ON {schema}.job_attempts
    FOR ALL
    USING (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user AND g.queue IN (job_attempts.queue, '*')
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user AND g.queue IN (job_attempts.queue, '*')
        )
    );

-- Serves the policy predicate above, which runs once per candidate row.
CREATE INDEX role_queue_grants_role_idx
    ON {schema}.role_queue_grants (role_name, queue);
