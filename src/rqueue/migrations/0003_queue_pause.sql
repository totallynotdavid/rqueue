-- rqueue 0003: durable, queue-wide pause.
--
-- Pausing stops a queue admitting *new* work across every worker replica,
-- without stopping any process and without touching work already leased.
-- `Worker.stop()` and `Worker.drain()` only ever affect one worker instance;
-- this is the fleet-wide switch, and like every other state transition in this
-- package it is a durable row, not process memory (§3).
--
-- Oban keeps this flag only in each producer process and rebroadcasts it over
-- PubSub, so a restarted queue comes back unpaused. River stores it in a real
-- table and polls it, which is the model followed here -- with one difference:
-- the pause is enforced inside the claim query itself (see
-- `Storage._Statements.claim_candidates`), so a worker that has not yet heard
-- about the pause still claims nothing. The NOTIFY below is only a latency
-- optimization for an *idle* worker, exactly as it is for job wake-ups.

CREATE TABLE {schema}.queue_pauses (
    -- One row per queue that has ever been paused; '*' is the wildcard row
    -- meaning every queue, the same convention role_queue_grants uses.
    queue      text        PRIMARY KEY,
    -- NULL means running. A timestamp means paused, and records when -- an
    -- admin view gets "paused since" for free, which a boolean would lose.
    paused_at  timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT queue_pauses_queue_length CHECK (length(queue) BETWEEN 1 AND 64)
);

-- The claim-time check is the only hot read of this table:
--   SELECT 1 FROM queue_pauses
--   WHERE queue IN ($1, '*') AND paused_at IS NOT NULL
-- It names no column of `jobs`, so PostgreSQL hoists it into an InitPlan and
-- evaluates it once per claim, not once per candidate row. The table holds one
-- row per queue ever paused, so that read is a single page today and the
-- primary key -- which is here for uniqueness first -- takes over if a
-- deployment ever accumulates enough of them to make the difference.

-- Deliberately *not* under row-level security, unlike jobs and job_attempts.
-- Those policies scope a role to its granted queues; applying the same rule
-- here would hide the '*' row from every queue-scoped worker -- which is to
-- say, it would let a queue-scoped worker ignore a global pause. Enforcement
-- has to outrank visibility scoping, and the table holds no application data:
-- a worker role gets SELECT only, so it can read a pause but never set one.

-- Wake trigger, matching jobs_notify_insert/jobs_notify_update in 0001: fire
-- only on the transition *into* an immediately-runnable state. Pausing makes
-- nothing runnable, and an INSERT only ever creates a paused row (resume is a
-- plain UPDATE), so resume is the only event worth a wake-up.
CREATE FUNCTION {schema}.notify_queue_resumed() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(TG_ARGV[0], NEW.queue);
    RETURN NULL;
END;
$$;

CREATE TRIGGER queue_pauses_notify_resume
    AFTER UPDATE ON {schema}.queue_pauses
    FOR EACH ROW
    WHEN (NEW.paused_at IS NULL AND OLD.paused_at IS NOT NULL)
    EXECUTE FUNCTION {schema}.notify_queue_resumed('rqueue_{schema}');
