-- rqueue 0007: make retention cost what it deletes, and let a superuser run it.
--
-- Two corrections to 0006, which cannot be edited: a migration is checksummed
-- and forward-only, so the routine is replaced rather than amended.
--
-- The index first. `purge_terminal_jobs` selects candidates by queue *and*
-- age, and the only index offering both was `jobs_queue_state_idx (queue,
-- state, seq)` -- which finds the queue's terminal rows and then discards the
-- ones outside the cutoff. That is invisible on one queue and quadratic-ish
-- across many: a whole-schema purge visits every queue holding rows below its
-- horizon, so 200 queues of 200 rows read 40,000 rows to delete 10,000, while
-- the API promises work proportional to the limit. Ordering the index by
-- (queue, finished_at) turns each of those scans into a range read of exactly
-- the rows it will delete.
--
-- Not CONCURRENTLY: 0005 already requires the fleet stopped, and these two
-- migrations are applied in that same window. A plain CREATE INDEX keeps this
-- file transactional -- an interrupted CONCURRENTLY leaves an INVALID index
-- behind and no ledger row to explain it.

CREATE INDEX jobs_retention_queue_idx
    ON {schema}.jobs (queue, finished_at)
    WHERE state IN ('succeeded', 'failed', 'cancelled');

-- Then the routine. Only the authorization branch changes; everything else is
-- 0006 verbatim, because a SECURITY DEFINER body is not a place to make
-- unrelated edits while passing by.

CREATE OR REPLACE FUNCTION {schema}.purge_terminal_jobs(
    target_queue    text,
    terminal_states text[],
    finished_before timestamptz,
    max_rows        integer
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    removed integer;
BEGIN
    -- One queue, by name. '*' is a grant wildcard, never a delete target.
    IF target_queue IS NULL
       OR target_queue = '*'
       OR length(target_queue) NOT BETWEEN 1 AND 64
    THEN
        RAISE EXCEPTION 'purge needs exactly one queue name, got %',
            coalesce(quote_literal(target_queue), 'NULL')
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF terminal_states IS NULL OR cardinality(terminal_states) = 0 THEN
        RAISE EXCEPTION 'purge needs at least one terminal state'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- The list is checked rather than intersected: silently dropping
    -- 'pending' from the request would delete *something*, and a caller that
    -- asked for the wrong thing should learn that it did.
    IF EXISTS (
        SELECT 1 FROM unnest(terminal_states) AS requested(state)
        WHERE requested.state IS NULL
           OR requested.state NOT IN ('succeeded', 'failed', 'cancelled')
    ) THEN
        RAISE EXCEPTION 'purge may only delete terminal jobs, got %',
            terminal_states::text
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF finished_before IS NULL OR finished_before > now() THEN
        RAISE EXCEPTION 'purge needs a cutoff in the past, got %',
            coalesce(finished_before::text, 'NULL')
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF max_rows IS NULL OR max_rows NOT BETWEEN 1 AND 100000 THEN
        RAISE EXCEPTION 'purge needs a row limit between 1 and 100000, got %',
            coalesce(max_rows::text, 'NULL')
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- A superuser is added to the two identities that were already let
    -- through. It is not a weakening: a superuser bypasses row-level security
    -- and every table grant in this schema, so it can already delete any row
    -- this routine could, by hand. Refusing it here only made retention the
    -- one Admin operation a DBA could not run -- and the refusal arrived as a
    -- raw driver error, which the CLI could not present.
    IF session_user <> current_user
       AND NOT (
           SELECT r.rolsuper FROM pg_roles AS r WHERE r.rolname = session_user
       )
       AND NOT EXISTS (
           SELECT 1 FROM {schema}.role_queue_grants AS g
           WHERE g.role_name = session_user AND g.queue IN (target_queue, '*')
       )
    THEN
        RAISE EXCEPTION 'role % is not granted queue %', session_user, target_queue
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    WITH doomed AS (
        SELECT j.id
        FROM {schema}.jobs AS j
        WHERE j.queue = target_queue
          AND j.state = ANY (terminal_states)
          AND j.finished_at IS NOT NULL
          AND j.finished_at < finished_before
        ORDER BY j.finished_at
        LIMIT max_rows
        FOR UPDATE SKIP LOCKED
    ),
    -- Every predicate restated: `doomed` was chosen from one snapshot, and an
    -- operator retry can move a row out of a terminal state between the two.
    -- The row lock above makes that narrow; this makes it impossible.
    deleted AS (
        DELETE FROM {schema}.jobs AS j
        USING doomed AS d
        WHERE j.id = d.id
          AND j.queue = target_queue
          AND j.state = ANY (terminal_states)
          AND j.finished_at IS NOT NULL
          AND j.finished_at < finished_before
        RETURNING j.id
    )
    SELECT count(*)::integer INTO removed FROM deleted;

    RETURN removed;
END;
$$;
