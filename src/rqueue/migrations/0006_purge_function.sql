-- rqueue 0006: a bounded, queue-scoped purge that needs no DELETE grant.
--
-- Retention has to delete rows, and until now the only way to let a
-- maintenance role do that was `GRANT DELETE ON jobs` -- a privilege that says
-- "delete any row you can see" and relies on the caller's own SQL to stay
-- inside queue, terminal state, age, and batch size. That makes the caller the
-- safety boundary, which is exactly backwards: a purger with a bug, or a
-- purger whose statement is rewritten by whoever controls its arguments,
-- deletes live work.
--
-- So the boundary moves into the database. This function is the only thing the
-- PURGE capability is granted, it runs SECURITY DEFINER as the schema owner,
-- and it re-derives every limit from its own arguments before deleting
-- anything. A crafted call cannot widen it: a non-terminal state, a wildcard
-- queue, a future cutoff, or an unbounded batch is rejected outright, and the
-- DELETE itself restates queue, state, and cutoff so a row that changes
-- underneath the candidate scan is still not deleted.
--
-- SECURITY DEFINER means the row-level-security policies from 0002 do *not*
-- apply inside the body -- the owner is exempt from its own unforced policies
-- -- so the queue grant is checked here explicitly, against `session_user`.
-- `current_user` is the definer once the body is entered and would authorize
-- everything; `session_user` is the role that actually logged in. A caller
-- that reached this function through SET ROLE is therefore judged on the login
-- role's grants, which fails closed rather than open. The definer itself is
-- allowed through: it holds DELETE on the table regardless.
--
-- `search_path` is pinned and every reference is schema-qualified, so a caller
-- cannot shadow `now()` or a table name with something of their own.

CREATE FUNCTION {schema}.purge_terminal_jobs(
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

    IF session_user <> current_user AND NOT EXISTS (
        SELECT 1 FROM {schema}.role_queue_grants AS g
        WHERE g.role_name = session_user AND g.queue IN (target_queue, '*')
    ) THEN
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

-- PostgreSQL grants EXECUTE on a new function to PUBLIC. On a SECURITY
-- DEFINER function that is the whole vulnerability, so it goes first; the
-- PURGE capability re-grants it to one role at a time (rqueue.roles).
REVOKE ALL ON FUNCTION
    {schema}.purge_terminal_jobs(text, text[], timestamptz, integer)
    FROM PUBLIC;
