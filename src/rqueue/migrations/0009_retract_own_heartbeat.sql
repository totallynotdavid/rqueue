-- rqueue 0009: a role can always see and retract what it wrote.
--
-- 0008 scoped heartbeat writes by owner so no component could touch another's
-- liveness row, and left the DELETE policy's queue check in place beside the
-- ownership one. Its own comment had already worked out why a *kind* check
-- there would be wrong -- "a role whose capability was narrowed must still be
-- able to take its old claims down" -- and the queue check is the same
-- sentence with a different noun.
--
-- Narrowing a scheduler's queue grants is the ordinary way in. Drop `beta`
-- from its grants, and its own `beta` heartbeat stops satisfying the policy:
-- the row cannot be deleted, by anyone but the schema owner, ever. Nothing
-- reports this. Row-level security makes a row it excludes invisible rather
-- than forbidden, so the retraction deletes nothing and says it deleted
-- nothing, and readiness goes on naming that scheduler as feeding a queue that
-- was taken away from it.
--
-- Fixing DELETE alone would not be enough, which is the part worth spelling
-- out: `Storage.stale_heartbeat_queues` looks the rows up before asking to
-- delete them, and the read policy is queue-scoped too, so after the narrowing
-- the orphan is invisible to the very query that would have found it. The
-- retraction would stay correct and never fire. So the read widens by the same
-- rule -- your own rows, always -- and the delete narrows to ownership alone.
--
-- Corrective rather than an edit to 0008, so a database that has already
-- applied it is not told its schema was tampered with.

DROP POLICY runtime_heartbeats_read ON {schema}.runtime_heartbeats;

-- Own rows *or* granted queues. Readiness still asks "is any scheduler alive
-- on this queue?" and must see components it did not write, which is the
-- second half; the first half is what lets a role find the claims it has left
-- behind on a queue that is no longer its.
CREATE POLICY runtime_heartbeats_read ON {schema}.runtime_heartbeats
    FOR SELECT
    USING (
        runtime_heartbeats.role_name = current_user
        OR EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (runtime_heartbeats.queue, '*')
        )
    );

DROP POLICY runtime_heartbeats_delete ON {schema}.runtime_heartbeats;

-- Ownership alone. Retracting a claim asserts nothing and grants nothing: it
-- is the writer saying it is no longer serving that queue, which is true
-- whatever it is currently entitled to serve. Every check here is a way for a
-- claim to outlive the thing it claims.
CREATE POLICY runtime_heartbeats_delete ON {schema}.runtime_heartbeats
    FOR DELETE
    USING (runtime_heartbeats.role_name = current_user);
