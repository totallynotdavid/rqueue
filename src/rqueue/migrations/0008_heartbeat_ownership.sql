-- rqueue 0008: a heartbeat belongs to the role that wrote it.
--
-- 0005 put runtime_heartbeats inside the queue boundary and stopped there. The
-- boundary it drew answers "may this role touch this queue?", which is the
-- right question for a job and the wrong one for a liveness claim: on a queue
-- two components share, it lets either of them write the other's row. A worker
-- role can refresh a scheduler's heartbeat -- readiness then reports a
-- scheduler feeding a queue that has none -- or age it out, and a scheduler
-- role can DELETE every worker row on a queue it is granted. Both are inside
-- 0005's policy, because the policy never looks at whose row it is.
--
-- `kind` and `instance` cannot answer that. They are what the claim is *about*
-- -- a pod name, a container ordinal -- and the claimant supplies them, so a
-- policy written over them is a policy over forged input. The one identity a
-- writer cannot choose is the role it authenticated as, so that is what the
-- row records and what the policy compares.
--
-- Corrective rather than an edit to 0005: a migration is checksummed, and a
-- database that has already applied 0005 must not be told its schema was
-- tampered with.

-- --------------------------------------------------------------------- drain

-- 0005 makes the same demand for the same reason, and this migration cannot
-- inherit it. A migration body runs exactly once: a database that applied 0005
-- months ago and is only now picking up this release never re-executes 0005's
-- check, so on an upgrade in place the only thing between a running fleet and
-- the failure below is the check written here.
--
-- The reasoning is 0005's and is not repeated: an empty table is checkable
-- where an age threshold is not; the lock leaves a process that ticks
-- concurrently either committed first and caught, or blocked and waking to the
-- new key; and clearing the table without waiting a poll interval proves less
-- than it looks, because a process that has not yet ticked has written nothing
-- to see.
LOCK TABLE {schema}.runtime_heartbeats IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    beating integer;
BEGIN
    SELECT count(*) INTO beating FROM {schema}.runtime_heartbeats;

    IF beating > 0 THEN
        RAISE EXCEPTION
            'rqueue: % runtime heartbeat row(s) present', beating
            USING ERRCODE = 'object_in_use',
                  DETAIL = 'Migration 0008 changes the runtime_heartbeats key '
                           'to (kind, instance, queue, role_name). A worker or '
                           'scheduler from the previous release writes ON '
                           'CONFLICT (kind, instance, queue) and fails with '
                           '"there is no unique or exclusion constraint '
                           'matching the ON CONFLICT specification" on its '
                           'very next tick -- retrying forever, claiming and '
                           'scheduling nothing. A heartbeat row means such a '
                           'process has ticked: rqueue never deletes these '
                           'rows, and a running process rewrites its own '
                           'within one poll interval.',
                  HINT = 'Stop every rqueue worker and scheduler and confirm '
                         'the processes have exited. Then DELETE FROM '
                         '{schema}.runtime_heartbeats, wait one poll interval, '
                         'and check it is still empty. Re-run the migration, '
                         'then deploy -- this release, not the previous one.';
    END IF;
END
$$;

-- ------------------------------------------------------------------ ownership

-- DEFAULT current_user, never a caller-supplied value: the column exists
-- precisely because the caller must not get to choose it. The drain above
-- leaves nothing for it to attribute wrongly -- the table is empty by the time
-- this runs -- but were a row to predate this migration it would be attributed
-- to the migration role and be unreachable by the component that wrote it,
-- which is self-healing: that component's next tick inserts its own row under
-- its own name and the orphan ages out of the staleness window.
ALTER TABLE {schema}.runtime_heartbeats
    ADD COLUMN role_name text NOT NULL DEFAULT current_user;

-- In the key, not merely beside it. Two roles claiming one (kind, instance,
-- queue) is a misconfiguration, but resolving it by making one of them
-- unwritable would stall that component forever: its upsert would conflict
-- with a row its own policy forbids it to update, which is an error on every
-- tick and no way out. Keyed by role, each claim is its own row, the wrong one
-- goes stale on its own, and a readiness probe -- which counts DISTINCT
-- instances -- is unaffected.
ALTER TABLE {schema}.runtime_heartbeats DROP CONSTRAINT runtime_heartbeats_pkey;
ALTER TABLE {schema}.runtime_heartbeats
    ADD CONSTRAINT runtime_heartbeats_pkey
    PRIMARY KEY (kind, instance, queue, role_name);

-- ------------------------------------------------------------ claimable kinds

-- `role_name` settles whose row it is; it does not settle what the row may
-- claim to be. A worker role that inserts (kind 'scheduler', instance 'ghost')
-- owns that row legitimately, and `check_readiness` then reports a scheduler
-- feeding a queue that has none -- the same lie as refreshing a real
-- scheduler's row, reached by a different door.
--
-- `kind` is not derivable from the role: one deployment provisioned with both
-- CONSUME and SCHEDULE writes both kinds, and that is correct. So it is
-- recorded, the way queue access already is, and the policy reads it the same
-- way. Outside RLS for the same reason `role_queue_grants` is: it is what the
-- policies read, and under a policy it would filter itself.
CREATE TABLE {schema}.role_runtime_kinds (
    role_name text NOT NULL,
    kind      text NOT NULL,

    PRIMARY KEY (role_name, kind),
    CONSTRAINT role_runtime_kinds_kind_valid CHECK (kind IN ('worker', 'scheduler'))
);

COMMENT ON TABLE {schema}.role_runtime_kinds IS
    'Which runtime component kinds a role may claim liveness as. Written by '
    'rqueue.roles.provision_role; read by role_claims_runtime_kind.';

-- Read through a SECURITY DEFINER function, never directly, because a policy
-- expression is evaluated as the querying role: a policy that selects from
-- this table needs every runtime role to hold SELECT on it, and the roles that
-- have to keep working across this migration were provisioned before the table
-- existed. Their first heartbeat afterwards would fail with "permission denied
-- for table role_runtime_kinds", and unlike a retraction that can be skipped,
-- writing your own heartbeat is the liveness mechanism itself -- there is
-- nothing to degrade to.
--
-- Handing the grant out here would be possible; 0011 does exactly that for a
-- grant of its own. It would also be worse. A migration reaches only the roles
-- that exist when it runs and are already recorded in `role_queue_grants`,
-- which makes every later provisioning depend on a grant this file happened to
-- make, and it would expose the whole table to do it. The function needs no
-- grant from anyone, ever.
--
-- EXECUTE therefore stays with PUBLIC, which is what makes it reachable by a
-- role nothing has re-granted. That is deliberate and it is the whole point,
-- so it is worth being exact about what it exposes: one boolean, about a role
-- the *caller names*, over a table of role names and the strings 'worker' and
-- 'scheduler'. It is the shape of the deployment, which `role_queue_grants`
-- already gives any provisioned role, and it authorizes nothing by itself --
-- the policy decides what to do with the answer, and passes `current_user`,
-- which a caller cannot influence.
CREATE FUNCTION {schema}.role_claims_runtime_kind(claimant text, claimed text)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT EXISTS (
        SELECT 1 FROM {schema}.role_runtime_kinds AS k
        WHERE k.role_name = claimant AND k.kind = claimed
    );
$$;

-- Backfilled from the grants each role already holds, so a role provisioned
-- before this migration keeps working without being re-provisioned. Tightening
-- the policy without this would stall every existing worker and scheduler on
-- its next tick, which is the failure this migration is careful not to cause.
--
-- The two fingerprints are exact, not heuristic: only SCHEDULE grants INSERT on
-- `schedules`, and only CONSUME grants UPDATE on `jobs.state` -- SCHEDULE's
-- UPDATE there is column-scoped to `updated_at`. Addressed by oid so a grant
-- row naming a role that no longer exists is skipped rather than raising.
INSERT INTO {schema}.role_runtime_kinds (role_name, kind)
SELECT DISTINCT g.role_name, 'worker'
FROM {schema}.role_queue_grants AS g
JOIN pg_roles AS r ON r.rolname = g.role_name
WHERE has_column_privilege(
    r.oid, '{schema}.jobs'::regclass, 'state', 'UPDATE'
)
ON CONFLICT DO NOTHING;

INSERT INTO {schema}.role_runtime_kinds (role_name, kind)
SELECT DISTINCT g.role_name, 'scheduler'
FROM {schema}.role_queue_grants AS g
JOIN pg_roles AS r ON r.rolname = g.role_name
WHERE has_table_privilege(
    r.oid, '{schema}.schedules'::regclass, 'INSERT'
)
ON CONFLICT DO NOTHING;

-- -------------------------------------------------------------------- policy

DROP POLICY runtime_heartbeats_queue_scope ON {schema}.runtime_heartbeats;

-- Read broadly, write narrowly, as schedule_occurrences already does.
--
-- Reading is the whole point of the table: `check_readiness` asks whether any
-- worker and any scheduler are alive on a queue, and an answer restricted to
-- the asking role's own rows would report every deployment as unmonitored. So
-- SELECT stays scoped by queue alone.
CREATE POLICY runtime_heartbeats_read ON {schema}.runtime_heartbeats
    FOR SELECT
    USING (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (runtime_heartbeats.queue, '*')
        )
    );

-- Writing is a claim about the writer, so it carries the writer's name. The
-- WITH CHECK is not redundant with the DEFAULT: the default only applies to a
-- statement that omits the column, and a hand-written INSERT can name it.
CREATE POLICY runtime_heartbeats_insert ON {schema}.runtime_heartbeats
    FOR INSERT
    WITH CHECK (
        {schema}.role_claims_runtime_kind(
            current_user, runtime_heartbeats.kind
        )
        AND runtime_heartbeats.role_name = current_user
        AND EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (runtime_heartbeats.queue, '*')
        )
    );

-- Both clauses: USING decides which rows may be refreshed, WITH CHECK stops
-- the refresh from reassigning the row to someone else on the way out.
CREATE POLICY runtime_heartbeats_update ON {schema}.runtime_heartbeats
    FOR UPDATE
    USING (
        runtime_heartbeats.role_name = current_user
        AND EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (runtime_heartbeats.queue, '*')
        )
    )
    WITH CHECK (
        {schema}.role_claims_runtime_kind(
            current_user, runtime_heartbeats.kind
        )
        AND runtime_heartbeats.role_name = current_user
        AND EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (runtime_heartbeats.queue, '*')
        )
    );

-- Retracting a claim is making one, in reverse. Scoped by owner alone: a role
-- whose capability was narrowed must still be able to take its old claims
-- down, which a kind check here would forbid.
CREATE POLICY runtime_heartbeats_delete ON {schema}.runtime_heartbeats
    FOR DELETE
    USING (
        runtime_heartbeats.role_name = current_user
        AND EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (runtime_heartbeats.queue, '*')
        )
    );
