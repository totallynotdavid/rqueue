-- rqueue 0010: a concurrency slot is held by a role, not just by a queue.
--
-- 0005 put concurrency_slots inside the queue boundary and asked only "may
-- this role touch this queue?". For a job row that is the whole question. For
-- a slot it is not: a slot *is* the mutual exclusion, and every worker on a
-- queue shares it, so a policy that stops at the queue lets any worker role on
-- that queue UPDATE or DELETE a live slot another one is holding. Delete it
-- and the key is immediately acquirable by both, which is the one thing this
-- table exists to prevent.
--
-- This is 0008's finding on runtime_heartbeats, one table over.
--
-- The remedy is the same and the key change is not. A heartbeat is a claim
-- *about* its writer, so 0008 put the owner in the key and let two roles hold
-- separate rows. A slot is a claim *against everyone*, and two rows for one
-- (queue, key) would be the failure -- so the key stays exactly as it was and
-- the owner is recorded beside it.

-- --------------------------------------------------------------------- drain

-- Checked here rather than inherited from 0005, for the reason 0008 states: a
-- migration body runs exactly once, so a database that applied 0005 long ago
-- and is only now picking up this release arrives at this file with nothing
-- checked. Two conditions, because this migration has two ways to hurt a
-- running fleet.
--
-- A slot still leased is the load-bearing one, and it is where the parallel
-- with 0008 stops. A heartbeat is a liveness signal: an orphaned row is
-- harmless and its replacement arrives on the next tick. A slot *is* the
-- mutual exclusion for an attempt that is running right now. The column below
-- attributes every pre-existing row to the migration role, and its real holder
-- can then neither extend nor release a row it no longer owns -- so the lease
-- runs out under a still-running attempt, and at that instant the expiry
-- branch of the new policy hands the key to any worker on the queue. A second
-- worker starts the same logical job while the first is still inside it, which
-- is the one thing this table exists to prevent. No backfill avoids it: the
-- table has never recorded who holds a slot, which is the whole defect.
--
-- An empty runtime_heartbeats is the second, and it is what makes "stopped"
-- checkable at all. Slots cannot show it -- a worker between jobs holds none
-- and is invisible here -- and it has to be shown, because the WITH CHECK
-- below also breaks the previous release's takeover path: that upsert never
-- sets `role_name`, so taking over a slot another role left expired fails with
-- "new row violates row-level security policy". Within one `migrate` this
-- costs nothing, 0008 having just demanded the same; it is the staged upgrade
-- -- `--target 9`, deploy, `--target 10` -- that it catches.
LOCK TABLE {schema}.concurrency_slots IN ACCESS EXCLUSIVE MODE;
LOCK TABLE {schema}.runtime_heartbeats IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    held    integer;
    beating integer;
BEGIN
    SELECT count(*) INTO held
    FROM {schema}.concurrency_slots
    WHERE leased_until > now();

    IF held > 0 THEN
        RAISE EXCEPTION
            'rqueue: % concurrency slot(s) still leased', held
            USING ERRCODE = 'object_in_use',
                  DETAIL = 'Migration 0010 records who holds each slot, and '
                           'has no way to recover that for a row written '
                           'before it. Every existing row is attributed to the '
                           'migrating role, which locks the real holder out of '
                           'extending or releasing it: the lease expires under '
                           'a running attempt and the key is then acquirable '
                           'by another worker, running the same logical job '
                           'twice.',
                  HINT = 'Stop every rqueue worker, confirm the processes have '
                         'exited, and let the outstanding leases expire -- '
                         'bounded by the lease_seconds the workers ran with. '
                         'A slot outliving its worker is already stale, so '
                         'DELETE FROM {schema}.concurrency_slots is equally '
                         'good once nothing is running. Re-run the migration, '
                         'then deploy.';
    END IF;

    SELECT count(*) INTO beating FROM {schema}.runtime_heartbeats;

    IF beating > 0 THEN
        RAISE EXCEPTION
            'rqueue: % runtime heartbeat row(s) present', beating
            USING ERRCODE = 'object_in_use',
                  DETAIL = 'Migration 0010 requires the fleet stopped, and a '
                           'heartbeat row is the evidence that it is not: a '
                           'worker between jobs holds no slot, so the slot '
                           'check above cannot see it. A worker from the '
                           'previous release fails with "new row violates row-'
                           'level security policy" the first time it takes '
                           'over a slot another role left expired, because its '
                           'upsert does not set the owner column this '
                           'migration adds.',
                  HINT = 'Stop every rqueue worker and scheduler and confirm '
                         'the processes have exited. Then DELETE FROM '
                         '{schema}.runtime_heartbeats, wait one poll interval, '
                         'and check it is still empty. Re-run the migration, '
                         'then deploy -- this release, not the previous one.';
    END IF;
END
$$;

ALTER TABLE {schema}.concurrency_slots
    ADD COLUMN role_name text NOT NULL DEFAULT current_user;

-- The drain above is what makes this column safe to add: the rows it can reach
-- are expired ones, whose holder is gone and whose key the expiry branches
-- below hand to whoever wants it next -- the path a crashed worker has always
-- taken.

DROP POLICY concurrency_slots_queue_scope ON {schema}.concurrency_slots;

-- Reading stays queue-scoped. A worker has to see a slot it does not hold --
-- that is how it learns the key is taken -- and `INSPECT` reports on all of
-- them.
CREATE POLICY concurrency_slots_read ON {schema}.concurrency_slots
    FOR SELECT
    USING (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (concurrency_slots.queue, '*')
        )
    );

CREATE POLICY concurrency_slots_insert ON {schema}.concurrency_slots
    FOR INSERT
    WITH CHECK (
        concurrency_slots.role_name = current_user
        AND EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (concurrency_slots.queue, '*')
        )
    );

-- Yours, or expired.
--
-- The expiry branch is not a loophole in the ownership rule, it is the rule the
-- table already had: acquisition is an upsert that takes over any slot whose
-- lease has run out, which is how a crashed holder's key becomes usable again
-- with no reclaim job. That takeover crosses roles whenever two worker
-- deployments share a queue, so ownership alone here would let one crashed
-- worker wedge a key until an operator intervened.
--
-- What it cannot do is touch a *live* slot, which is the whole of the defect.
-- And WITH CHECK makes the taker the new owner rather than leaving the row
-- attributed to the holder it displaced.
CREATE POLICY concurrency_slots_update ON {schema}.concurrency_slots
    FOR UPDATE
    USING (
        (
            concurrency_slots.role_name = current_user
            OR concurrency_slots.leased_until <= now()
        )
        AND EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (concurrency_slots.queue, '*')
        )
    )
    WITH CHECK (
        concurrency_slots.role_name = current_user
        AND EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (concurrency_slots.queue, '*')
        )
    );

-- Same two branches, for the same two callers: a holder releasing its own slot
-- when the attempt finishes, and lease recovery clearing an expired one --
-- which any worker may run, for any holder, because the holder it is cleaning
-- up after is by definition not around to do it.
CREATE POLICY concurrency_slots_delete ON {schema}.concurrency_slots
    FOR DELETE
    USING (
        (
            concurrency_slots.role_name = current_user
            OR concurrency_slots.leased_until <= now()
        )
        AND EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (concurrency_slots.queue, '*')
        )
    );
