-- rqueue 0011: give an existing scheduler role the grant this release's
-- enqueue needs.
--
-- The grant table in `rqueue.roles` is part of the *release*, not part of the
-- schema, and nothing has ever reconciled the two. `provision_role` writes the
-- current set every time it runs, so a role is correct on the day it is
-- provisioned and drifts from then on: the schema moves forward with
-- `migrate`, the grants only move when an operator remembers to provision
-- again. Read the README section this migration is named in before adding to
-- the grant table.
--
-- This release moves SCHEDULE's grant on `jobs` from `SELECT, INSERT` to
-- `SELECT, INSERT, UPDATE (updated_at)`, because every enqueue is an
-- `INSERT ... ON CONFLICT ... DO UPDATE SET updated_at`, and PostgreSQL
-- demands UPDATE on every column a `DO UPDATE SET` names. Without it the
-- scheduler does not degrade -- it cannot enqueue at all:
--
--     asyncpg.exceptions.InsufficientPrivilegeError: permission denied for
--     table jobs
--
-- and `Scheduler.run` catches that with every other PostgreSQL error, logging
-- "could not reach PostgreSQL; retrying" once a tick, forever. A scheduler
-- that fires nothing while reporting a connectivity problem is a bad enough
-- failure to repair here rather than document.
--
-- Additive repair only, and the asymmetry is the reason. The fingerprint below
-- is the one 0008 uses and is exact in one direction: only SCHEDULE grants
-- INSERT on `schedules`, so every role it matches already holds INSERT on
-- `jobs` and this adds nothing to what an operator already trusted it with --
-- a column-scoped UPDATE of a timestamp on rows it may already create. Going
-- the other way is not safe the same way. This release also takes INSERT on
-- `jobs` away from CONSUME, and no fingerprint can tell a worker role that
-- holds it from a role provisioned with both CONSUME and PRODUCE, which is a
-- supported combination and needs it. Revoking on a guess breaks a working
-- producer; that one stays with `provision_role`, which knows the capability
-- set because the caller passes it.
DO $$
DECLARE
    scheduler text;
BEGIN
    FOR scheduler IN
        SELECT DISTINCT g.role_name
        FROM {schema}.role_queue_grants AS g
        JOIN pg_roles AS r ON r.rolname = g.role_name
        WHERE has_table_privilege(
                  r.oid, '{schema}.schedules'::regclass, 'INSERT'
              )
          AND NOT has_column_privilege(
                  r.oid, '{schema}.jobs'::regclass, 'updated_at', 'UPDATE'
              )
        ORDER BY 1
    LOOP
        EXECUTE format(
            'GRANT UPDATE (updated_at) ON {schema}.jobs TO %I', scheduler
        );
    END LOOP;
END
$$;
