-- rqueue 0005: put every queue-bearing table inside the queue boundary.
--
-- 0002 gave `jobs` and `job_attempts` row-level security driven by
-- `role_queue_grants`, and stopped there. `concurrency_slots`,
-- `runtime_heartbeats`, `schedules`, and `schedule_occurrences` were left
-- global, so a role scoped to one queue could read every other queue's slot
-- leases, worker liveness, and schedule definitions, could delete a slot held
-- by a queue it was never granted, could disable another queue's schedule, and
-- could poison another queue's occurrence key so that schedule never fired
-- again. In a deployment with one role per queue -- the arrangement §8's
-- least-privilege model exists for -- that is a hole in the boundary, not a
-- detail.
--
-- Two of these tables could not be scoped as they stood, for the same reason:
-- the queue was reachable only through a foreign key, which is to say through
-- a row the reader's own policy may hide. A policy cannot be built on a join
-- whose right-hand side is itself filtered by the thing being decided. Both
-- therefore store the queue on the row.
--
-- Where a table's own key was global, it is narrowed to include the queue. A
-- global key and a queue-scoped policy cannot both hold: the row that decides
-- an upsert would be one the writer's policy forbids it to see, and the
-- conflict resolves into an error instead of an update. Scoping the policy
-- without scoping the key would trade a visibility hole for a runtime failure.
--
-- A policy can only test the queue the writer *wrote*. On a row that also
-- points at a job, that is a label, not a boundary: a role granted queue A
-- could attach an A-labelled row to a queue-B job and pass its own policy. So
-- every such label is tied to its job by composite foreign key -- attempt
-- records, concurrency slots, and schedule occurrences alike -- which
-- PostgreSQL enforces underneath every policy, role, and statement. The unique
-- constraint on `jobs (id, queue)` below exists to be their target.
--
-- The job is the right anchor because its queue never changes. A schedule's
-- can: it is a mutable field, and a constraint pinned to it would either
-- forbid the move or drag an occurrence's queue away from the job it records.
-- Where a schedule needs the same protection -- the occurrence key is what
-- makes a firing exactly-once (§6), and a forged row would deny it -- the
-- check goes into the policy, which can ask whether the writer can see the
-- schedule at all without pinning anything to a value that moves.

-- ------------------------------------------------------- upgrade pre-flight

-- This is the first rqueue migration that the *previous* release's runtime
-- code cannot survive. It changes the heartbeat key (see the section below),
-- and the previous release writes `ON CONFLICT (kind, instance)`, which stops
-- matching any index the moment this runs: "there is no unique or exclusion
-- constraint matching the ON CONFLICT specification". That statement is the
-- first one in every worker tick, and the tick's error handler retries it once
-- a second forever -- so an old worker does not crash, it stalls, claiming
-- nothing. A stall is harder to spot than a crash, so this refuses to be the
-- cause of one.
--
-- Safety is established by a barrier and an assertion, not by a guess about
-- how recently something beat. The lock comes first: ACCESS EXCLUSIVE on the
-- heartbeat table means no other session can read or write it between this
-- check and this migration's commit. A worker mid-tick has either already
-- committed its row -- and is therefore visible below -- or is blocked here
-- and will find the new key when it wakes. The ALTER TABLEs further down take
-- this lock anyway, so taking it early costs nothing; it only moves the
-- decision inside it.
--
-- The assertion is that the table is *empty*. An age threshold cannot be
-- right: a worker with a long poll interval is alive with an old heartbeat,
-- and any window generous enough for it is too generous to mean anything. An
-- empty table is checkable instead of merely asserted, because a running
-- process refills its own row within a poll interval.
--
-- Be exact about what that proves: not "nothing is running", but "nothing has
-- ticked since the table was cleared". A process that has started and not yet
-- reached its first tick has written nothing, so it is invisible here -- which
-- is why clearing the table and migrating in the same breath proves less than
-- it looks. Clear it, wait a poll interval, and confirm it is still empty:
-- that turns the pre-tick process into a visible one, because ticking is the
-- only thing it can do next.
--
-- Migrating is therefore: stop the fleet, clear the table, wait, re-check,
-- run this, deploy. That ordering is the opposite of every other rqueue
-- migration, which is why the parts a database can check are checked rather
-- than documented and hoped for. What remains is a process started after the
-- check: the lock below means it either committed a heartbeat first -- and is
-- caught -- or blocks and finds the new key on waking. Such a process fails
-- from its first tick, having never claimed a job, rather than degrading a
-- fleet that was working. That window is the drain, and it belongs to the
-- deployment.
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
                  DETAIL = 'Migration 0005 changes the runtime_heartbeats key '
                           'to (kind, instance, queue). A worker or scheduler '
                           'from the previous release writes ON CONFLICT '
                           '(kind, instance) and will stall -- claiming '
                           'nothing -- as soon as this is applied. A heartbeat '
                           'row means such a process has ticked: rqueue never '
                           'deletes these rows, and a running process rewrites '
                           'its own within one poll interval. An empty table '
                           'shows that nothing has ticked since it was '
                           'cleared, which is why the wait below matters -- a '
                           'process that has started but not yet ticked has '
                           'written nothing to see.',
                  HINT = 'Stop every rqueue worker and scheduler and confirm '
                         'the processes have exited. Then DELETE FROM '
                         '{schema}.runtime_heartbeats, wait one poll interval, '
                         'and check it is still empty -- a process that had '
                         'not yet ticked refills it, which is the only way to '
                         'see one. Re-run the migration, then deploy.';
    END IF;
END
$$;

-- ------------------------------------------------------ queue-tied references

ALTER TABLE {schema}.jobs ADD CONSTRAINT jobs_id_queue_uq UNIQUE (id, queue);

-- ---------------------------------------------------------------- attempts

-- 0002 scoped `job_attempts` by policy but left its queue a bare label, and
-- the damage that allows is the same shape as the occurrence key: the unique
-- index on (job_id, attempt) is global, so a role granted queue A can insert
-- an A-labelled attempt row for a queue-B job and burn that attempt number.
-- The next real claim of that job fails to open its attempt record. The job's
-- own queue is authoritative, so rows that disagree are corrected -- there
-- should be none, since `open_attempts` copies the queue from the job -- and
-- then the reference is tied.
UPDATE {schema}.job_attempts AS a
SET queue = j.queue
FROM {schema}.jobs AS j
WHERE j.id = a.job_id AND a.queue IS DISTINCT FROM j.queue;

ALTER TABLE {schema}.job_attempts DROP CONSTRAINT job_attempts_job_id_fkey;
ALTER TABLE {schema}.job_attempts
    ADD CONSTRAINT job_attempts_job_fkey
    FOREIGN KEY (job_id, queue) REFERENCES {schema}.jobs (id, queue)
    ON DELETE CASCADE;

-- ------------------------------------------------------------ concurrency slots

ALTER TABLE {schema}.concurrency_slots ADD COLUMN queue text;

-- The job's queue is authoritative here, as it is for every row that points at
-- a job. Every slot row references one (ON DELETE CASCADE), so this backfill
-- is total; the NOT NULL below would fail loudly if it somehow were not.
UPDATE {schema}.concurrency_slots AS s
SET queue = j.queue
FROM {schema}.jobs AS j
WHERE j.id = s.job_id;

ALTER TABLE {schema}.concurrency_slots ALTER COLUMN queue SET NOT NULL;

ALTER TABLE {schema}.concurrency_slots
    ADD CONSTRAINT concurrency_slots_queue_length
    CHECK (length(queue) BETWEEN 1 AND 64);

-- The slot's queue is the job's queue, not whatever the acquirer wrote.
ALTER TABLE {schema}.concurrency_slots
    DROP CONSTRAINT concurrency_slots_job_id_fkey;
ALTER TABLE {schema}.concurrency_slots
    ADD CONSTRAINT concurrency_slots_job_fkey
    FOREIGN KEY (job_id, queue) REFERENCES {schema}.jobs (id, queue)
    ON DELETE CASCADE;

-- A named concurrency key is now scoped to its queue, exactly as `dedupe_key`
-- already was. Two queues that must genuinely exclude each other on one
-- resource belong on one queue; that is what a queue is.
ALTER TABLE {schema}.concurrency_slots DROP CONSTRAINT concurrency_slots_pkey;
ALTER TABLE {schema}.concurrency_slots
    ADD CONSTRAINT concurrency_slots_pkey PRIMARY KEY (queue, key);

-- Same shape as jobs_queue_scope in 0002: enabled, not forced, so the schema
-- owner still sees everything and every other role reaches only its granted
-- queues, with '*' meaning all of them.
ALTER TABLE {schema}.concurrency_slots ENABLE ROW LEVEL SECURITY;

CREATE POLICY concurrency_slots_queue_scope ON {schema}.concurrency_slots
    FOR ALL
    USING (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (concurrency_slots.queue, '*')
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (concurrency_slots.queue, '*')
        )
    );

-- ---------------------------------------------------------- runtime heartbeats

-- A heartbeat is one runtime component's liveness *on one queue*, so (kind,
-- instance) was never the whole identity. It only looked like it while the
-- table was global. A Worker serves exactly one queue; a Scheduler writes one
-- heartbeat per queue its tick actually serves, which can be several. Under a
-- queue-scoped policy that difference is not cosmetic: an instance id that has
-- ever beaten on another queue leaves a row the next queue's upsert can
-- neither see nor replace, and every tick fails on a conflict with an
-- invisible row. Instance ids are reused constantly -- a pod name, a hostname,
-- a container ordinal -- so this is the normal case, not a corner.
--
-- No row needs clearing before the NOT NULL below. The pre-flight above has
-- already established that this table is empty; a queue-less row could only
-- arrive from something other than rqueue, and it would have failed that
-- check first.
ALTER TABLE {schema}.runtime_heartbeats ALTER COLUMN queue SET NOT NULL;

ALTER TABLE {schema}.runtime_heartbeats
    ADD CONSTRAINT runtime_heartbeats_queue_length
    CHECK (length(queue) BETWEEN 1 AND 64);

ALTER TABLE {schema}.runtime_heartbeats DROP CONSTRAINT runtime_heartbeats_pkey;
ALTER TABLE {schema}.runtime_heartbeats
    ADD CONSTRAINT runtime_heartbeats_pkey PRIMARY KEY (kind, instance, queue);

ALTER TABLE {schema}.runtime_heartbeats ENABLE ROW LEVEL SECURITY;

CREATE POLICY runtime_heartbeats_queue_scope ON {schema}.runtime_heartbeats
    FOR ALL
    USING (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (runtime_heartbeats.queue, '*')
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (runtime_heartbeats.queue, '*')
        )
    );

-- ------------------------------------------------------------------- schedules

-- `schedules` already carries its queue; it simply had no policy. A scheduler
-- role scoped to one queue could read every schedule in the database and
-- disable any of them -- an off switch for another team's periodic work.
--
-- `schedules.name` stays globally unique. It is the schedule's identity
-- everywhere it is used (`Admin.get_schedule`, `set_schedule_enabled`,
-- `delete_schedule` are all name-keyed, and the scheduler tick reads the whole
-- enabled set), so narrowing it to (queue, name) would make those lookups
-- ambiguous rather than safer. The consequence is worth stating plainly: a
-- scoped role that upserts a schedule under a name another queue already holds
-- gets a policy error where the owner gets a unique violation. Both refuse the
-- write, which is the answer either way -- schedule names are one namespace.
ALTER TABLE {schema}.schedules ENABLE ROW LEVEL SECURITY;

CREATE POLICY schedules_queue_scope ON {schema}.schedules
    FOR ALL
    USING (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user AND g.queue IN (schedules.queue, '*')
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user AND g.queue IN (schedules.queue, '*')
        )
    );

-- -------------------------------------------------------- schedule occurrences

-- The occurrence row is what makes a firing exactly-once (§6), so an
-- unprotected one is worse than a leak: a scoped role could insert
-- (schedule_id, occurrence_at) rows for a schedule on a queue it cannot even
-- see, and every one of them is an occurrence that schedule will now never
-- fire. Foreign-key checks run as the table owner, so the FKs to `schedules`
-- and `jobs` do not stop it -- only a policy on this table does.
--
-- The queue comes from the *job*. That is the reference whose queue can never
-- change -- a job is enqueued onto one queue and stays there -- so it is the
-- only one a constraint can hold to. Deriving it from the schedule instead
-- puts a mutable value on the far side of the tie: a schedule that moves
-- between queues would have to drag its history across with it, and every
-- occurrence would then point at a job on the queue it just left.
ALTER TABLE {schema}.schedule_occurrences ADD COLUMN queue text;

UPDATE {schema}.schedule_occurrences AS o
SET queue = j.queue
FROM {schema}.jobs AS j
WHERE j.id = o.job_id;

ALTER TABLE {schema}.schedule_occurrences ALTER COLUMN queue SET NOT NULL;

ALTER TABLE {schema}.schedule_occurrences
    ADD CONSTRAINT schedule_occurrences_queue_length
    CHECK (length(queue) BETWEEN 1 AND 64);

-- Tied to the job, which is the reference that carries a real queue. Without
-- it, an occurrence may name a job on a queue its writer does not hold, and
-- the damage arrives later and from the other side: retention on *that* queue
-- purges the job, the cascade deletes this occurrence, and the schedule is
-- free to fire that instant a second time. A queue's own purge must not be
-- able to reopen another queue's occurrence key.
ALTER TABLE {schema}.schedule_occurrences
    DROP CONSTRAINT schedule_occurrences_job_id_fkey;
ALTER TABLE {schema}.schedule_occurrences
    ADD CONSTRAINT schedule_occurrences_job_fkey
    FOREIGN KEY (job_id, queue) REFERENCES {schema}.jobs (id, queue)
    ON DELETE CASCADE;

-- The occurrence key stays (schedule_id, occurrence_at), queue-free. It is the
-- whole exactly-once guarantee (§6) and it has to mean the same thing from
-- every queue: put the queue in it and a schedule moved between queues fires
-- its old occurrences a second time, because the row proving otherwise now
-- sits under a different key. The forgery this could otherwise allow -- an
-- occurrence claiming another queue's schedule, consuming a key that schedule
-- has not fired yet -- is refused by the policy below rather than by the key.
--
-- The *schedule* reference is not tied to the queue either. A schedule's queue
-- is mutable, so a constraint pinned to it would forbid the move or drag an
-- occurrence away from the job it records.
ALTER TABLE {schema}.schedule_occurrences ENABLE ROW LEVEL SECURITY;

-- Read broadly, write narrowly, and both halves lean on the schedule.
--
-- A policy expression runs as the querying role, so the subquery on
-- `schedules` is itself filtered by that table's own policy: "the schedule is
-- visible" means "the reader holds the schedule's queue", with no second copy
-- of the grant rules and no reference to a queue the writer chose.
--
-- USING adds it so a scheduler sees the whole history of every schedule it
-- owns, including occurrences fired before the schedule moved queues. Without
-- that, `Storage.last_occurrence` reads an empty history after a move and the
-- scheduler re-fires occurrences it already fired -- caught by the key above,
-- but only after attempting it every tick.
--
-- WITH CHECK requires it, which is what makes the queue-free key safe: a role
-- can only consume (schedule_id, occurrence_at) for a schedule it can see, and
-- it can only see schedules on queues it was granted.
CREATE POLICY schedule_occurrences_queue_scope ON {schema}.schedule_occurrences
    FOR ALL
    USING (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (schedule_occurrences.queue, '*')
        )
        OR EXISTS (
            SELECT 1 FROM {schema}.schedules AS s
            WHERE s.id = schedule_occurrences.schedule_id
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM {schema}.role_queue_grants AS g
            WHERE g.role_name = current_user
              AND g.queue IN (schedule_occurrences.queue, '*')
        )
        AND EXISTS (
            SELECT 1 FROM {schema}.schedules AS s
            WHERE s.id = schedule_occurrences.schedule_id
        )
    );
