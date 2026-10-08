"""The packaged migration files, checked without a database."""

from __future__ import annotations

import pathlib
import re
from typing import Final

from rqueue import migrations
from rqueue.limits import MAX_PURGE_LIMIT
from rqueue.models import TERMINAL_STATES
from rqueue.storage import Storage


def test_migrations_are_contiguous_and_ordered() -> None:
    loaded = migrations.load_migrations()
    assert [m.version for m in loaded] == list(range(1, len(loaded) + 1))
    assert len(loaded) >= 2, "an upgrade test needs a previous release to upgrade from"


def test_every_migration_is_transactional() -> None:
    # PostgreSQL runs DDL transactionally; nothing shipped today needs the
    # no-transaction escape hatch.
    assert all(m.transactional for m in migrations.load_migrations())


def test_checksums_are_stable_and_distinct() -> None:
    loaded = migrations.load_migrations()
    again = migrations.load_migrations()
    assert [m.checksum for m in loaded] == [m.checksum for m in again]
    assert len({m.checksum for m in loaded}) == len(loaded)


#: sha256 of each released migration file. A database records these, and
#: ``migrate`` and ``status`` refuse one whose file differs, comments included.
_RELEASED_CHECKSUMS: Final = {
    1: "65d25c8b12cc81b31bb0b700b105535968a1be5837225493955da2ff34b487d8",
    2: "cf3abfe0f6aa9e20e46382555078bd7542b4d66e4b90055c2bc9b872be6f4c9e",
    3: "29f69063ae55bcd50ae1faa6d48840df1ec339e06f29fdb2a03f3b3a934cf6e6",
    4: "2e1368f3ddcba18fd7fd7dd0729e083fef9dcaf81cb69f1a0efda07ffc6dd691",
    5: "3fc8ed3fa8084c6c1a203f587738fd739aaa5163bd3079ac69770a046565fc6b",
    6: "b1eab9efb4803f9d5cb99831d5812ae8e55dc1cdaa1141a8f8c757e6d424e3b3",
    7: "a221609df90c9bc5e165a2fd43cf34366074f8a1d9465d325b780f3687a6b079",
    8: "6b1794b040c2137488e1968b3054211ff0114dad97f785b70e8fa6f7f50cc645",
    9: "b4c8b6bc76b120e26cee0c7805b757ef5494c66b64c040c03a2647efa45e4ccc",
    10: "02dc3f8e920d1632ee96acaf64013cc9fe98359588d169300197a83a6fcfa678",
    11: "c01a196caafe4b9292c951f9934d3abcb9c03fed639c6903e70e3b080d9f7778",
}


def test_a_released_migration_file_is_unchanged() -> None:
    packaged = {m.version: m.checksum for m in migrations.load_migrations()}
    for version, released in _RELEASED_CHECKSUMS.items():
        assert packaged[version] == released, (
            f"migration {version:04d} differs from the released file; a database "
            "that applied it would refuse to migrate. Add a new migration instead."
        )


def test_schema_placeholder_is_the_only_interpolation() -> None:
    # Anything else in braces would be an accidental format field.
    pattern = re.compile(r"\{([^}]*)\}")
    for migration in migrations.load_migrations():
        for match in pattern.findall(migration.sql):
            assert match in {"schema", ""}, f"{migration.name} interpolates {{{match}}}"


def test_advisory_lock_key_is_stable_and_schema_specific() -> None:
    first = migrations._advisory_lock_key("task_queue")
    assert first == migrations._advisory_lock_key("task_queue")
    assert first != migrations._advisory_lock_key("other_queue")
    assert -(2**63) <= first < 2**63


def test_substituted_sql_names_the_configured_schema() -> None:
    sql = migrations.load_migrations()[0].sql.replace("{schema}", "jobs_alt")
    assert "jobs_alt.jobs" in sql
    assert "{schema}" not in sql
    assert Storage("jobs_alt").notify_channel == "rqueue_jobs_alt"


def _sql(name: str) -> str:
    return next(m.sql for m in migrations.load_migrations() if m.name == name)


def test_the_purge_routine_is_locked_down_where_it_is_defined() -> None:
    """A SECURITY DEFINER function is only as safe as the DDL around it.

    PostgreSQL grants EXECUTE on a new function to PUBLIC, and resolves
    unqualified names through the *caller's* search_path. Either default turns
    an owner-privileged routine into an open one, so both are closed in the
    same migration that creates it rather than left to provisioning.
    """
    sql = _sql("purge_function")
    assert "SECURITY DEFINER" in sql
    assert "SET search_path = pg_catalog, pg_temp" in sql
    assert "FROM PUBLIC" in sql
    # Every table reference inside the body is schema-qualified, which is what
    # makes the pinned search_path sufficient.
    assert "FROM jobs" not in sql
    assert "{schema}.jobs" in sql


def test_the_purge_routine_authorizes_the_login_role() -> None:
    """current_user is the definer once the body runs; session_user is not."""
    sql = _sql("purge_function")
    assert "g.role_name = session_user" in sql


QUEUE_SCOPED_TABLES: Final = (
    "jobs",
    "job_attempts",
    "concurrency_slots",
    "runtime_heartbeats",
    "schedules",
    "schedule_occurrences",
)


def test_every_queue_scoped_table_has_row_level_security_enabled() -> None:
    """The boundary covers every table that holds queue-scoped work.

    Only the ENABLE, not the policies: a policy can be dropped and replaced by
    a later migration -- 0008 replaces the heartbeat one with four narrower
    ones -- so the set of `CREATE POLICY` statements across the packaged files
    is not the set a migrated database ends up with. That is checked against
    `pg_policies` in the integration suite, where it can be read rather than
    guessed at.
    """
    policies = "".join(m.sql for m in migrations.load_migrations())
    for table in QUEUE_SCOPED_TABLES:
        assert f"ALTER TABLE {{schema}}.{table} ENABLE ROW LEVEL SECURITY" in policies


def test_the_kind_lookup_needs_no_grant_a_migration_cannot_make() -> None:
    """A policy runs as the querying role, so what it reads, the role must hold.

    0008 tightened the heartbeat policies to check which component kinds a role
    may claim. Reading that table from the policy directly would have required
    every runtime role to hold SELECT on a table created by the same migration
    -- and only `provision_role` can grant that, so every role provisioned
    before it would fail on its first heartbeat, which is the liveness
    mechanism itself and has nothing to degrade to.

    A SECURITY DEFINER function EXECUTE-able by PUBLIC is reachable without any
    re-granting, which is the whole reason it is one.
    """
    sql = _sql("heartbeat_ownership")
    assert "CREATE FUNCTION {schema}.role_claims_runtime_kind(" in sql
    assert "SECURITY DEFINER" in sql
    assert "SET search_path = pg_catalog, pg_temp" in sql
    # Deliberately *not* revoked from PUBLIC, unlike the purge routine: that is
    # what makes it reachable by a role nothing has re-granted.
    assert "REVOKE ALL ON FUNCTION" not in sql

    # The policies go through it and never touch the table.
    policies = sql[sql.index("CREATE POLICY runtime_heartbeats_read") :]
    assert "role_runtime_kinds" not in policies
    assert policies.count("{schema}.role_claims_runtime_kind(") == 2
    # And it is passed the identity the caller cannot choose.
    assert "role_claims_runtime_kind(\n            current_user," in policies


def test_the_tables_left_out_of_rls_are_the_two_that_have_to_be() -> None:
    """The policy list is not "every table with a queue column", and says so.

    Two are excluded deliberately, and each for a reason that makes enabling it
    a bug rather than a hardening:

    `queue_pauses` holds the wildcard pause as a row on the queue `'*'`. A
    policy scoped to a role's own queues hides it, and a worker that cannot see
    the global pause does not honour it.

    `role_queue_grants` is what every policy reads, as a subquery evaluated as
    the querying role. Under RLS it would filter itself and every other policy
    would then match nothing.

    Both are readable in full by any role that reaches the schema, which the
    docs/roles.md states as the cost. This test exists so that claim stays checkable:
    a third exclusion appearing without a reason should fail here.
    """
    sql = "".join(m.sql for m in migrations.load_migrations())
    for table in ("queue_pauses", "role_queue_grants"):
        assert f"CREATE TABLE {{schema}}.{table}" in sql
        assert f"ALTER TABLE {{schema}}.{table} ENABLE ROW LEVEL SECURITY" not in sql
        assert f"CREATE POLICY {table}_queue_scope" not in sql

    # `role_runtime_kinds` (0008) joins them, for the same reason as
    # `role_queue_grants`: the heartbeat policies read it. It carries no queue
    # column, so the claim both documents make -- that exactly two
    # *queue-carrying* tables are excluded -- is still true.
    assert (
        "ALTER TABLE {schema}.role_runtime_kinds ENABLE ROW LEVEL SECURITY" not in sql
    )
    assert (
        "queue"
        not in _sql("heartbeat_ownership")
        .split("CREATE TABLE {schema}.role_runtime_kinds (")[1]
        .split(");")[0]
    )


def test_a_queue_scoped_table_is_not_left_with_a_queueless_key() -> None:
    """A policy that hides a row an upsert must find is a runtime failure.

    An upsert whose arbiter can match a row the writer's policy forbids it to
    see resolves into an error rather than an update, so a key beside a queue
    column has to carry it -- unless the key means something that must hold
    across queues, which the occurrence key does (see below).
    """
    sql = _sql("queue_scoped_runtime")
    assert "PRIMARY KEY (queue, key)" in sql
    assert "PRIMARY KEY (kind, instance, queue)" in sql


def test_a_queue_label_is_tied_to_the_job_it_points_at() -> None:
    """A policy can only test the queue the writer wrote.

    On a row that also references a job, that is a label rather than a
    boundary: a role granted one queue could attach its own label to another
    queue's job and pass its own policy. Composite foreign keys make the label
    agree with the reference underneath every policy.
    """
    sql = _sql("queue_scoped_runtime")
    assert "UNIQUE (id, queue)" in sql
    assert "FOREIGN KEY (job_id, queue) REFERENCES {schema}.jobs (id, queue)" in sql


def test_every_table_that_points_at_a_job_ties_its_queue_to_it() -> None:
    """The tie is the rule, not a fix applied where someone noticed.

    Each of these has a globally unique key next to its queue column -- the
    attempt number, the slot key -- so an untied label lets a role granted one
    queue consume another queue's key and deny it service.
    """
    sql = _sql("queue_scoped_runtime")
    for constraint in ("job_attempts_job_fkey", "concurrency_slots_job_fkey"):
        assert f"ADD CONSTRAINT {constraint}" in sql


def test_a_schedule_is_not_the_anchor_a_queue_is_tied_to() -> None:
    """The job's queue never changes; a schedule's is a mutable field.

    Pinning an occurrence's queue to its schedule would either forbid moving a
    schedule or drag the occurrence's queue away from the job it records.
    """
    sql = _sql("queue_scoped_runtime")
    assert "REFERENCES {schema}.schedules (id, queue)" not in sql


def test_the_occurrence_key_stays_queue_free() -> None:
    """It is the exactly-once guarantee, and it has to mean one thing.

    Queue-scoping it would let a schedule moved between queues fire its old
    occurrences again, because the row proving otherwise would sit under a
    different key. The forgery that key-scoping would have blocked is refused
    by the policy instead: an occurrence may only name a schedule its writer
    can see, and schedules are themselves queue-scoped.
    """
    sql = _sql("queue_scoped_runtime")
    assert "PRIMARY KEY (queue, schedule_id, occurrence_at)" not in sql
    occurrences = sql[sql.index("CREATE POLICY schedule_occurrences_queue_scope") :]
    using, _, check = occurrences.partition("WITH CHECK")
    visible = (
        "EXISTS (\n"
        "            SELECT 1 FROM {schema}.schedules AS s\n"
        "            WHERE s.id = schedule_occurrences.schedule_id\n"
        "        )"
    )
    # Read broadly (OR), write narrowly (AND): a scheduler sees the history of
    # a schedule it owns even across a queue move, but may only write rows for
    # a schedule it can see.
    assert f"OR {visible}" in using
    assert f"AND {visible}" in check


def test_an_occurrence_takes_its_queue_from_its_job() -> None:
    """Not from its schedule: a schedule can move, and then the two disagree."""
    sql = _sql("queue_scoped_runtime")
    backfill = sql[sql.index("ALTER TABLE {schema}.schedule_occurrences ADD COLUMN") :]
    assert "FROM {schema}.jobs AS j" in backfill.split(";")[1]


def test_the_upgrade_guard_locks_before_it_looks() -> None:
    """An age threshold cannot decide this; a barrier plus an empty table can.

    A worker with a long poll interval is alive with an old heartbeat, so any
    window generous enough for it means nothing. ACCESS EXCLUSIVE makes the
    check and the key change one indivisible decision, and an empty table is a
    state no running worker can leave in place.
    """
    sql = _sql("queue_scoped_runtime")
    lock = sql.index("LOCK TABLE {schema}.runtime_heartbeats IN ACCESS EXCLUSIVE MODE")
    assert lock < sql.index("SELECT count(*) INTO beating")
    assert lock < sql.index("ALTER TABLE {schema}.runtime_heartbeats")
    # The count is of the whole table. Any predicate here would be a guess
    # about how recently a live worker last managed to beat.
    assert "SELECT count(*) INTO beating FROM {schema}.runtime_heartbeats;" in sql


def test_the_readme_migrate_transcript_matches_the_packaged_set() -> None:
    """A shipped example of a command's output is a claim about that output.

    This one has gone stale twice, both times because a release added a
    migration and the block was transcribed rather than derived. A reader
    running `rqueue migrate` against a fresh database and seeing more lines
    than docs/migrations.md shows has no way to tell whether that is drift or a
    problem, which is the whole value of printing it.
    """
    readme = (
        pathlib.Path(__file__).resolve().parent.parent / "docs" / "migrations.md"
    ).read_text(encoding="utf-8")
    block = re.search(r"\$ rqueue [^\n]*migrate\n((?:applied \d{4}_\w+\n)+)", readme)
    assert block is not None, "docs/migrations.md no longer shows a migrate transcript"
    shown = [
        line.removeprefix("applied ") for line in block.group(1).splitlines() if line
    ]
    assert shown == [
        f"{migration.version:04d}_{migration.name}"
        for migration in migrations.load_migrations()
    ], shown


def test_no_migration_hardcodes_the_default_schema() -> None:
    """Schema is a deployment's choice, including inside a message.

    A recovery hint naming `task_queue` is worse than useless to someone who
    passed `--schema`: it tells them to run a statement against a schema they
    do not have.
    """
    for migration in migrations.load_migrations():
        assert "task_queue" not in migration.sql, migration.name


def _purge_routine_definitions() -> list[str]:
    """Every migration that defines the routine, not just the first.

    It can be replaced -- 0007 does -- and a bound that only matched the
    original would stop meaning anything the moment one was.
    """
    found = [
        migration.sql
        for migration in migrations.load_migrations()
        if "FUNCTION {schema}.purge_terminal_jobs(" in migration.sql
    ]
    assert found
    return found


def test_the_purge_limit_matches_the_bound_the_routine_enforces() -> None:
    """Two copies of one bound, and only one of them can ever be edited.

    A migration is checksummed and forward-only, so the routine's constant is
    fixed for good. Raising the Python one would pass `validate_purge_limit`
    and then hit a raw PostgresError from inside the routine -- precisely the
    failure that validator exists to turn into a typed error.
    """
    for sql in _purge_routine_definitions():
        bound = re.search(r"max_rows NOT BETWEEN 1 AND (\d+)", sql)
        assert bound is not None
        assert int(bound.group(1)) == MAX_PURGE_LIMIT


def test_the_purge_states_match_the_states_the_routine_accepts() -> None:
    """Same shape: `TERMINAL_STATES` is checked in Python, spelled in SQL."""
    for sql in _purge_routine_definitions():
        listed = re.search(r"requested\.state NOT IN \(([^)]*)\)", sql)
        assert listed is not None
        assert {
            value.strip().strip("'") for value in listed.group(1).split(",")
        } == set(TERMINAL_STATES)


def test_the_purge_cutoff_is_compared_against_the_database_clock() -> None:
    """The third of the duplicated bounds, and the easiest to get wrong.

    `_validate_purge_cutoff` stands in for this comparison, so it has to read
    the same clock: checking a caller's cutoff against the *client's* wall
    clock leaves a window the width of the skew between them, and a call
    inside it passes the typed check and then takes a raw PostgresError.
    """
    for sql in _purge_routine_definitions():
        assert "finished_before > now()" in sql


def test_every_definition_of_the_purge_routine_is_locked_down() -> None:
    """A replacement inherits the ACL, but not the rest of its own lockdown.

    `CREATE OR REPLACE` keeps the grants PostgreSQL already recorded, so the
    REVOKE from PUBLIC in 0006 still holds -- but SECURITY DEFINER and the
    pinned search_path are properties of the definition itself, and a
    replacement that omitted either would silently undo them.
    """
    for sql in _purge_routine_definitions():
        assert "SECURITY DEFINER" in sql
        assert "SET search_path = pg_catalog, pg_temp" in sql
        assert "g.role_name = session_user" in sql
