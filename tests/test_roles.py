"""Guardrails on the grant model itself (§8).

The privilege *shapes* rqueue hands PostgreSQL are decided in Python, so they
can be asserted without a database; the integration suite proves the resulting
role can actually enqueue.
"""

from __future__ import annotations

import inspect
from typing import cast

import asyncpg

from rqueue.roles import (
    _ELEVATED_ATTRIBUTES,
    _FUNCTION_GRANTS,
    _TABLE_GRANTS,
    PURGE_FUNCTION,
    Capability,
    _drop_subsumed_column_privileges,
    provision_role,
)


def merged(*capabilities: Capability) -> dict[str, set[str]]:
    """The per-table privilege sets ``provision_role`` would grant."""
    granted: dict[str, set[str]] = {}
    for capability in capabilities:
        for table, privileges in _TABLE_GRANTS[capability]:
            granted.setdefault(table, set()).update(
                part.strip() for part in privileges.split(",")
            )
    return {
        table: _drop_subsumed_column_privileges(allowed)
        for table, allowed in granted.items()
    }


def test_a_producer_may_only_touch_updated_at() -> None:
    """The enqueue conflict path needs UPDATE; §8 says only on that column."""
    assert merged(Capability.PRODUCE)["jobs"] == {
        "SELECT",
        "INSERT",
        "UPDATE (updated_at)",
    }


def test_a_whole_table_grant_subsumes_the_column_grant() -> None:
    """PRODUCE + CONSUME must not emit UPDATE and UPDATE (updated_at) both."""
    assert merged(Capability.PRODUCE, Capability.CONSUME)["jobs"] == {
        "SELECT",
        "INSERT",
        "UPDATE",
    }


def test_a_column_grant_survives_an_unrelated_whole_table_privilege() -> None:
    assert _drop_subsumed_column_privileges(
        {"SELECT", "INSERT", "UPDATE (updated_at)"}
    ) == {"SELECT", "INSERT", "UPDATE (updated_at)"}


def test_only_the_matching_action_is_subsumed() -> None:
    assert _drop_subsumed_column_privileges(
        {"SELECT", "SELECT (payload)", "UPDATE (updated_at)"}
    ) == {"SELECT", "UPDATE (updated_at)"}


def test_a_worker_may_read_the_pause_table_but_not_write_it() -> None:
    """Claiming reads queue_pauses; pausing is an operator action, not a worker's."""
    assert merged(Capability.CONSUME)["queue_pauses"] == {"SELECT"}


def test_a_consumer_cannot_enqueue() -> None:
    """Claiming work and creating it are separate authorities.

    A worker role that can INSERT into jobs can hand the fleet any task it
    likes, under any payload -- the one privilege that turns a compromised
    worker into arbitrary execution across every replica.
    """
    assert merged(Capability.CONSUME)["jobs"] == {"SELECT", "UPDATE"}


def test_a_role_that_needs_both_asks_for_both() -> None:
    """Dropping INSERT from CONSUME must not make the combination unusable."""
    assert "INSERT" in merged(Capability.PRODUCE, Capability.CONSUME)["jobs"]


def test_no_capability_grants_delete_on_jobs() -> None:
    """Retention deletes through the routine, so nothing needs the privilege."""
    for capability in Capability:
        assert "DELETE" not in merged(capability).get("jobs", set())


def test_purge_reads_jobs_and_touches_nothing_else() -> None:
    assert merged(Capability.PURGE) == {
        "jobs": {"SELECT"},
        "schema_migrations": {"SELECT"},
    }


def test_purge_is_the_only_capability_granted_a_routine() -> None:
    assert set(_FUNCTION_GRANTS) == {Capability.PURGE}
    assert _FUNCTION_GRANTS[Capability.PURGE] == (PURGE_FUNCTION,)


def test_the_purge_signature_matches_the_migration() -> None:
    """The grant names a signature; PostgreSQL resolves functions by one."""
    from rqueue import migrations

    sql = next(
        migration.sql
        for migration in migrations.load_migrations()
        if migration.name == "purge_function"
    )
    name, argument_types = PURGE_FUNCTION
    assert f"CREATE FUNCTION {{schema}}.{name}(" in sql
    for argument_type in argument_types.split(", "):
        assert argument_type in sql


def test_hardening_clears_every_elevated_attribute() -> None:
    """§8: a repaired role keeps none of the attributes that outrank the model."""
    assert {clause for _, clause in _ELEVATED_ATTRIBUTES} == {
        "NOSUPERUSER",
        "NOCREATEDB",
        "NOCREATEROLE",
        "NOBYPASSRLS",
        "NOREPLICATION",
        "NOINHERIT",
    }
    # Each clause has to name the pg_roles column that reports it, or a repair
    # would emit a clause the connection may not be entitled to use.
    assert all(column.startswith("rol") for column, _ in _ELEVATED_ATTRIBUTES)


async def test_granted_by_is_used_only_where_it_exists() -> None:
    """§2 says PostgreSQL 14+; `GRANTED BY` on a membership REVOKE is 16+.

    16 is where one membership became grantable several times by different
    roles, and where a bare REVOKE narrowed to "only the grant I made" -- so
    naming the grantor is necessary there and a syntax error before there.
    Getting the threshold wrong in either direction breaks role repair on a
    supported version: too low is a syntax error, too high silently leaves
    another grantor's membership in place while reporting the role narrowed.
    """
    from rqueue.roles import _tracks_multiple_grantors

    class FakeServer:
        def __init__(self, version: int) -> None:
            self.version = version

        async def fetchval(self, _sql: str) -> int:
            return self.version

    # The boundary itself, and one release either side of it.
    for version, expected in (
        (140012, False),
        (150007, False),
        (159999, False),
        (160000, True),
        (160004, True),
        (180006, True),
    ):
        server = cast("asyncpg.Connection", FakeServer(version))
        assert await _tracks_multiple_grantors(server) is expected, version


def test_hardening_cannot_be_declined() -> None:
    """§8 promises no opt-out, so there must be no argument that is one.

    The promise is only as good as the signature: a `harden=False`-shaped
    parameter added later would leave the spec describing a guarantee the
    function no longer makes, and the caller's next act is handing out the
    role's credentials.
    """
    accepted = set(inspect.signature(provision_role).parameters)
    assert not accepted & {"harden", "hardened", "repair", "strict", "secure"}


def test_a_scheduler_can_run_the_enqueue_statement() -> None:
    """Firing a schedule is an enqueue, and every enqueue writes updated_at.

    ``INSERT ... ON CONFLICT ... DO UPDATE SET updated_at`` needs UPDATE on
    that column even when no conflict fires, so SELECT+INSERT alone makes
    every schedule firing fail with a privilege error.
    """
    assert merged(Capability.SCHEDULE)["jobs"] == {
        "SELECT",
        "INSERT",
        "UPDATE (updated_at)",
    }


def test_a_scheduler_still_cannot_transition_a_job() -> None:
    """The UPDATE above stayed a column grant; a whole-table one would not."""
    assert "UPDATE" not in merged(Capability.SCHEDULE)["jobs"]


def test_an_inspector_can_read_what_readiness_reads() -> None:
    """check_readiness reads liveness on every probe.

    Without this grant an inspect-only role does not report "no worker" -- the
    failure escapes as a connection error and a healthy database is reported
    unreachable.
    """
    inspect = merged(Capability.INSPECT)
    assert inspect["runtime_heartbeats"] == {"SELECT"}
    assert inspect["schema_migrations"] == {"SELECT"}


def test_an_inspector_writes_nothing() -> None:
    assert all(
        privileges == {"SELECT"} for privileges in merged(Capability.INSPECT).values()
    )


def test_an_inspector_can_read_every_table_another_capability_touches() -> None:
    """INSPECT is the read-only view of the whole model, not of most of it.

    Enumerated rather than listed, so a table introduced for one capability
    cannot quietly become invisible to the role whose entire purpose is
    looking at it -- which is how `concurrency_slots` was missed once already.
    """
    readable = set(merged(Capability.INSPECT))
    for capability in Capability:
        if capability is Capability.INSPECT:
            continue
        assert set(merged(capability)) <= readable, capability
