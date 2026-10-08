"""Guardrails on the SQL layer itself.

These do not need a database: they assert on the statement text rqueue builds,
which is where caller-supplied values must stay out of the SQL string.
"""

from __future__ import annotations

import re

import pytest

from rqueue.errors import ValidationError
from rqueue.storage import Storage, _Statements


def statements() -> dict[str, str]:
    built = _Statements("task_queue")
    return {
        name: value for name, value in vars(built).items() if isinstance(value, str)
    }


def test_schema_must_be_a_bare_identifier() -> None:
    for bad in ("public schema", "Public", "pub;lic", "task_queue; DROP TABLE x"):
        with pytest.raises(ValidationError):
            Storage(bad)


def test_schema_is_short_enough_for_the_notify_channel() -> None:
    with pytest.raises(ValidationError, match="NOTIFY channel"):
        Storage("s" * 60)
    assert Storage("s" * 56).notify_channel == "rqueue_" + "s" * 56


def test_every_statement_targets_the_configured_schema() -> None:
    for name, sql in statements().items():
        assert "task_queue." in sql or name in {"heartbeat"}, name


def test_no_statement_uses_string_formatting_for_values() -> None:
    # Values reach PostgreSQL only as $n placeholders. The sole literals in the
    # generated SQL are the state names, which come from the JobState enum.
    allowed_literals = {
        "'pending'",
        "'leased'",
        "'succeeded'",
        "'failed'",
        "'cancelled'",
        "'retry'",
        "'lease_expired'",
        "'LeaseExpired'",
        "'Cancelled'",
        "'lease expired before the attempt finished'",
        "'cancelled while leased; lease expired unfinalized'",
        "'cancelled before execution'",
        "'ConfigurationError'",
        "'max_attempts'",
        "'worker'",
        "'scheduler'",
        # The queue wildcard is a fixed sentinel, like the state names above,
        # and the same one migration 0002's RLS policies already spell out.
        "'*'",
        # The resolution of a timestamptz column, not a value from a caller:
        # the smallest step that makes the routine's strict `<` include every
        # row sharing the boundary instant.
        "'1 microsecond'",
    }
    literal = re.compile(r"'[^']*'")
    for name, sql in statements().items():
        for found in literal.findall(sql):
            assert found in allowed_literals, f"{name} embeds literal {found}"


def test_claim_uses_skip_locked_and_a_deterministic_order() -> None:
    sql = statements()["claim_candidates"]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "ORDER BY priority DESC, scheduled_at, seq" in sql


def test_claim_is_gated_on_the_pause_table_in_sql() -> None:
    """A paused queue must yield nothing regardless of what a Worker believes."""
    sql = statements()["claim_candidates"]
    assert "task_queue.queue_pauses" in sql
    assert "p.queue IN ($1, '*')" in sql
    assert "p.paused_at IS NOT NULL" in sql


def test_pausing_twice_keeps_the_first_paused_at() -> None:
    sql = statements()["pause_queue"]
    assert "COALESCE(queue_pauses.paused_at, EXCLUDED.paused_at)" in sql


def test_resuming_the_wildcard_clears_every_pause() -> None:
    sql = statements()["resume_queue"]
    assert "CASE WHEN $1 = '*' THEN true ELSE queue = $1 END" in sql


def test_every_lease_fenced_write_checks_the_token() -> None:
    fenced = (
        "complete",
        "fail_terminal",
        "fail_invalid_policy",
        "cancel_leased",
        "reschedule",
        "heartbeat",
    )
    for name in fenced:
        sql = statements()[name]
        assert "lease_token = $2" in sql, name
        assert "state = 'leased'" in sql, name


def test_dedupe_upsert_returns_the_existing_row_rather_than_dropping_it() -> None:
    sql = statements()["insert_job"]
    assert "ON CONFLICT (queue, dedupe_key)" in sql
    assert "DO UPDATE" in sql
    assert "DO NOTHING" not in sql


def test_json_columns_are_cast_on_the_way_in_and_out() -> None:
    built = statements()
    assert "$4::text::jsonb" in built["insert_job"]
    assert "payload::text AS payload" in built["get_job"]
    assert "metadata::text AS metadata" in built["get_job"]
    assert "retry_policy::text AS retry_policy" in built["get_job"]


def test_operator_retry_updates_the_persisted_attempt_ceiling() -> None:
    sql = statements()["retry_terminal"]
    assert "jsonb_set" in sql
    assert "ARRAY['max_attempts']" in sql


def test_a_global_liveness_probe_counts_each_instance_once() -> None:
    """Heartbeats are keyed per queue, so one process holds several rows.

    Without DISTINCT, `check_readiness(queue=None)` reports one scheduler
    serving three queues as three schedulers -- a readiness answer that counts
    deployments which do not exist.
    """
    assert "SELECT DISTINCT instance" in statements()["live_instances"]


def test_a_heartbeat_is_keyed_by_the_queue_and_the_role_that_wrote_it() -> None:
    """The arbiter has to match the unique index, role_name included.

    The statement never supplies `role_name` -- it defaults to `current_user`,
    which is the whole point of the column -- but the arbiter still names it,
    because PostgreSQL resolves `ON CONFLICT` against an index, not against the
    columns the INSERT happens to list. Naming three of four raises 42P10.
    """
    upsert = statements()["record_runtime_heartbeat"]
    assert "ON CONFLICT (kind, instance, queue, role_name) DO UPDATE" in upsert
    assert "role_name" not in upsert.split("ON CONFLICT")[0]


def test_retracting_a_heartbeat_reaches_only_the_writers_own_rows() -> None:
    """The policy says so too; the statement says it where the owner can hear.

    The schema owner bypasses row-level security, so a predicate left to the
    policy alone has a different reach for the owner than for a scoped role.
    Spelling it in both keeps `clear_runtime_heartbeats` and the check that
    precedes it deleting exactly the same rows for everyone.
    """
    for name in ("clear_runtime_heartbeats", "stale_heartbeat_queues"):
        sql = " ".join(statements()[name].split())
        assert "kind = $1 AND instance = $2 AND role_name = current_user" in sql, name
        assert "queue <> ALL($3::text[])" in sql, name


def test_retention_deletes_through_the_routine_not_a_delete() -> None:
    """PURGE grants EXECUTE and no DELETE, so `Admin.purge` cannot use one.

    The routine re-derives queue, terminal state, cutoff, and batch size for
    itself; a DELETE here would carry those predicates instead, which is the
    arrangement the capability exists to end.
    """
    purge = statements()["purge"]
    assert "purge_terminal_jobs" in purge
    assert "DELETE" not in purge.upper()


def test_a_bounded_purge_visits_queues_oldest_first() -> None:
    """A partial budget must be spent on the oldest rows in the schema.

    One DELETE ordered by finished_at did that for free. Fanning out per queue
    only keeps it if the queues are visited by the age of their oldest
    candidate -- alphabetical order would delete a fresh job on 'aaa' and leave
    a month-old one on 'zzz'.
    """
    purge_queues = statements()["purge_queues"]
    assert "ORDER BY min(finished_at)" in purge_queues
    assert "ORDER BY queue" not in purge_queues


def test_the_queue_scan_carries_the_budget_as_well_as_the_horizon() -> None:
    """The horizon alone bounds nothing, because a tie group has no size.

    It is inclusive of the rows sharing the budget's last instant on purpose,
    and one transaction can finish any number of jobs at that instant, so
    `finished_at < horizon` can match the whole table. The scan that learns
    which queues to visit therefore takes `limit` too -- the `limit` oldest
    rows name every queue the budget can reach, and reading further only costs.
    """
    purge_queues = " ".join(statements()["purge_queues"].split())
    assert "ORDER BY finished_at LIMIT $3" in purge_queues
    # The grouping runs over that bounded scan, not over the table.
    assert purge_queues.index("LIMIT $3") < purge_queues.index("GROUP BY queue")


def test_the_purge_horizon_is_bounded_by_the_budget() -> None:
    """Both ends of the candidate range come from LIMIT-ed index scans.

    Without them the fan-out reads every terminal row behind the cutoff to
    answer "which queues have anything?", which is the whole backlog on the
    only deployments where retention cost matters.
    """
    horizon = " ".join(statements()["purge_horizon"].split())
    assert "ORDER BY finished_at OFFSET $3 - 1 LIMIT 1" in horizon
    # Inclusive of the boundary instant: the routine compares with a strict
    # `<`, so every row tied with the boundary would otherwise be dropped even
    # though it is inside the budget.
    assert "finished_at + interval '1 microsecond'" in horizon
