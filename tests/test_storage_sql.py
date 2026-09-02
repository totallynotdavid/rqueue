"""Guardrails on the SQL layer itself (§8).

These do not need a database: they assert on the statement text rqueue builds,
which is the thing §8 constrains.
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
        "'worker'",
        "'scheduler'",
        # The queue wildcard is a fixed sentinel, like the state names above,
        # and the same one migration 0002's RLS policies already spell out.
        "'*'",
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
    fenced = ("complete", "fail_terminal", "cancel_leased", "reschedule", "heartbeat")
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
