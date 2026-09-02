"""Guardrails on the grant model itself (§8).

The privilege *shapes* rqueue hands PostgreSQL are decided in Python, so they
can be asserted without a database; the integration suite proves the resulting
role can actually enqueue.
"""

from __future__ import annotations

from rqueue.roles import _TABLE_GRANTS, Capability, _drop_subsumed_column_privileges


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
