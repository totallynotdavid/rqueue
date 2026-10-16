"""Privilege merging and the PostgreSQL version switch of role provisioning."""

from __future__ import annotations

from typing import cast

import asyncpg
import pytest

from rqueue.roles import _resolve_privileges, _tracks_multiple_grantors


@pytest.mark.parametrize(
    ("wanted", "resolved"),
    [
        pytest.param(
            [
                "SELECT, INSERT, UPDATE (updated_at)",
                "SELECT, UPDATE (state, updated_at)",
            ],
            ["INSERT", "SELECT", "UPDATE (state, updated_at)"],
            id="column lists for one action become one clause",
        ),
        pytest.param(
            ["SELECT, UPDATE (updated_at)", "UPDATE"],
            ["SELECT", "UPDATE"],
            id="a whole-table grant swallows its column lists",
        ),
        pytest.param(
            ["SELECT, SELECT (payload), UPDATE (updated_at)"],
            ["SELECT", "UPDATE (updated_at)"],
            id="only the matching action is swallowed",
        ),
    ],
)
def test_privileges_merge_into_one_clause_per_action(
    wanted: list[str], resolved: list[str]
) -> None:
    assert _resolve_privileges(wanted) == resolved


class FakeServer:
    def __init__(self, version: int) -> None:
        self.version = version

    async def fetchval(self, _sql: str) -> int:
        return self.version


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        (140012, False),
        (150007, False),
        (159999, False),
        (160000, True),
        (160004, True),
        (180006, True),
    ],
)
async def test_granted_by_is_used_only_where_it_exists(
    version: int, expected: bool
) -> None:
    """`GRANTED BY` on a membership REVOKE exists from PostgreSQL 16."""
    server = cast("asyncpg.Connection", FakeServer(version))
    assert await _tracks_multiple_grantors(server) is expected
