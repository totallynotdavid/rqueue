"""Which database errors a worker or scheduler retries, and which it raises."""

from __future__ import annotations

import asyncpg
import pytest

from rqueue import ConfigurationError
from rqueue.database_errors import is_permanent, raise_if_permanent


def test_a_missing_privilege_is_permanent() -> None:
    assert is_permanent(
        asyncpg.InsufficientPrivilegeError("permission denied for table jobs")
    )


@pytest.mark.parametrize(
    "exc",
    [
        asyncpg.UndefinedTableError("relation does not exist"),
        asyncpg.UndefinedColumnError("column does not exist"),
        asyncpg.InvalidColumnReferenceError("no unique constraint matches ON CONFLICT"),
        asyncpg.ConnectionDoesNotExistError("connection was closed"),
        asyncpg.CannotConnectNowError("the database system is starting up"),
        asyncpg.AdminShutdownError("terminating connection"),
        asyncpg.TooManyConnectionsError("too many clients"),
        asyncpg.DeadlockDetectedError("deadlock detected"),
        asyncpg.QueryCanceledError("canceling statement"),
        asyncpg.InterfaceError("connection is closed"),
        ConnectionResetError("reset by peer"),
    ],
)
def test_anything_else_is_retryable(exc: BaseException) -> None:
    assert not is_permanent(exc)
    raise_if_permanent(exc)


def test_a_missing_privilege_names_the_remedy() -> None:
    cause = asyncpg.InsufficientPrivilegeError("permission denied for table jobs")
    with pytest.raises(ConfigurationError, match="provision_role") as raised:
        raise_if_permanent(cause)
    assert raised.value.__cause__ is cause
