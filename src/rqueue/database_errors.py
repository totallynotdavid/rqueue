"""Tell a database error worth retrying from one that retrying cannot fix.

The worker and the scheduler retry a failed tick, because a restarted or
unreachable PostgreSQL recovers by itself. A missing privilege (SQLSTATE 42501)
does not: the role has the same grants on every attempt, and retrying only hides
the cause behind a log line that says the database is unreachable.

Every other error stays retryable, including a missing table or column (42P01,
42703). A worker that starts on a schema that is behind sees those, and it
recovers once ``rqueue migrate`` has run.
"""

from __future__ import annotations

from typing import Final

import asyncpg

from rqueue.errors import ConfigurationError

__all__ = ["is_permanent", "raise_if_permanent"]

_INSUFFICIENT_PRIVILEGE: Final = "42501"


def is_permanent(exc: BaseException) -> bool:
    """Whether ``exc`` is a refused privilege, which a retry will repeat."""
    return (
        isinstance(exc, asyncpg.PostgresError)
        and exc.sqlstate == _INSUFFICIENT_PRIVILEGE
    )


def raise_if_permanent(exc: BaseException) -> None:
    """Raise a :class:`~rqueue.ConfigurationError` for a refused privilege.

    The error names the remedy and has the same type :meth:`rqueue.Admin.purge`
    raises for a refused role. Any other error returns, so the caller retries.
    """
    if is_permanent(exc):
        raise ConfigurationError(
            f"the database role lacks a privilege rqueue needs: {exc}. "
            "Run provision_role again for this role, or grant the privilege."
        ) from exc
