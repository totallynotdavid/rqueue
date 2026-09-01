"""Shared fixtures for the fast, database-free suite."""

from __future__ import annotations

from typing import cast

import asyncpg
import pytest

from rqueue import Queue


@pytest.fixture
def offline_queue() -> Queue:
    """A Queue that never touches its pool.

    Registration and enqueue validation happen entirely in Python, so the fast
    suite can exercise them without a database (§11).
    """
    return Queue(cast("asyncpg.Pool", None), name="fast")
