"""Fixtures for the integration suite.

Everything here runs against the disposable database that
``scripts/integration.sh`` creates and drops; there is no mocked database
anywhere in this directory.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Iterator

import asyncpg
import pytest

from rqueue import Admin, Queue

pytestmark = pytest.mark.integration


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Everything under tests/integration is an integration test."""
    for item in items:
        if "tests/integration/" in str(item.path).replace(os.sep, "/"):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def admin_dsn() -> str:
    dsn = os.environ.get("RQUEUE_ADMIN_DATABASE_URL")
    if not dsn:
        pytest.skip(
            "integration tests need RQUEUE_ADMIN_DATABASE_URL; "
            "run them through scripts/integration.sh (mise run test-integration)"
        )
    return dsn


@pytest.fixture(scope="session")
def app_dsn() -> str | None:
    return os.environ.get("RQUEUE_APP_DATABASE_URL")


@pytest.fixture(scope="session")
def app_role() -> str | None:
    return os.environ.get("RQUEUE_APP_ROLE")


@pytest.fixture
async def pool(admin_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    created = await asyncpg.create_pool(admin_dsn, min_size=2, max_size=16)
    assert created is not None
    try:
        yield created
    finally:
        await created.close()


@pytest.fixture
def queue_name() -> str:
    """A queue name unique to one test, so tests never see each other's jobs."""
    return f"q{uuid.uuid4().hex[:16]}"


@pytest.fixture
def queue(pool: asyncpg.Pool, queue_name: str) -> Queue:
    return Queue(pool, name=queue_name)


@pytest.fixture
def admin(pool: asyncpg.Pool) -> Admin:
    return Admin(pool)


@pytest.fixture
def worker_id() -> Iterator[str]:
    yield f"w-{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def widgets(pool: asyncpg.Pool) -> AsyncIterator[str]:
    """A real application table, in the application's own schema.

    Transactional enqueue is about business data and a queue job sharing one
    transaction, so the business side has to be real too.
    """
    name = f"widgets_{uuid.uuid4().hex[:12]}"
    async with pool.acquire() as connection:
        await connection.execute(
            f"CREATE TABLE public.{name} (id uuid PRIMARY KEY, label text NOT NULL, "
            "queue_job_id uuid)"
        )
    try:
        yield name
    finally:
        async with pool.acquire() as connection:
            await connection.execute(f"DROP TABLE IF EXISTS public.{name}")


@pytest.fixture(autouse=True)
async def isolate_schedules(pool: asyncpg.Pool) -> AsyncIterator[None]:
    """Give every test a clean schedule table.

    Jobs are already isolated by the per-test queue name, but a Scheduler fires
    every enabled schedule in the database -- by design, since one scheduler
    deployment usually serves several queues -- so leftovers from an earlier
    test would show up in the next one's tick.
    """
    yield
    async with pool.acquire() as connection:
        await connection.execute("TRUNCATE task_queue.schedules CASCADE")
