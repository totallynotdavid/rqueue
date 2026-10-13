"""Helpers shared by the integration suite."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

import asyncpg

from rqueue import Worker
from rqueue.roles import Capability, provision_role, revoke_role
from scripts.database import database_target


@asynccontextmanager
async def scoped_role_pool(
    admin_dsn: str,
    *,
    capabilities: Sequence[Capability],
    queues: Sequence[str],
    prefix: str = "rq_scoped",
) -> AsyncIterator[asyncpg.Pool]:
    """A pool logged in as a freshly provisioned role, dropped afterwards.

    Python can assert grant shapes (see ``tests/test_roles.py``). A privilege
    only really exists once PostgreSQL enforces it against a real
    connection. Every claim about what a capability cannot do is made here by
    trying it.
    """
    role = f"{prefix}_{uuid.uuid4().hex[:8]}"
    password = "scoped-role-test-password"
    database = urlsplit(admin_dsn).path.lstrip("/")
    target = database_target(admin_dsn, database, user=role, password=password)

    admin_connection = await asyncpg.connect(admin_dsn)
    try:
        await provision_role(
            admin_connection,
            role=role,
            capabilities=capabilities,
            queues=queues,
            password=password,
        )
        pool = await asyncpg.create_pool(target.database_url, min_size=1, max_size=2)
        assert pool is not None
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        try:
            await revoke_role(admin_connection, role=role, drop=True)
        finally:
            await admin_connection.close()


@asynccontextmanager
async def running(worker: Worker) -> AsyncIterator[Worker]:
    """Run a worker in the background for the duration of the block."""
    task = asyncio.create_task(worker.run(), name=f"worker-{worker.worker_id}")
    started = asyncio.create_task(worker.wait_started())
    # A worker that fails before it starts never sets the event, so waiting on
    # the event alone would hang instead of raising its error.
    await asyncio.wait({task, started}, return_when=asyncio.FIRST_COMPLETED)
    started.cancel()
    if task.done():
        await task
    try:
        yield worker
    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=30)


async def eventually[T](
    predicate: Callable[[], Awaitable[T]],
    *,
    timeout: float = 15.0,
    interval: float = 0.05,
    message: str = "condition never became true",
) -> T:
    """Poll ``predicate`` until it returns something truthy, or fail loudly."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last: T | None = None
    while loop.time() < deadline:
        last = await predicate()
        if last:
            return last
        await asyncio.sleep(interval)
    raise AssertionError(f"{message} (last value: {last!r})")
