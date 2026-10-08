"""A connection the driver gives up on must not cost the pool a slot.

When PostgreSQL restarts under a busy pool, asyncpg can abort a connection from
inside its protocol. The pool must still get the slot back, so a later
`acquire()` and `Pool.close()` complete. These tests reproduce the abort on a
real connection and look at the pool through its public API.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import asyncpg
import pytest

from rqueue import Admin, Queue, check_readiness


@pytest.fixture
async def single_slot_pool(admin_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    created = await asyncpg.create_pool(admin_dsn, min_size=1, max_size=1)
    assert created is not None
    try:
        yield created
    finally:
        created.terminate()


def _abort_like_the_driver(connection: asyncpg.Connection) -> None:
    """Abort the connection the way asyncpg does on a critical protocol error.

    The protocol closes its transport without going through the connection's
    own close, which is the state a restart leaves behind.
    """
    connection._con._protocol.abort()


async def _slot_is_usable(pool: asyncpg.Pool) -> None:
    async with asyncio.timeout(10):
        async with pool.acquire() as connection:
            assert await connection.fetchval("SELECT 1") == 1
        await pool.close()


async def test_a_driver_aborted_connection_frees_its_slot(
    single_slot_pool: asyncpg.Pool,
) -> None:
    async with single_slot_pool.acquire() as connection:
        _abort_like_the_driver(connection)

    await _slot_is_usable(single_slot_pool)


async def test_a_queue_connection_aborted_by_the_driver_frees_its_slot(
    single_slot_pool: asyncpg.Pool, queue_name: str
) -> None:
    queue = Queue(single_slot_pool, name=queue_name)

    async with queue.connection() as connection:
        _abort_like_the_driver(connection)

    await _slot_is_usable(single_slot_pool)


async def test_a_failed_call_on_an_aborted_connection_frees_its_slot(
    single_slot_pool: asyncpg.Pool,
) -> None:
    with pytest.raises(asyncpg.InterfaceError):
        async with single_slot_pool.acquire() as connection:
            _abort_like_the_driver(connection)
            await connection.fetchval("SELECT 1")

    await _slot_is_usable(single_slot_pool)


async def test_admin_and_readiness_use_the_pool_without_keeping_a_slot(
    single_slot_pool: asyncpg.Pool, queue_name: str
) -> None:
    await Admin(single_slot_pool).paused_queues()
    report = await check_readiness(single_slot_pool, queue=queue_name)
    assert report.connected

    await _slot_is_usable(single_slot_pool)
