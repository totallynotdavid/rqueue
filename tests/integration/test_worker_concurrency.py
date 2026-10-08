"""Blocking handlers stay inside the configured concurrency bound."""

from __future__ import annotations

import asyncio
import threading
import time

import asyncpg

from rqueue import Queue, TaskContext, Worker

from .support import running


class Peak:
    """Greatest number of threads inside ``block`` at any one moment."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current = 0
        self.peak = 0

    def block(self, seconds: float) -> None:
        with self._lock:
            self._current += 1
            self.peak = max(self.peak, self._current)
        time.sleep(seconds)
        with self._lock:
            self._current -= 1


async def test_the_worker_executor_caps_concurrent_to_thread_work(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """The documented bound, measured on threads rather than on tasks.

    One job's handler fires far more ``asyncio.to_thread`` calls than the
    worker's concurrency. Task-level concurrency cannot explain the result --
    there is only one job -- so the ceiling can only come from the bounded
    default executor the Worker installed.
    """
    peak = Peak()
    submissions = 30

    async def burst(payload: object, context: TaskContext) -> None:
        await asyncio.gather(
            *(asyncio.to_thread(peak.block, 0.05) for _ in range(submissions))
        )

    queue.register(name="burst", handler=burst)
    async with pool.acquire() as connection, connection.transaction():
        await queue.enqueue(connection, task="burst")

    worker = Worker(queue, worker_id="bounded", concurrency=3, poll_interval=0.05)
    await worker.drain(timeout=60)

    assert peak.peak <= 3, f"to_thread ran {peak.peak} threads at once, over the bound"
    assert peak.peak > 1, "the executor should still run blocking work in parallel"
    stats = await queue.stats()
    assert stats.succeeded == 1


async def test_without_the_installed_executor_the_bound_is_gone(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """The positive control: the cap really is the Worker's executor.

    With ``install_default_executor=False`` the same handler runs on the
    interpreter's default pool, which is sized from the CPU count and happily
    exceeds the worker's concurrency.
    """
    peak = Peak()

    async def burst(payload: object, context: TaskContext) -> None:
        await asyncio.gather(*(asyncio.to_thread(peak.block, 0.05) for _ in range(30)))

    queue.register(name="burst", handler=burst)
    async with pool.acquire() as connection, connection.transaction():
        await queue.enqueue(connection, task="burst")

    worker = Worker(
        queue,
        worker_id="unbounded",
        concurrency=1,
        poll_interval=0.05,
        install_default_executor=False,
    )
    await worker.drain(timeout=60)
    assert peak.peak > 1


async def test_the_executor_size_can_be_set_apart_from_concurrency(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    peak = Peak()

    async def burst(payload: object, context: TaskContext) -> None:
        await asyncio.gather(*(asyncio.to_thread(peak.block, 0.05) for _ in range(30)))

    queue.register(name="burst", handler=burst)
    async with pool.acquire() as connection, connection.transaction():
        await queue.enqueue(connection, task="burst")

    worker = Worker(
        queue,
        worker_id="wide",
        concurrency=1,
        executor_max_workers=6,
        poll_interval=0.05,
    )
    await worker.drain(timeout=60)
    assert 1 < peak.peak <= 6


async def test_blocking_handlers_do_not_starve_unrelated_jobs(
    queue: Queue, pool: asyncpg.Pool
) -> None:
    """A slow blocking job delays others by its duration, not forever."""
    handler_peak = Peak()
    finished: list[str] = []
    lock = asyncio.Lock()

    async def blocking(payload: dict[str, str], context: TaskContext) -> None:
        await asyncio.to_thread(handler_peak.block, 0.15)
        async with lock:
            finished.append(payload["tag"])

    async def quick(payload: dict[str, str], context: TaskContext) -> None:
        async with lock:
            finished.append(payload["tag"])

    queue.register(name="blocking", handler=blocking)
    queue.register(name="quick", handler=quick)

    async with pool.acquire() as connection, connection.transaction():
        for index in range(6):
            await queue.enqueue(
                connection, task="blocking", payload={"tag": f"slow-{index}"}
            )
        for index in range(6):
            await queue.enqueue(
                connection, task="quick", payload={"tag": f"fast-{index}"}
            )

    worker = Worker(queue, worker_id="mixed", concurrency=2, poll_interval=0.05)
    async with running(worker):
        deadline = asyncio.get_running_loop().time() + 30
        while len(finished) < 12 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)

    assert sorted(finished) == sorted(
        [f"slow-{index}" for index in range(6)]
        + [f"fast-{index}" for index in range(6)]
    )
    assert handler_peak.peak <= 2
    stats = await queue.stats()
    assert stats.succeeded == 12
