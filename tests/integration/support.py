"""Helpers shared by the integration suite."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from rqueue import Worker


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
