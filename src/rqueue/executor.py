"""The bounded default executor the Worker installs.

``asyncio.to_thread`` uses the loop's default executor, which Python sizes at
``min(32, cpu_count + 4)`` threads. The worker replaces it with one sized from
its ``concurrency``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

__all__ = ["bounded_default_executor"]


@contextmanager
def bounded_default_executor(
    max_workers: int,
    *,
    thread_name_prefix: str = "rqueue",
) -> Iterator[ThreadPoolExecutor]:
    """Install a bounded default executor for the running loop, then restore.

    The previous default executor is put back on exit, so embedding a Worker in
    a larger application does not permanently reshape that application's loop.
    Exit does not wait for running threads. Python cannot interrupt a thread.
    Waiting would hold the event loop and process for as long as the slowest
    thread runs. Work still queued behind the pool is cancelled. A running
    thread finishes in the background, but its result is dropped because the
    asyncio future it would resolve is already cancelled. A handler that must
    stop early polls a signal of its own.
    """
    loop = asyncio.get_running_loop()
    previous = getattr(loop, "_default_executor", None)
    executor = ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix=thread_name_prefix
    )
    loop.set_default_executor(executor)
    try:
        yield executor
    finally:
        # set_default_executor rejects None, which is the usual prior state.
        if isinstance(previous, ThreadPoolExecutor):
            loop.set_default_executor(previous)
        else:
            loop._default_executor = previous  # type: ignore[attr-defined]
        executor.shutdown(wait=False, cancel_futures=True)
