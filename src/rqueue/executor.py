"""The bounded default executor the Worker installs.

Handlers are async only. Blocking work goes through ``asyncio.to_thread``,
which submits to the running loop's *default* executor -- and the default
executor Python installs is unbounded in practice (``min(32, cpu_count + 4)``
threads, chosen by the interpreter rather than by the deployment). Replacing it
with one sized from the worker's ``concurrency`` means a bare
``asyncio.to_thread(...)`` in a handler is capacity-limited without every task
author constructing an executor of their own.
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
    Exit waits for running threads: Python cannot safely interrupt a thread, so
    the only way to be rid of one is to let it return.
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
        # set_default_executor rejects None, and "no executor yet" is the
        # normal starting state, so restoring goes through the attribute the
        # loop actually reads. Assigning the previous value back is the only
        # way to leave a host application's loop exactly as it was found.
        if isinstance(previous, ThreadPoolExecutor):
            loop.set_default_executor(previous)
        else:
            loop._default_executor = previous  # type: ignore[attr-defined]
        executor.shutdown(wait=True)
