"""The bounded default executor that caps asyncio.to_thread work."""

from __future__ import annotations

import asyncio
import threading
import time

from rqueue.executor import bounded_default_executor


class Peak:
    """Track the greatest number of threads inside the block at one time."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.current = 0
        self.peak = 0

    def block(self, seconds: float) -> None:
        with self.lock:
            self.current += 1
            self.peak = max(self.peak, self.current)
        time.sleep(seconds)
        with self.lock:
            self.current -= 1


async def test_to_thread_is_capped_by_the_installed_executor() -> None:
    peak = Peak()
    with bounded_default_executor(3):
        await asyncio.gather(*(asyncio.to_thread(peak.block, 0.05) for _ in range(24)))
    assert peak.peak <= 3
    assert peak.peak > 1, "the executor should still run work in parallel"


async def test_the_previous_default_executor_is_restored() -> None:
    loop = asyncio.get_running_loop()
    before = getattr(loop, "_default_executor", None)
    with bounded_default_executor(2) as executor:
        assert loop._default_executor is executor  # type: ignore[attr-defined]
    assert getattr(loop, "_default_executor", None) is before


async def test_threads_carry_the_configured_name_prefix() -> None:
    with bounded_default_executor(1, thread_name_prefix="rqueue-test"):
        name = await asyncio.to_thread(lambda: threading.current_thread().name)
    assert name.startswith("rqueue-test")


async def test_exit_does_not_wait_for_a_running_thread() -> None:
    release = threading.Event()
    started = threading.Event()

    def block() -> None:
        started.set()
        release.wait(10)
        raise RuntimeError("raised after the loop stopped waiting")

    loop = asyncio.get_running_loop()
    try:
        with bounded_default_executor(1):
            blocked = asyncio.ensure_future(asyncio.to_thread(block))
            while not started.is_set():
                await asyncio.sleep(0.01)
            blocked.cancel()
            began = time.monotonic()
        assert time.monotonic() - began < 1.0
    finally:
        release.set()
    # The cancelled future must not report the thread's late exception.
    errors: list[object] = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        await asyncio.sleep(0.1)
    finally:
        loop.set_exception_handler(None)
    assert errors == []


async def test_exit_cancels_work_still_queued_behind_the_pool() -> None:
    release = threading.Event()
    started = threading.Event()
    ran_queued = threading.Event()

    def block() -> None:
        started.set()
        release.wait(10)

    try:
        with bounded_default_executor(1):
            first = asyncio.ensure_future(asyncio.to_thread(block))
            while not started.is_set():
                await asyncio.sleep(0.01)
            queued = asyncio.ensure_future(asyncio.to_thread(ran_queued.set))
            await asyncio.sleep(0.05)
        release.set()
        await asyncio.sleep(0.1)
        assert queued.cancelled()
        assert not ran_queued.is_set()
        first.cancel()
    finally:
        release.set()
