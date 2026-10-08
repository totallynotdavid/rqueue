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
